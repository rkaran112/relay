# relay: Build Spec

**Save this as `SPEC.md` in the repo root before writing any code.**
Point Claude Code at it with: `read SPEC.md and implement Phase 0`. One phase at a time.

**Build this one first.** It appears on three of the four resumes, it is roughly a weekend for a working
version plus a week for the harnesses, and the concepts in it (idempotency, retries, at-least-once delivery)
come up in almost every backend interview whether you built it or not.

---

## 0. Instructions for the coding agent

You are building a distributed task queue on top of PostgreSQL where handlers can crash, be killed, be
duplicated, or be paused indefinitely, and the observable side effects still happen **exactly once**.

Hard rules for this repo:

1. **No Celery, no RQ, no Dramatiq, no SQS, no Kafka.** Postgres and Redis only. The queue is the project.
2. **Every test runs against a real PostgreSQL** in Docker. No SQLite, no mocked cursors, no fakes.
3. **No claim without a harness.** Throughput, latency and correctness numbers all come from scripts in
   `bench/` and `chaos/`.
4. Type-checked with mypy strict, linted with ruff, all in CI.
5. When you find yourself writing "this should be fine", write a test instead.

---

## 1. Resume contract

| Claim | Proof |
|---|---|
| Postgres-backed distributed queue giving **at-least-once delivery with exactly-once side effects**, combining a **transactional outbox**, caller-supplied **idempotency keys**, and `SELECT ... FOR UPDATE SKIP LOCKED` leasing so N workers never double-claim a job | `chaos/` invariant suite |
| **X,XXX jobs/sec** across **8 workers** at **p99 dequeue latency of XXms** under a seeded load generator | `make bench` prints a results table |
| **Visibility timeouts, exponential backoff with jitter, and a dead-letter queue** exposed through a replay endpoint | integration tests plus the API |
| Chaos harness killing workers mid-handler, partitioning the database and replaying duplicate deliveries: **zero duplicate side effects and zero lost jobs across 500 runs** | `make chaos SESSIONS=500` |
| OpenTelemetry traces and per-queue depth and age metrics | Grafana dashboard screenshot in `RESULTS.md` |

---

## 2. What this actually is, and the one idea that matters

A queue hands work to workers. The hard part is not the handing out. The hard part is this: **a worker can
die at any instant, and you cannot tell the difference between "died before doing the work" and "did the
work then died before telling you".**

If you assume the first, you re-run the job and the customer gets charged twice. If you assume the second,
you drop the job and the customer never gets charged at all.

The industry answer, and the sentence you need to be able to say fluently in an interview:

> **Exactly-once *delivery* is impossible. Exactly-once *side effects* are achievable.** You accept
> at-least-once delivery and make the effect idempotent, so a duplicate delivery is a no-op.

Three mechanisms get you there, and this project implements all three:

1. **Effect and completion in one transaction.** If the handler's business write and the "job done" marker
   commit together, then either both happened or neither did. There is no gap to crash in.
2. **Idempotency keys.** For effects you cannot put in your transaction (charging a card, sending an email),
   claim the effect first by inserting a unique key. If the insert conflicts, someone already did it; load
   the stored result and return it.
3. **Fencing tokens.** A worker that pauses for longer than its lease is a zombie: the system already gave
   its job to someone else. When the zombie wakes and tries to report success, its stale token is rejected.

Most candidates know the first. Some know the second. Almost nobody has implemented the third.

---

## 3. Schema

```sql
CREATE TYPE job_state AS ENUM ('pending','leased','succeeded','failed','dead');

CREATE TABLE jobs (
  id               BIGSERIAL PRIMARY KEY,
  queue            TEXT        NOT NULL,
  payload          JSONB       NOT NULL,
  state            job_state   NOT NULL DEFAULT 'pending',
  priority         SMALLINT    NOT NULL DEFAULT 0,
  attempts         INT         NOT NULL DEFAULT 0,
  max_attempts     INT         NOT NULL DEFAULT 5,
  run_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  lease_owner      UUID,
  lease_token      BIGINT,                       -- the fencing token
  lease_expires_at TIMESTAMPTZ,
  idempotency_key  TEXT,
  traceparent      TEXT,                         -- W3C trace context, carried through the queue
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT uq_idem UNIQUE (queue, idempotency_key)
);

-- The index that makes dequeue fast. Partial, so it only covers rows that can actually be claimed.
CREATE INDEX idx_jobs_claimable ON jobs (queue, priority DESC, run_at)
  WHERE state = 'pending';
CREATE INDEX idx_jobs_leases ON jobs (lease_expires_at) WHERE state = 'leased';

CREATE SEQUENCE lease_token_seq;                 -- monotonic, cluster-wide

CREATE TABLE job_results (                       -- the exactly-once claim table
  idempotency_key TEXT PRIMARY KEY,
  job_id          BIGINT NOT NULL,
  status          TEXT   NOT NULL,               -- 'in_flight' | 'done'
  result          JSONB,
  claimed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at    TIMESTAMPTZ
);

CREATE TABLE outbox (
  id           BIGSERIAL PRIMARY KEY,
  aggregate_id TEXT        NOT NULL,
  event_type   TEXT        NOT NULL,
  payload      JSONB       NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ
);
CREATE INDEX idx_outbox_unpublished ON outbox (id) WHERE published_at IS NULL;

CREATE TABLE dead_letters (
  id BIGSERIAL PRIMARY KEY, job_id BIGINT NOT NULL, queue TEXT NOT NULL,
  payload JSONB NOT NULL, attempts INT NOT NULL, last_error TEXT,
  died_at TIMESTAMPTZ NOT NULL DEFAULT now(), replayed_at TIMESTAMPTZ
);

-- Used only by the chaos harness: the observable "side effect".
CREATE TABLE side_effects (
  job_id BIGINT PRIMARY KEY, value TEXT NOT NULL, written_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

---

## 4. The four operations

### Enqueue (dedupe at the door)
```sql
INSERT INTO jobs (queue, payload, idempotency_key, run_at, max_attempts, traceparent)
VALUES ($1,$2,$3,$4,$5,$6)
ON CONFLICT (queue, idempotency_key) DO NOTHING
RETURNING id;
```
No row returned means this exact job was already enqueued. Return the existing id. The caller retrying a
POST because the response timed out must not create a second job.

### Dequeue (`SKIP LOCKED` leasing)
```sql
WITH claimed AS (
  SELECT id FROM jobs
   WHERE queue = $1 AND state = 'pending' AND run_at <= now()
   ORDER BY priority DESC, run_at
   FOR UPDATE SKIP LOCKED
   LIMIT $2
)
UPDATE jobs j
   SET state = 'leased',
       lease_owner = $3,
       lease_token = nextval('lease_token_seq'),
       lease_expires_at = now() + $4::interval,
       attempts = attempts + 1
  FROM claimed c
 WHERE j.id = c.id
RETURNING j.*;
```

`FOR UPDATE SKIP LOCKED` is the whole trick: row locks are taken on candidate rows, and any row another
transaction has already locked is *skipped* instead of waited on. Eight workers hitting this simultaneously
get eight disjoint batches with no coordination, no advisory locks and no contention. Know why this beats
`SELECT ... FOR UPDATE` (which would serialize every worker behind the first) and why it beats an advisory
lock per job (an extra round trip and a lock table that grows).

### Complete (fenced, atomic)
The handler runs inside a transaction that also finalizes the job:
```sql
UPDATE jobs
   SET state = 'succeeded', lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL
 WHERE id = $1 AND lease_owner = $2 AND lease_token = $3;
-- 0 rows updated means this worker is a zombie: its lease was reaped and reassigned.
-- ROLL BACK THE WHOLE TRANSACTION, including the side effect.
```
That `lease_token` check is the fencing token, and it is the answer to "what if a worker pauses for two
minutes on a GC or a suspended VM".

### Reap (visibility timeout)
A background loop, every second:
```sql
UPDATE jobs
   SET state = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'pending' END,
       run_at = now() + backoff(attempts),
       lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL
 WHERE state = 'leased' AND lease_expires_at < now()
RETURNING id, state;
```
Rows that go to `dead` get copied into `dead_letters`.

**Backoff with full jitter:**
```python
def backoff(attempts: int, base: float = 1.0, cap: float = 300.0) -> float:
    return random.uniform(0, min(cap, base * (2 ** attempts)))
```
Full jitter, not fixed exponential. Fixed backoff makes every failed job in a batch retry at the same
instant and hammer the dependency that just fell over. Be able to explain that in one sentence.

---

## 5. The two exactly-once paths

### Path A: effect is in your database
Give the handler the open transaction. It writes its rows, the framework writes the completion and any
outbox events, one commit. Nothing else needed. Prefer this path whenever possible.

### Path B: effect is external (payment, email, third-party API)
Claim first, then call, then record:
```
BEGIN
  INSERT INTO job_results (idempotency_key, job_id, status) VALUES ($key, $id, 'in_flight')
  ON CONFLICT (idempotency_key) DO NOTHING RETURNING 1;
  -- no row: someone else owns this effect
COMMIT

if not claimed:
    row = SELECT * FROM job_results WHERE idempotency_key = $key
    if row.status == 'done':  return row.result          # already happened, no-op
    else:                     raise RetryLater()          # in flight elsewhere, back off

result = call_external_api(idempotency_key=$key)          # pass the key downstream too
UPDATE job_results SET status='done', result=$result, completed_at=now() WHERE idempotency_key=$key
```
Be honest about the residual gap: if the process dies between the external call and the `UPDATE`, the row
stays `in_flight`. A sweeper reconciles those by querying the provider with the same idempotency key. Say
this out loud in an interview; knowing where your guarantee ends is worth more than pretending it does not.

### Transactional outbox
The dual-write problem: writing to your database and publishing to a message bus are two systems, and there
is no transaction across them. The outbox fixes it by making the publish intent part of the same database
transaction. A separate publisher process then polls:
```sql
SELECT * FROM outbox WHERE published_at IS NULL ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 100;
```
pushes to Redis Streams, marks published. That is at-least-once to the bus, so consumers dedupe on event id.
The chain of guarantees end to end is: exactly-once effect in the database, at-least-once publish,
idempotent consume.

---

## 6. Build phases

### Phase 0: scaffold
`docker compose` with Postgres 16, Redis 7 and the app. Poetry or uv, Alembic, pytest with a real-Postgres
fixture, ruff, mypy strict, GitHub Actions.
**Done when:** `make test` spins up Postgres in CI and runs one trivial test against it.

### Phase 1: schema and enqueue
Migrations for everything in section 3, plus `enqueue()` with idempotency dedupe.
**Done when:** enqueuing the same idempotency key 100 times concurrently creates exactly one row.

### Phase 2: dequeue, lease, complete
`SKIP LOCKED` claim, fenced completion, the `Worker` loop.
**Done when:** a test runs 8 workers against 10,000 jobs and asserts every job was handled by exactly one
worker, with no gaps and no duplicates.

### Phase 3: retries, reaper, dead letters
Backoff with full jitter, the visibility-timeout reaper, `dead_letters` population.
**Done when:** a handler that always raises lands in `dead_letters` after exactly `max_attempts`, with
observed gaps matching the jitter distribution; and a worker killed with SIGKILL mid-handler has its job
re-leased after the visibility timeout.

### Phase 4: fencing tokens
Add `lease_token`, enforce it on completion, and write the zombie test: lease a job, freeze the worker past
its lease (`docker pause` or a debugger sleep), let the reaper reassign it, let the second worker finish,
then unfreeze the first and assert its completion is rejected and its transaction rolls back.
**Done when:** that test passes and `side_effects` has exactly one row.

### Phase 5: outbox and publisher
Outbox writes inside the handler transaction; publisher process with `SKIP LOCKED` polling to Redis Streams.
**Done when:** killing the publisher mid-batch and restarting it publishes every event at least once and
never loses one.

### Phase 6: control plane
FastAPI: `POST /queues/{q}/jobs`, `GET /jobs/{id}`, `GET /queues/{q}/stats`, `GET /queues/{q}/dlq`,
`POST /queues/{q}/dlq/{id}/replay`, `GET /metrics`, `GET /healthz`.
**Done when:** a dead-lettered job can be replayed through the API and succeeds.

### Phase 7: observability
OpenTelemetry spans: `enqueue` -> `lease` -> `handle` -> `complete`, with the W3C `traceparent` stored on the
job row so the trace survives the queue hop. This is a detail interviewers like, because most queue
integrations lose the trace at the boundary. Prometheus metrics: queue depth by state, oldest pending job
age, lease expiries per minute, handler duration histogram, throughput. Grafana dashboard in
`docker compose`.
**Done when:** one trace in Jaeger spans producer through handler, and the Grafana screenshot is in
`RESULTS.md`.

### Phase 8: load benchmark
`bench/load.py`: seeded generator, fixed handler-duration distribution, sweep 1/2/4/8 workers and batch
sizes 1/10/50. Measure throughput (jobs/sec) and **dequeue latency** (enqueue timestamp to lease timestamp)
at p50/p95/p99, plus end-to-end latency.

Print a table like:
```
workers  batch  throughput/s   p50    p95    p99
      1     10          ....   ...    ...    ...
      8     50          ....   ...    ...    ...
```

Two honesty notes. First, **write down whatever you measure**, including the machine spec. Postgres-backed
queues on a laptop typically land in the low thousands per second; that is a perfectly respectable number
and inventing a bigger one is how you fail an interview. Second, expect throughput to stop scaling somewhere
around 4 to 8 workers as you hit Postgres contention. Find that knee and put it in `RESULTS.md`. Knowing
where your system stops scaling is a senior signal.

**Done when:** `make bench` prints the table and `RESULTS.md` records it with hardware details.

### Phase 9: chaos harness
`chaos/runner.py`. Each run:
1. Seed a PRNG from the run index.
2. Enqueue N jobs (N random, 100 to 1000) whose handler inserts one row into `side_effects`.
3. Start 4 to 8 worker processes.
4. Inject faults on a random schedule:
   - `SIGKILL` a worker mid-handler
   - `docker pause` / `unpause` the Postgres container (partition)
   - `docker pause` a worker for longer than its lease (zombie)
   - manually re-lease a job to force a duplicate delivery
   - clock skew on the reaper
5. Heal everything, drain the queue, wait for quiescence.
6. Assert the invariants.

**Invariants** (`chaos/invariants.py`):
```
1. count(side_effects) == count(distinct enqueued job ids)         -> no duplicates, no losses
2. every side_effects.job_id maps to a job in state 'succeeded'
3. no job left in state 'leased' with lease_expires_at < now()
4. dead_letters contains only jobs whose attempts >= max_attempts
5. every outbox row for a succeeded job is eventually published
```

**Negative control, do not skip:** remove the fencing-token check, rerun, and confirm invariant 1 fails.
A harness that has never caught a bug is not evidence that there are none. Record this in `RESULTS.md`.

**Done when:** `make chaos SESSIONS=500` passes and the negative control was observed to fail.

---

## 7. Definition of done

- [ ] 8 workers, 10k jobs, every job handled exactly once
- [ ] Zombie-worker test passes with fencing tokens
- [ ] `make bench` table in `RESULTS.md` with hardware specs and the scaling knee
- [ ] `make chaos SESSIONS=500` green, negative control observed failing
- [ ] One OpenTelemetry trace spans producer to handler
- [ ] Every test runs against real Postgres in CI

---

## 8. Questions an interviewer will ask

1. *Is exactly-once delivery possible?* No. Exactly-once effects are. Lead with this.
2. *Why `SKIP LOCKED` instead of `SELECT ... FOR UPDATE`?* The latter serializes every worker behind the
   first lock holder.
3. *A worker's VM is suspended for five minutes. Walk me through what happens.* Lease expires, reaper
   reassigns, second worker completes, zombie wakes and its fenced update matches zero rows, transaction
   rolls back including the side effect.
4. *What is the dual-write problem and how does the outbox solve it?*
5. *Why jitter?* Thundering herd on the dependency that just recovered.
6. *Where does your exactly-once guarantee actually break?* The Path B gap between external call and result
   write. Have the reconciliation sweeper answer ready.
7. *Why not just use Celery or SQS?* Because I wanted to understand the leasing, fencing and idempotency
   mechanics rather than trust them. Also: one fewer moving part when Postgres is already in the stack.
8. *What is your throughput ceiling and why?* Point at the knee in your own benchmark.
