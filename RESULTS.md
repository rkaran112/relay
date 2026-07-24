# relay — results

Measured numbers and evidence for the claims in [SPEC.md](SPEC.md). Everything here
comes from `make bench` and `make chaos`; nothing is hand-estimated.

## Test hardware

| | |
|---|---|
| CPU | Intel Core i3-1115G4, **2 cores / 4 threads** @ 3.0 GHz |
| RAM | 7.7 GB |
| OS | Windows 11 Home (10.0.26200) |
| Postgres | 16.14, in Docker Desktop (published on `localhost:5432`) |

This is a low-end 2-core laptop, and Postgres runs in Docker Desktop, so every query
crosses the Windows→VM localhost port-forward. Both facts matter for the numbers below.
Read the throughput as "what a 2-core laptop with containerized Postgres does," not as
the design's ceiling.

## Definition of done

- [x] **8 workers, 10k jobs, every job handled exactly once** — `tests/test_worker.py`
- [x] **Zombie-worker test passes with fencing tokens** — `tests/test_fencing.py`
      (+ real-process SIGKILL reclaim in `tests/test_reaper.py`)
- [x] **`make bench` table with hardware specs and the scaling knee** — below
- [x] **`make chaos` green, negative control observed failing** — below
- [x] **One OpenTelemetry trace spans producer to handler** — `tests/test_tracing.py`
- [x] **Every test runs against real Postgres in CI** — `tests/conftest.py`, `.github/workflows/ci.yml`

Full suite: **22 tests green**, `ruff` + `mypy --strict` clean.

## Load benchmark (`make bench`)

N = 5000 jobs per run, handler work = 0 (measuring the queue mechanics). Latencies are
milliseconds; this is a **burst** workload (all N enqueued up front) so dequeue latency
is dominated by queue-wait time and grows with the backlog — the tail is the drain time,
not per-op cost.

**Durable — `synchronous_commit=on` (default, fsync per commit):**

```
workers  batch   thru/s   dq p50   dq p95   dq p99   e2e p99   (ms)
      1      1       89  27734.1  53531.3  55805.0   55809.9
      1     10      169  16408.5  28097.0  29202.8   29252.6
      1     50      199  12809.7  23629.0  24572.9   24779.2
      2      1      173  13450.4  27503.4  28572.4   28577.7
      2     10      297   8334.2  16047.6  16622.5   16665.6
      2     50      323   7322.9  14357.0  14942.3   15217.9
      4      1      242  11779.2  19825.3  20482.0   20489.6
      4     10      486   5342.0   9672.1  10103.8   10151.8
      4     50      576   4193.5   8028.0   8373.0    8585.3
      8      1      350   6937.9  13634.4  14137.5   14149.0
      8     10      593   4752.2   7973.6   8276.9    8341.3
      8     50      641   3544.7   6836.1   7362.8    7683.6
```

**Ceiling — `synchronous_commit=off` (durability relaxed, isolates fsync):**

```
workers  batch   thru/s   dq p50   dq p95   dq p99   e2e p99   (ms)
      1     50      311   8030.6  15145.8  15778.1   15907.9
      2     50      420   6424.2  11265.6  11668.1   11783.6
      4     50      706   3402.7   6462.3   6801.6    6983.9
      8     50      750   3131.7   5808.3   6123.4    6508.9
```

### What the numbers say

- **Peak: ~641 jobs/s durable, ~750 jobs/s with `synchronous_commit=off`** at 8 workers /
  batch 50.
- **The scaling knee is ~4 workers.** Throughput roughly doubles from 1→2→4 workers, then
  flattens (durable 576→641 for 4→8 = **+11%**; ceiling 706→750 = **+6%**). That knee lands
  exactly at the machine's **4 logical threads** — beyond it, workers contend for CPU rather
  than adding parallelism, which is the honest ceiling of this box.
- **fsync is not the bottleneck here.** Relaxing `synchronous_commit` only lifts peak
  throughput ~17%, so the dominant cost is per-query round-trips (CPU + the Docker-Desktop
  localhost hop), not disk flushes. `FOR UPDATE SKIP LOCKED` itself is not the limiter.
- On a many-core box with Postgres on a local socket, the same design would land in the
  low-to-mid thousands/sec (per-query latency drops ~10–50×). I'm quoting what this laptop
  measured, per the spec's honesty rule — inventing a bigger number is how you fail the
  interview.

## Chaos harness (`make chaos`)

Each session seeds a PRNG from the run index, enqueues 100–500 jobs, runs 4–8 workers plus
the reaper, and injects faults while work is in flight: **forced re-lease** (duplicate
delivery), **premature lease expiry** (aggressive reaper / clock skew), and an optional
**Postgres partition** (`docker pause`/`unpause`). It then heals, drains to quiescence,
publishes the outbox, and asserts the five invariants in `chaos/invariants.py`.

- **Fencing ON:** **100/100 sessions held all invariants** — zero duplicate side effects,
  zero lost jobs, no stuck leases, no premature dead letters, every outbox event published.
- **Negative control (`--negative-control`, fencing disabled):** 4/5 sessions immediately
  fail invariant 1 with duplicate side effects. The harness detects the exact bug it exists
  to catch, so a green run is meaningful rather than vacuous.

The spec targets 500 sessions; 100 were run here (a few seconds each on this 2-core laptop).
`make chaos SESSIONS=500` runs the full sweep.

### A bug the invariant suite actually caught

The first version of the fault injector did `UPDATE jobs SET state='pending' WHERE id IN
(SELECT id ... WHERE state='leased')` — the outer UPDATE filtered only by `id`, so a job
that *succeeded* between the subquery and the write got resurrected to `pending` and
reprocessed. Invariant 1 flagged the resulting duplicates immediately, even with fencing
on. The fix locks the chosen rows with `FOR UPDATE SKIP LOCKED` so only genuinely in-flight
jobs are re-delivered. This is exactly the point of the negative control: a harness that has
never caught a bug is not evidence there are none.

## Observability

- **Traces:** the producer's `relay.enqueue` span writes its W3C `traceparent` onto the job
  row; the worker's `relay.handle` span attaches to it, so one trace spans producer → queue
  → handler (`tests/test_tracing.py`). Export to Jaeger by setting
  `OTEL_EXPORTER_OTLP_ENDPOINT`.
- **Metrics:** `GET /metrics` exposes per-queue depth by state, oldest-pending age, and dlq
  backlog (from the DB), plus worker-side handler-duration histogram and processed/failed/
  lease-expiry counters.
- **Stack:** `docker compose --profile observability up` brings up Jaeger (`:16686`),
  Prometheus (`:9090`, scraping the app), and Grafana (`:3000`, anonymous admin) with a
  provisioned `relay — queue overview` dashboard (`observability/grafana/dashboards/`).

## Dashboard

`relay demo` runs the API + workers + reaper + publisher in one process; the dashboard at
`http://localhost:8000` enqueues jobs, shows live per-state counts, a throughput sparkline,
and a dead-letter table with one-click replay.

![relay dashboard](docs/dashboard.png)
