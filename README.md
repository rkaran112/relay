# relay

A distributed task queue on **PostgreSQL** (plus Redis for the outbox bus) that gives
**at-least-once delivery with exactly-once side effects**. The interesting part isn't
handing work out — it's that a worker can die at any instant and you can't tell "died
before doing the work" from "did the work then died before saying so." relay resolves
that with three mechanisms: effect-and-completion in one transaction, caller-supplied
idempotency keys, and **fencing tokens** that reject a zombie worker's late completion.

No Celery, RQ, Dramatiq, SQS or Kafka — the queue *is* the project. See
[`SPEC.md`](SPEC.md) for the full design, the resume contract it substantiates, and the
phase-by-phase build plan.

## Status

**All phases (0–9) complete.** Schema + idempotent enqueue, `SKIP LOCKED` leasing, fenced
completion, retries with full-jitter backoff, the visibility-timeout reaper + dead letters,
fencing tokens, the transactional outbox + Redis-Streams publisher, the FastAPI control
plane + live dashboard, OpenTelemetry tracing + Prometheus/Grafana, a load benchmark, and a
chaos harness with a negative control. 22 tests against real Postgres; `ruff` + `mypy
--strict` clean. Measured numbers and the definition-of-done checklist are in
[RESULTS.md](RESULTS.md).

### See it run

```bash
docker compose up -d db redis
uv run relay migrate
uv run relay demo          # API + workers + reaper + publisher -> http://localhost:8000
```

## Requirements

- [Docker](https://www.docker.com/) (Postgres + Redis run in containers)
- [uv](https://github.com/astral-sh/uv)
- `make` is optional. On Windows it isn't installed by default — either `scoop install make`
  or run the underlying commands directly (they're shown below).

## Quickstart

```bash
cp .env.example .env
make up          # start Postgres + Redis      (docker compose up -d db redis)
make migrate     # apply migrations            (uv run relay migrate)
make test        # test against real Postgres  (uv run pytest -q)
make check       # lint + typecheck + test
```

Windows PowerShell without `make`, using uv via the module runner:

```powershell
docker compose up -d db redis
python -m uv run relay migrate
python -m uv run pytest -q
```

## Layout

```
relay/          the package: config, db, queue ops, worker, reaper, publisher,
                outbox, api + dashboard, tracing, metrics, CLI
relay/web/      the dashboard (self-contained index.html)
alembic/        migrations
tests/          pytest suite; conftest.py provisions and migrates a real test DB
docker/         container entrypoint (migrate-on-boot)
observability/  Prometheus + Grafana provisioning
bench/          load benchmark        (make bench)
chaos/          chaos harness + invariants (make chaos)
```
