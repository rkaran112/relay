"""Load benchmark.

Sweeps worker count x batch size against a burst of N seeded jobs and reports
throughput and latency. Two latencies are measured, both on the client clock (so
there's no host/container clock skew):

  dequeue latency  enqueue -> the moment the job is leased
  end-to-end       enqueue -> the job's completion commits

Workers are threads, each with its own connection; the GIL is released across every
DB call, so this genuinely exercises Postgres-side contention (the real ceiling of a
Postgres-backed queue). Handler work is fixed by --work-ms (default 0 = measure the
queue mechanics themselves). Write down the machine spec with any number you quote.
"""

from __future__ import annotations

import argparse
import threading
import time
from datetime import timedelta
from uuid import uuid4

from relay.config import get_settings
from relay.db import connect
from relay.queue import complete, dequeue


def _pct(xs: list[float], p: float) -> float:
    if not xs:
        return 0.0
    s = sorted(xs)
    k = (len(s) - 1) * p
    lo = int(k)
    hi = min(lo + 1, len(s) - 1)
    return (s[lo] + (s[hi] - s[lo]) * (k - lo)) * 1000.0  # -> ms


def _drain(
    dsn: str,
    queue: str,
    batch: int,
    work_ms: float,
    t0: float,
    sync_commit: str,
    out: list[tuple[list[float], list[float]]],
) -> None:
    owner = uuid4()
    lease = timedelta(seconds=60)
    dq: list[float] = []
    e2e: list[float] = []
    with connect(dsn) as conn:
        # Session setting; validated to on/off by argparse choices, so safe to inline.
        conn.execute(f"SET synchronous_commit = {sync_commit}")
        while True:
            jobs = dequeue(conn, queue, owner=owner, lease=lease, batch=batch)
            if not jobs:
                break  # burst fully enqueued up front, so empty == drained
            leased_at = time.perf_counter()
            for job in jobs:
                if work_ms:
                    time.sleep(work_ms / 1000.0)
                with conn.transaction():
                    conn.execute(
                        "INSERT INTO side_effects (job_id, value) VALUES (%s, 'b')", (job.id,)
                    )
                    assert job.lease_token is not None and job.lease_owner is not None
                    complete(conn, job.id, lease_owner=job.lease_owner, lease_token=job.lease_token)
                done_at = time.perf_counter()
                dq.append(leased_at - t0)
                e2e.append(done_at - t0)
    out.append((dq, e2e))


def _seed(dsn: str, queue: str, n: int) -> None:
    with connect(dsn, autocommit=True) as conn:
        conn.execute("TRUNCATE jobs, side_effects RESTART IDENTITY")
        conn.execute(
            "INSERT INTO jobs (queue, payload) SELECT %s, '{}'::jsonb FROM generate_series(1, %s)",
            (queue, n),
        )


def _run(
    dsn: str, n: int, workers: int, batch: int, work_ms: float, sync_commit: str
) -> dict[str, float]:
    _seed(dsn, "bench", n)
    out: list[tuple[list[float], list[float]]] = []
    t0 = time.perf_counter()
    threads = [
        threading.Thread(target=_drain, args=(dsn, "bench", batch, work_ms, t0, sync_commit, out))
        for _ in range(workers)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    wall = time.perf_counter() - t0

    dq = [x for part in out for x in part[0]]
    e2e = [x for part in out for x in part[1]]
    return {
        "throughput": len(e2e) / wall if wall else 0.0,
        "dq_p50": _pct(dq, 0.50), "dq_p95": _pct(dq, 0.95), "dq_p99": _pct(dq, 0.99),
        "e2e_p99": _pct(e2e, 0.99),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="relay load benchmark")
    parser.add_argument("--n", type=int, default=5000, help="jobs per run")
    parser.add_argument("--work-ms", type=float, default=0.0, help="fixed handler duration")
    parser.add_argument("--sync-commit", choices=("on", "off"), default="on",
                        help="synchronous_commit: on=durable (fsync per commit), off=ceiling")
    parser.add_argument("--dsn", default=None)
    args = parser.parse_args()
    dsn = args.dsn or get_settings().psycopg_dsn

    print(f"relay load benchmark — N={args.n} jobs/run, handler work={args.work_ms}ms, "
          f"synchronous_commit={args.sync_commit}\n")
    print(f"{'workers':>7}  {'batch':>5}  {'thru/s':>9}  {'dq p50':>7}  {'dq p95':>7}  "
          f"{'dq p99':>7}  {'e2e p99':>8}   (latency ms)")
    print("-" * 74)
    for workers in (1, 2, 4, 8):
        for batch in (1, 10, 50):
            r = _run(dsn, args.n, workers, batch, args.work_ms, args.sync_commit)
            print(f"{workers:>7}  {batch:>5}  {r['throughput']:>9.0f}  {r['dq_p50']:>7.1f}  "
                  f"{r['dq_p95']:>7.1f}  {r['dq_p99']:>7.1f}  {r['e2e_p99']:>8.1f}")


if __name__ == "__main__":
    main()
