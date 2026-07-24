"""Chaos harness.

Each session seeds a PRNG from the run index, enqueues N jobs whose handler writes
one side_effects row, starts a pool of workers plus the reaper, then injects faults
while they run:

  - forced re-lease   a leased job is reset to pending, so a second worker also gets
                      it (a duplicate delivery)
  - premature expiry  a leased job's lease is expired under it, so the reaper reclaims
                      it mid-flight (a zombie / aggressive-reaper clock skew)
  - partition         (optional) `docker pause`/`unpause` Postgres briefly

It then heals, drains to quiescence, publishes the outbox, and asserts the invariants
in invariants.py. Every fault is survivable — with fencing, each job still produces
exactly one side effect.

Real-process SIGKILL mid-handler is covered by tests/test_reaper.py; here faults are
injected at the database level so hundreds of sessions run fast and deterministically.

    python -m chaos.runner --sessions 500
    python -m chaos.runner --negative-control --sessions 5   # fence off -> must fail
"""

from __future__ import annotations

import argparse
import os
import random
import subprocess
import threading
import time
from datetime import timedelta

from chaos.invariants import check_invariants
from relay.config import get_settings
from relay.db import connect
from relay.handlers import side_effect_handler
from relay.publisher import Publisher
from relay.reaper import Reaper
from relay.worker import Worker

QUEUE = "chaos"
_DB_CONTAINER = "relay-db-1"


def _reset(dsn: str) -> None:
    with connect(dsn, autocommit=True) as conn:
        conn.execute("TRUNCATE jobs, side_effects, dead_letters, outbox RESTART IDENTITY")
        conn.execute("ALTER SEQUENCE lease_token_seq RESTART WITH 1")


def _enqueue(dsn: str, n: int, work_ms: int) -> set[int]:
    with connect(dsn, autocommit=True) as conn:
        rows = conn.execute(
            "INSERT INTO jobs (queue, payload, max_attempts) "
            "SELECT %s, jsonb_build_object('work_ms', %s), 100 FROM generate_series(1, %s) "
            "RETURNING id",
            (QUEUE, work_ms, n),
        ).fetchall()
    return {int(r[0]) for r in rows}


def _inject(dsn: str, rng: random.Random, stop: threading.Event, partition: bool) -> None:
    with connect(dsn, autocommit=True) as conn:
        while not stop.is_set():
            fault = rng.choice(["release", "expire", "release", "expire", "none"])
            k = rng.randint(1, 5)
            # FOR UPDATE SKIP LOCKED locks the chosen leased rows so they can't finish
            # between selection and the write. Without it the UPDATE could resurrect a
            # job that just succeeded — a fault no real system produces.
            if fault == "release":
                # Duplicate delivery: hand a still-leased job back to the pool.
                conn.execute(
                    "UPDATE jobs SET state='pending', lease_owner=NULL, lease_token=NULL "
                    "WHERE id IN (SELECT id FROM jobs WHERE queue='chaos' AND state='leased' "
                    "ORDER BY random() LIMIT %s FOR UPDATE SKIP LOCKED)",
                    (k,),
                )
            elif fault == "expire":
                # Aggressive reaper / clock skew: expire a lease under a live worker.
                conn.execute(
                    "UPDATE jobs SET lease_expires_at = now() - interval '5 seconds' "
                    "WHERE id IN (SELECT id FROM jobs WHERE queue='chaos' AND state='leased' "
                    "ORDER BY random() LIMIT %s FOR UPDATE SKIP LOCKED)",
                    (k,),
                )
            stop.wait(rng.uniform(0.05, 0.15))
    if partition:
        _partition(rng)


def _partition(rng: random.Random) -> None:
    try:
        subprocess.run(["docker", "pause", _DB_CONTAINER], check=True, capture_output=True)
        time.sleep(rng.uniform(0.3, 0.8))
    finally:
        subprocess.run(["docker", "unpause", _DB_CONTAINER], check=False, capture_output=True)


def _await_quiescence(dsn: str, timeout: float = 60.0) -> bool:
    deadline = time.monotonic() + timeout
    with connect(dsn, autocommit=True) as conn:
        while time.monotonic() < deadline:
            row = conn.execute(
                "SELECT count(*) FROM jobs WHERE queue = %s AND state IN ('pending', 'leased')",
                (QUEUE,),
            ).fetchone()
            if row is not None and row[0] == 0:
                return True
            time.sleep(0.1)
    return False


def run_session(dsn: str, redis_url: str, seed: int, *, partition: bool, work_ms: int) -> list[str]:
    rng = random.Random(seed)
    n = rng.randint(100, 500)
    n_workers = rng.choice([4, 6, 8])
    lease = timedelta(seconds=rng.uniform(0.5, 1.5))

    _reset(dsn)
    enqueued = _enqueue(dsn, n, work_ms)

    workers = [
        Worker(dsn, QUEUE, side_effect_handler, batch=rng.choice([1, 5, 10]),
               lease=lease, backoff_base=0.0)
        for _ in range(n_workers)
    ]
    reaper = Reaper(dsn, interval=0.2, base=0.0)
    stop_inject = threading.Event()

    threads = [threading.Thread(target=w.run_forever, daemon=True) for w in workers]
    threads.append(threading.Thread(target=reaper.run_forever, daemon=True))
    do_partition = partition and rng.random() < 0.15
    threads.append(
        threading.Thread(target=_inject, args=(dsn, rng, stop_inject, do_partition), daemon=True)
    )
    for t in threads:
        t.start()

    time.sleep(rng.uniform(1.0, 2.0))  # let faults land while work is in flight
    stop_inject.set()

    quiesced = _await_quiescence(dsn)
    for w in workers:
        w.stop()
    reaper.stop()

    # Publish the outbox so invariant 5 can be checked.
    Publisher(dsn, redis_url, stream=f"chaos.{seed}").drain()

    with connect(dsn, autocommit=True) as conn:
        violations = check_invariants(conn, enqueued)
    if not quiesced:
        violations.append("did not reach quiescence within timeout")
    return violations


def main() -> None:
    parser = argparse.ArgumentParser(description="relay chaos harness")
    parser.add_argument("--sessions", type=int, default=int(os.environ.get("SESSIONS", "100")))
    parser.add_argument("--partition", action="store_true", help="also inject Postgres partitions")
    parser.add_argument(
        "--negative-control",
        action="store_true",
        help="disable the fencing check; invariant 1 MUST then fail",
    )
    args = parser.parse_args()

    settings = get_settings()
    dsn, redis_url = settings.psycopg_dsn, settings.redis_url
    work_ms = 3
    if args.negative_control:
        os.environ["RELAY_DISABLE_FENCE"] = "1"
        work_ms = 40  # widen the race window so a duplicate reliably occurs
        print("NEGATIVE CONTROL: fencing disabled — expecting invariant 1 to fail\n")

    failed_sessions = 0
    for i in range(args.sessions):
        violations = run_session(
            dsn, redis_url, seed=i, partition=args.partition, work_ms=work_ms
        )
        if violations:
            failed_sessions += 1
            print(f"session {i:>4}: VIOLATIONS: {'; '.join(violations)}")
        elif (i + 1) % 10 == 0 or args.sessions <= 20:
            print(f"session {i:>4}: ok")

    print()
    if args.negative_control:
        if failed_sessions:
            print(f"negative control PASSED: {failed_sessions}/{args.sessions} sessions "
                  f"caught a violation (the harness detects the bug it should)")
            raise SystemExit(0)
        print("negative control FAILED: no violations with fencing disabled — "
              "the harness is not actually checking anything")
        raise SystemExit(1)

    if failed_sessions:
        print(f"CHAOS FAILED: {failed_sessions}/{args.sessions} sessions had violations")
        raise SystemExit(1)
    print(f"CHAOS PASSED: {args.sessions}/{args.sessions} sessions held all invariants")


if __name__ == "__main__":
    main()
