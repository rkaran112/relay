"""Phase 3: retries with full-jitter backoff, the visibility-timeout reaper, and
dead-lettering."""

from __future__ import annotations

import subprocess
import sys
import time
from datetime import timedelta
from pathlib import Path

from psycopg import Connection
from psycopg.rows import TupleRow

from relay.db import connect
from relay.models import Job
from relay.queue import enqueue
from relay.reaper import Reaper, reap
from relay.worker import Worker

_BLOCKING_WORKER = Path(__file__).parent / "scripts" / "blocking_worker.py"


def _boom(_conn: Connection[TupleRow], _job: Job) -> None:
    raise RuntimeError("handler always fails")


def _side_effect(conn: Connection[TupleRow], job: Job) -> None:
    conn.execute("INSERT INTO side_effects (job_id, value) VALUES (%s, 'ok')", (job.id,))


def test_always_failing_handler_dead_letters_after_max_attempts(
    db: Connection[TupleRow], dsn: str
) -> None:
    job_id = enqueue(db, "flaky", {"x": 1}, max_attempts=3)

    # base=0 => immediate retries, so run_until_empty drives all attempts then exits
    # once the job is dead (no longer pending).
    worker = Worker(dsn, "flaky", _boom, backoff_base=0.0, lease=timedelta(seconds=60))
    worker.run_until_empty()

    state, attempts = db.execute(
        "SELECT state, attempts FROM jobs WHERE id = %s", (job_id,)
    ).fetchone()
    assert state == "dead"
    assert attempts == 3  # exactly max_attempts tries, no more

    dl = db.execute(
        "SELECT job_id, attempts FROM dead_letters WHERE job_id = %s", (job_id,)
    ).fetchall()
    assert dl == [(job_id, 3)]
    assert worker.failed == 3
    assert db.execute("SELECT count(*) FROM side_effects").fetchone() == (0,)


def test_reaper_backoff_is_jittered_not_fixed(db: Connection[TupleRow]) -> None:
    # Lease a batch of jobs, let the leases expire, then reap. If backoff were fixed
    # every job would get the same run_at; full jitter spreads them across (0, window).
    db.execute(
        "INSERT INTO jobs (queue, payload, state, attempts, lease_owner, lease_token, "
        "lease_expires_at) SELECT 'j', '{}'::jsonb, 'leased', 3, gen_random_uuid(), g, "
        "now() - interval '1 second' FROM generate_series(1, 200) g"
    )
    requeued, dead = reap(db, base=10.0, cap=300.0)
    assert (requeued, dead) == (200, 0)

    offsets = [
        r[0]
        for r in db.execute(
            "SELECT EXTRACT(EPOCH FROM (run_at - now())) FROM jobs WHERE state = 'pending'"
        ).fetchall()
    ]
    window = 10.0 * (2**3)  # base * 2^attempts = 80s
    assert all(-1.0 <= o <= window for o in offsets)  # within [0, window] (small clock slack)
    assert max(offsets) - min(offsets) > window / 4  # genuinely spread, not constant


def test_sigkilled_worker_job_is_reclaimed_and_completed(dsn: str) -> None:
    with connect(dsn, autocommit=True) as conn:
        job_id = enqueue(conn, "crash", {"n": 1})

    proc = subprocess.Popen(
        [sys.executable, str(_BLOCKING_WORKER), dsn, "crash"],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().strip()  # blocks until "leased <id>"
        assert line == f"leased {job_id}"

        with connect(dsn, autocommit=True) as conn:
            assert conn.execute(
                "SELECT state FROM jobs WHERE id = %s", (job_id,)
            ).fetchone() == ("leased",)

        proc.kill()  # SIGKILL equivalent, mid-handler, lease still committed
        proc.wait(timeout=10)
    finally:
        if proc.poll() is None:
            proc.kill()

    time.sleep(2.2)  # let the 2s lease expire
    # base=0 => reclaim with no extra backoff, so the job is immediately runnable.
    requeued, _ = Reaper(dsn, base=0.0).reap_once()
    assert requeued == 1

    with connect(dsn, autocommit=True) as conn:
        assert conn.execute(
            "SELECT state FROM jobs WHERE id = %s", (job_id,)
        ).fetchone() == ("pending",)

    # A healthy worker now picks it up and completes it — exactly one side effect.
    Worker(dsn, "crash", _side_effect, lease=timedelta(seconds=30)).run_until_empty()
    with connect(dsn, autocommit=True) as conn:
        assert conn.execute("SELECT count(*) FROM side_effects").fetchone() == (1,)
        assert conn.execute(
            "SELECT state FROM jobs WHERE id = %s", (job_id,)
        ).fetchone() == ("succeeded",)
