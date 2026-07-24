"""Phase 2: dequeue / lease / complete under concurrent workers.

The headline invariant: N workers draining one queue handle every job exactly
once — no job dropped, none processed twice.
"""

from __future__ import annotations

import threading
from datetime import timedelta

from psycopg import Connection
from psycopg.rows import TupleRow

from relay.db import connect
from relay.models import Job
from relay.worker import Worker


def _side_effect_handler(conn: Connection[TupleRow], job: Job) -> None:
    conn.execute(
        "INSERT INTO side_effects (job_id, value) VALUES (%s, %s)",
        (job.id, str(job.payload)),
    )


def test_8_workers_10k_jobs_each_handled_exactly_once(dsn: str) -> None:
    n_jobs = 10_000
    n_workers = 8

    with connect(dsn, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO jobs (queue, payload) "
            "SELECT 'work', jsonb_build_object('i', g) FROM generate_series(1, %s) g",
            (n_jobs,),
        )

    workers = [
        Worker(dsn, "work", _side_effect_handler, lease=timedelta(seconds=60), batch=25)
        for _ in range(n_workers)
    ]
    threads = [threading.Thread(target=w.run_until_empty) for w in workers]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=180)

    assert not any(t.is_alive() for t in threads), "a worker thread hung"

    with connect(dsn, autocommit=True) as conn:
        effects = conn.execute("SELECT count(*) FROM side_effects").fetchone()
        succeeded = conn.execute("SELECT count(*) FROM jobs WHERE state = 'succeeded'").fetchone()
        unfinished = conn.execute("SELECT count(*) FROM jobs WHERE state <> 'succeeded'").fetchone()
        # A FULL JOIN row with a NULL on either side is a job with no side effect or a
        # side effect with no job — i.e. a loss or a phantom. There must be none.
        mismatched = conn.execute(
            "SELECT count(*) FROM jobs j "
            "FULL JOIN side_effects s ON j.id = s.job_id "
            "WHERE j.id IS NULL OR s.job_id IS NULL"
        ).fetchone()

    assert effects == (n_jobs,)  # no duplicates (job_id is PK), no losses
    assert succeeded == (n_jobs,)
    assert unfinished == (0,)
    assert mismatched == (0,)

    total = sum(w.processed for w in workers)
    assert total == n_jobs  # every job handled by exactly one worker
    assert max(w.processed for w in workers) < n_jobs  # work was actually distributed
