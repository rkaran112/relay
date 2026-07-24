"""Phase 4: fencing tokens.

A worker that pauses past its lease is a zombie — the system already reassigned its
job. When it wakes and reports success, the lease_token predicate on completion
matches zero rows, so its whole transaction (side effect included) rolls back. This
is the one mechanism almost nobody implements.
"""

from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

from psycopg import Connection, Rollback
from psycopg.rows import TupleRow

from relay.db import connect
from relay.queue import complete, dequeue, enqueue
from relay.reaper import reap


def _expire_leases(conn: Connection[TupleRow], job_id: int) -> None:
    conn.execute(
        "UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = %s",
        (job_id,),
    )


def test_stale_lease_token_is_rejected(dsn: str) -> None:
    with connect(dsn, autocommit=True) as conn:
        job_id = enqueue(conn, "z", {"n": 1})

        owner_a = uuid4()
        [job_a] = dequeue(conn, "z", owner=owner_a, lease=timedelta(seconds=30), batch=1)
        assert job_a.lease_token == 1

        # A is frozen; its lease expires and the reaper reclaims the job.
        _expire_leases(conn, job_id)
        assert reap(conn, base=0.0) == (1, 0)

        owner_b = uuid4()
        [job_b] = dequeue(conn, "z", owner=owner_b, lease=timedelta(seconds=30), batch=1)
        assert job_b.lease_token == 2

        # Zombie A's completion, with its now-stale token, is rejected.
        assert complete(conn, job_id, lease_owner=owner_a, lease_token=1) is False
        # The rightful holder B completes normally.
        assert complete(conn, job_id, lease_owner=owner_b, lease_token=2) is True
        assert conn.execute(
            "SELECT state FROM jobs WHERE id = %s", (job_id,)
        ).fetchone() == ("succeeded",)


def test_zombie_side_effect_rolls_back_exactly_one_survives(dsn: str) -> None:
    with connect(dsn, autocommit=True) as ctrl:
        job_id = enqueue(ctrl, "z", {"n": 1})

    conn_a = connect(dsn)  # the zombie worker's connection
    conn_b = connect(dsn)  # the healthy worker's connection
    try:
        owner_a, owner_b = uuid4(), uuid4()
        [job_a] = dequeue(conn_a, "z", owner=owner_a, lease=timedelta(seconds=30), batch=1)

        # Freeze A past its lease: the reaper reclaims the job (clears its token).
        with connect(dsn, autocommit=True) as ctrl:
            _expire_leases(ctrl, job_id)
            assert reap(ctrl, base=0.0) == (1, 0)

        # Zombie A wakes: it writes its side effect, then its fenced completion fails,
        # so the whole transaction — side effect included — must roll back.
        with conn_a.transaction():
            conn_a.execute(
                "INSERT INTO side_effects (job_id, value) VALUES (%s, 'zombie')", (job_id,)
            )
            rejected = complete(conn_a, job_id, lease_owner=owner_a, lease_token=job_a.lease_token)
            assert rejected is False
            raise Rollback()

        # Healthy worker B leases and completes for real.
        [job_b] = dequeue(conn_b, "z", owner=owner_b, lease=timedelta(seconds=30), batch=1)
        with conn_b.transaction():
            conn_b.execute(
                "INSERT INTO side_effects (job_id, value) VALUES (%s, 'healthy')", (job_id,)
            )
            accepted = complete(conn_b, job_id, lease_owner=owner_b, lease_token=job_b.lease_token)
            assert accepted is True
    finally:
        conn_a.close()
        conn_b.close()

    with connect(dsn, autocommit=True) as ctrl:
        rows = ctrl.execute(
            "SELECT value FROM side_effects WHERE job_id = %s", (job_id,)
        ).fetchall()
        assert rows == [("healthy",)]  # exactly one side effect, from the healthy worker
        assert ctrl.execute("SELECT count(*) FROM side_effects").fetchone() == (1,)
        assert ctrl.execute(
            "SELECT state FROM jobs WHERE id = %s", (job_id,)
        ).fetchone() == ("succeeded",)
