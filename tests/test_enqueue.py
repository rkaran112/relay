"""Phase 1: enqueue and idempotency dedupe."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import psycopg

from relay.db import connect
from relay.queue import enqueue

# Kept under Postgres' default max_connections (100) so this is safe in CI, while
# 100 attempts across 50 simultaneous workers still forces heavy contention on the
# unique constraint.
CONCURRENCY = 50
ATTEMPTS = 100


def test_enqueue_returns_job_id(db: psycopg.Connection) -> None:
    jid = enqueue(db, "emails", {"to": "a@b.com"}, idempotency_key="k1")
    row = db.execute("SELECT queue, payload, state FROM jobs WHERE id = %s", (jid,)).fetchone()
    assert row == ("emails", {"to": "a@b.com"}, "pending")


def test_enqueue_without_key_never_dedupes(db: psycopg.Connection) -> None:
    a = enqueue(db, "emails", {"n": 1})
    b = enqueue(db, "emails", {"n": 1})
    assert a != b
    assert db.execute("SELECT count(*) FROM jobs").fetchone() == (2,)


def test_same_key_dedupes_and_keeps_first(db: psycopg.Connection) -> None:
    a = enqueue(db, "emails", {"n": 1}, idempotency_key="dup")
    b = enqueue(db, "emails", {"n": 2}, idempotency_key="dup")
    assert a == b
    # The second enqueue is a no-op: the original payload is preserved.
    row = db.execute("SELECT payload FROM jobs WHERE id = %s", (a,)).fetchone()
    assert row == ({"n": 1},)
    assert db.execute("SELECT count(*) FROM jobs").fetchone() == (1,)


def test_same_key_different_queue_is_distinct(db: psycopg.Connection) -> None:
    a = enqueue(db, "q1", {}, idempotency_key="k")
    b = enqueue(db, "q2", {}, idempotency_key="k")
    assert a != b


def test_concurrent_same_key_creates_exactly_one_row(dsn: str) -> None:
    key = f"race-{uuid4()}"

    def attempt(_: int) -> int:
        with connect(dsn, autocommit=True) as conn:
            return enqueue(conn, "emails", {"n": 1}, idempotency_key=key)

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        ids = list(pool.map(attempt, range(ATTEMPTS)))

    assert len(ids) == ATTEMPTS
    assert len(set(ids)) == 1  # every concurrent caller resolved to the same job

    with connect(dsn, autocommit=True) as conn:
        count = conn.execute(
            "SELECT count(*) FROM jobs WHERE idempotency_key = %s", (key,)
        ).fetchone()
    assert count == (1,)
