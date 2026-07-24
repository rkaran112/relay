"""Phase 5: transactional outbox and the Redis Streams publisher."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import redis
from psycopg import Rollback

from relay.db import connect
from relay.outbox import emit
from relay.publisher import Publisher

_SLOW_PUBLISHER = Path(__file__).parent / "scripts" / "slow_publisher.py"


def test_emit_is_atomic_with_its_transaction(dsn: str) -> None:
    with connect(dsn) as conn:
        with conn.transaction():
            emit(conn, "order-1", "created", {"total": 10})
        # committed -> visible
        assert conn.execute("SELECT count(*) FROM outbox").fetchone() == (1,)

        try:
            with conn.transaction():
                emit(conn, "order-2", "created", {"total": 20})
                raise Rollback()
        except Rollback:
            pass
        # rolled back -> still just the first event
        assert conn.execute("SELECT count(*) FROM outbox").fetchone() == (1,)


def test_publisher_publishes_all_events(dsn: str, redis_url: str) -> None:
    stream = f"relay.events.{uuid4()}"
    with connect(dsn, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO outbox (aggregate_id, event_type, payload) "
            "SELECT 'agg', 'evt', jsonb_build_object('i', g) FROM generate_series(1, 25) g"
        )
    Publisher(dsn, redis_url, stream=stream).drain()

    with connect(dsn, autocommit=True) as conn:
        unpublished = conn.execute(
            "SELECT count(*) FROM outbox WHERE published_at IS NULL"
        ).fetchone()
        assert unpublished == (0,)
    r = redis.Redis.from_url(redis_url)
    try:
        assert r.xlen(stream) == 25
    finally:
        r.delete(stream)
        r.close()


def test_publisher_crash_midbatch_loses_nothing(dsn: str, redis_url: str) -> None:
    stream = f"relay.events.{uuid4()}"
    n_events = 50
    with connect(dsn, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO outbox (aggregate_id, event_type, payload) "
            "SELECT 'agg', 'evt', jsonb_build_object('i', g) FROM generate_series(1, %s) g",
            (n_events,),
        )

    proc = subprocess.Popen(
        [sys.executable, str(_SLOW_PUBLISHER), dsn, redis_url, stream, "0.05"],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert proc.stdout is not None
        seen = 0
        while seen < 5:  # let it XADD a few, then kill mid-batch (pre-commit)
            line = proc.stdout.readline()
            assert line, "publisher exited before publishing anything"
            if line.startswith("xadd"):
                seen += 1
        proc.kill()
        proc.wait(timeout=10)
    finally:
        if proc.poll() is None:
            proc.kill()

    # The killed transaction committed nothing: every row is still unpublished.
    with connect(dsn, autocommit=True) as conn:
        assert conn.execute(
            "SELECT count(*) FROM outbox WHERE published_at IS NULL"
        ).fetchone() == (n_events,)

    # Restart a healthy publisher; it drains the rest.
    Publisher(dsn, redis_url, stream=stream, batch=100).drain()

    with connect(dsn, autocommit=True) as conn:
        assert conn.execute(
            "SELECT count(*) FROM outbox WHERE published_at IS NULL"
        ).fetchone() == (0,)

    r = redis.Redis.from_url(redis_url)
    try:
        entries = r.xrange(stream)
        distinct = {fields[b"event_id"] for _id, fields in entries}
    finally:
        r.delete(stream)
        r.close()

    assert len(distinct) == n_events  # every event published (no loss)
    assert len(entries) > n_events  # the pre-crash XADDs are duplicates (at-least-once)
