"""The outbox publisher.

Relays unpublished ``outbox`` rows to a Redis Stream. Ordering matters for crash
safety: within one transaction it locks a batch of unpublished rows
(``FOR UPDATE SKIP LOCKED`` so multiple publishers don't clash), XADDs each to
Redis, then marks them published and commits. If it dies after XADD but before
commit, the rows stay unpublished and are re-sent on restart — at-least-once, never
lost. Redis writes aren't transactional, so restarts can duplicate; consumers dedupe
on ``event_id`` (the outbox row id).
"""

from __future__ import annotations

import json
import threading
from collections.abc import Callable

import redis
from psycopg import Connection
from psycopg.rows import TupleRow

from .db import connect

_SELECT_UNPUBLISHED = """
SELECT id, aggregate_id, event_type, payload
  FROM outbox
 WHERE published_at IS NULL
 ORDER BY id
 FOR UPDATE SKIP LOCKED
 LIMIT %s
"""


class Publisher:
    def __init__(
        self,
        dsn: str,
        redis_url: str,
        *,
        stream: str = "relay.events",
        batch: int = 100,
        interval: float = 0.1,
        per_event_delay: float = 0.0,
        on_event: Callable[[int], None] | None = None,
    ) -> None:
        self.dsn = dsn
        self.redis_url = redis_url
        self.stream = stream
        self.batch = batch
        self.interval = interval
        # Test hooks: slow the loop and observe each XADD so a crash can be injected
        # mid-batch.
        self.per_event_delay = per_event_delay
        self.on_event = on_event
        self.published = 0
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def _publish_batch(self, conn: Connection[TupleRow], r: redis.Redis) -> int:
        published = 0
        with conn.transaction(), conn.cursor() as cur:
            cur.execute(_SELECT_UNPUBLISHED, (self.batch,))
            rows = cur.fetchall()
            for row_id, aggregate_id, event_type, payload in rows:
                r.xadd(
                    self.stream,
                    {
                        "event_id": str(row_id),
                        "aggregate_id": aggregate_id,
                        "event_type": event_type,
                        "payload": json.dumps(payload),
                    },
                )
                published += 1
                if self.on_event is not None:
                    self.on_event(row_id)
                if self.per_event_delay:
                    self._stop.wait(self.per_event_delay)
            if rows:
                cur.execute(
                    "UPDATE outbox SET published_at = now() WHERE id = ANY(%s)",
                    ([row[0] for row in rows],),
                )
        self.published += published
        return published

    def drain(self) -> int:
        """Publish everything currently unpublished, then return the total. For tests."""
        with connect(self.dsn) as conn:
            r = redis.Redis.from_url(self.redis_url)
            try:
                while self._publish_batch(conn, r):
                    pass
            finally:
                r.close()
        return self.published

    def run_forever(self) -> None:
        with connect(self.dsn) as conn:
            r = redis.Redis.from_url(self.redis_url)
            try:
                while not self._stop.is_set():
                    if self._publish_batch(conn, r) == 0:
                        self._stop.wait(self.interval)
            finally:
                r.close()
