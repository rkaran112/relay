"""Transactional outbox.

The dual-write problem: updating your database and publishing to a message bus are
two systems with no shared transaction, so a crash between them either loses the
event or publishes one that never happened. The outbox dodges it — the event is
written to an ``outbox`` row *inside the same transaction as the business change*,
so it commits atomically with it. A separate publisher (see ``publisher.py``)
relays those rows to Redis, giving at-least-once publish that consumers dedupe.
"""

from __future__ import annotations

from typing import Any

from psycopg import Connection
from psycopg.rows import TupleRow
from psycopg.types.json import Jsonb


def emit(
    conn: Connection[TupleRow],
    aggregate_id: str,
    event_type: str,
    payload: dict[str, Any],
) -> int:
    """Write an outbox event. Call this from inside the handler transaction so the
    event and the job's side effect commit together (or not at all).
    """
    row = conn.execute(
        """
        INSERT INTO outbox (aggregate_id, event_type, payload)
        VALUES (%s, %s, %s)
        RETURNING id
        """,
        (aggregate_id, event_type, Jsonb(payload)),
    ).fetchone()
    assert row is not None
    return int(row[0])
