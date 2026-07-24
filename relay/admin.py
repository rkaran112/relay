"""Control-plane queries: job lookup, per-queue stats, and dead-letter listing /
replay. These back the HTTP API and the dashboard; they never lease or complete.
"""

from __future__ import annotations

from typing import Any

from psycopg import Connection
from psycopg.rows import TupleRow, dict_row
from psycopg.types.json import Jsonb

from .models import Job, JobState


def get_job(conn: Connection[TupleRow], job_id: int) -> Job | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT * FROM jobs WHERE id = %s", (job_id,))
        row = cur.fetchone()
    return Job.from_row(row) if row is not None else None


def queue_stats(conn: Connection[TupleRow], queue: str) -> dict[str, Any]:
    counts: dict[str, int] = {state.value: 0 for state in JobState}
    for state, n in conn.execute(
        "SELECT state, count(*) FROM jobs WHERE queue = %s GROUP BY state", (queue,)
    ).fetchall():
        counts[str(state)] = int(n)

    oldest = conn.execute(
        "SELECT EXTRACT(EPOCH FROM (now() - min(run_at))) "
        "FROM jobs WHERE queue = %s AND state = 'pending'",
        (queue,),
    ).fetchone()
    dlq = conn.execute(
        "SELECT count(*) FROM dead_letters WHERE queue = %s AND replayed_at IS NULL",
        (queue,),
    ).fetchone()

    return {
        "queue": queue,
        **counts,
        "oldest_pending_age_seconds": (
            float(oldest[0]) if oldest is not None and oldest[0] is not None else 0.0
        ),
        "dead_letters": int(dlq[0]) if dlq is not None else 0,
    }


def list_dead_letters(
    conn: Connection[TupleRow], queue: str, *, limit: int = 50
) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT id, job_id, queue, payload, attempts, last_error, died_at, replayed_at
              FROM dead_letters
             WHERE queue = %s
             ORDER BY died_at DESC
             LIMIT %s
            """,
            (queue, limit),
        )
        return cur.fetchall()


def replay_dead_letter(conn: Connection[TupleRow], queue: str, dlq_id: int) -> int | None:
    """Re-enqueue a dead-lettered job as a fresh pending job. Returns the new job id,
    or None if the dead letter doesn't exist (or was already replayed).
    """
    with conn.transaction():
        row = conn.execute(
            "SELECT payload FROM dead_letters "
            "WHERE id = %s AND queue = %s AND replayed_at IS NULL",
            (dlq_id, queue),
        ).fetchone()
        if row is None:
            return None
        new = conn.execute(
            "INSERT INTO jobs (queue, payload) VALUES (%s, %s) RETURNING id",
            (queue, Jsonb(row[0])),
        ).fetchone()
        assert new is not None
        conn.execute("UPDATE dead_letters SET replayed_at = now() WHERE id = %s", (dlq_id,))
        return int(new[0])
