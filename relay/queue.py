"""The four queue operations: enqueue, dequeue, complete, fail.

Everything is hand-written SQL over psycopg. Callers pass in a connection so the
work and the queue bookkeeping can share one transaction (the exactly-once
Path A). Enqueue and dequeue manage their own transactions; complete/fail run
inside the caller's handler transaction.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from psycopg import Connection
from psycopg.rows import TupleRow, dict_row
from psycopg.types.json import Jsonb

from . import tracing
from .models import Job

# ---------------------------------------------------------------------------
# Enqueue
# ---------------------------------------------------------------------------


def enqueue(
    conn: Connection[TupleRow],
    queue: str,
    payload: dict[str, Any],
    *,
    idempotency_key: str | None = None,
    run_at: datetime | None = None,
    max_attempts: int = 5,
    priority: int = 0,
    traceparent: str | None = None,
) -> int:
    """Insert a job, deduping on ``(queue, idempotency_key)``.

    Returns the job id — the freshly inserted one, or the existing one if this
    idempotency key was already enqueued. A caller retrying a POST after a timeout
    therefore never creates a second job. Jobs enqueued with no idempotency key
    always insert (NULLs don't collide under the unique constraint).
    """
    with tracing.tracer().start_as_current_span(
        "relay.enqueue", attributes={"relay.queue": queue}
    ):
        # Stamp the producer's trace context onto the row so the worker's handle span
        # can attach to it across the queue boundary.
        if traceparent is None:
            traceparent = tracing.current_traceparent()
        with conn.transaction():
            row = conn.execute(
                """
                INSERT INTO jobs (queue, payload, idempotency_key, run_at,
                                  max_attempts, priority, traceparent)
                VALUES (%s, %s, %s, COALESCE(%s, now()), %s, %s, %s)
                ON CONFLICT (queue, idempotency_key) DO NOTHING
                RETURNING id
                """,
                (
                    queue,
                    Jsonb(payload),
                    idempotency_key,
                    run_at,
                    max_attempts,
                    priority,
                    traceparent,
                ),
            ).fetchone()
            if row is not None:
                return int(row[0])
            # Conflict: ON CONFLICT waited for the concurrent inserter to commit, so a
            # fresh statement (new READ COMMITTED snapshot) is guaranteed to see the row.
            existing = conn.execute(
                "SELECT id FROM jobs WHERE queue = %s AND idempotency_key = %s",
                (queue, idempotency_key),
            ).fetchone()
            assert existing is not None, "conflict without a visible row"
            return int(existing[0])


# ---------------------------------------------------------------------------
# Dequeue (SKIP LOCKED leasing)
# ---------------------------------------------------------------------------

_DEQUEUE_SQL = """
WITH claimed AS (
  SELECT id FROM jobs
   WHERE queue = %(queue)s AND state = 'pending' AND run_at <= now()
   ORDER BY priority DESC, run_at
   FOR UPDATE SKIP LOCKED
   LIMIT %(batch)s
)
UPDATE jobs j
   SET state = 'leased',
       lease_owner = %(owner)s,
       lease_token = nextval('lease_token_seq'),
       lease_expires_at = now() + %(lease)s,
       attempts = attempts + 1
  FROM claimed c
 WHERE j.id = c.id
RETURNING j.id, j.queue, j.payload, j.state, j.priority, j.attempts, j.max_attempts,
          j.run_at, j.lease_owner, j.lease_token, j.lease_expires_at,
          j.idempotency_key, j.traceparent, j.created_at
"""


def dequeue(
    conn: Connection[TupleRow],
    queue: str,
    *,
    owner: UUID,
    lease: timedelta,
    batch: int = 1,
) -> list[Job]:
    """Atomically lease up to ``batch`` claimable jobs.

    ``FOR UPDATE SKIP LOCKED`` lets N workers claim disjoint batches with no
    coordination: each locks its candidate rows and skips any row another worker
    already holds. The lease (state='leased' + expiry + a fresh fencing token) is
    committed immediately so it survives this worker dying.
    """
    with conn.transaction():
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                _DEQUEUE_SQL,
                {"queue": queue, "batch": batch, "owner": owner, "lease": lease},
            )
            rows = cur.fetchall()
    return [Job.from_row(row) for row in rows]


# ---------------------------------------------------------------------------
# Complete / fail (fenced) — run INSIDE the caller's handler transaction
# ---------------------------------------------------------------------------


def complete(
    conn: Connection[TupleRow],
    job_id: int,
    *,
    lease_owner: UUID,
    lease_token: int,
) -> bool:
    """Mark a leased job succeeded, but only if this worker still holds the lease.

    The ``lease_owner``/``lease_token`` predicate is the fencing check: if the
    reaper already expired this lease and handed the job to someone else, zero
    rows update and this returns False. The caller MUST roll back its transaction
    (including the side effect) in that case.

    Runs inside the caller's transaction so the business write and this completion
    commit together — the heart of exactly-once side effects (Path A).
    """
    if os.environ.get("RELAY_DISABLE_FENCE") == "1":
        # NEGATIVE CONTROL ONLY (chaos harness): drop the fencing predicate so a
        # zombie's stale completion is wrongly accepted, proving the invariant suite
        # catches the resulting duplicate side effect. Never set this in production.
        cur = conn.execute(
            "UPDATE jobs SET state = 'succeeded', lease_owner = NULL, "
            "lease_token = NULL, lease_expires_at = NULL WHERE id = %s",
            (job_id,),
        )
        return cur.rowcount == 1
    cur = conn.execute(
        """
        UPDATE jobs
           SET state = 'succeeded', lease_owner = NULL, lease_token = NULL,
               lease_expires_at = NULL
         WHERE id = %s AND lease_owner = %s AND lease_token = %s
        """,
        (job_id, lease_owner, lease_token),
    )
    return cur.rowcount == 1


def fail(
    conn: Connection[TupleRow],
    job_id: int,
    *,
    lease_owner: UUID,
    lease_token: int,
    error: str,
    backoff: timedelta,
) -> str | None:
    """Record a handler failure, fenced like ``complete``.

    Requeues the job with a delayed ``run_at`` when attempts remain, or marks it
    ``dead`` (and copies it into ``dead_letters``) once ``attempts >= max_attempts``.
    Returns the resulting state ('pending' | 'dead'), or None if the fencing check
    failed (a zombie — the lease was already reassigned).
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            UPDATE jobs
               SET state = CASE WHEN attempts >= max_attempts THEN 'dead'::job_state
                                ELSE 'pending'::job_state END,
                   run_at = now() + %(backoff)s,
                   lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL
             WHERE id = %(id)s AND lease_owner = %(owner)s AND lease_token = %(token)s
            RETURNING id, queue, payload, attempts, max_attempts, state
            """,
            {"id": job_id, "owner": lease_owner, "token": lease_token, "backoff": backoff},
        )
        row = cur.fetchone()
        if row is None:
            return None
        if row["state"] == "dead":
            cur.execute(
                """
                INSERT INTO dead_letters (job_id, queue, payload, attempts, last_error)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (row["id"], row["queue"], Jsonb(row["payload"]), row["attempts"], error),
            )
        return str(row["state"])
