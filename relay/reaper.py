"""The visibility-timeout reaper.

A worker that dies (or whose VM is suspended) leaves its job stuck in ``leased``
with a lease that will never be renewed. The reaper sweeps expired leases back to
``pending`` — or to ``dead`` once attempts are exhausted — so no job is lost to a
crashed worker. It runs as its own loop (every second by default) and is safe to
run in multiple copies: ``FOR UPDATE SKIP LOCKED`` isn't needed because the single
set-based UPDATE is atomic and idempotent under concurrency.
"""

from __future__ import annotations

import threading

from psycopg import Connection
from psycopg.rows import TupleRow

from .db import connect
from .metrics import LEASE_EXPIRIES

_DEFAULT_REASON = "lease expired (visibility timeout)"

# One atomic statement: requeue expired leases with full-jitter backoff computed in
# SQL (random() * window), flip exhausted ones to dead, copy those into dead_letters,
# and report how many of each. `2 ^ attempts` is double precision; LEAST caps it.
_REAP_SQL = """
WITH expired AS (
  UPDATE jobs
     SET state = CASE WHEN attempts >= max_attempts THEN 'dead'::job_state
                      ELSE 'pending'::job_state END,
         run_at = now() + make_interval(
                    secs => random() * LEAST(%(cap)s, %(base)s * (2 ^ attempts))),
         lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL
   WHERE state = 'leased' AND lease_expires_at < now()
  RETURNING id, queue, payload, attempts, state
),
dead_ins AS (
  INSERT INTO dead_letters (job_id, queue, payload, attempts, last_error)
  SELECT id, queue, payload, attempts, %(reason)s FROM expired WHERE state = 'dead'
)
SELECT
  count(*) FILTER (WHERE state = 'pending')::int AS requeued,
  count(*) FILTER (WHERE state = 'dead')::int    AS deadlettered
FROM expired
"""


def reap(
    conn: Connection[TupleRow],
    *,
    base: float = 1.0,
    cap: float = 300.0,
    reason: str = _DEFAULT_REASON,
) -> tuple[int, int]:
    """Reclaim expired leases once. Returns ``(requeued, deadlettered)``."""
    with conn.transaction():
        row = conn.execute(_REAP_SQL, {"base": base, "cap": cap, "reason": reason}).fetchone()
    if row is None:
        return (0, 0)
    return (int(row[0]), int(row[1]))


class Reaper:
    def __init__(
        self,
        dsn: str,
        *,
        interval: float = 1.0,
        base: float = 1.0,
        cap: float = 300.0,
    ) -> None:
        self.dsn = dsn
        self.interval = interval
        self.base = base
        self.cap = cap
        self.requeued = 0
        self.deadlettered = 0
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def reap_once(self) -> tuple[int, int]:
        with connect(self.dsn) as conn:
            requeued, dead = reap(conn, base=self.base, cap=self.cap)
        self._account(requeued, dead)
        return (requeued, dead)

    def run_forever(self) -> None:
        with connect(self.dsn) as conn:
            while not self._stop.is_set():
                requeued, dead = reap(conn, base=self.base, cap=self.cap)
                self._account(requeued, dead)
                self._stop.wait(self.interval)

    def _account(self, requeued: int, dead: int) -> None:
        self.requeued += requeued
        self.deadlettered += dead
        if requeued or dead:
            LEASE_EXPIRIES.inc(requeued + dead)
