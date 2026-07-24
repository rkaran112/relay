"""The worker loop.

A worker leases a batch, then runs each job's handler inside a transaction that
also fences and commits the completion. If the fenced completion updates zero rows
(the lease was reaped and reassigned) the whole transaction — handler side effect
included — rolls back. That is the exactly-once guarantee for in-database effects.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from datetime import timedelta
from uuid import UUID, uuid4

from psycopg import Connection
from psycopg.rows import TupleRow

from .backoff import full_jitter
from .db import connect
from .models import Job
from .queue import complete, dequeue, fail

# A handler gets the open transaction and the job. Any DB writes it makes commit
# atomically with the job's completion; raising triggers a fenced retry/backoff.
Handler = Callable[[Connection[TupleRow], Job], None]


class _LeaseLost(Exception):
    """Internal signal: the fenced completion matched zero rows, so roll back."""


class Worker:
    def __init__(
        self,
        dsn: str,
        queue: str,
        handler: Handler,
        *,
        owner: UUID | None = None,
        lease: timedelta = timedelta(seconds=30),
        batch: int = 1,
        poll_interval: float = 0.05,
    ) -> None:
        self.dsn = dsn
        self.queue = queue
        self.handler = handler
        self.owner: UUID = owner or uuid4()
        self.lease = lease
        self.batch = batch
        self.poll_interval = poll_interval
        self.processed = 0
        self.failed = 0
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run_forever(self) -> None:
        """Poll until :meth:`stop` is called. Used by the ``relay worker`` command."""
        with connect(self.dsn) as conn:
            while not self._stop.is_set():
                if not self._drain_once(conn):
                    time.sleep(self.poll_interval)

    def run_until_empty(self, *, idle_rounds: int = 3) -> int:
        """Drain the queue then return, once ``idle_rounds`` polls in a row find
        nothing. Used by tests and the benchmark. Returns the count processed.
        """
        with connect(self.dsn) as conn:
            empties = 0
            while not self._stop.is_set():
                if self._drain_once(conn):
                    empties = 0
                else:
                    empties += 1
                    if empties >= idle_rounds:
                        break
                    time.sleep(self.poll_interval)
        return self.processed

    def _drain_once(self, conn: Connection[TupleRow]) -> bool:
        jobs = dequeue(conn, self.queue, owner=self.owner, lease=self.lease, batch=self.batch)
        for job in jobs:
            self._process(conn, job)
        return bool(jobs)

    def _process(self, conn: Connection[TupleRow], job: Job) -> None:
        assert job.lease_owner is not None and job.lease_token is not None
        try:
            with conn.transaction():
                self.handler(conn, job)
                if not complete(
                    conn, job.id, lease_owner=job.lease_owner, lease_token=job.lease_token
                ):
                    raise _LeaseLost(job.id)
        except _LeaseLost:
            return  # zombie: another worker owns this job now; side effect rolled back
        except Exception as exc:
            self._fail(conn, job, exc)
            return
        self.processed += 1

    def _fail(self, conn: Connection[TupleRow], job: Job, exc: Exception) -> None:
        assert job.lease_owner is not None and job.lease_token is not None
        with conn.transaction():
            fail(
                conn,
                job.id,
                lease_owner=job.lease_owner,
                lease_token=job.lease_token,
                error=repr(exc),
                backoff=timedelta(seconds=full_jitter(job.attempts)),
            )
        self.failed += 1
