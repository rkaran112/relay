"""Domain types shared across the queue engine."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID


class JobState(StrEnum):
    pending = "pending"
    leased = "leased"
    succeeded = "succeeded"
    failed = "failed"
    dead = "dead"


@dataclass(slots=True)
class Job:
    """A row from the ``jobs`` table, as handed to a worker after a lease."""

    id: int
    queue: str
    payload: dict[str, Any]
    state: JobState
    priority: int
    attempts: int
    max_attempts: int
    run_at: datetime
    lease_owner: UUID | None
    lease_token: int | None
    lease_expires_at: datetime | None
    idempotency_key: str | None
    traceparent: str | None
    created_at: datetime

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> Job:
        return cls(
            id=row["id"],
            queue=row["queue"],
            payload=row["payload"],
            state=JobState(row["state"]),
            priority=row["priority"],
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            run_at=row["run_at"],
            lease_owner=row["lease_owner"],
            lease_token=row["lease_token"],
            lease_expires_at=row["lease_expires_at"],
            idempotency_key=row["idempotency_key"],
            traceparent=row["traceparent"],
            created_at=row["created_at"],
        )
