"""Low-level database access. The queue engine talks to Postgres through psycopg
directly (raw, hand-tuned SQL and explicit transaction control) rather than an ORM.
"""

from __future__ import annotations

import psycopg
from psycopg import Connection
from psycopg.rows import TupleRow

from .config import get_settings


def connect(dsn: str | None = None, *, autocommit: bool = False) -> Connection[TupleRow]:
    """Open a new psycopg connection to the queue database."""
    return psycopg.connect(dsn or get_settings().psycopg_dsn, autocommit=autocommit)
