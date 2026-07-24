"""Phase 0 smoke tests: prove the harness can reach a real Postgres 16."""

from __future__ import annotations

import psycopg
from psycopg.rows import TupleRow


def test_postgres_is_reachable(db: psycopg.Connection[TupleRow]) -> None:
    assert db.execute("SELECT 1").fetchone() == (1,)


def test_server_is_postgres_16_or_newer(db: psycopg.Connection[TupleRow]) -> None:
    row = db.execute("SHOW server_version_num").fetchone()
    assert row is not None
    assert int(row[0]) >= 160000
