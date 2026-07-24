"""Pytest fixtures. Every test runs against a real PostgreSQL — no SQLite, no
mocks. The test database is created on demand and migrated to head once per
session, so `docker compose up -d db && pytest` just works.
"""

from __future__ import annotations

import os
from collections.abc import Iterator

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import TupleRow

from relay import migrations
from relay.config import to_psycopg_dsn

# A dedicated test DB by default, so tests never touch a dev database.
TEST_SQLALCHEMY_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql+psycopg://relay:relay@localhost:5432/relay_test",
)
TEST_DSN = to_psycopg_dsn(TEST_SQLALCHEMY_URL)


def _ensure_database(dsn: str) -> None:
    """Create the target database if it does not already exist."""
    params = conninfo_to_dict(dsn)
    dbname = params["dbname"]
    admin = {**params, "dbname": "postgres"}
    with psycopg.connect(**admin, autocommit=True) as conn:  # type: ignore[arg-type]
        exists = conn.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s", (dbname,)
        ).fetchone()
        if not exists:
            conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(str(dbname))))


@pytest.fixture(scope="session", autouse=True)
def _database() -> Iterator[None]:
    _ensure_database(TEST_DSN)
    migrations.upgrade(TEST_SQLALCHEMY_URL)
    yield


@pytest.fixture
def db() -> Iterator[psycopg.Connection[TupleRow]]:
    conn = psycopg.connect(TEST_DSN, autocommit=True)
    try:
        yield conn
    finally:
        conn.close()
