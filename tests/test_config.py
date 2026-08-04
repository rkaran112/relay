"""Config: managed-Postgres URL normalization (deploy portability)."""

from __future__ import annotations

import pytest

from relay.config import to_psycopg_dsn, to_sqlalchemy_url

# Render/Heroku give postgres://, Neon gives postgresql://, local dev uses +psycopg.
_TAIL = "u:p@host:5432/db"


@pytest.mark.parametrize(
    "url",
    [
        f"postgres://{_TAIL}",
        f"postgresql://{_TAIL}",
        f"postgresql+psycopg://{_TAIL}",
    ],
)
def test_all_postgres_schemes_normalize(url: str) -> None:
    assert to_sqlalchemy_url(url) == f"postgresql+psycopg://{_TAIL}"
    assert to_psycopg_dsn(url) == f"postgresql://{_TAIL}"
