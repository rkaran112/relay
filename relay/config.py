"""Runtime configuration, loaded from the environment / a ``.env`` file."""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


def _after_scheme(url: str) -> str:
    """Everything after ``scheme://`` — the user:pass@host/db portion."""
    return url.split("://", 1)[1] if "://" in url else url


def to_sqlalchemy_url(url: str) -> str:
    """Normalize any Postgres URL to the SQLAlchemy form with the psycopg-3 driver.

    Managed hosts hand out different schemes — Render/Heroku give ``postgres://``,
    Neon gives ``postgresql://`` — and SQLAlchemy 2.0 rejects both (it needs the
    explicit ``+psycopg`` driver). This makes the app portable across all of them.
    """
    return f"postgresql+psycopg://{_after_scheme(url)}"


def to_psycopg_dsn(url: str) -> str:
    """Normalize any Postgres URL to a native libpq DSN for psycopg."""
    return f"postgresql://{_after_scheme(url)}"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Any Postgres URL scheme works (postgres://, postgresql://, +psycopg); the
    # properties below normalize it for Alembic (SQLAlchemy) and psycopg respectively.
    database_url: str = "postgresql+psycopg://relay:relay@localhost:5432/relay"
    # Tests target this DB. When unset, the test fixture picks its own default so
    # it never runs against the dev database.
    test_database_url: str | None = None
    redis_url: str = "redis://localhost:6379/0"

    @property
    def sqlalchemy_url(self) -> str:
        return to_sqlalchemy_url(self.database_url)

    @property
    def psycopg_dsn(self) -> str:
        return to_psycopg_dsn(self.database_url)


@lru_cache
def get_settings() -> Settings:
    return Settings()
