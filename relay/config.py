"""Runtime configuration, loaded from the environment / a ``.env`` file."""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict

_SQLALCHEMY_PREFIX = "postgresql+psycopg://"
_LIBPQ_PREFIX = "postgresql://"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # SQLAlchemy-style URL (with the +psycopg driver). Alembic consumes it as-is;
    # the runtime queue code uses `psycopg_dsn`, which strips the driver prefix.
    database_url: str = "postgresql+psycopg://relay:relay@localhost:5432/relay"
    # Tests target this DB. When unset, the test fixture picks its own default so
    # it never runs against the dev database.
    test_database_url: str | None = None
    redis_url: str = "redis://localhost:6379/0"

    @property
    def sqlalchemy_url(self) -> str:
        return self.database_url

    @property
    def psycopg_dsn(self) -> str:
        return to_psycopg_dsn(self.database_url)


def to_psycopg_dsn(url: str) -> str:
    """Turn a ``postgresql+psycopg://`` SQLAlchemy URL into a native libpq DSN."""
    if url.startswith(_SQLALCHEMY_PREFIX):
        return _LIBPQ_PREFIX + url[len(_SQLALCHEMY_PREFIX) :]
    return url


@lru_cache
def get_settings() -> Settings:
    return Settings()
