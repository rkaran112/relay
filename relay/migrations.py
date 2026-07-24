"""Programmatic Alembic entry points, so both the CLI and the test fixture drive
migrations the same way without shelling out.
"""

from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config

from .config import get_settings

_ALEMBIC_INI = Path(__file__).resolve().parent.parent / "alembic.ini"


def _config(url: str | None) -> Config:
    cfg = Config(str(_ALEMBIC_INI))
    cfg.set_main_option("sqlalchemy.url", url or get_settings().sqlalchemy_url)
    return cfg


def upgrade(url: str | None = None, revision: str = "head") -> None:
    """Apply migrations up to ``revision`` (default: latest)."""
    command.upgrade(_config(url), revision)


def downgrade(url: str | None = None, revision: str = "base") -> None:
    """Roll migrations back to ``revision`` (default: empty schema)."""
    command.downgrade(_config(url), revision)
