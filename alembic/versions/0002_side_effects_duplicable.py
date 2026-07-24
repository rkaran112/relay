"""make side_effects duplicable (surrogate PK)

SPEC section 3 gives side_effects a ``job_id PRIMARY KEY``. But the Phase 9 negative
control removes the fencing check and must then *observe* a duplicate side effect —
and a job_id PK would silently reject the second insert, hiding the very bug the
chaos harness exists to catch. So side_effects gets a surrogate id and a plain
(non-unique) job_id: correct runs still write exactly one row per job, but a double
completion writes two, and invariant 1 (count == distinct enqueued) fails as it must.

Revision ID: 0002
Revises: 0001
Create Date: 2026-07-24

"""

from collections.abc import Sequence

from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("DROP TABLE side_effects")
    op.execute(
        """
        CREATE TABLE side_effects (
          id         BIGSERIAL PRIMARY KEY,
          job_id     BIGINT      NOT NULL,
          value      TEXT        NOT NULL,
          written_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("CREATE INDEX idx_side_effects_job ON side_effects (job_id)")


def downgrade() -> None:
    op.execute("DROP TABLE side_effects")
    op.execute(
        """
        CREATE TABLE side_effects (
          job_id BIGINT PRIMARY KEY, value TEXT NOT NULL,
          written_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
