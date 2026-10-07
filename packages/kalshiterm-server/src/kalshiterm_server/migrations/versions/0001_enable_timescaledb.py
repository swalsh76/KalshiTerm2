"""enable the TimescaleDB extension

Revision ID: 0001
Revises:
Create Date: 2026-10-07
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")


def downgrade() -> None:
    # No CASCADE: this fails loudly if any hypertable still exists instead of dropping data.
    op.execute("DROP EXTENSION IF EXISTS timescaledb")
