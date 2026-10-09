"""backup_runs: what the scheduled backup did, for ``status``

Revision ID: 0017
Revises: 0016
Create Date: 2026-10-09

The backup files and their manifests on the target are the durable record; this table is how
``kterm-server status`` learns, without access to the NAS, when the last backup succeeded.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0017"
down_revision: str | None = "0016"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE backup_runs (
            id           bigserial   PRIMARY KEY,
            started_at   timestamptz NOT NULL DEFAULT now(),
            finished_at  timestamptz,
            status       text        NOT NULL DEFAULT 'running'
                         CHECK (status IN ('running', 'ok', 'failed')),
            file         text,
            size_bytes   bigint,
            seconds      double precision,
            error        text        NOT NULL DEFAULT ''
        )
        """
    )
    op.execute("CREATE INDEX backup_runs_started_idx ON backup_runs (started_at DESC)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS backup_runs")
