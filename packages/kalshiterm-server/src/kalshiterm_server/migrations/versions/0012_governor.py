"""storage governor: size samples, action log, remembered state

Revision ID: 0012
Revises: 0011
Create Date: 2026-10-08

``storage_samples`` is the history behind growth rate and days-to-full (kept 30 days by the
governor); ``governor_events`` records every action it takes (tighten, shed, restore, drive
alerts) so ``status`` can show what happened and when; ``governor_state`` remembers the current
mode and the retention windows it changed, so they can be restored exactly.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0012"
down_revision: str | None = "0011"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE storage_samples (
            ts          timestamptz PRIMARY KEY,
            db_bytes    bigint      NOT NULL,
            wal_bytes   bigint,
            used_bytes  bigint      NOT NULL,
            disk_total  bigint,
            disk_free   bigint,
            tables      jsonb       NOT NULL
        )
        """
    )
    op.execute(
        """
        CREATE TABLE governor_events (
            id      bigserial   PRIMARY KEY,
            ts      timestamptz NOT NULL DEFAULT now(),
            kind    text        NOT NULL,
            detail  jsonb       NOT NULL DEFAULT '{}'
        )
        """
    )
    op.execute("CREATE INDEX governor_events_ts_idx ON governor_events (ts DESC)")
    op.execute(
        """
        CREATE TABLE governor_state (
            key         text        PRIMARY KEY,
            value       text        NOT NULL,
            updated_at  timestamptz NOT NULL DEFAULT now()
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS governor_state")
    op.execute("DROP TABLE IF EXISTS governor_events")
    op.execute("DROP TABLE IF EXISTS storage_samples")
