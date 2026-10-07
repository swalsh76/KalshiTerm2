"""ingest_gaps: a record of every period the live stream may have missed

Revision ID: 0008
Revises: 0007
Create Date: 2026-10-07

One row per outage (a reconnect, a receive-queue overflow, or a restart). Trades are
backfilled from REST for the window; tickers, lifecycle events and orderbook deltas cannot be
replayed, so the row is the permanent record that they are missing for that window. Combo
per-minute counters are not backfilled either (they cannot be de-duplicated), only the
large-trade log is.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0008"
down_revision: str | None = "0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE ingest_gaps (
            id                bigserial   PRIMARY KEY,
            started_at        timestamptz NOT NULL,
            ended_at          timestamptz NOT NULL,
            reason            text        NOT NULL,
            dropped           integer     NOT NULL DEFAULT 0,
            status            text        NOT NULL DEFAULT 'pending'
                              CHECK (status IN ('pending', 'running', 'done', 'failed',
                                                'interrupted')),
            window_start      timestamptz,
            window_end        timestamptz,
            trades_found      integer     NOT NULL DEFAULT 0,
            trades_added      integer     NOT NULL DEFAULT 0,
            duplicates        integer     NOT NULL DEFAULT 0,
            combo_large_added integer     NOT NULL DEFAULT 0,
            combo_skipped     integer     NOT NULL DEFAULT 0,
            note              text        NOT NULL DEFAULT '',
            created_at        timestamptz NOT NULL DEFAULT now(),
            finished_at       timestamptz
        )
        """
    )
    op.execute("CREATE INDEX ingest_gaps_started_idx ON ingest_gaps (started_at DESC)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS ingest_gaps")
