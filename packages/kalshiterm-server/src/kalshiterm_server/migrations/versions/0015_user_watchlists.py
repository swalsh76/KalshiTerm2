"""per-user watchlists; coverage periods can be opened on a user's behalf

Revision ID: 0015
Revises: 0014
Create Date: 2026-10-09

``user_watchlists`` is each user's own list of markets they want the server to capture
orderbooks for. Removing a user removes their list. The ingest controller watches the union of
the config file, the automatic top-N and every user's list (PLAN decisions 12-13), and a period
opened for a market only users asked for has source ``user``.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0015"
down_revision: str | None = "0014"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE user_watchlists (
            user_id    integer     NOT NULL REFERENCES users (id) ON DELETE CASCADE,
            market_id  integer     NOT NULL REFERENCES markets (id),
            added_at   timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (user_id, market_id)
        )
        """
    )
    op.execute("CREATE INDEX user_watchlists_market_idx ON user_watchlists (market_id)")
    op.execute("ALTER TABLE watchlist_periods DROP CONSTRAINT watchlist_periods_source_check")
    op.execute(
        "ALTER TABLE watchlist_periods ADD CONSTRAINT watchlist_periods_source_check "
        "CHECK (source IN ('manual', 'auto', 'user'))"
    )


def downgrade() -> None:
    op.execute("UPDATE watchlist_periods SET source = 'manual' WHERE source = 'user'")
    op.execute("ALTER TABLE watchlist_periods DROP CONSTRAINT watchlist_periods_source_check")
    op.execute(
        "ALTER TABLE watchlist_periods ADD CONSTRAINT watchlist_periods_source_check "
        "CHECK (source IN ('manual', 'auto'))"
    )
    op.execute("DROP TABLE IF EXISTS user_watchlists")
