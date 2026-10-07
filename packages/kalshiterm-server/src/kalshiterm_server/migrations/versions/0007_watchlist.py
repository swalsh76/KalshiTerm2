"""watchlist: coverage periods and the permanent raw-trade copy

Revision ID: 0007
Revises: 0006
Create Date: 2026-10-07

``watchlist_periods`` records when a market's orderbook was being captured (decision 12);
``source`` is how the period was opened ('manual' or 'auto'), not necessarily who holds it now.
``trades_watchlist`` keeps the raw trades of every market that has ever been watched, with no
retention. It has the same columns as ``trades``; 7-day chunks because it is small.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0007"
down_revision: str | None = "0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE watchlist_periods (
            id          bigserial   PRIMARY KEY,
            market_id   integer     NOT NULL REFERENCES markets (id),
            source      text        NOT NULL CHECK (source IN ('manual', 'auto')),
            added_at    timestamptz NOT NULL DEFAULT now(),
            removed_at  timestamptz
        )
        """
    )
    # A market has at most one open period.
    op.execute(
        "CREATE UNIQUE INDEX watchlist_periods_open_idx ON watchlist_periods (market_id) "
        "WHERE removed_at IS NULL"
    )
    op.execute(
        """
        CREATE TABLE trades_watchlist (
            ts              timestamptz NOT NULL,
            received_at     timestamptz NOT NULL,
            market_id       integer     NOT NULL,
            trade_id        uuid        NOT NULL,
            yes_price_e6    bigint      NOT NULL,
            count_e2        bigint      NOT NULL,
            taker_side      text,
            is_block_trade  boolean     NOT NULL DEFAULT false
        )
        """
    )
    op.execute(
        "SELECT create_hypertable('trades_watchlist', 'ts', "
        "chunk_time_interval => interval '7 days', create_default_indexes => false)"
    )
    op.execute(
        "CREATE INDEX trades_watchlist_market_ts_idx ON trades_watchlist (market_id, ts DESC)"
    )
    op.execute(
        """
        CREATE VIEW watchlist_periods_v AS
        SELECT p.id, m.ticker, p.market_id, p.source, p.added_at, p.removed_at
        FROM watchlist_periods p JOIN markets m ON m.id = p.market_id
        """
    )
    op.execute(
        """
        CREATE VIEW trades_watchlist_v AS
        SELECT t.ts, t.received_at, m.ticker, t.market_id, t.trade_id,
               (t.yes_price_e6::numeric / 1000000)::numeric(30,6) AS yes_price,
               ((1000000 - t.yes_price_e6)::numeric / 1000000)::numeric(30,6) AS no_price,
               (t.count_e2::numeric / 100)::numeric(30,2) AS count,
               t.taker_side, t.is_block_trade
        FROM trades_watchlist t JOIN markets m ON m.id = t.market_id
        """
    )


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS trades_watchlist_v")
    op.execute("DROP VIEW IF EXISTS watchlist_periods_v")
    op.execute("DROP TABLE IF EXISTS trades_watchlist")
    op.execute("DROP TABLE IF EXISTS watchlist_periods")
