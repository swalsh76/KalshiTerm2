"""streaming hypertables: tickers, trades, market_lifecycle

Revision ID: 0003
Revises: 0002
Create Date: 2026-10-07

Every number is bigint fixed-point (``*_e6`` dollars, ``*_e2`` counts). ``ts`` is the exchange
time (the partitioning column), ``received_at`` is when this process read the message, so lag
can be measured. No unique index on ``trades``: Kalshi's trade ids are random UUIDs and the
index would cost roughly 0.7 GB/day; backfill dedupes against the outage window instead.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE tickers (
            ts                  timestamptz NOT NULL,
            received_at         timestamptz NOT NULL,
            market_id           integer     NOT NULL,
            price_e6            bigint,
            yes_bid_e6          bigint,
            yes_ask_e6          bigint,
            yes_bid_size_e2     bigint,
            yes_ask_size_e2     bigint,
            volume_e2           bigint,
            open_interest_e2    bigint,
            last_trade_size_e2  bigint
        )
        """
    )
    op.execute(
        "SELECT create_hypertable('tickers', 'ts', chunk_time_interval => interval '1 day', "
        "create_default_indexes => false)"
    )
    op.execute("CREATE INDEX tickers_market_ts_idx ON tickers (market_id, ts DESC)")

    op.execute(
        """
        CREATE TABLE trades (
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
        "SELECT create_hypertable('trades', 'ts', chunk_time_interval => interval '1 day', "
        "create_default_indexes => false)"
    )
    op.execute("CREATE INDEX trades_market_ts_idx ON trades (market_id, ts DESC)")

    op.execute(
        """
        CREATE TABLE market_lifecycle (
            ts                   timestamptz NOT NULL,
            received_at          timestamptz NOT NULL,
            market_id            integer     NOT NULL,
            event_type           text        NOT NULL,
            open_ts              timestamptz,
            close_ts             timestamptz,
            determination_ts     timestamptz,
            settled_ts           timestamptz,
            result               text,
            settlement_value_e6  bigint,
            is_deactivated       boolean
        )
        """
    )
    op.execute(
        "SELECT create_hypertable('market_lifecycle', 'ts', "
        "chunk_time_interval => interval '7 days', create_default_indexes => false)"
    )
    op.execute(
        "CREATE INDEX market_lifecycle_market_ts_idx ON market_lifecycle (market_id, ts DESC)"
    )

    op.execute(
        """
        CREATE VIEW tickers_v AS
        SELECT t.ts, t.received_at, m.ticker, t.market_id,
               (t.price_e6::numeric / 1000000)::numeric(30,6) AS price,
               (t.yes_bid_e6::numeric / 1000000)::numeric(30,6) AS yes_bid,
               (t.yes_ask_e6::numeric / 1000000)::numeric(30,6) AS yes_ask,
               (t.yes_bid_size_e2::numeric / 100)::numeric(30,2) AS yes_bid_size,
               (t.yes_ask_size_e2::numeric / 100)::numeric(30,2) AS yes_ask_size,
               (t.volume_e2::numeric / 100)::numeric(30,2) AS volume,
               (t.open_interest_e2::numeric / 100)::numeric(30,2) AS open_interest,
               (t.last_trade_size_e2::numeric / 100)::numeric(30,2) AS last_trade_size
        FROM tickers t JOIN markets m ON m.id = t.market_id
        """
    )
    op.execute(
        """
        CREATE VIEW trades_v AS
        SELECT t.ts, t.received_at, m.ticker, t.market_id, t.trade_id,
               (t.yes_price_e6::numeric / 1000000)::numeric(30,6) AS yes_price,
               ((1000000 - t.yes_price_e6)::numeric / 1000000)::numeric(30,6) AS no_price,
               (t.count_e2::numeric / 100)::numeric(30,2) AS count,
               t.taker_side, t.is_block_trade
        FROM trades t JOIN markets m ON m.id = t.market_id
        """
    )


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS trades_v")
    op.execute("DROP VIEW IF EXISTS tickers_v")
    op.execute("DROP TABLE IF EXISTS market_lifecycle")
    op.execute("DROP TABLE IF EXISTS trades")
    op.execute("DROP TABLE IF EXISTS tickers")
