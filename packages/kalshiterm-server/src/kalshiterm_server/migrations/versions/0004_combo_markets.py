"""combo (multivariate) markets: combo_markets, combo_tickers, combo_trades, combo_stats_1m

Revision ID: 0004
Revises: 0003
Create Date: 2026-10-07

Decision 10 (revised after measurement): ~6 M combo markets are created per day and only ~12 %
ever show activity, so a per-market row is kept only for combos that have *traded*
(``first_trade_at``). Legs are stored as integer ids of ordinary ``markets`` rows plus a
parallel array of sides (true = yes). ``combo_stats_1m`` counts the whole combo universe per
minute and ticker family, so the overall picture survives even though most combos get no row.
Retention (14 days after settlement; raw tickers/trades 3 days) is applied in the retention
slice.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def create_per_combo_tables() -> None:
    """The per-combo tables and views (also recreated by the 0005 downgrade)."""
    op.execute(
        """
        CREATE TABLE combo_markets (
            id                   integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            ticker               text NOT NULL UNIQUE,
            collection_ticker    text NOT NULL,
            event_ticker         text NOT NULL,
            status               text NOT NULL,
            created_time         timestamptz,
            open_time            timestamptz,
            close_time           timestamptz,
            first_trade_at       timestamptz NOT NULL,
            result               text NOT NULL DEFAULT '',
            settlement_value_e6  bigint,
            settled_at           timestamptz,
            leg_market_ids       integer[] NOT NULL,
            leg_yes              boolean[] NOT NULL,
            first_seen_at        timestamptz NOT NULL DEFAULT now(),
            updated_at           timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    for table, columns in (
        (
            "combo_tickers",
            """
            ts timestamptz NOT NULL, received_at timestamptz NOT NULL, market_id integer NOT NULL,
            price_e6 bigint, yes_bid_e6 bigint, yes_ask_e6 bigint, yes_bid_size_e2 bigint,
            yes_ask_size_e2 bigint, volume_e2 bigint, open_interest_e2 bigint,
            last_trade_size_e2 bigint
            """,
        ),
        (
            "combo_trades",
            """
            ts timestamptz NOT NULL, received_at timestamptz NOT NULL, market_id integer NOT NULL,
            trade_id uuid NOT NULL, yes_price_e6 bigint NOT NULL, count_e2 bigint NOT NULL,
            taker_side text, is_block_trade boolean NOT NULL DEFAULT false
            """,
        ),
    ):
        op.execute(f"CREATE TABLE {table} ({columns})")
        op.execute(
            f"SELECT create_hypertable('{table}', 'ts', chunk_time_interval => interval '1 day', "
            "create_default_indexes => false)"
        )
        op.execute(f"CREATE INDEX {table}_market_ts_idx ON {table} (market_id, ts DESC)")
    op.execute(
        """
        CREATE VIEW combo_markets_v AS
        SELECT c.*, (c.settlement_value_e6::numeric / 1000000)::numeric(30,6) AS settlement_value,
               cardinality(c.leg_market_ids) AS leg_count
        FROM combo_markets c
        """
    )
    op.execute(
        """
        CREATE VIEW combo_trades_v AS
        SELECT t.ts, t.received_at, c.ticker, t.market_id, t.trade_id,
               (t.yes_price_e6::numeric / 1000000)::numeric(30,6) AS yes_price,
               (t.count_e2::numeric / 100)::numeric(30,2) AS count,
               t.taker_side, t.is_block_trade
        FROM combo_trades t JOIN combo_markets c ON c.id = t.market_id
        """
    )


def upgrade() -> None:
    create_per_combo_tables()
    op.execute(
        """
        CREATE TABLE combo_stats_1m (
            minute         timestamptz NOT NULL,
            family         text        NOT NULL,
            created        integer     NOT NULL DEFAULT 0,
            determined     integer     NOT NULL DEFAULT 0,
            settled        integer     NOT NULL DEFAULT 0,
            close_updated  integer     NOT NULL DEFAULT 0,
            ticker_msgs    integer     NOT NULL DEFAULT 0,
            trades         integer     NOT NULL DEFAULT 0,
            contracts_e2   bigint      NOT NULL DEFAULT 0,
            PRIMARY KEY (minute, family)
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS combo_trades_v")
    op.execute("DROP VIEW IF EXISTS combo_markets_v")
    op.execute("DROP TABLE IF EXISTS combo_stats_1m")
    op.execute("DROP TABLE IF EXISTS combo_trades")
    op.execute("DROP TABLE IF EXISTS combo_tickers")
    op.execute("DROP TABLE IF EXISTS combo_markets")
