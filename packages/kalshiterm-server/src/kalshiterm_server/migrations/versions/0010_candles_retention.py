"""candles (continuous aggregates) and raw-data retention

Revision ID: 0010
Revises: 0009
Create Date: 2026-10-07

Trade candles (1 minute / 1 hour, kept forever) and ticker aggregates (1 hour kept forever;
1 minute kept 30 days: measured at ~7,400 rows per minute, a year of them would not fit the
budget). Each aggregate is refreshed by a policy that only looks at the last day or two, so
dropping old raw chunks never touches the materialized history. Aggregates are compressed
(segmented by market: each has thousands of rows per market per chunk) after 3 days.

Raw-data retention is switched on here, and only here, because it must not exist before the
candles that replace the raw rows: trades 30 d, tickers 14 d, orderbook deltas 14 d,
orderbook snapshots 365 d (they already are the 5-minute downsample), combo_large_trades
365 d. ``trades_watchlist`` has none (decision 12).

Ticker open/close skip ticks that carry no trade price; bid/ask are the last quote as sent
(a missing bid or ask means that side of the book was empty).

Tie caveat: Kalshi stamps many trades with the same microsecond, so open/close within one
timestamp is arbitrary. The aggregates are created WITH NO DATA (needed inside a transaction);
the policies fill them from the last day or two, which is complete for a new deployment.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0010"
down_revision: str | None = "0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

CANDLE_COLUMNS = """
    first(yes_price_e6, ts) AS open_e6, max(yes_price_e6) AS high_e6,
    min(yes_price_e6) AS low_e6, last(yes_price_e6, ts) AS close_e6,
    sum(count_e2) AS volume_e2, count(*) AS trades
"""
TICKER_COLUMNS = """
    first(price_e6, ts) FILTER (WHERE price_e6 IS NOT NULL) AS open_e6,
    max(price_e6) AS high_e6, min(price_e6) AS low_e6,
    last(price_e6, ts) FILTER (WHERE price_e6 IS NOT NULL) AS close_e6,
    last(yes_bid_e6, ts) AS yes_bid_e6,
    last(yes_ask_e6, ts) AS yes_ask_e6, last(volume_e2, ts) AS volume_e2,
    last(open_interest_e2, ts) AS open_interest_e2, count(*) AS ticks
"""

# name, bucket, source table, columns, refresh start offset, refresh every
AGGREGATES = [
    ("candles_1m", "1 minute", "trades", CANDLE_COLUMNS, "1 day", "1 minute"),
    ("candles_1h", "1 hour", "trades", CANDLE_COLUMNS, "2 days", "15 minutes"),
    ("ticker_1m", "1 minute", "tickers", TICKER_COLUMNS, "1 day", "1 minute"),
    ("ticker_1h", "1 hour", "tickers", TICKER_COLUMNS, "2 days", "15 minutes"),
]
# table or aggregate, drop data older than
RETENTION = [
    ("trades", "30 days"),
    ("tickers", "14 days"),
    ("orderbook_deltas", "14 days"),
    ("orderbook_snapshots", "365 days"),
    ("combo_large_trades", "365 days"),
    ("ticker_1m", "30 days"),
]


def upgrade() -> None:
    for name, bucket, source, columns, start, every in AGGREGATES:
        op.execute(
            f"""
            CREATE MATERIALIZED VIEW {name} WITH (timescaledb.continuous) AS
            SELECT time_bucket('{bucket}', ts) AS bucket, market_id, {columns}
            FROM {source} GROUP BY 1, 2 WITH NO DATA
            """
        )
        op.execute(
            f"SELECT add_continuous_aggregate_policy('{name}', start_offset => INTERVAL '{start}',"
            f" end_offset => INTERVAL '1 minute', schedule_interval => INTERVAL '{every}')"
        )
        op.execute(
            f"ALTER MATERIALIZED VIEW {name} SET (timescaledb.compress = true, "
            "timescaledb.compress_segmentby = 'market_id', "
            "timescaledb.compress_orderby = 'bucket DESC')"
        )
        op.execute(f"SELECT add_compression_policy('{name}', INTERVAL '3 days')")
        shown = ", ".join(
            f"(a.{c}::numeric / 1000000)::numeric(30,6) AS {c[:-3]}"
            for c in ("open_e6", "high_e6", "low_e6", "close_e6")
        )
        rest = (
            "(a.volume_e2::numeric / 100)::numeric(30,2) AS volume, a.trades"
            if source == "trades"
            else "(a.yes_bid_e6::numeric / 1000000)::numeric(30,6) AS yes_bid, "
            "(a.yes_ask_e6::numeric / 1000000)::numeric(30,6) AS yes_ask, "
            "(a.volume_e2::numeric / 100)::numeric(30,2) AS volume, "
            "(a.open_interest_e2::numeric / 100)::numeric(30,2) AS open_interest, a.ticks"
        )
        op.execute(
            f"CREATE VIEW {name}_v AS SELECT a.bucket, m.ticker, a.market_id, {shown}, {rest} "
            f"FROM {name} a JOIN markets m ON m.id = a.market_id"
        )
    for table, keep in RETENTION:
        op.execute(f"SELECT add_retention_policy('{table}', INTERVAL '{keep}')")


def downgrade() -> None:
    for table, _ in RETENTION:
        op.execute(f"SELECT remove_retention_policy('{table}', if_exists => true)")
    for name, *_ in AGGREGATES:
        op.execute(f"DROP VIEW IF EXISTS {name}_v")
        op.execute(f"DROP MATERIALIZED VIEW IF EXISTS {name}")
