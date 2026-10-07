"""compression: columnar storage for the high-volume hypertables, compressed after 1 day

Revision ID: 0009
Revises: 0008
Create Date: 2026-10-07

Layout (measured on 8 minutes of real data, 2026-10-07): batches ordered by ``market_id, ts
DESC`` with no ``segmentby``. Segmenting by market left only a few dozen rows per batch for
the ticker table and compressed it 2.5x; ordering by market gives 6.8x, and per-market queries
stay sub-millisecond because each batch's min/max ``market_id`` prunes the rest. Orderbook
deltas, trades and the watchlist copy differ by under ~10% between the two layouts. Re-check
with a full day of data in the calibration run (slice 2.10).

Not compressed: ``market_lifecycle`` and ``combo_large_trades`` (a few MB a year).
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0009"
down_revision: str | None = "0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TABLES = ("tickers", "trades", "trades_watchlist", "orderbook_deltas", "orderbook_snapshots")


def upgrade() -> None:
    for table in TABLES:
        op.execute(
            f"ALTER TABLE {table} SET (timescaledb.compress, "
            "timescaledb.compress_segmentby = '', "
            "timescaledb.compress_orderby = 'market_id, ts DESC')"
        )
        op.execute(f"SELECT add_compression_policy('{table}', INTERVAL '1 day')")


def downgrade() -> None:
    for table in TABLES:
        op.execute(f"SELECT remove_compression_policy('{table}', if_exists => true)")
        op.execute(f"SELECT decompress_chunk(c, true) FROM show_chunks('{table}') c")
        op.execute(f"ALTER TABLE {table} SET (timescaledb.compress = false)")
