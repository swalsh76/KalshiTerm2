"""combos reduced to universe counters plus a large-trade log (replaces per-combo rows)

Revision ID: 0005
Revises: 0004
Create Date: 2026-10-07

Measured (decision 17): combos are ~14 % of taker dollar value, their outcomes are fully
determined by their legs (345/345 settled combos matched "YES iff every leg matched its side"),
and 6 % of traded combos hold 73 % of the dollars. Per-combo rows cost ~436 B each for ~1 M
combos/day, so ``combo_markets``/``combo_tickers``/``combo_trades`` are dropped (they only ever
held dev data). Kept: ``combo_stats_1m`` (now with taker dollar value) and a log of individual
combo trades above a dollar threshold.
"""

from collections.abc import Sequence
from importlib import import_module

from alembic import op

revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("DROP VIEW IF EXISTS combo_trades_v")
    op.execute("DROP VIEW IF EXISTS combo_markets_v")
    op.execute("DROP TABLE IF EXISTS combo_trades")
    op.execute("DROP TABLE IF EXISTS combo_tickers")
    op.execute("DROP TABLE IF EXISTS combo_markets")
    op.execute("ALTER TABLE combo_stats_1m ADD COLUMN notional_e6 bigint NOT NULL DEFAULT 0")
    op.execute(
        """
        CREATE TABLE combo_large_trades (
            ts            timestamptz NOT NULL,
            received_at   timestamptz NOT NULL,
            ticker        text        NOT NULL,
            trade_id      uuid        NOT NULL,
            yes_price_e6  bigint      NOT NULL,
            count_e2      bigint      NOT NULL,
            taker_side    text,
            notional_e6   bigint      NOT NULL
        )
        """
    )
    op.execute(
        "SELECT create_hypertable('combo_large_trades', 'ts', "
        "chunk_time_interval => interval '7 days')"
    )
    op.execute(
        """
        CREATE VIEW combo_large_trades_v AS
        SELECT ts, received_at, ticker, trade_id,
               (yes_price_e6::numeric / 1000000)::numeric(30,6) AS yes_price,
               (count_e2::numeric / 100)::numeric(30,2) AS count,
               taker_side,
               (notional_e6::numeric / 1000000)::numeric(30,2) AS notional
        FROM combo_large_trades
        """
    )


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS combo_large_trades_v")
    op.execute("DROP TABLE IF EXISTS combo_large_trades")
    op.execute("ALTER TABLE combo_stats_1m DROP COLUMN IF EXISTS notional_e6")
    import_module(
        "kalshiterm_server.migrations.versions.0004_combo_markets"
    ).create_per_combo_tables()
