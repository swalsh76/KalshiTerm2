"""orderbook storage: snapshots (full-depth, bigint arrays) and deltas

Revision ID: 0006
Revises: 0005
Create Date: 2026-10-07

Snapshots are checkpoints to replay deltas from: best price first, sizes in the same order.
``seq`` is the subscription's sequence number (it restarts on reconnect, so order by
``ts, seq``); it is NULL for a REST-built snapshot, which is also flagged ``approximate``.
Measured on 50 sports-heavy markets: ~455 B per compressed snapshot, ~53 B per compressed
delta; deep crypto books are ~20x larger, so watchlist composition dominates the cost.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE orderbook_snapshots (
            ts             timestamptz NOT NULL,
            received_at    timestamptz NOT NULL,
            market_id      integer     NOT NULL,
            seq            bigint,
            approximate    boolean     NOT NULL DEFAULT false,
            yes_prices_e6  bigint[]    NOT NULL,
            yes_sizes_e2   bigint[]    NOT NULL,
            no_prices_e6   bigint[]    NOT NULL,
            no_sizes_e2    bigint[]    NOT NULL
        )
        """
    )
    op.execute(
        """
        CREATE TABLE orderbook_deltas (
            ts           timestamptz NOT NULL,
            received_at  timestamptz NOT NULL,
            market_id    integer     NOT NULL,
            seq          bigint      NOT NULL,
            is_yes       boolean     NOT NULL,
            price_e6     bigint      NOT NULL,
            delta_e2     bigint      NOT NULL
        )
        """
    )
    for table in ("orderbook_snapshots", "orderbook_deltas"):
        op.execute(
            f"SELECT create_hypertable('{table}', 'ts', chunk_time_interval => interval '1 day', "
            "create_default_indexes => false)"
        )
        op.execute(f"CREATE INDEX {table}_market_ts_idx ON {table} (market_id, ts DESC)")
    op.execute(
        """
        CREATE VIEW orderbook_snapshots_v AS
        SELECT s.ts, s.received_at, m.ticker, s.market_id, s.seq, s.approximate,
               (s.yes_prices_e6[1]::numeric / 1000000)::numeric(30,6) AS best_yes_bid,
               (s.no_prices_e6[1]::numeric / 1000000)::numeric(30,6) AS best_no_bid,
               cardinality(s.yes_prices_e6) AS yes_levels,
               cardinality(s.no_prices_e6) AS no_levels
        FROM orderbook_snapshots s JOIN markets m ON m.id = s.market_id
        """
    )
    op.execute(
        """
        CREATE VIEW orderbook_deltas_v AS
        SELECT d.ts, d.received_at, m.ticker, d.market_id, d.seq,
               CASE WHEN d.is_yes THEN 'yes' ELSE 'no' END AS side,
               (d.price_e6::numeric / 1000000)::numeric(30,6) AS price,
               (d.delta_e2::numeric / 100)::numeric(30,2) AS delta
        FROM orderbook_deltas d JOIN markets m ON m.id = d.market_id
        """
    )


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS orderbook_deltas_v")
    op.execute("DROP VIEW IF EXISTS orderbook_snapshots_v")
    op.execute("DROP TABLE IF EXISTS orderbook_deltas")
    op.execute("DROP TABLE IF EXISTS orderbook_snapshots")
