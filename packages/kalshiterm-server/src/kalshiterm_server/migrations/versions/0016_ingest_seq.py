"""a write-order cursor for live push: ``ingest_seq`` and a committed watermark

Revision ID: 0016
Revises: 0015
Create Date: 2026-10-09

``received_at`` cannot be a cursor: measured on live data, 40-69% of rows were written after a
row with a later ``received_at`` (up to 22 s with backfill, ~3 s on live streams), because
messages arrive in bursts that are stamped once and interleave. A client that remembered "I have
everything up to X" would silently miss rows written later with a smaller stamp.

Instead every row of the four pushed streams (trades, tickers, orderbook snapshots, orderbook
deltas) takes the next value of ONE sequence as its default. There is a single writer and a batch
commits atomically, so sequence order is commit order. ``ingest_progress.last_seq`` is advanced
in the same transaction as each batch, so a reader that only trusts values up to it can never
skip a row that has not committed yet. The column is NULL for rows stored before this migration.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0016"
down_revision: str | None = "0015"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TABLES = ("trades", "tickers", "orderbook_snapshots", "orderbook_deltas")


def upgrade() -> None:
    op.execute("CREATE SEQUENCE ingest_seq")
    for table in TABLES:
        op.execute(f"ALTER TABLE {table} ADD COLUMN ingest_seq bigint")  # NULL for old rows
        op.execute(f"ALTER TABLE {table} ALTER COLUMN ingest_seq SET DEFAULT nextval('ingest_seq')")
        op.execute(f"CREATE INDEX {table}_seq_idx ON {table} (market_id, ingest_seq)")
    op.execute(
        """
        CREATE TABLE ingest_progress (
            id          boolean     PRIMARY KEY DEFAULT true CHECK (id),
            last_seq    bigint      NOT NULL DEFAULT 0,
            updated_at  timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("INSERT INTO ingest_progress (id) VALUES (true)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS ingest_progress")
    for table in TABLES:
        op.execute(f"DROP INDEX IF EXISTS {table}_seq_idx")
        op.execute(f"ALTER TABLE {table} DROP COLUMN IF EXISTS ingest_seq")
    op.execute("DROP SEQUENCE IF EXISTS ingest_seq")
