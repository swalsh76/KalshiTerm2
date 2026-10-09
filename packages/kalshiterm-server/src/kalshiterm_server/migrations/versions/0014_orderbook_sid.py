"""orderbook rows remember which subscription they came from (``sid``)

Revision ID: 0014
Revises: 0013
Create Date: 2026-10-09

Kalshi orders orderbook messages by ``seq`` within one subscription (``sid``); both restart on
every reconnect. Timestamps are not a safe substitute: live validation found deltas that come
*after* a snapshot in sequence yet carry an *earlier* exchange timestamp, so replaying "deltas
newer than the snapshot" by time silently dropped them. A book is rebuilt correctly from the
latest snapshot plus the deltas of the same ``sid`` with a larger ``seq``.

The column is nullable: rows stored before this migration, and snapshots rebuilt from REST,
have no ``sid`` and fall back to the (less precise) timestamp rule. A constant column
compresses to almost nothing.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0014"
down_revision: str | None = "0013"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("ALTER TABLE orderbook_snapshots ADD COLUMN sid integer")
    op.execute("ALTER TABLE orderbook_deltas ADD COLUMN sid integer")


def downgrade() -> None:
    op.execute("ALTER TABLE orderbook_deltas DROP COLUMN IF EXISTS sid")
    op.execute("ALTER TABLE orderbook_snapshots DROP COLUMN IF EXISTS sid")
