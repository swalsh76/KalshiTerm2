"""per-user named UI layouts

Revision ID: 0020
Revises: 0019
Create Date: 2026-10-14

A layout is the browser UI's panel arrangement, saved by name. The server does not interpret it:
``data`` is a JSON object the front end defines (and versions itself). Layouts belong to a user and
go with the user (``ON DELETE CASCADE``). The size and count limits are enforced by the API; the
checks here are a backstop so a bug cannot store something absurd.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0020"
down_revision: str | None = "0019"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE ui_layouts (
            user_id    integer     NOT NULL REFERENCES users (id) ON DELETE CASCADE,
            name       text        NOT NULL CHECK (length(name) BETWEEN 1 AND 64),
            data       jsonb       NOT NULL CHECK (jsonb_typeof(data) = 'object'),
            size_bytes integer     NOT NULL CHECK (size_bytes BETWEEN 2 AND 1048576),
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (user_id, name)
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS ui_layouts")
