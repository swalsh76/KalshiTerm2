"""API users and tokens

Revision ID: 0013
Revises: 0012
Create Date: 2026-10-08

Only a SHA-256 hash of each token's random secret is stored (the secret is 256 bits of
randomness, so a fast hash is the right tool, and a database leak yields nothing usable). The
token id is part of the token itself, so a request is checked against exactly one row.
Removing a user removes their tokens (and, from slice 3.6, their watchlists).
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0013"
down_revision: str | None = "0012"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE users (
            id          serial      PRIMARY KEY,
            name        text        NOT NULL UNIQUE CHECK (name ~ '^[a-z][a-z0-9_.-]{0,62}$'),
            created_at  timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        """
        CREATE TABLE api_tokens (
            id            serial      PRIMARY KEY,
            user_id       integer     NOT NULL REFERENCES users (id) ON DELETE CASCADE,
            label         text        NOT NULL DEFAULT '',
            role          text        NOT NULL CHECK (role IN ('read', 'admin')),
            token_hash    bytea       NOT NULL CHECK (octet_length(token_hash) = 32),
            created_at    timestamptz NOT NULL DEFAULT now(),
            expires_at    timestamptz,
            revoked_at    timestamptz,
            last_used_at  timestamptz
        )
        """
    )
    op.execute("CREATE INDEX api_tokens_user_idx ON api_tokens (user_id)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS api_tokens")
    op.execute("DROP TABLE IF EXISTS users")
