"""reference tables: series, events, markets, discovery_state

Revision ID: 0002
Revises: 0001
Create Date: 2026-10-07

Numbers are bigint fixed-point (see kalshiterm_server.fixedpoint): ``*_e6`` = millionths.
No foreign keys on purpose: discovery fetches series, events and markets independently, in any
order, and a strict reference could block ingestion.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE series (
            ticker             text PRIMARY KEY,
            title              text NOT NULL,
            frequency          text NOT NULL,
            category           text NOT NULL,
            tags               text[],
            fee_type           text,
            fee_multiplier_e6  bigint,
            source_updated_at  timestamptz,
            first_seen_at      timestamptz NOT NULL DEFAULT now(),
            updated_at         timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        """
        CREATE TABLE events (
            event_ticker       text PRIMARY KEY,
            series_ticker      text NOT NULL,
            title              text NOT NULL,
            sub_title          text NOT NULL DEFAULT '',
            mutually_exclusive boolean NOT NULL,
            strike_date        timestamptz,
            strike_period      text,
            source_updated_at  timestamptz,
            first_seen_at      timestamptz NOT NULL DEFAULT now(),
            updated_at         timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("CREATE INDEX events_series_ticker_idx ON events (series_ticker)")
    op.execute(
        """
        CREATE TABLE markets (
            id                      integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            ticker                  text NOT NULL UNIQUE,
            event_ticker            text NOT NULL,
            market_type             text NOT NULL,
            status                  text NOT NULL,
            yes_sub_title           text NOT NULL DEFAULT '',
            no_sub_title            text NOT NULL DEFAULT '',
            created_time            timestamptz,
            updated_time            timestamptz,
            open_time               timestamptz,
            close_time              timestamptz,
            latest_expiration_time  timestamptz,
            result                  text NOT NULL DEFAULT '',
            settlement_value_e6     bigint,
            settlement_ts           timestamptz,
            rules_primary           text NOT NULL DEFAULT '',
            rules_secondary         text NOT NULL DEFAULT '',
            strike_type             text,
            floor_strike_e6         bigint,
            cap_strike_e6           bigint,
            exchange_index          integer,
            first_seen_at           timestamptz NOT NULL DEFAULT now(),
            updated_at              timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    op.execute("CREATE INDEX markets_event_ticker_idx ON markets (event_ticker)")
    op.execute("CREATE INDEX markets_status_idx ON markets (status)")
    op.execute(
        """
        CREATE TABLE discovery_state (
            key         text PRIMARY KEY,
            value       text NOT NULL,
            updated_at  timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        """
        CREATE VIEW series_v AS
        SELECT s.*, (s.fee_multiplier_e6::numeric / 1000000)::numeric(30,6) AS fee_multiplier
        FROM series s
        """
    )
    op.execute(
        """
        CREATE VIEW markets_v AS
        SELECT m.*,
               (m.settlement_value_e6::numeric / 1000000)::numeric(30,6) AS settlement_value,
               (m.floor_strike_e6::numeric / 1000000)::numeric(30,6)     AS floor_strike,
               (m.cap_strike_e6::numeric / 1000000)::numeric(30,6)       AS cap_strike
        FROM markets m
        """
    )


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS markets_v")
    op.execute("DROP VIEW IF EXISTS series_v")
    op.execute("DROP TABLE IF EXISTS discovery_state")
    op.execute("DROP TABLE IF EXISTS markets")
    op.execute("DROP TABLE IF EXISTS events")
    op.execute("DROP TABLE IF EXISTS series")
