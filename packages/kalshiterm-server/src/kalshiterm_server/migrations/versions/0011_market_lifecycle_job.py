"""settled-market slimming and tombstoning (decision 15) as a scheduled job, plus pins

Revision ID: 0011
Revises: 0010
Create Date: 2026-10-08

``expire_markets`` runs daily as a Timescale job (so it needs no server process and shows up
beside the other policies). Ages count from ``settlement_ts``; unsettled markets never expire.

* settled > 30 days: rules text and sub-titles are blanked (every market, watched or not);
* settled > 90 days and neither ever watched nor pinned: reduced to a *tombstone* (id, ticker,
  event ticker, type, status, result, settlement value and time). The row is kept, not deleted,
  because candles and aggregates are kept forever and refer to the market by id: deleting the
  row would orphan them and let a reappearing ticker get a second id (decided 2026-10-08);
* an event is deleted once it has markets and every one of them is expired.

The job is predicate-driven, not flag-driven: discovery may rewrite a reduced row if Kalshi
updates it, and the next run simply reduces it again. Windows live in the job config
(``alter_job(..., config => ...)``). ``pins`` holds markets kept past day 90 on request; how
users pin arrives with the Phase 3 API.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0011"
down_revision: str | None = "0010"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# A market past ``delete_after`` that nobody asked to keep.
EXPIRED = """
    m.settlement_ts < now() - delete_after
    AND NOT EXISTS (SELECT 1 FROM watchlist_periods p WHERE p.market_id = m.id)
    AND NOT EXISTS (SELECT 1 FROM pins x WHERE x.market_id = m.id)
"""
# Something is still stored beyond the tombstone fields.
HAS_DETAIL = """
    m.created_time IS NOT NULL OR m.updated_time IS NOT NULL OR m.open_time IS NOT NULL
    OR m.close_time IS NOT NULL OR m.latest_expiration_time IS NOT NULL
    OR m.strike_type IS NOT NULL OR m.floor_strike_e6 IS NOT NULL
    OR m.cap_strike_e6 IS NOT NULL OR m.exchange_index IS NOT NULL
    OR m.yes_sub_title <> '' OR m.no_sub_title <> ''
    OR m.rules_primary <> '' OR m.rules_secondary <> ''
"""

PROCEDURE = f"""
CREATE PROCEDURE expire_markets(job_id integer, config jsonb)
LANGUAGE plpgsql AS $$
DECLARE
    slim_after interval := COALESCE((config->>'slim_after')::interval, interval '30 days');
    delete_after interval := COALESCE((config->>'delete_after')::interval, interval '90 days');
    slimmed bigint;
    tombstoned bigint;
    events_gone bigint;
BEGIN
    UPDATE markets m
    SET yes_sub_title = '', no_sub_title = '', rules_primary = '', rules_secondary = '',
        updated_at = now()
    WHERE m.settlement_ts < now() - slim_after
      AND (m.yes_sub_title <> '' OR m.no_sub_title <> ''
           OR m.rules_primary <> '' OR m.rules_secondary <> '');
    GET DIAGNOSTICS slimmed = ROW_COUNT;

    UPDATE markets m
    SET yes_sub_title = '', no_sub_title = '', rules_primary = '', rules_secondary = '',
        created_time = NULL, updated_time = NULL, open_time = NULL, close_time = NULL,
        latest_expiration_time = NULL, strike_type = NULL, floor_strike_e6 = NULL,
        cap_strike_e6 = NULL, exchange_index = NULL, updated_at = now()
    WHERE ({EXPIRED}) AND ({HAS_DETAIL});
    GET DIAGNOSTICS tombstoned = ROW_COUNT;

    DELETE FROM events e
    WHERE EXISTS (SELECT 1 FROM markets m WHERE m.event_ticker = e.event_ticker)
      AND NOT EXISTS (
          SELECT 1 FROM markets m
          WHERE m.event_ticker = e.event_ticker AND NOT COALESCE(({EXPIRED}), false)
      );
    GET DIAGNOSTICS events_gone = ROW_COUNT;

    RAISE LOG 'expire_markets: % slimmed, % tombstoned, % events deleted',
        slimmed, tombstoned, events_gone;
END $$
"""


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE pins (
            market_id  integer     PRIMARY KEY REFERENCES markets (id),
            pinned_at  timestamptz NOT NULL DEFAULT now(),
            note       text        NOT NULL DEFAULT ''
        )
        """
    )
    op.execute(PROCEDURE)
    op.execute(
        "SELECT add_job('expire_markets', INTERVAL '1 day', "
        """config => '{"slim_after": "30 days", "delete_after": "90 days"}')"""
    )


def downgrade() -> None:
    op.execute(
        "SELECT delete_job(job_id) FROM timescaledb_information.jobs "
        "WHERE proc_name = 'expire_markets'"
    )
    op.execute("DROP PROCEDURE IF EXISTS expire_markets(integer, jsonb)")
    op.execute("DROP TABLE IF EXISTS pins")
