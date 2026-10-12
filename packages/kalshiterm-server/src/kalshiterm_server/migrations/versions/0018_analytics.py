"""analytics framework: analysis results and the log of analyzer runs

Revision ID: 0018
Revises: 0017
Create Date: 2026-10-10

``analysis_results`` is one narrow table for every analyzer (decision in PLAN §10.5): a metric
name, a subject (a market id or an event ticker) and ``value_e6``, a ``bigint`` in millionths,
so ratios such as volatility are stored as scaled integers and nothing numeric lives in JSON
(decision 14). It is a hypertable, compressed after 1 day and dropped after 30 days; the
volume depends on the analyzers installed, so re-check both with the first real analyzer (4.2).
``analyzer_runs`` records every run (started, finished, status, rows written, error) and is
what ``status`` reads; the runner keeps the last 7 days.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0018"
down_revision: str | None = "0017"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE analysis_results (
            ts           timestamptz NOT NULL,
            analyzer     text        NOT NULL,
            metric       text        NOT NULL,
            market_id    integer,
            event_ticker text,
            value_e6     bigint      NOT NULL,
            CONSTRAINT analysis_results_one_subject
                CHECK (num_nonnulls(market_id, event_ticker) = 1)
        )
        """
    )
    op.execute(
        "SELECT create_hypertable('analysis_results', 'ts', "
        "chunk_time_interval => interval '1 day', create_default_indexes => false)"
    )
    op.execute(
        "CREATE INDEX analysis_results_market_idx ON analysis_results "
        "(analyzer, metric, market_id, ts DESC) WHERE market_id IS NOT NULL"
    )
    op.execute(
        "CREATE INDEX analysis_results_event_idx ON analysis_results "
        "(analyzer, metric, event_ticker, ts DESC) WHERE event_ticker IS NOT NULL"
    )
    op.execute(
        "ALTER TABLE analysis_results SET (timescaledb.compress, "
        "timescaledb.compress_segmentby = 'analyzer, metric', "
        "timescaledb.compress_orderby = 'ts DESC')"
    )
    op.execute("SELECT add_compression_policy('analysis_results', INTERVAL '1 day')")
    op.execute("SELECT add_retention_policy('analysis_results', INTERVAL '30 days')")
    op.execute(
        """
        CREATE TABLE analyzer_runs (
            id               bigserial   PRIMARY KEY,
            analyzer         text        NOT NULL,
            started_at       timestamptz NOT NULL,
            finished_at      timestamptz,
            status           text        NOT NULL DEFAULT 'running'
                CHECK (status IN ('running', 'ok', 'failed', 'timeout', 'load_failed')),
            results          integer     NOT NULL DEFAULT 0,
            interval_seconds integer     NOT NULL,
            error            text        NOT NULL DEFAULT ''
        )
        """
    )
    op.execute("CREATE INDEX analyzer_runs_idx ON analyzer_runs (analyzer, started_at DESC)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS analyzer_runs")
    op.execute("SELECT remove_retention_policy('analysis_results', if_exists => true)")
    op.execute("SELECT remove_compression_policy('analysis_results', if_exists => true)")
    op.execute("DROP TABLE IF EXISTS analysis_results")
