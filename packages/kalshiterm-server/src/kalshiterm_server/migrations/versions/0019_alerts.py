"""per-user alert rules and the alerts they raise

Revision ID: 0019
Revises: 0018
Create Date: 2026-10-12

A rule says "when <analyzer>.<metric> of this market or event is above / below / further from
zero than a threshold, tell me". ``active`` is the edge-trigger state (the condition held when
the rule was last evaluated), ``last_fired_at`` backs the cooldown. An alert keeps a copy of
what fired it (metric, subject, threshold, value), so it stays meaningful after the rule is
edited or deleted; ``rule_id`` only goes NULL then. Alerts older than 90 days are pruned by the
evaluator.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0019"
down_revision: str | None = "0018"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE alert_rules (
            id               bigserial   PRIMARY KEY,
            user_id          integer     NOT NULL REFERENCES users (id) ON DELETE CASCADE,
            name             text        NOT NULL DEFAULT '' CHECK (length(name) <= 100),
            analyzer         text        NOT NULL,
            metric           text        NOT NULL,
            market_id        integer     REFERENCES markets (id),
            event_ticker     text,
            op               text        NOT NULL CHECK (op IN ('gt', 'lt', 'abs_gt')),
            threshold_e6     bigint      NOT NULL,
            cooldown_seconds integer     NOT NULL CHECK (cooldown_seconds >= 60),
            enabled          boolean     NOT NULL DEFAULT true,
            active           boolean     NOT NULL DEFAULT false,
            last_fired_at    timestamptz,
            created_at       timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT alert_rules_one_subject CHECK (num_nonnulls(market_id, event_ticker) = 1)
        )
        """
    )
    op.execute("CREATE INDEX alert_rules_user_idx ON alert_rules (user_id, id)")
    op.execute("CREATE INDEX alert_rules_enabled_idx ON alert_rules (id) WHERE enabled")
    op.execute(
        """
        CREATE TABLE alerts (
            id              bigserial   PRIMARY KEY,
            rule_id         bigint      REFERENCES alert_rules (id) ON DELETE SET NULL,
            user_id         integer     NOT NULL REFERENCES users (id) ON DELETE CASCADE,
            fired_at        timestamptz NOT NULL,
            rule_name       text        NOT NULL DEFAULT '',
            analyzer        text        NOT NULL,
            metric          text        NOT NULL,
            market_id       integer     REFERENCES markets (id),
            event_ticker    text,
            op              text        NOT NULL,
            threshold_e6    bigint      NOT NULL,
            value_e6        bigint      NOT NULL,
            acknowledged_at timestamptz
        )
        """
    )
    op.execute("CREATE INDEX alerts_user_idx ON alerts (user_id, id DESC)")
    op.execute(
        "CREATE INDEX alerts_unacknowledged_idx ON alerts (user_id, id DESC) "
        "WHERE acknowledged_at IS NULL"
    )
    op.execute("CREATE INDEX alerts_fired_idx ON alerts (fired_at)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS alerts")
    op.execute("DROP TABLE IF EXISTS alert_rules")
