"""Turns analyzer results into alerts (slice 4.4).

Registered as an analyzer so it gets the runner's schedule, timeout, lock and ``status`` line,
but unlike the others it writes to ``alerts`` (and updates ``alert_rules``) instead of returning
results. Each run, for every enabled rule: find the latest result of the rule's metric for its
market or event; if it is missing or older than the metric's freshness, the condition counts as
not met. Then (``decide``):

* condition met, rule not active, cooldown over  -> FIRE: store an alert, mark the rule active;
* condition not met, rule active                 -> RE-ARM: mark the rule not active;
* anything else                                  -> nothing.

So a rule fires when its condition *becomes* true, not on every run while it stays true, and
never more often than its cooldown. A condition that is true again right after a lapse waits out
the cooldown. Alerts older than 90 days are deleted.
"""

from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any, Literal

from sqlalchemy import text

from kalshiterm_server.alerts import triggered
from kalshiterm_server.analytics.base import AnalysisContext, Result
from kalshiterm_server.analytics.catalog import CATALOG

CHANNEL = "kterm_alerts"
KEEP_ALERTS = timedelta(days=90)
LOOKBACK = timedelta(seconds=max(m.fresh_seconds for m in CATALOG.values()))

Decision = Literal["fire", "rearm"] | None


def decide(
    *,
    op: str,
    threshold: int,
    value: int | None,
    active: bool,
    last_fired_at: datetime | None,
    cooldown: timedelta,
    now: datetime,
) -> Decision:
    met = value is not None and triggered(op, value, threshold)
    if met and not active:
        if last_fired_at is None or now - last_fired_at >= cooldown:
            return "fire"
        return None  # still cooling down; it fires when the cooldown ends, if still met
    if not met and active:
        return "rearm"
    return None


class AlertEvaluator:
    name = "alert_evaluator"
    interval = timedelta(seconds=30)
    timeout = timedelta(seconds=25)

    # Latest result per rule, found with the analysis_results indexes (one query per subject
    # kind, because each index is partial on the subject column).
    MARKET_RULES = text(
        """
        SELECT r.id, r.user_id, r.name, r.analyzer, r.metric, r.market_id, r.event_ticker, r.op,
               r.threshold_e6, r.cooldown_seconds, r.active, r.last_fired_at, q.ts, q.value_e6
        FROM alert_rules r
        LEFT JOIN LATERAL (
            SELECT ts, value_e6 FROM analysis_results a
            WHERE a.analyzer = r.analyzer AND a.metric = r.metric AND a.market_id = r.market_id
              AND a.ts > :since AND a.ts <= :now
            ORDER BY a.ts DESC LIMIT 1) q ON true
        WHERE r.enabled AND r.market_id IS NOT NULL
        """
    )
    EVENT_RULES = text(
        """
        SELECT r.id, r.user_id, r.name, r.analyzer, r.metric, r.market_id, r.event_ticker, r.op,
               r.threshold_e6, r.cooldown_seconds, r.active, r.last_fired_at, q.ts, q.value_e6
        FROM alert_rules r
        LEFT JOIN LATERAL (
            SELECT ts, value_e6 FROM analysis_results a
            WHERE a.analyzer = r.analyzer AND a.metric = r.metric
              AND a.event_ticker = r.event_ticker AND a.ts > :since AND a.ts <= :now
            ORDER BY a.ts DESC LIMIT 1) q ON true
        WHERE r.enabled AND r.event_ticker IS NOT NULL
        """
    )

    async def run(self, ctx: AnalysisContext) -> Sequence[Result]:
        params = {"since": ctx.now - LOOKBACK, "now": ctx.now}
        async with ctx.engine.connect() as conn:
            rows: list[Any] = []
            for query in (self.MARKET_RULES, self.EVENT_RULES):
                rows += (await conn.execute(query, params)).all()
        fires, rearms = [], []
        for r in rows:
            fresh = CATALOG.get(f"{r.analyzer}.{r.metric}")
            fresh_for = timedelta(seconds=fresh.fresh_seconds) if fresh else timedelta(0)
            value = r.value_e6 if r.ts is not None and ctx.now - r.ts <= fresh_for else None
            what = decide(
                op=r.op,
                threshold=r.threshold_e6,
                value=value,
                active=r.active,
                last_fired_at=r.last_fired_at,
                cooldown=timedelta(seconds=r.cooldown_seconds),
                now=ctx.now,
            )
            if what == "fire":
                fires.append((r, value))
            elif what == "rearm":
                rearms.append(r.id)
        async with ctx.engine.begin() as conn:
            if rearms:
                await conn.execute(
                    text("UPDATE alert_rules SET active = false WHERE id = ANY(:ids)"),
                    {"ids": rearms},
                )
            for r, value in fires:
                claimed = (  # the guard keeps a rule from firing twice if two runs ever overlap
                    await conn.execute(
                        text(
                            "UPDATE alert_rules SET active = true, last_fired_at = :now "
                            "WHERE id = :id AND enabled AND NOT active RETURNING id"
                        ),
                        {"now": ctx.now, "id": r.id},
                    )
                ).first()
                if claimed is None:
                    continue
                alert_id = (
                    await conn.execute(
                        text(
                            "INSERT INTO alerts (rule_id, user_id, fired_at, rule_name, analyzer, "
                            "metric, market_id, event_ticker, op, threshold_e6, value_e6) VALUES "
                            "(:rule, :u, :now, :name, :a, :m, :mk, :ev, :op, :th, :v) RETURNING id"
                        ),
                        {
                            "rule": r.id,
                            "u": r.user_id,
                            "now": ctx.now,
                            "name": r.name,
                            "a": r.analyzer,
                            "m": r.metric,
                            "mk": r.market_id,
                            "ev": r.event_ticker,
                            "op": r.op,
                            "th": r.threshold_e6,
                            "v": value,
                        },
                    )  # fmt: skip
                ).scalar_one()
                await conn.execute(
                    text("SELECT pg_notify(:c, :p)"), {"c": CHANNEL, "p": f"{r.user_id}:{alert_id}"}
                )
            await conn.execute(
                text("DELETE FROM alerts WHERE fired_at < :cutoff"),
                {"cutoff": ctx.now - KEEP_ALERTS},
            )
        return []
