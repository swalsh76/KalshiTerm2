"""Per-user alert rules and the alerts they raise (slice 4.4).

Rules are checked against the latest analyzer results by the ``alert_evaluator`` analyzer; this
module is the part both it and the API use: the trigger test, rule creation with its limits, and
listing / acknowledging alerts. Everything is scoped to one user and a rule or alert that belongs
to someone else is simply "not found".
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from kalshiterm_server.analytics.catalog import CATALOG, AlertMetric

RULE_LOCK = 8_001  # advisory lock (with the user id) serialising a user's rule creation
COMBO_PREFIX = "KXMVE"
OPS = ("gt", "lt", "abs_gt")


class AlertError(Exception):
    def __init__(self, status: int, code: str, **extra: Any) -> None:
        super().__init__(code)
        self.status = status
        self.code = code
        self.extra = extra


def triggered(op: str, value: int, threshold: int) -> bool:
    """Is the condition met? ``abs_gt``: further from zero than the threshold, either way."""
    if op == "gt":
        return value > threshold
    if op == "lt":
        return value < threshold
    if op == "abs_gt":
        return abs(value) > threshold
    raise ValueError(f"unknown comparison {op!r}")


def metric_for(key: str) -> AlertMetric:
    metric = CATALOG.get(key)
    if metric is None:
        raise AlertError(422, "unknown_metric", metric=key)
    return metric


@dataclass(frozen=True, slots=True)
class Rule:
    id: int
    name: str
    analyzer: str
    metric: str
    ticker: str | None
    event_ticker: str | None
    op: str
    threshold_e6: int
    cooldown_seconds: int
    enabled: bool
    active: bool
    last_fired_at: datetime | None
    created_at: datetime

    @property
    def key(self) -> str:
        return f"{self.analyzer}.{self.metric}"


RULE_SELECT = (
    "SELECT r.id, r.name, r.analyzer, r.metric, m.ticker, r.event_ticker, r.op, "
    "r.threshold_e6, r.cooldown_seconds, r.enabled, r.active, r.last_fired_at, r.created_at "
    "FROM alert_rules r LEFT JOIN markets m ON m.id = r.market_id "
)


async def create_rule(
    engine: AsyncEngine,
    user_id: int,
    *,
    key: str,
    subject: str,
    op: str,
    threshold_e6: int,
    cooldown_seconds: int,
    name: str,
    max_rules: int,
    min_cooldown: int,
) -> Rule:
    """Validate and store a rule. ``subject`` is a market ticker or an event ticker, whichever
    the metric is about."""
    metric = metric_for(key)
    if op not in metric.ops:
        raise AlertError(422, "invalid_comparison", metric=key, allowed=list(metric.ops))
    if op == "abs_gt" and threshold_e6 <= 0:
        raise AlertError(422, "invalid_parameter", fields=["threshold"])
    if cooldown_seconds < min_cooldown:
        raise AlertError(422, "cooldown_too_short", minimum=min_cooldown)
    async with engine.begin() as conn:
        await conn.execute(
            text("SELECT pg_advisory_xact_lock(:k, :u)"), {"k": RULE_LOCK, "u": user_id}
        )
        market_id: int | None = None
        event_ticker: str | None = None
        if metric.subject == "market":
            market = (
                await conn.execute(
                    text(
                        "SELECT id, settlement_ts IS NOT NULL AS settled "
                        "FROM markets WHERE ticker = :t"
                    ),
                    {"t": subject},
                )
            ).first()
            if market is None:
                raise AlertError(404, "not_found")
            if subject.startswith(COMBO_PREFIX):
                raise AlertError(422, "not_alertable", reason="combo_market")
            if market.settled:
                raise AlertError(422, "not_alertable", reason="settled")
            watched = (
                await conn.execute(
                    text("SELECT 1 FROM user_watchlists WHERE user_id = :u AND market_id = :m"),
                    {"u": user_id, "m": market.id},
                )
            ).first()
            if watched is None:  # analytics only cover watched markets
                raise AlertError(422, "not_watched", ticker=subject)
            market_id = market.id
        else:
            event = (
                await conn.execute(
                    text("SELECT mutually_exclusive FROM events WHERE event_ticker = :e"),
                    {"e": subject},
                )
            ).first()
            if event is None:
                raise AlertError(404, "not_found")
            if not event.mutually_exclusive:
                raise AlertError(422, "not_alertable", reason="not_mutually_exclusive")
            event_ticker = subject
        count = (
            await conn.execute(
                text("SELECT count(*) FROM alert_rules WHERE user_id = :u"), {"u": user_id}
            )
        ).scalar_one()
        if count >= max_rules:
            raise AlertError(409, "rule_limit", limit=max_rules)
        rule_id = (
            await conn.execute(
                text(
                    "INSERT INTO alert_rules (user_id, name, analyzer, metric, market_id, "
                    "event_ticker, op, threshold_e6, cooldown_seconds) VALUES "
                    "(:u, :n, :a, :m, :mk, :ev, :op, :th, :cd) RETURNING id"
                ),
                {
                    "u": user_id,
                    "n": name,
                    "a": metric.analyzer,
                    "m": metric.metric,
                    "mk": market_id,
                    "ev": event_ticker,
                    "op": op,
                    "th": threshold_e6,
                    "cd": cooldown_seconds,
                },
            )  # fmt: skip
        ).scalar_one()
    return await get_rule(engine, user_id, rule_id)


async def get_rule(engine: AsyncEngine, user_id: int, rule_id: int) -> Rule:
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text(RULE_SELECT + "WHERE r.id = :id AND r.user_id = :u"),
                {"id": rule_id, "u": user_id},
            )
        ).first()
    if row is None:
        raise AlertError(404, "not_found")
    return Rule(*row)


async def list_rules(engine: AsyncEngine, user_id: int) -> list[Rule]:
    async with engine.connect() as conn:
        rows = await conn.execute(
            text(RULE_SELECT + "WHERE r.user_id = :u ORDER BY r.id"), {"u": user_id}
        )
        return [Rule(*r) for r in rows]


async def update_rule(
    engine: AsyncEngine,
    user_id: int,
    rule_id: int,
    changes: dict[str, Any],
    min_cooldown: int,
) -> Rule:
    """Change ``enabled``, ``threshold_e6``, ``cooldown_seconds`` or ``name``. Changing the
    threshold or enabling a rule re-arms it, so it can fire against the new condition."""
    rule = await get_rule(engine, user_id, rule_id)
    if "cooldown_seconds" in changes and changes["cooldown_seconds"] < min_cooldown:
        raise AlertError(422, "cooldown_too_short", minimum=min_cooldown)
    threshold = changes.get("threshold_e6", rule.threshold_e6)
    if rule.op == "abs_gt" and threshold <= 0:
        raise AlertError(422, "invalid_parameter", fields=["threshold"])
    rearm = ("threshold_e6" in changes and changes["threshold_e6"] != rule.threshold_e6) or (
        changes.get("enabled") is True and not rule.enabled
    )
    sets = [f"{column} = :{column}" for column in changes]
    if rearm:
        sets.append("active = false")
    if sets:
        async with engine.begin() as conn:
            await conn.execute(
                text(f"UPDATE alert_rules SET {', '.join(sets)} WHERE id = :id AND user_id = :u"),
                {**changes, "id": rule_id, "u": user_id},
            )
    return await get_rule(engine, user_id, rule_id)


async def delete_rule(engine: AsyncEngine, user_id: int, rule_id: int) -> None:
    async with engine.begin() as conn:
        deleted = (
            await conn.execute(
                text("DELETE FROM alert_rules WHERE id = :id AND user_id = :u RETURNING id"),
                {"id": rule_id, "u": user_id},
            )
        ).first()
    if deleted is None:
        raise AlertError(404, "not_found")


@dataclass(frozen=True, slots=True)
class Alert:
    id: int
    rule_id: int | None
    rule_name: str
    fired_at: datetime
    analyzer: str
    metric: str
    ticker: str | None
    event_ticker: str | None
    op: str
    threshold_e6: int
    value_e6: int
    acknowledged_at: datetime | None


async def list_alerts(
    engine: AsyncEngine,
    user_id: int,
    *,
    unacknowledged: bool,
    before_id: int | None,
    limit: int,
) -> list[Alert]:
    """Newest first. Returns up to ``limit + 1`` rows so the caller can tell if there is more."""
    where = ["a.user_id = :u"]
    params: dict[str, Any] = {"u": user_id, "n": limit + 1}
    if unacknowledged:
        where.append("a.acknowledged_at IS NULL")
    if before_id is not None:
        where.append("a.id < :before")
        params["before"] = before_id
    async with engine.connect() as conn:
        rows = await conn.execute(
            text(
                "SELECT a.id, a.rule_id, a.rule_name, a.fired_at, a.analyzer, a.metric, m.ticker, "
                "a.event_ticker, a.op, a.threshold_e6, a.value_e6, a.acknowledged_at "
                "FROM alerts a LEFT JOIN markets m ON m.id = a.market_id "
                f"WHERE {' AND '.join(where)} ORDER BY a.id DESC LIMIT :n"
            ),
            params,
        )
        return [Alert(*r) for r in rows]


async def acknowledge(engine: AsyncEngine, user_id: int, alert_id: int) -> None:
    """Idempotent: acknowledging twice keeps the first time."""
    async with engine.begin() as conn:
        found = (
            await conn.execute(
                text(
                    "UPDATE alerts SET acknowledged_at = coalesce(acknowledged_at, now()) "
                    "WHERE id = :id AND user_id = :u RETURNING id"
                ),
                {"id": alert_id, "u": user_id},
            )
        ).first()
    if found is None:
        raise AlertError(404, "not_found")
