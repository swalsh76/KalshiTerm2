"""Alert rules and alerts: every route is about the caller's own."""

from decimal import Decimal, InvalidOperation
from typing import Annotated, Any

from fastapi import APIRouter, Path, Query, Request
from kalshi_core.fixedpoint import PrecisionError, to_e6
from pydantic import BaseModel, ConfigDict, Field

from kalshiterm_server import alerts
from kalshiterm_server.analytics.catalog import CATALOG
from kalshiterm_server.api import format as fmt
from kalshiterm_server.api.errors import ApiError
from kalshiterm_server.api.markets import Reader

router = APIRouter(prefix="/v1")
Id = Annotated[int, Path(ge=1, le=2**62)]
DEFAULT_COOLDOWN = 900  # 15 minutes, unless the server's minimum is longer
INT64 = 2**63


class NewRule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    metric: Annotated[str, Field(pattern=r"^[a-z][a-z0-9_]*\.[a-z][a-z0-9_.]*$", max_length=100)]
    ticker: Annotated[str | None, Field(pattern=r"^[A-Za-z0-9._-]{1,128}$")] = None
    event_ticker: Annotated[str | None, Field(pattern=r"^[A-Za-z0-9._-]{1,100}$")] = None
    op: Annotated[str, Field(pattern=r"^(gt|lt|abs_gt)$")]
    threshold: Annotated[str, Field(max_length=40)]
    cooldown_seconds: Annotated[int | None, Field(ge=1, le=7 * 86_400)] = None
    name: Annotated[str, Field(max_length=100)] = ""


class RulePatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool | None = None
    threshold: Annotated[str | None, Field(max_length=40)] = None
    cooldown_seconds: Annotated[int | None, Field(ge=1, le=7 * 86_400)] = None
    name: Annotated[str | None, Field(max_length=100)] = None


def failure(exc: alerts.AlertError) -> ApiError:
    return ApiError(exc.status, exc.code, **exc.extra)


def parse_threshold(text: str) -> int:
    """A decimal string with at most six places, exactly; anything else is a 422."""
    try:
        value = to_e6(Decimal(text))
    except (InvalidOperation, PrecisionError, ValueError):
        value = None
    if value is None or not -INT64 < value < INT64:
        raise ApiError(422, "invalid_parameter", fields=["threshold"])
    return value


def rule_json(rule: alerts.Rule) -> dict[str, Any]:
    metric = CATALOG.get(rule.key)
    body: dict[str, Any] = {
        "id": rule.id,
        "name": rule.name,
        "metric": rule.key,
        "op": rule.op,
        "threshold": fmt.dollars(rule.threshold_e6),  # millionths of the metric's unit
        "unit": metric.unit if metric else None,
        "cooldown_seconds": rule.cooldown_seconds,
        "enabled": rule.enabled,
        "active": rule.active,
        "last_fired_at": fmt.moment(rule.last_fired_at),
        "created_at": fmt.moment(rule.created_at),
    }
    body["ticker" if rule.ticker else "event_ticker"] = rule.ticker or rule.event_ticker
    return body


def alert_json(alert: alerts.Alert) -> dict[str, Any]:
    metric = CATALOG.get(f"{alert.analyzer}.{alert.metric}")
    body: dict[str, Any] = {
        "id": alert.id,
        "rule_id": alert.rule_id,
        "rule_name": alert.rule_name,
        "fired_at": fmt.moment(alert.fired_at),
        "metric": f"{alert.analyzer}.{alert.metric}",
        "op": alert.op,
        "threshold": fmt.dollars(alert.threshold_e6),
        "value": fmt.dollars(alert.value_e6),
        "unit": metric.unit if metric else None,
        "acknowledged_at": fmt.moment(alert.acknowledged_at),
    }
    body["ticker" if alert.ticker else "event_ticker"] = alert.ticker or alert.event_ticker
    return body


@router.get("/alert-metrics")
async def alert_metrics(_: Reader) -> dict[str, Any]:
    """What you can set alerts on. ``threshold`` is in the metric's unit (dollars, contracts or
    seconds); ``subject`` says whether the rule names a market ``ticker`` or an ``event_ticker``."""
    return {
        "metrics": [
            {
                "metric": m.key,
                "subject": m.subject,
                "unit": m.unit,
                "ops": list(m.ops),
                "meaning": m.meaning,
            }
            for m in CATALOG.values()
        ]
    }


@router.post("/alert-rules", status_code=201)
async def create_rule(request: Request, body: NewRule, who: Reader) -> dict[str, Any]:
    """Create a rule. It fires when the condition *becomes* true (not on every check while it
    stays true) and at most once per ``cooldown_seconds``. Market rules need the market on your
    watchlist, because analytics only cover watched markets."""
    config = request.app.state.settings
    try:
        metric = alerts.metric_for(body.metric)
    except alerts.AlertError as exc:
        raise failure(exc) from exc
    subject = body.ticker if metric.subject == "market" else body.event_ticker
    other = body.event_ticker if metric.subject == "market" else body.ticker
    if subject is None or other is not None:
        wanted = "ticker" if metric.subject == "market" else "event_ticker"
        raise ApiError(422, "invalid_parameter", fields=[wanted])
    try:
        rule = await alerts.create_rule(
            request.app.state.engine,
            who.user_id,
            key=body.metric,
            subject=subject,
            op=body.op,
            threshold_e6=parse_threshold(body.threshold),
            cooldown_seconds=body.cooldown_seconds
            or max(DEFAULT_COOLDOWN, config.alert_min_cooldown_seconds),
            name=body.name,
            max_rules=config.alert_rules_max_per_user,
            min_cooldown=config.alert_min_cooldown_seconds,
        )
    except alerts.AlertError as exc:
        raise failure(exc) from exc
    return rule_json(rule)


@router.get("/alert-rules")
async def my_rules(request: Request, who: Reader) -> dict[str, Any]:
    rules = await alerts.list_rules(request.app.state.engine, who.user_id)
    return {
        "rules": [rule_json(r) for r in rules],
        "limit": request.app.state.settings.alert_rules_max_per_user,
    }


@router.get("/alert-rules/{rule_id}")
async def get_rule(request: Request, rule_id: Id, who: Reader) -> dict[str, Any]:
    try:
        return rule_json(await alerts.get_rule(request.app.state.engine, who.user_id, rule_id))
    except alerts.AlertError as exc:
        raise failure(exc) from exc


@router.patch("/alert-rules/{rule_id}")
async def change_rule(
    request: Request, rule_id: Id, body: RulePatch, who: Reader
) -> dict[str, Any]:
    """Enable or disable, or change the threshold, cooldown or name. A new threshold (or
    enabling a rule) re-arms it."""
    changes: dict[str, Any] = {}
    if body.enabled is not None:
        changes["enabled"] = body.enabled
    if body.threshold is not None:
        changes["threshold_e6"] = parse_threshold(body.threshold)
    if body.cooldown_seconds is not None:
        changes["cooldown_seconds"] = body.cooldown_seconds
    if body.name is not None:
        changes["name"] = body.name
    try:
        rule = await alerts.update_rule(
            request.app.state.engine,
            who.user_id,
            rule_id,
            changes,
            request.app.state.settings.alert_min_cooldown_seconds,
        )
    except alerts.AlertError as exc:
        raise failure(exc) from exc
    return rule_json(rule)


@router.delete("/alert-rules/{rule_id}")
async def remove_rule(request: Request, rule_id: Id, who: Reader) -> dict[str, Any]:
    try:
        await alerts.delete_rule(request.app.state.engine, who.user_id, rule_id)
    except alerts.AlertError as exc:
        raise failure(exc) from exc
    return {"id": rule_id, "deleted": True}


@router.get("/alerts")
async def my_alerts(
    request: Request,
    who: Reader,
    unacknowledged: bool = False,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    cursor: Annotated[str | None, Query(max_length=100)] = None,
) -> dict[str, Any]:
    """Your alerts, newest first; page with ``next_cursor``."""
    before: int | None = None
    if cursor is not None:
        decoded = fmt.decode_cursor(cursor)
        if decoded is None or not decoded.isdigit():
            raise ApiError(422, "invalid_parameter", fields=["cursor"])
        before = int(decoded)
    rows = await alerts.list_alerts(
        request.app.state.engine,
        who.user_id,
        unacknowledged=unacknowledged,
        before_id=before,
        limit=limit,
    )
    page = rows[:limit]
    return {
        "alerts": [alert_json(a) for a in page],
        "next_cursor": fmt.encode_cursor(str(page[-1].id)) if len(rows) > limit else None,
    }


@router.post("/alerts/{alert_id}/acknowledge")
async def acknowledge(request: Request, alert_id: Id, who: Reader) -> dict[str, Any]:
    try:
        await alerts.acknowledge(request.app.state.engine, who.user_id, alert_id)
    except alerts.AlertError as exc:
        raise failure(exc) from exc
    return {"id": alert_id, "acknowledged": True}
