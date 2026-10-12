"""Analyzer results over REST: the latest values for a market or event, a metric's history, and
the current arbitrage opportunities. Read-only and shared by all users (results are market data,
unlike alerts)."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Annotated, Any

from fastapi import APIRouter, Path, Query, Request
from sqlalchemy import text

from kalshiterm_server.analytics.catalog import CATALOG
from kalshiterm_server.api import format as fmt
from kalshiterm_server.api.errors import ApiError
from kalshiterm_server.api.markets import Reader
from kalshiterm_server.fixedpoint import PrecisionError, to_e6

router = APIRouter(prefix="/v1/analytics")
Ticker = Annotated[str, Path(pattern=r"^[A-Za-z0-9._-]{1,128}$")]
MetricKey = Annotated[str, Query(pattern=r"^[a-z][a-z0-9_-]*\.[a-z][a-z0-9_.]*$", max_length=100)]
ARBITRAGE = "arbitrage_bid"
DEFAULT_AGE = 3600
MAX_AGE = 86_400


def unit_of(analyzer: str, metric: str) -> str | None:
    known = CATALOG.get(f"{analyzer}.{metric}")
    return known.unit if known else None


def value_json(
    analyzer: str, metric: str, ts: datetime, value: int, now: datetime
) -> dict[str, Any]:
    return {
        "metric": f"{analyzer}.{metric}",
        "value": fmt.dollars(value),  # millionths of the unit below
        "unit": unit_of(analyzer, metric),
        "time": fmt.moment(ts),
        "age_seconds": max(0, int((now - ts).total_seconds())),
    }


def now_of(request: Request) -> datetime:
    moment: datetime = request.app.state.clock()
    return moment


@router.get("/markets/{ticker}")
async def market_results(
    request: Request,
    ticker: Ticker,
    _: Reader,
    max_age_seconds: Annotated[int, Query(ge=1, le=MAX_AGE)] = DEFAULT_AGE,
) -> dict[str, Any]:
    """The newest value of every metric computed for this market within ``max_age_seconds``.
    Analytics only cover watched markets, so an unwatched market has an empty list."""
    now = now_of(request)
    async with request.app.state.engine.connect() as conn:
        market = (
            await conn.execute(text("SELECT id FROM markets WHERE ticker = :t"), {"t": ticker})
        ).first()
        if market is None:
            raise ApiError(404, "not_found")
        rows = await conn.execute(
            text(
                "SELECT DISTINCT ON (analyzer, metric) analyzer, metric, ts, value_e6 "
                "FROM analysis_results WHERE market_id = :m AND ts > :since AND ts <= :now "
                "ORDER BY analyzer, metric, ts DESC"
            ),
            {"m": market.id, "since": now - timedelta(seconds=max_age_seconds), "now": now},
        )
        results = [value_json(r.analyzer, r.metric, r.ts, r.value_e6, now) for r in rows]
    return {"ticker": ticker, "as_of": fmt.moment(now), "results": results}


@router.get("/markets/{ticker}/history")
async def market_history(
    request: Request,
    ticker: Ticker,
    _: Reader,
    metric: MetricKey,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 200,
    cursor: Annotated[str | None, Query(max_length=100)] = None,
) -> dict[str, Any]:
    """One metric over time, newest first (default: the last 24 hours); page with
    ``next_cursor``."""
    now = now_of(request)
    for name, value in (("since", since), ("until", until)):
        if value is not None and value.tzinfo is None:
            raise ApiError(422, "invalid_parameter", fields=[name])
    lower = since or now - timedelta(hours=24)
    upper = until or now
    if lower >= upper:
        raise ApiError(422, "invalid_parameter", fields=["since", "until"])
    if cursor is not None:
        decoded = fmt.decode_cursor(cursor)
        if decoded is None or not decoded.lstrip("-").isdigit():
            raise ApiError(422, "invalid_parameter", fields=["cursor"])
        upper = min(upper, datetime.fromtimestamp(0, UTC) + timedelta(microseconds=int(decoded)))
    analyzer, name = metric.split(".", 1)
    async with request.app.state.engine.connect() as conn:
        market = (
            await conn.execute(text("SELECT id FROM markets WHERE ticker = :t"), {"t": ticker})
        ).first()
        if market is None:
            raise ApiError(404, "not_found")
        found = (
            await conn.execute(
                text(
                    "SELECT ts, value_e6 FROM analysis_results WHERE analyzer = :a AND metric = :m "
                    "AND market_id = :id AND ts >= :lower AND ts < :upper "
                    "ORDER BY ts DESC LIMIT :n"
                ),
                {
                    "a": analyzer,
                    "m": name,
                    "id": market.id,
                    "lower": lower,
                    "upper": upper,
                    "n": limit + 1,
                },
            )  # fmt: skip
        ).all()
    page = found[:limit]
    last = page[-1].ts if page else None
    epoch = datetime.fromtimestamp(0, UTC)
    return {
        "ticker": ticker,
        "metric": metric,
        "unit": unit_of(analyzer, name),
        "points": [{"time": fmt.moment(r.ts), "value": fmt.dollars(r.value_e6)} for r in page],
        "next_cursor": (
            fmt.encode_cursor(str((last - epoch) // timedelta(microseconds=1)))
            if len(found) > limit and last
            else None
        ),
    }


@router.get("/events/{event_ticker}")
async def event_results(
    request: Request,
    event_ticker: Ticker,
    _: Reader,
    max_age_seconds: Annotated[int, Query(ge=1, le=MAX_AGE)] = DEFAULT_AGE,
) -> dict[str, Any]:
    """The newest value of every event-level metric (today: the arbitrage check) within
    ``max_age_seconds``. The arbitrage analyzer stores a result only while an event has a
    positive gross edge, so an empty list means no opportunity right now."""
    now = now_of(request)
    async with request.app.state.engine.connect() as conn:
        known = (
            await conn.execute(
                text("SELECT 1 FROM events WHERE event_ticker = :e"), {"e": event_ticker}
            )
        ).first()
        if known is None:
            raise ApiError(404, "not_found")
        rows = await conn.execute(
            text(
                "SELECT DISTINCT ON (analyzer, metric) analyzer, metric, ts, value_e6 "
                "FROM analysis_results WHERE event_ticker = :e AND ts > :since AND ts <= :now "
                "ORDER BY analyzer, metric, ts DESC"
            ),
            {"e": event_ticker, "since": now - timedelta(seconds=max_age_seconds), "now": now},
        )
        results = [value_json(r.analyzer, r.metric, r.ts, r.value_e6, now) for r in rows]
    return {"event_ticker": event_ticker, "as_of": fmt.moment(now), "results": results}


@router.get("/arbitrage")
async def arbitrage(
    request: Request,
    _: Reader,
    min_profit: Annotated[str | None, Query(max_length=40)] = None,
    max_age_seconds: Annotated[int, Query(ge=1, le=3600)] = 300,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
) -> dict[str, Any]:
    """Events with a positive gross edge right now, most profitable first. ``profit`` is after
    Kalshi's taker fees and is often negative: pass ``min_profit=0`` to see only the ones that
    would make money. These are indications from stored quotes (see ``oldest_quote_age``);
    re-read the live order book before trading."""
    threshold: int | None = None
    if min_profit is not None:
        try:
            threshold = to_e6(Decimal(min_profit))
        except (InvalidOperation, PrecisionError, ValueError):
            threshold = None
        if threshold is None:
            raise ApiError(422, "invalid_parameter", fields=["min_profit"])
    now = now_of(request)
    async with request.app.state.engine.connect() as conn:
        rows = await conn.execute(
            text(
                "SELECT DISTINCT ON (event_ticker, metric) event_ticker, metric, ts, value_e6 "
                "FROM analysis_results WHERE analyzer = :a AND ts > :since AND ts <= :now "
                "ORDER BY event_ticker, metric, ts DESC"
            ),
            {"a": ARBITRAGE, "since": now - timedelta(seconds=max_age_seconds), "now": now},
        )
        by_event: dict[str, dict[str, Any]] = {}
        for r in rows:
            entry = by_event.setdefault(r.event_ticker, {"time": r.ts})
            entry[r.metric] = r.value_e6
            entry["time"] = max(entry["time"], r.ts)
    complete = [
        (event, e)
        for event, e in by_event.items()
        if "profit" in e and "edge" in e and (threshold is None or e["profit"] >= threshold)
    ]
    complete.sort(key=lambda item: item[1]["profit"], reverse=True)
    return {
        "as_of": fmt.moment(now),
        "opportunities": [
            {
                "event_ticker": event,
                "time": fmt.moment(e["time"]),
                "bid_sum": fmt.dollars(e.get("bid_sum")),
                "edge": fmt.dollars(e["edge"]),
                "size": fmt.dollars(e.get("size")),  # contracts
                "fees": fmt.dollars(e.get("fees")),
                "profit": fmt.dollars(e["profit"]),
                "legs": None if "legs" not in e else e["legs"] // 1_000_000,
                "legs_total": None if "legs_total" not in e else e["legs_total"] // 1_000_000,
                "oldest_quote_age_seconds": (
                    None if "oldest_quote_age" not in e else e["oldest_quote_age"] // 1_000_000
                ),
            }
            for event, e in complete[:limit]
        ],
        "total": len(complete),
    }
