"""Reference-data and candle routes: markets, events, candles."""

from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy import text

from kalshiterm_server import auth
from kalshiterm_server.api import format as fmt
from kalshiterm_server.api.deps import principal
from kalshiterm_server.api.errors import ApiError

router = APIRouter(prefix="/v1")

MAX_PAGE = 500
MAX_CANDLES = 5_000
STAGE = """CASE
    WHEN m.settlement_ts IS NOT NULL AND m.created_time IS NULL THEN 'tombstone'
    WHEN m.settlement_ts IS NOT NULL AND m.rules_primary = '' THEN 'slim'
    ELSE 'full' END"""
MARKET_COLUMNS = f"""
    m.ticker, m.event_ticker, e.series_ticker, e.title AS event_title, m.market_type, m.status,
    m.yes_sub_title, m.no_sub_title, m.open_time, m.close_time, m.latest_expiration_time,
    m.result, m.settlement_value_e6, m.settlement_ts, m.strike_type, m.floor_strike_e6,
    m.cap_strike_e6, {STAGE} AS stage"""
Reader = Annotated[auth.Principal, Depends(principal)]


def market_json(r: Any, detail: bool = False) -> dict[str, Any]:
    body = {
        "ticker": r.ticker,
        "event_ticker": r.event_ticker,
        "series_ticker": r.series_ticker,
        "event_title": r.event_title,
        "type": r.market_type,
        "status": r.status,
        "yes_label": r.yes_sub_title,
        "no_label": r.no_sub_title,
        "open_time": fmt.moment(r.open_time),
        "close_time": fmt.moment(r.close_time),
        "expiration_time": fmt.moment(r.latest_expiration_time),
        "result": r.result,
        "settlement_value": fmt.dollars(r.settlement_value_e6),
        "settlement_time": fmt.moment(r.settlement_ts),
        "strike_type": r.strike_type,
        "floor_strike": fmt.dollars(r.floor_strike_e6),
        "cap_strike": fmt.dollars(r.cap_strike_e6),
        "stage": r.stage,  # full | slim | tombstone: how much of the row is still kept
    }
    if detail:
        body["rules_primary"] = r.rules_primary
        body["rules_secondary"] = r.rules_secondary
        body["created_time"] = fmt.moment(r.created_time)
        body["updated_time"] = fmt.moment(r.updated_time)
    return body


def like_pattern(term: str) -> str:
    escaped = term.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{escaped}%"


@router.get("/markets")
async def list_markets(
    request: Request,
    _: Reader,
    status: Annotated[str | None, Query(pattern=r"^[a-z_]{1,32}$")] = None,
    event: Annotated[str | None, Query(max_length=128)] = None,
    series: Annotated[str | None, Query(max_length=128)] = None,
    q: Annotated[str | None, Query(min_length=2, max_length=64)] = None,
    limit: Annotated[int, Query(ge=1, le=MAX_PAGE)] = 100,
    cursor: Annotated[str | None, Query(max_length=300)] = None,
) -> dict[str, Any]:
    """Markets in ticker order. Filter by status, event, series, or a substring of the ticker
    or event title; page with ``next_cursor``."""
    after = ""
    if cursor is not None:
        decoded = fmt.decode_cursor(cursor)
        if decoded is None:
            raise ApiError(422, "invalid_parameter", fields=["cursor"])
        after = decoded
    async with request.app.state.engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    f"SELECT {MARKET_COLUMNS} FROM markets m "
                    "LEFT JOIN events e ON e.event_ticker = m.event_ticker "
                    "WHERE m.ticker > :after "
                    "AND (CAST(:status AS text) IS NULL OR m.status = :status) "
                    "AND (CAST(:event AS text) IS NULL OR m.event_ticker = :event) "
                    "AND (CAST(:series AS text) IS NULL OR e.series_ticker = :series) "
                    "AND (CAST(:pattern AS text) IS NULL OR m.ticker ILIKE :pattern ESCAPE '\\' "
                    "     OR e.title ILIKE :pattern ESCAPE '\\') "
                    "ORDER BY m.ticker LIMIT :n"
                ),
                {
                    "after": after,
                    "status": status,
                    "event": event,
                    "series": series,
                    "pattern": like_pattern(q) if q else None,
                    "n": limit + 1,
                },
            )
        ).all()
    page = rows[:limit]
    return {
        "markets": [market_json(r) for r in page],
        "next_cursor": fmt.encode_cursor(page[-1].ticker) if len(rows) > limit else None,
    }


@router.get("/markets/{ticker}")
async def get_market(request: Request, ticker: str, _: Reader) -> dict[str, Any]:
    async with request.app.state.engine.connect() as conn:
        row = (
            await conn.execute(
                text(
                    f"SELECT {MARKET_COLUMNS}, m.rules_primary, m.rules_secondary, m.created_time, "
                    "m.updated_time FROM markets m "
                    "LEFT JOIN events e ON e.event_ticker = m.event_ticker WHERE m.ticker = :t"
                ),
                {"t": ticker},
            )
        ).first()
    if row is None:
        raise ApiError(404, "not_found")
    return market_json(row, detail=True)


@router.get("/events/{event_ticker}")
async def get_event(request: Request, event_ticker: str, _: Reader) -> dict[str, Any]:
    async with request.app.state.engine.connect() as conn:
        event = (
            await conn.execute(
                text(
                    "SELECT event_ticker, series_ticker, title, sub_title, mutually_exclusive, "
                    "strike_date, strike_period FROM events WHERE event_ticker = :e"
                ),
                {"e": event_ticker},
            )
        ).first()
        if event is None:
            raise ApiError(404, "not_found")
        markets = (
            await conn.execute(
                text(
                    f"SELECT {MARKET_COLUMNS} FROM markets m "
                    "LEFT JOIN events e ON e.event_ticker = m.event_ticker "
                    "WHERE m.event_ticker = :e ORDER BY m.ticker LIMIT :n"
                ),
                {"e": event_ticker, "n": MAX_PAGE},
            )
        ).all()
    return {
        "event_ticker": event.event_ticker,
        "series_ticker": event.series_ticker,
        "title": event.title,
        "sub_title": event.sub_title,
        "mutually_exclusive": event.mutually_exclusive,
        "strike_date": fmt.moment(event.strike_date),
        "strike_period": event.strike_period,
        "markets": [market_json(r) for r in markets],
    }


# ---------------------------------------------------------------- candles

INTERVAL = {"1m": timedelta(minutes=1), "1h": timedelta(hours=1)}
TRADE_BAR = "bucket, open_e6, high_e6, low_e6, close_e6, volume_e2, trades"
TICKER_BAR = (
    "bucket, open_e6, high_e6, low_e6, close_e6, yes_bid_e6, yes_ask_e6, volume_e2, "
    "open_interest_e2, ticks"
)


def bar_json(r: Any, source: str) -> dict[str, Any]:
    body: dict[str, Any] = {
        "time": fmt.moment(r.bucket),
        "open": fmt.dollars(r.open_e6),
        "high": fmt.dollars(r.high_e6),
        "low": fmt.dollars(r.low_e6),
        "close": fmt.dollars(r.close_e6),
        "volume": fmt.count(r.volume_e2),
    }
    if source == "trades":
        body["trades"] = r.trades
    else:
        body["yes_bid"] = fmt.dollars(r.yes_bid_e6)
        body["yes_ask"] = fmt.dollars(r.yes_ask_e6)
        body["open_interest"] = fmt.count(r.open_interest_e2)
        body["ticks"] = r.ticks
    return body


def aware(value: datetime | None, field: str) -> datetime | None:
    if value is not None and value.tzinfo is None:
        raise ApiError(422, "invalid_parameter", fields=[field], reason="timezone_required")
    return value


@router.get("/markets/{ticker}/candles")
async def candles(
    request: Request,
    ticker: str,
    _: Reader,
    interval: Literal["1m", "1h"] = "1m",
    source: Literal["trades", "ticker"] = "trades",
    start: datetime | None = None,
    end: datetime | None = None,
    limit: Annotated[int, Query(ge=1, le=MAX_CANDLES)] = 500,
    cursor: Annotated[str | None, Query(max_length=100)] = None,
) -> dict[str, Any]:
    """Bars for one market, oldest first, from the continuous aggregates.

    ``source=trades``: open/high/low/close of trade prices (yes side), contracts traded.
    ``source=ticker``: price OHLC from ticker updates plus the last quote, volume and open
    interest. Minutes with no activity have no bar. Without ``start`` you get the most recent
    ``limit`` bars before ``end`` (default now); with it, bars from ``start`` onwards, paged
    forward with ``next_cursor``. The newest bar can lag by the aggregate's refresh interval.
    """
    start, end = aware(start, "start"), aware(end, "end")
    end = end or datetime.now(UTC)
    if start is not None and start >= end:
        raise ApiError(422, "invalid_parameter", fields=["start", "end"], reason="start_after_end")
    if cursor is not None:
        decoded = fmt.decode_cursor(cursor)
        try:
            start = datetime.fromisoformat(decoded or "")
        except ValueError as exc:
            raise ApiError(422, "invalid_parameter", fields=["cursor"]) from exc
        if start.tzinfo is None:
            raise ApiError(422, "invalid_parameter", fields=["cursor"])
    view = f"{'candles' if source == 'trades' else 'ticker'}_{interval}"
    columns = TRADE_BAR if source == "trades" else TICKER_BAR
    async with request.app.state.engine.connect() as conn:
        market_id = (
            await conn.execute(text("SELECT id FROM markets WHERE ticker = :t"), {"t": ticker})
        ).scalar_one_or_none()
        if market_id is None:
            raise ApiError(404, "not_found")
        if start is None:  # the latest `limit` bars, then put them in time order
            found = (
                await conn.execute(
                    text(
                        f"SELECT {columns} FROM {view} WHERE market_id = :m AND bucket < :end "
                        "ORDER BY bucket DESC LIMIT :n"
                    ),
                    {"m": market_id, "end": end, "n": limit},
                )
            ).all()
            bars, more = list(reversed(found)), False
        else:
            found = (
                await conn.execute(
                    text(
                        f"SELECT {columns} FROM {view} WHERE market_id = :m AND bucket >= :start "
                        "AND bucket < :end ORDER BY bucket LIMIT :n"
                    ),
                    {"m": market_id, "start": start, "end": end, "n": limit + 1},
                )
            ).all()
            bars, more = found[:limit], len(found) > limit
    next_cursor = None
    if more:  # resume just after the last bar returned
        next_cursor = fmt.encode_cursor((bars[-1].bucket + INTERVAL[interval]).isoformat())
    return {
        "ticker": ticker,
        "interval": interval,
        "source": source,
        "bars": [bar_json(r, source) for r in bars],
        "next_cursor": next_cursor,
    }
