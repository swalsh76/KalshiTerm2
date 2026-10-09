"""Raw-data routes: trades, ticker history, the orderbook at a moment, and the gap log."""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any

from fastapi import APIRouter, Query, Request
from sqlalchemy import text

from kalshiterm_server.api import format as fmt
from kalshiterm_server.api.errors import ApiError
from kalshiterm_server.api.markets import Reader, aware

router = APIRouter(prefix="/v1")

MAX_ROWS = 1_000
NIL_UUID = "00000000-0000-0000-0000-000000000000"
ONE_DOLLAR_E6 = 1_000_000


async def market_id(request: Request, ticker: str) -> int:
    async with request.app.state.engine.connect() as conn:
        found = (
            await conn.execute(text("SELECT id FROM markets WHERE ticker = :t"), {"t": ticker})
        ).scalar_one_or_none()
    if found is None:
        raise ApiError(404, "not_found")
    return int(found)


def invalid(*fields: str, reason: str | None = None) -> ApiError:
    if reason:
        return ApiError(422, "invalid_parameter", fields=list(fields), reason=reason)
    return ApiError(422, "invalid_parameter", fields=list(fields))


def window(start: datetime | None, end: datetime | None) -> tuple[datetime | None, datetime]:
    start, end = aware(start, "start"), aware(end, "end")
    end = end or datetime.now(UTC) + timedelta(seconds=1)
    if start is not None and start >= end:
        raise invalid("start", "end", reason="start_after_end")
    return start, end


# ---------------------------------------------------------------- trades

TRADE_COLUMNS = "ts, trade_id, yes_price_e6, count_e2, taker_side, is_block_trade"


def trade_json(r: Any) -> dict[str, Any]:
    return {
        "time": fmt.moment(r.ts),
        "trade_id": str(r.trade_id),
        "yes_price": fmt.dollars(r.yes_price_e6),
        "no_price": fmt.dollars(ONE_DOLLAR_E6 - r.yes_price_e6),
        "count": fmt.count(r.count_e2),
        "taker_side": r.taker_side,
        "is_block_trade": r.is_block_trade,
    }


@router.get("/markets/{ticker}/trades")
async def trades(
    request: Request,
    ticker: str,
    _: Reader,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: Annotated[int, Query(ge=1, le=MAX_ROWS)] = 200,
    cursor: Annotated[str | None, Query(max_length=100)] = None,
) -> dict[str, Any]:
    """Executed trades, oldest first, from the 30-day table and (for markets that have ever
    been watched) the permanent copy, each trade once. Without ``start``: the most recent
    ``limit`` trades before ``end``. With it: forward from ``start``, paged by ``next_cursor``."""
    start, end = window(start, end)
    after: tuple[datetime, str] | None = None
    if cursor is not None:
        parts = (fmt.decode_cursor(cursor) or "").split("|")
        try:
            after = (datetime.fromisoformat(parts[0]), str(uuid.UUID(parts[1])))
            assert after[0].tzinfo is not None and len(parts) == 2
        except (ValueError, IndexError, AssertionError) as exc:
            raise invalid("cursor") from exc
    mid = await market_id(request, ticker)
    both = (
        f"SELECT {TRADE_COLUMNS} FROM trades WHERE market_id = :m AND ts >= :start AND ts < :end "
        f"UNION SELECT {TRADE_COLUMNS} FROM trades_watchlist "
        "WHERE market_id = :m AND ts >= :start AND ts < :end"
    )
    latest = start is None and after is None
    keyset = "(ts, trade_id) > (:cts, CAST(:ctid AS uuid))" if after else "TRUE"
    direction = "DESC" if latest else ""
    params: dict[str, Any] = {
        "m": mid,
        "start": start or datetime(1970, 1, 1, tzinfo=UTC),
        "end": end,
        "n": limit + 1,
        "cts": after[0] if after else None,
        "ctid": after[1] if after else NIL_UUID,
    }
    if after and start is not None and after[0] > params["start"]:
        params["start"] = after[0]  # the cursor can only move the window forward
    async with request.app.state.engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    f"SELECT {TRADE_COLUMNS} FROM ({both}) t WHERE {keyset} "
                    f"ORDER BY ts {direction}, trade_id {direction} LIMIT :n"
                ),
                params,
            )
        ).all()
    if latest:
        page, more = list(reversed(rows[:limit])), False
    else:
        page, more = rows[:limit], len(rows) > limit
    next_cursor = None
    if more:
        last = page[-1]
        next_cursor = fmt.encode_cursor(f"{last.ts.isoformat()}|{last.trade_id}")
    return {"ticker": ticker, "trades": [trade_json(r) for r in page], "next_cursor": next_cursor}


# ---------------------------------------------------------------- ticker history

TICK_ORDER = "ts, received_at, yes_bid_e6, yes_ask_e6, price_e6, volume_e2, open_interest_e2"


def tick_json(r: Any) -> dict[str, Any]:
    return {
        "time": fmt.moment(r.ts),
        "price": fmt.dollars(r.price_e6),
        "yes_bid": fmt.dollars(r.yes_bid_e6),
        "yes_ask": fmt.dollars(r.yes_ask_e6),
        "yes_bid_size": fmt.count(r.yes_bid_size_e2),
        "yes_ask_size": fmt.count(r.yes_ask_size_e2),
        "volume": fmt.count(r.volume_e2),
        "open_interest": fmt.count(r.open_interest_e2),
        "last_trade_size": fmt.count(r.last_trade_size_e2),
    }


@router.get("/markets/{ticker}/ticks")
async def ticks(
    request: Request,
    ticker: str,
    _: Reader,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: Annotated[int, Query(ge=1, le=MAX_ROWS)] = 200,
    cursor: Annotated[str | None, Query(max_length=100)] = None,
) -> dict[str, Any]:
    """Ticker updates (price, best bid/ask, volume, open interest), oldest first; the last
    14 days are kept. Paging is exact even when updates share a timestamp."""
    start, end = window(start, end)
    skip = 0
    resume: datetime | None = None
    if cursor is not None:
        parts = (fmt.decode_cursor(cursor) or "").split("|")
        try:
            resume = datetime.fromisoformat(parts[0])
            skip = int(parts[1])
            assert resume.tzinfo is not None and len(parts) == 2 and skip >= 0
        except (ValueError, IndexError, AssertionError) as exc:
            raise invalid("cursor") from exc
        start = resume
    mid = await market_id(request, ticker)
    columns = (
        "ts, price_e6, yes_bid_e6, yes_ask_e6, yes_bid_size_e2, yes_ask_size_e2, volume_e2, "
        "open_interest_e2, last_trade_size_e2, received_at"
    )
    latest = start is None
    params = {"m": mid, "start": start or datetime(1970, 1, 1, tzinfo=UTC), "end": end}
    async with request.app.state.engine.connect() as conn:
        if latest:
            found = (
                await conn.execute(
                    text(
                        f"SELECT {columns} FROM tickers WHERE market_id = :m AND ts >= :start "
                        "AND ts < :end ORDER BY ts DESC, received_at DESC LIMIT :n"
                    ),
                    {**params, "n": limit},
                )
            ).all()
            page, more = list(reversed(found)), False
        else:
            found = (
                await conn.execute(
                    text(
                        f"SELECT {columns} FROM tickers WHERE market_id = :m AND ts >= :start "
                        f"AND ts < :end ORDER BY {TICK_ORDER} LIMIT :n"
                    ),
                    {**params, "n": skip + limit + 1},
                )
            ).all()
            found = found[skip:]  # rows at the resume timestamp that were already sent
            page, more = found[:limit], len(found) > limit
    next_cursor = None
    if more:
        # Resume at the last timestamp sent, skipping the rows at that timestamp already sent:
        # this page's, plus the previous cursor's if this page never left its timestamp.
        last_ts = page[-1].ts
        already = sum(1 for r in page if r.ts == last_ts)
        if resume is not None and last_ts == resume:
            already += skip
        next_cursor = fmt.encode_cursor(f"{last_ts.isoformat()}|{already}")
    return {"ticker": ticker, "ticks": [tick_json(r) for r in page], "next_cursor": next_cursor}


# ---------------------------------------------------------------- the orderbook


def levels(prices: list[int], sizes: list[int]) -> dict[int, int]:
    return dict(zip(prices, sizes, strict=True))


def side_json(book: dict[int, int], depth: int) -> list[dict[str, str | None]]:
    best_first = sorted(book.items(), key=lambda kv: kv[0], reverse=True)[:depth]
    return [{"price": fmt.dollars(p), "size": fmt.count(s)} for p, s in best_first]


@router.get("/markets/{ticker}/orderbook")
async def orderbook(
    request: Request,
    ticker: str,
    _: Reader,
    at: datetime | None = None,
    depth: Annotated[int, Query(ge=1, le=1000)] = 100,
) -> dict[str, Any]:
    """The book at a moment: the latest stored snapshot at or before ``at`` plus the net of
    every change after it *in sequence* (same subscription, larger ``seq``; exchange timestamps
    are not used to order them: a change can follow a snapshot yet be stamped slightly
    earlier). Only markets that have been on the watchlist have books.

    ``complete`` is false when the stored changes cannot be trusted to be gap-free (the
    snapshot was rebuilt from REST, or an outage / load shed is recorded after the snapshot);
    ``gaps`` says which. Deltas are kept 14 days, snapshots a year, so older moments are
    only as precise as the nearest snapshot.
    """
    when = aware(at, "at") or datetime.now(UTC)
    mid = await market_id(request, ticker)
    async with request.app.state.engine.connect() as conn:
        snap = (
            await conn.execute(
                text(
                    "SELECT ts, seq, sid, approximate, yes_prices_e6, yes_sizes_e2, no_prices_e6, "
                    "no_sizes_e2 FROM orderbook_snapshots WHERE market_id = :m AND ts <= :at "
                    "ORDER BY ts DESC, seq DESC NULLS LAST LIMIT 1"
                ),
                {"m": mid, "at": when},
            )
        ).first()
        if snap is None:
            raise ApiError(404, "no_orderbook")
        changes = (
            await conn.execute(
                text(
                    "SELECT is_yes, price_e6, sum(delta_e2) AS net, count(*) AS n "
                    "FROM orderbook_deltas WHERE market_id = :m AND ts <= :at AND ("
                    # same subscription: sequence is the truth, whatever the timestamps say
                    "(CAST(:ssid AS integer) IS NOT NULL AND sid = :ssid AND seq > :sseq "
                    " AND ts >= CAST(:sts AS timestamptz) - interval '1 minute') "
                    # no subscription recorded (REST-built or pre-migration snapshot): time only
                    "OR (CAST(:ssid AS integer) IS NULL AND ts > CAST(:sts AS timestamptz))) "
                    "GROUP BY is_yes, price_e6"
                ),
                {"m": mid, "sts": snap.ts, "sseq": snap.seq, "ssid": snap.sid, "at": when},
            )
        ).all()
        gaps = (
            await conn.execute(
                text(
                    "SELECT reason, started_at, ended_at FROM ingest_gaps "
                    "WHERE ended_at >= :sts AND started_at <= :at ORDER BY started_at LIMIT 5"
                ),
                {"sts": snap.ts, "at": when},
            )
        ).all()
    yes = levels(snap.yes_prices_e6, snap.yes_sizes_e2)
    no = levels(snap.no_prices_e6, snap.no_sizes_e2)
    inconsistent = False
    for change in changes:
        book = yes if change.is_yes else no
        size = book.get(change.price_e6, 0) + int(change.net)
        if size < 0:
            inconsistent = True  # more removed than existed: a delta we never saw
            size = 0
        if size:
            book[change.price_e6] = size
        else:
            book.pop(change.price_e6, None)
    best_yes = max(yes, default=None)
    best_no = max(no, default=None)
    problems = [
        {"reason": g.reason, "from": fmt.moment(g.started_at), "to": fmt.moment(g.ended_at)}
        for g in gaps
    ]
    return {
        "ticker": ticker,
        "as_of": fmt.moment(when),
        "snapshot_time": fmt.moment(snap.ts),
        "changes_applied": sum(int(c.n) for c in changes),
        "approximate_snapshot": bool(snap.approximate),
        "complete": not (snap.approximate or problems or inconsistent),
        "gaps": problems,
        "yes": side_json(yes, depth),
        "no": side_json(no, depth),
        "best_yes_bid": fmt.dollars(best_yes),
        "best_no_bid": fmt.dollars(best_no),
        "best_yes_ask": fmt.dollars(None if best_no is None else ONE_DOLLAR_E6 - best_no),
        "best_no_ask": fmt.dollars(None if best_yes is None else ONE_DOLLAR_E6 - best_yes),
    }


# ---------------------------------------------------------------- gap log


@router.get("/gaps")
async def gaps(
    request: Request,
    _: Reader,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: Annotated[int, Query(ge=1, le=MAX_ROWS)] = 100,
) -> dict[str, Any]:
    """Periods the live stream may have missed (newest first): what is missing for each is
    tickers, lifecycle events and orderbook changes; trades are backfilled (see ``status``)."""
    start, end = window(start, end)
    async with request.app.state.engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    "SELECT id, reason, status, started_at, ended_at, dropped, trades_added, "
                    "note FROM ingest_gaps WHERE ended_at >= :start AND started_at < :end "
                    "ORDER BY started_at DESC, id DESC LIMIT :n"
                ),
                {"start": start or datetime.now(UTC) - timedelta(days=7), "end": end, "n": limit},
            )
        ).all()
    return {
        "gaps": [
            {
                "id": r.id,
                "reason": r.reason,
                "status": r.status,
                "from": fmt.moment(r.started_at),
                "to": fmt.moment(r.ended_at),
                "messages_dropped": r.dropped,
                "trades_recovered": r.trades_added,
                "note": r.note,
            }
            for r in rows
        ]
    }
