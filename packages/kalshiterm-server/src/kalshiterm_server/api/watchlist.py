"""A user's own watchlist: the markets they want the server to capture orderbooks for."""

from typing import Annotated, Any

from fastapi import APIRouter, Path, Request

from kalshiterm_server import watchlists
from kalshiterm_server.api import format as fmt
from kalshiterm_server.api.errors import ApiError
from kalshiterm_server.api.markets import Reader

router = APIRouter(prefix="/v1")
Ticker = Annotated[str, Path(pattern=r"^[A-Za-z0-9._-]{1,128}$")]


def failure(exc: watchlists.WatchlistError) -> ApiError:
    return ApiError(exc.status, exc.code, **exc.extra)


@router.get("/watchlist")
async def my_watchlist(request: Request, who: Reader) -> dict[str, Any]:
    """Your list. ``capturing`` is true once the server is storing the market's orderbook
    (new entries are picked up within about 15 seconds)."""
    markets = await watchlists.list_watched(request.app.state.engine, who.user_id)
    return {
        "markets": [
            {
                "ticker": m.ticker,
                "event_title": m.event_title,
                "status": m.status,
                "added_at": fmt.moment(m.added_at),
                "capturing": m.capturing,
            }
            for m in markets
        ],
        "limit": request.app.state.settings.watchlist_max_per_user,
    }


@router.put("/watchlist/{ticker}")
async def watch(request: Request, ticker: Ticker, who: Reader) -> dict[str, Any]:
    """Add a market to your list (idempotent). Combos and settled markets cannot be watched."""
    config = request.app.state.settings
    try:
        added = await watchlists.add(
            request.app.state.engine,
            who.user_id,
            ticker,
            config.watchlist_max_per_user,
            config.watchlist_max_total,
        )
    except watchlists.WatchlistError as exc:
        raise failure(exc) from exc
    return {"ticker": ticker, "added": added}


@router.delete("/watchlist/{ticker}")
async def unwatch(request: Request, ticker: Ticker, who: Reader) -> dict[str, Any]:
    """Remove a market from your list (idempotent). The server stops capturing it once nobody
    wants it, and never within 12 hours of starting."""
    try:
        removed = await watchlists.remove(request.app.state.engine, who.user_id, ticker)
    except watchlists.WatchlistError as exc:
        raise failure(exc) from exc
    return {"ticker": ticker, "removed": removed}
