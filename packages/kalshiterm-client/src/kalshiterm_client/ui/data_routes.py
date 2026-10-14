"""The KalshiTerm server's data, as the browser sees it.

Each route calls one ``ServerClient`` method and returns its typed model, so the browser gets what
the CLI gets and the OpenAPI schema (and from it the TypeScript types) describes it exactly.
Rules that matter:

* **An explicit list, not a proxy.** Only the routes below exist; the browser cannot ask the
  backend to fetch an arbitrary path from the server with the server token.
* **The server token never reaches the browser.** It stays in the ``ServerClient`` (the keyring);
  an error message passes through only after the client has worded it, and none contains a token.
* **Decimals stay strings** (pydantic's JSON mode), so a price survives the trip exactly.
* **The server being down is a normal state, not a crash**: with no profile, no token or an
  unreachable server these routes answer 503 / 502 with a short reason and the rest of the UI
  (trading included) keeps working.
* Datetimes must carry a timezone; a naive one is a 400.
"""

from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Path, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from kalshiterm_client.client import ServerClient
from kalshiterm_client.errors import (
    ApiProblem,
    CertificateChanged,
    ConnectionProblem,
    Forbidden,
    InvalidRequest,
    NotFound,
    RateLimited,
    Unauthorized,
)
from kalshiterm_client.models import (
    AlertMetrics,
    AlertPage,
    AlertRule,
    AlertRules,
    CandlePage,
    Event,
    EventResults,
    Gap,
    Layout,
    LayoutList,
    LayoutSaved,
    MarketDetail,
    MarketPage,
    MarketResults,
    MetricHistory,
    Opportunities,
    OrderBook,
    TickPage,
    TradePage,
    Watchlist,
)


class ServerUnavailable(Exception):
    """No KalshiTerm server is configured for this session (no profile or no token)."""


class DataAccess:
    """Where the routes get their ``ServerClient``. ``client`` is None when no server is set up;
    ``reason`` then says why, for the browser to show."""

    def __init__(
        self,
        client: ServerClient | None,
        *,
        profile: str | None = None,
        url: str | None = None,
        reason: str = "no server is configured",
        owns_client: bool = False,
    ) -> None:
        self.client = client
        self.profile = profile
        self.url = url
        self.reason = reason
        self.owns_client = owns_client  # close the client when the app shuts down

    async def close(self) -> None:
        if self.client is not None and self.owns_client:
            await self.client.aclose()

    def require(self) -> ServerClient:
        if self.client is None:
            raise ServerUnavailable(self.reason)
        return self.client


class ServerInfo(BaseModel):
    configured: bool
    reachable: bool
    profile: str | None = None
    url: str | None = None
    user: str | None = None
    role: str | None = None
    error: str | None = None


class NewRule(BaseModel):
    model_config = ConfigDict(extra="forbid")
    metric: str
    op: str
    threshold: Annotated[
        str, Field(pattern=r"^-?[0-9]+(\.[0-9]+)?$")
    ]  # a string: never a JS number
    ticker: str | None = None
    event_ticker: str | None = None
    cooldown_seconds: int | None = None
    name: str = ""


class RulePatch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled: bool | None = None
    threshold: Annotated[str, Field(pattern=r"^-?[0-9]+(\.[0-9]+)?$")] | None = None
    cooldown_seconds: int | None = None
    name: str | None = None


class Changed(BaseModel):
    changed: bool


class LayoutBody(BaseModel):
    """What the page sends to save a layout: any JSON object (the front end defines it)."""

    model_config = ConfigDict(extra="forbid")
    data: dict[str, Any]


def _access(request: Request) -> DataAccess:
    access: DataAccess = request.app.state.data
    return access


def _client(request: Request) -> ServerClient:
    return _access(request).require()


Client = Annotated[ServerClient, Depends(_client)]


def aware(value: datetime | None, name: str) -> datetime | None:
    if value is not None and value.tzinfo is None:
        raise HTTPException(400, f"{name} must include a time zone, e.g. 2026-10-12T18:00:00Z")
    return value


LayoutName = Annotated[str, Path(pattern=r"^[A-Za-z0-9]([A-Za-z0-9 _.-]{0,62}[A-Za-z0-9_.-])?$")]
Limit = Annotated[int | None, Query(ge=1, le=1000)]
Cursor = Annotated[str | None, Query(max_length=512)]
Instant = Annotated[datetime | None, Query()]
router = APIRouter(prefix="/api")


# ------------------------------------------------------------------ the server itself


@router.get("/server", response_model=ServerInfo)
async def server_info(request: Request) -> ServerInfo:
    """Is a server configured, and does it answer? Never fails: the browser shows the answer."""
    access = _access(request)
    if access.client is None:
        return ServerInfo(configured=False, reachable=False, error=access.reason)
    base = ServerInfo(configured=True, reachable=False, profile=access.profile, url=access.url)
    try:
        me = await access.client.me()
    except (ConnectionProblem, ApiProblem) as exc:
        return base.model_copy(update={"error": str(exc)})
    return base.model_copy(update={"reachable": True, "user": me.user, "role": me.role})


# ------------------------------------------------------------------ reference data


@router.get("/markets", response_model=MarketPage)
async def markets(
    client: Client,
    status: Annotated[str | None, Query(max_length=32)] = None,
    event: Annotated[str | None, Query(max_length=128)] = None,
    series: Annotated[str | None, Query(max_length=128)] = None,
    q: Annotated[str | None, Query(max_length=128)] = None,
    limit: Limit = None,
    cursor: Cursor = None,
) -> Any:
    return await client.markets_page(
        status=status, event=event, series=series, q=q, limit=limit, cursor=cursor
    )


@router.get("/markets/{ticker}", response_model=MarketDetail)
async def market(client: Client, ticker: str) -> Any:
    return await client.market(ticker)


@router.get("/events/{event_ticker}", response_model=Event)
async def event(client: Client, event_ticker: str) -> Any:
    return await client.event(event_ticker)


# ------------------------------------------------------------------ market data


@router.get("/markets/{ticker}/candles", response_model=CandlePage)
async def candles(
    client: Client,
    ticker: str,
    interval: Annotated[str, Query(pattern=r"^[0-9]+[mhd]$")] = "1m",
    source: Annotated[str, Query(pattern=r"^(trades|ticks)$")] = "trades",
    start: Instant = None,
    end: Instant = None,
    limit: Limit = None,
    cursor: Cursor = None,
) -> Any:
    return await client.candles_page(
        ticker,
        interval=interval,
        source=source,
        start=aware(start, "start"),
        end=aware(end, "end"),
        limit=limit,
        cursor=cursor,
    )


@router.get("/markets/{ticker}/trades", response_model=TradePage)
async def trades(
    client: Client,
    ticker: str,
    start: Instant = None,
    end: Instant = None,
    limit: Limit = None,
    cursor: Cursor = None,
) -> Any:
    return await client.trades_page(
        ticker, start=aware(start, "start"), end=aware(end, "end"), limit=limit, cursor=cursor
    )


@router.get("/markets/{ticker}/ticks", response_model=TickPage)
async def ticks(
    client: Client,
    ticker: str,
    start: Instant = None,
    end: Instant = None,
    limit: Limit = None,
    cursor: Cursor = None,
) -> Any:
    return await client.ticks_page(
        ticker, start=aware(start, "start"), end=aware(end, "end"), limit=limit, cursor=cursor
    )


@router.get("/markets/{ticker}/orderbook", response_model=OrderBook)
async def orderbook(
    client: Client,
    ticker: str,
    at: Instant = None,
    depth: Annotated[int | None, Query(ge=1, le=200)] = None,
) -> Any:
    return await client.orderbook(ticker, at=aware(at, "at"), depth=depth)


@router.get("/gaps", response_model=list[Gap])
async def gaps(
    client: Client, start: Instant = None, end: Instant = None, limit: Limit = None
) -> Any:
    return await client.gaps(start=aware(start, "start"), end=aware(end, "end"), limit=limit)


# ------------------------------------------------------------------ watchlist


@router.get("/watchlist", response_model=Watchlist)
async def watchlist(client: Client) -> Any:
    return await client.watchlist()


@router.put("/watchlist/{ticker}", response_model=Changed)
async def watch(client: Client, ticker: str) -> Changed:
    return Changed(changed=await client.watch(ticker))


@router.delete("/watchlist/{ticker}", response_model=Changed)
async def unwatch(client: Client, ticker: str) -> Changed:
    return Changed(changed=await client.unwatch(ticker))


# ------------------------------------------------------------------ analytics


@router.get("/analytics/markets/{ticker}", response_model=MarketResults)
async def market_analytics(
    client: Client, ticker: str, max_age_seconds: Annotated[int | None, Query(ge=1)] = None
) -> Any:
    return await client.market_results(ticker, max_age_seconds=max_age_seconds)


@router.get("/analytics/markets/{ticker}/history", response_model=MetricHistory)
async def market_history(
    client: Client,
    ticker: str,
    metric: Annotated[str, Query(max_length=64)],
    since: Instant = None,
    until: Instant = None,
    limit: Limit = None,
    cursor: Cursor = None,
) -> Any:
    return await client.market_history_page(
        ticker,
        metric,
        since=aware(since, "since"),
        until=aware(until, "until"),
        limit=limit,
        cursor=cursor,
    )


@router.get("/analytics/events/{event_ticker}", response_model=EventResults)
async def event_analytics(
    client: Client, event_ticker: str, max_age_seconds: Annotated[int | None, Query(ge=1)] = None
) -> Any:
    return await client.event_results(event_ticker, max_age_seconds=max_age_seconds)


@router.get("/analytics/arbitrage", response_model=Opportunities)
async def arbitrage(
    client: Client,
    min_profit: Annotated[str | None, Query(pattern=r"^-?[0-9]+(\.[0-9]+)?$")] = None,
    max_age_seconds: Annotated[int | None, Query(ge=1)] = None,
    limit: Limit = None,
) -> Any:
    return await client.arbitrage(
        min_profit=None if min_profit is None else Decimal(min_profit),
        max_age_seconds=max_age_seconds,
        limit=limit,
    )


# ------------------------------------------------------------------ alerts


@router.get("/alerts/metrics", response_model=AlertMetrics)
async def alert_metrics(client: Client) -> Any:
    return await client.alert_metrics()


@router.get("/alerts/rules", response_model=AlertRules)
async def alert_rules(client: Client) -> Any:
    return await client.alert_rules()


@router.post("/alerts/rules", response_model=AlertRule, status_code=201)
async def create_alert_rule(client: Client, body: NewRule) -> Any:
    return await client.create_alert_rule(
        body.metric,
        body.op,
        body.threshold,
        ticker=body.ticker,
        event_ticker=body.event_ticker,
        cooldown_seconds=body.cooldown_seconds,
        name=body.name,
    )


@router.get("/alerts/rules/{rule_id}", response_model=AlertRule)
async def alert_rule(client: Client, rule_id: int) -> Any:
    return await client.alert_rule(rule_id)


@router.patch("/alerts/rules/{rule_id}", response_model=AlertRule)
async def update_alert_rule(client: Client, rule_id: int, body: RulePatch) -> Any:
    return await client.update_alert_rule(
        rule_id,
        enabled=body.enabled,
        threshold=body.threshold,
        cooldown_seconds=body.cooldown_seconds,
        name=body.name,
    )


@router.delete("/alerts/rules/{rule_id}", status_code=204)
async def delete_alert_rule(client: Client, rule_id: int) -> None:
    await client.delete_alert_rule(rule_id)


@router.get("/alerts", response_model=AlertPage)
async def alerts(
    client: Client, unacknowledged: bool = False, limit: Limit = None, cursor: Cursor = None
) -> Any:
    return await client.alerts_page(unacknowledged=unacknowledged, limit=limit, cursor=cursor)


@router.post("/alerts/{alert_id}/acknowledge", status_code=204)
async def acknowledge_alert(client: Client, alert_id: int) -> None:
    await client.acknowledge_alert(alert_id)


# ------------------------------------------------------------------ saved layouts


@router.get("/layouts", response_model=LayoutList)
async def layouts(client: Client) -> Any:
    return await client.layouts()


@router.get("/layouts/{name}", response_model=Layout)
async def layout(client: Client, name: LayoutName) -> Any:
    return await client.layout(name)


@router.put("/layouts/{name}", response_model=LayoutSaved)
async def save_layout(client: Client, name: LayoutName, body: LayoutBody) -> Any:
    return await client.save_layout(name, body.data)


@router.delete("/layouts/{name}", status_code=204)
async def delete_layout(client: Client, name: LayoutName) -> None:
    await client.delete_layout(name)


# ------------------------------------------------------------------ errors


def _json(status: int, error: str, detail: str, **extra: Any) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": error, "detail": detail, **extra})


def register_error_handlers(app: FastAPI) -> None:
    """Turn what the client library raises into HTTP answers the browser can act on."""

    @app.exception_handler(ServerUnavailable)
    async def _unavailable(_: Request, exc: ServerUnavailable) -> JSONResponse:
        return _json(503, "server_not_configured", str(exc))

    @app.exception_handler(NotFound)
    async def _not_found(_: Request, exc: NotFound) -> JSONResponse:
        return _json(404, "not_found", str(exc))

    @app.exception_handler(InvalidRequest)
    async def _invalid(_: Request, exc: InvalidRequest) -> JSONResponse:
        return _json(422, "rejected_by_server", str(exc), code=exc.code, extra=exc.extra)

    @app.exception_handler(Forbidden)
    async def _forbidden(_: Request, exc: Forbidden) -> JSONResponse:
        return _json(403, "forbidden_by_server", str(exc))

    @app.exception_handler(Unauthorized)
    async def _unauthorized(_: Request, exc: Unauthorized) -> JSONResponse:
        # 502, not 401: the browser's own token is fine, the *server* refused the backend's.
        return _json(502, "server_rejected_token", str(exc))

    @app.exception_handler(RateLimited)
    async def _limited(_: Request, exc: RateLimited) -> JSONResponse:
        return _json(429, "rate_limited", str(exc))

    @app.exception_handler(CertificateChanged)
    async def _cert(_: Request, exc: CertificateChanged) -> JSONResponse:
        return _json(502, "server_certificate_changed", str(exc))

    @app.exception_handler(ConnectionProblem)
    async def _connection(_: Request, exc: ConnectionProblem) -> JSONResponse:
        return _json(502, "server_unreachable", str(exc))

    @app.exception_handler(ApiProblem)
    async def _other(_: Request, exc: ApiProblem) -> JSONResponse:
        return _json(502, "server_error", str(exc))
