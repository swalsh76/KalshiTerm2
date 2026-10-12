"""The server API as typed async methods (PLAN §10.6).

One ``ServerClient`` holds one HTTP connection pool, the profile's token and TLS that trusts only
the pinned certificate, so the token is never sent to an untrusted or changed server. Reads
(``GET``) are retried on network errors and 5xx, with a short bounded backoff; a 429 is retried
only if the server asks for a short wait. Nothing that writes is ever retried silently.
"""

import asyncio
import ssl
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, TypeVar
from urllib.parse import urlsplit

import httpx
from pydantic import BaseModel, ValidationError

from kalshiterm_client import secrets
from kalshiterm_client.errors import (
    ApiProblem,
    CertificateChanged,
    ConnectionProblem,
    Forbidden,
    InvalidRequest,
    NotFound,
    RateLimited,
    ServerError,
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
    MarketDetail,
    MarketPage,
    MarketResults,
    Me,
    MetricHistory,
    Opportunities,
    OrderBook,
    StatusReport,
    TickPage,
    TradePage,
    Watchlist,
)
from kalshiterm_client.pinning import fetch_certificate, fingerprint_of_pem, trusting
from kalshiterm_client.profiles import Profile, ProfileError

M = TypeVar("M", bound=BaseModel)

RETRY_DELAYS: tuple[float, ...] = (0.5, 1.0, 2.0)  # seconds before each retry of a GET
MAX_RETRY_AFTER = 5.0  # a 429 asking for a longer wait is reported, not waited out
USER_AGENT = "kterm"


def ssl_cause(exc: BaseException) -> ssl.SSLCertVerificationError | None:
    seen: set[int] = set()
    while exc is not None and id(exc) not in seen:
        if isinstance(exc, ssl.SSLCertVerificationError):
            return exc
        seen.add(id(exc))
        exc = exc.__cause__ or exc.__context__  # type: ignore[assignment]
    return None


def _why(cause: ssl.SSLCertVerificationError) -> str:
    """OpenSSL's reason for the failure (not every error carries it)."""
    return str(getattr(cause, "verify_message", None) or cause)


async def explain_tls_failure(
    profile: Profile | None, cause: ssl.SSLCertVerificationError
) -> ConnectionProblem:
    """Tell a changed certificate (a security event) from an expired or mismatched one."""
    if profile is None:
        return ConnectionProblem(f"the server's certificate was rejected ({_why(cause)})")
    try:
        presented = fingerprint_of_pem(await asyncio.to_thread(fetch_certificate, profile.url))
    except ProfileError:
        presented = None
    if presented is not None and presented != profile.fingerprint:
        return CertificateChanged(
            f"THE CERTIFICATE OF {profile.name} HAS CHANGED. Nothing was sent.\n"
            f"  pinned:    {profile.fingerprint}\n"
            f"  presented: {presented}\n"
            "If you rotated the server's certificate, check the new fingerprint with the "
            "server's operator (`kterm-server cert show`), then run "
            f"`kterm --profile {profile.name} server trust`. If you did not, someone may be "
            "impersonating the server: do not trust it."
        )
    return ConnectionProblem(
        f"the pinned certificate is no longer acceptable ({_why(cause)}); the "
        "certificate is unchanged, so check the name in the URL against the certificate's "
        "names and its expiry (`kterm-server cert show`)"
    )


def _wire(params: dict[str, Any] | None) -> dict[str, str]:
    """Query parameters as the server wants them; ``None`` means "not given"."""
    out: dict[str, str] = {}
    for key, value in (params or {}).items():
        if value is None:
            continue
        if isinstance(value, datetime):
            if value.tzinfo is None:
                raise ValueError(f"{key} must be timezone-aware")
            out[key] = value.isoformat()
        elif isinstance(value, bool):
            out[key] = "true" if value else "false"
        else:
            out[key] = str(value)
    return out


def _present(body: dict[str, Any]) -> dict[str, Any]:
    """A request body without the fields that were not given."""
    return {k: v for k, v in body.items() if v is not None}


def decimal_text(name: str, value: Decimal | str) -> str:
    """A number for the wire, exactly. Floats are refused: a float is already not the number you
    meant (0.1), and the server would have to guess. At most six decimal places."""
    if isinstance(value, bool) or not isinstance(value, Decimal | str):  # (a float is neither)
        raise TypeError(
            f"{name}: give a Decimal or a string like '0.25', not {type(value).__name__}"
        )
    try:
        number = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"{name}: {value!r} is not a number") from exc
    if not number.is_finite():
        raise ValueError(f"{name}: must be a finite number")
    if number != number.quantize(Decimal("0.000001")):
        raise ValueError(f"{name}: at most six decimal places")
    return format(number, "f")


class ServerClient:
    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        verify: ssl.SSLContext | bool = True,
        profile: Profile | None = None,
        timeout: float = 10.0,
        transport: httpx.AsyncBaseTransport | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.base_url = base_url
        self._profile = profile
        self._name = profile.name if profile else base_url
        self._sleep = sleep
        self._http = httpx.AsyncClient(
            base_url=base_url,
            verify=verify,
            timeout=timeout,
            transport=transport,
            headers={"Authorization": f"Bearer {token}", "User-Agent": USER_AGENT},
        )

    @classmethod
    def for_profile(cls, profile: Profile, token: str | None = None, **kw: Any) -> "ServerClient":
        """A client for a saved server: the token from the keyring (or ``KTERM_TOKEN``) and, for
        https, TLS that trusts exactly the certificate pinned with ``kterm server trust``."""
        token = token or secrets.get_token(profile.name)
        if not token:
            raise ConnectionProblem(
                f"no token for {profile.name}: run `kterm config token {profile.name}`"
            )
        secure = urlsplit(profile.url).scheme == "https"
        verify: ssl.SSLContext | bool = trusting(profile) if secure else False
        return cls(profile.url, token, verify=verify, profile=profile, **kw)

    async def __aenter__(self) -> "ServerClient":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._http.aclose()

    # ------------------------------------------------------------------ transport

    async def _explain_tls(self, cause: ssl.SSLCertVerificationError) -> ConnectionProblem:
        return await explain_tls_failure(self._profile, cause)

    def _problem(self, response: httpx.Response) -> ApiProblem:
        """The error a failed response stands for."""
        try:
            body = response.json()
            code = str(body.get("error", "")) if isinstance(body, dict) else ""
        except ValueError:
            body, code = {}, ""
        extra = {k: v for k, v in body.items() if k != "error"} if isinstance(body, dict) else {}
        status = response.status_code
        if status == 401:
            return Unauthorized(
                status, code or "unauthorized",
                f"the server rejected the token for {self._name} (revoked, expired or wrong)",
            )  # fmt: skip
        if status == 403:
            return Forbidden(
                status, code or "forbidden", "that needs an admin token; this one is read-only"
            )  # noqa: E501
        if status == 404:
            return NotFound(
                status, code or "not_found", f"not found ({code or 'not_found'})", **extra
            )
        if status == 429:
            wait = response.headers.get("retry-after")
            return RateLimited(
                float(wait) if wait and wait.replace(".", "", 1).isdigit() else None,
                f"too many failed attempts from this address; retry in {wait or 'a minute'} s",
            )
        if status >= 500:
            return ServerError(status, code or "server_error", f"the server answered {status}")
        detail = f" ({', '.join(str(f) for f in extra['fields'])})" if extra.get("fields") else ""
        return InvalidRequest(
            status, code or "rejected", f"the server refused the request: {code}{detail}", **extra
        )  # noqa: E501

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any = None,
        retry: bool = False,
    ) -> Any:
        delays = RETRY_DELAYS if retry else ()
        attempt = 0
        while True:
            try:
                response = await self._http.request(method, path, params=_wire(params), json=json)
            except httpx.HTTPError as exc:
                cause = ssl_cause(exc)
                if cause is not None:  # never retried: a certificate problem will not go away
                    raise await self._explain_tls(cause) from exc
                if attempt < len(delays):
                    await self._sleep(delays[attempt])
                    attempt += 1
                    continue
                raise ConnectionProblem(
                    f"cannot reach {self.base_url}: {exc.__class__.__name__}"
                ) from exc
            if response.is_success:
                try:
                    return response.json()
                except ValueError as exc:
                    raise ConnectionProblem("the server's answer was not JSON") from exc
            problem = self._problem(response)
            transient = isinstance(problem, ServerError) or (
                isinstance(problem, RateLimited)
                and problem.retry_after is not None
                and problem.retry_after <= MAX_RETRY_AFTER
            )
            if transient and attempt < len(delays):
                wait = problem.retry_after if isinstance(problem, RateLimited) else None
                await self._sleep(wait if wait is not None else delays[attempt])
                attempt += 1
                continue
            raise problem

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        return await self._request("GET", path, params=params, retry=True)

    @staticmethod
    def _model(model: type[M], data: Any) -> M:
        try:
            return model.model_validate(data)
        except ValidationError as exc:
            fields = sorted({".".join(str(p) for p in e["loc"]) for e in exc.errors()})
            raise ConnectionProblem(
                f"the server's {model.__name__} is not what this client expects "
                f"({', '.join(fields[:5])}); is this client older than the server?"
            ) from exc

    # ------------------------------------------------------------------ who and how

    async def me(self) -> Me:
        return self._model(Me, await self._get("/v1/me"))

    async def status(self) -> StatusReport:
        """The operator's report (admin tokens only)."""
        data = await self._get("/v1/status")
        return self._model(
            StatusReport,
            {"time": data.get("time"), "problems": data.get("problems", []), "data": data},
        )

    # ------------------------------------------------------------------ reference data

    async def markets_page(
        self,
        *,
        status: str | None = None,
        event: str | None = None,
        series: str | None = None,
        q: str | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> MarketPage:
        params = {"status": status, "event": event, "series": series, "q": q, "limit": limit,
                  "cursor": cursor}  # fmt: skip
        return self._model(MarketPage, await self._get("/v1/markets", params))

    async def iter_markets(self, **filters: Any) -> AsyncIterator[Any]:
        async for page in self._pages(self.markets_page, filters):
            for market in page.markets:
                yield market

    async def market(self, ticker: str) -> MarketDetail:
        return self._model(MarketDetail, await self._get(f"/v1/markets/{ticker}"))

    async def event(self, event_ticker: str) -> Event:
        return self._model(Event, await self._get(f"/v1/events/{event_ticker}"))

    # ------------------------------------------------------------------ market data

    async def candles_page(
        self,
        ticker: str,
        *,
        interval: str = "1m",
        source: str = "trades",
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> CandlePage:
        """Bars, oldest first. Without ``start``: the most recent ``limit`` bars before ``end``
        (one page). With it: forward from ``start``, paged by ``next_cursor``."""
        params = {"interval": interval, "source": source, "start": start, "end": end,
                  "limit": limit, "cursor": cursor}  # fmt: skip
        return self._model(CandlePage, await self._get(f"/v1/markets/{ticker}/candles", params))

    async def iter_candles(self, ticker: str, **kw: Any) -> AsyncIterator[Any]:
        async for page in self._pages(lambda **p: self.candles_page(ticker, **p), kw):
            for bar in page.bars:
                yield bar

    async def trades_page(
        self,
        ticker: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> TradePage:
        """Trades, oldest first. Without ``start``: the most recent ``limit`` before ``end``."""
        params = {"start": start, "end": end, "limit": limit, "cursor": cursor}
        return self._model(TradePage, await self._get(f"/v1/markets/{ticker}/trades", params))

    async def iter_trades(self, ticker: str, **kw: Any) -> AsyncIterator[Any]:
        async for page in self._pages(lambda **p: self.trades_page(ticker, **p), kw):
            for trade in page.trades:
                yield trade

    async def ticks_page(
        self,
        ticker: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> TickPage:
        params = {"start": start, "end": end, "limit": limit, "cursor": cursor}
        return self._model(TickPage, await self._get(f"/v1/markets/{ticker}/ticks", params))

    async def iter_ticks(self, ticker: str, **kw: Any) -> AsyncIterator[Any]:
        async for page in self._pages(lambda **p: self.ticks_page(ticker, **p), kw):
            for tick in page.ticks:
                yield tick

    async def orderbook(
        self, ticker: str, *, at: datetime | None = None, depth: int | None = None
    ) -> OrderBook:
        """The book at a moment (default now). Only markets that have been watched have one."""
        params = {"at": at, "depth": depth}
        return self._model(OrderBook, await self._get(f"/v1/markets/{ticker}/orderbook", params))

    async def gaps(
        self,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
    ) -> list[Gap]:
        """Periods the live stream may have missed, newest first."""
        data = await self._get("/v1/gaps", {"start": start, "end": end, "limit": limit})
        return [self._model(Gap, g) for g in data["gaps"]]

    # ------------------------------------------------------------------ your watchlist

    async def watchlist(self) -> Watchlist:
        return self._model(Watchlist, await self._get("/v1/watchlist"))

    async def watch(self, ticker: str) -> bool:
        """Put a market on your list (the server then captures its orderbook). True if it was
        added, False if it was already there. Not retried if the connection drops."""
        data = await self._request("PUT", f"/v1/watchlist/{ticker}")
        return bool(data["added"])

    async def unwatch(self, ticker: str) -> bool:
        """Take a market off your list. True if it was there."""
        data = await self._request("DELETE", f"/v1/watchlist/{ticker}")
        return bool(data["removed"])

    # ------------------------------------------------------------------ analytics

    async def market_results(
        self, ticker: str, *, max_age_seconds: int | None = None
    ) -> MarketResults:
        """The newest value of every metric computed for a market (watched markets only)."""
        params = {"max_age_seconds": max_age_seconds}
        return self._model(
            MarketResults, await self._get(f"/v1/analytics/markets/{ticker}", params)
        )

    async def market_history_page(
        self,
        ticker: str,
        metric: str,
        *,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> MetricHistory:
        """One metric (``"analyzer.metric"``) over time, newest first."""
        params = {
            "metric": metric,
            "since": since,
            "until": until,
            "limit": limit,
            "cursor": cursor,
        }
        return self._model(
            MetricHistory, await self._get(f"/v1/analytics/markets/{ticker}/history", params)
        )

    async def iter_market_history(self, ticker: str, metric: str, **kw: Any) -> AsyncIterator[Any]:
        async for page in self._pages(
            lambda **p: self.market_history_page(ticker, metric, **p), kw
        ):
            for point in page.points:
                yield point

    async def event_results(
        self, event_ticker: str, *, max_age_seconds: int | None = None
    ) -> EventResults:
        params = {"max_age_seconds": max_age_seconds}
        return self._model(
            EventResults, await self._get(f"/v1/analytics/events/{event_ticker}", params)
        )

    async def arbitrage(
        self,
        *,
        min_profit: Decimal | str | None = None,
        max_age_seconds: int | None = None,
        limit: int | None = None,
    ) -> Opportunities:
        """Events whose YES bids sum to more than $1 right now, best profit first. ``profit`` is
        after Kalshi's taker fees and is often negative: pass ``min_profit=Decimal(0)`` to see only
        the ones that would make money. An indication, not an order book."""
        params = {
            "min_profit": None if min_profit is None else decimal_text("min_profit", min_profit),
            "max_age_seconds": max_age_seconds,
            "limit": limit,
        }
        return self._model(Opportunities, await self._get("/v1/analytics/arbitrage", params))

    # ------------------------------------------------------------------ alerts

    async def alert_metrics(self) -> AlertMetrics:
        """What alert rules can be set on, with units and the comparisons each allows."""
        return self._model(AlertMetrics, await self._get("/v1/alert-metrics"))

    async def create_alert_rule(
        self,
        metric: str,
        op: str,
        threshold: Decimal | str,
        *,
        ticker: str | None = None,
        event_ticker: str | None = None,
        cooldown_seconds: int | None = None,
        name: str = "",
    ) -> AlertRule:
        """A rule on ``"analyzer.metric"`` of a market (``ticker``, which must be on your
        watchlist) or an event (``event_ticker``). ``threshold`` is in the metric's unit. Not
        retried if the connection drops: check ``alert_rules()`` before trying again."""
        body = {
            "metric": metric,
            "op": op,
            "threshold": decimal_text("threshold", threshold),
            "ticker": ticker,
            "event_ticker": event_ticker,
            "cooldown_seconds": cooldown_seconds,
            "name": name,
        }
        data = await self._request("POST", "/v1/alert-rules", json=_present(body))
        return self._model(AlertRule, data)

    async def alert_rules(self) -> AlertRules:
        return self._model(AlertRules, await self._get("/v1/alert-rules"))

    async def alert_rule(self, rule_id: int) -> AlertRule:
        return self._model(AlertRule, await self._get(f"/v1/alert-rules/{rule_id}"))

    async def update_alert_rule(
        self,
        rule_id: int,
        *,
        enabled: bool | None = None,
        threshold: Decimal | str | None = None,
        cooldown_seconds: int | None = None,
        name: str | None = None,
    ) -> AlertRule:
        """Change only what is given. A new threshold, or enabling a rule, re-arms it."""
        body = {
            "enabled": enabled,
            "threshold": None if threshold is None else decimal_text("threshold", threshold),
            "cooldown_seconds": cooldown_seconds,
            "name": name,
        }
        data = await self._request("PATCH", f"/v1/alert-rules/{rule_id}", json=_present(body))
        return self._model(AlertRule, data)

    async def delete_alert_rule(self, rule_id: int) -> None:
        await self._request("DELETE", f"/v1/alert-rules/{rule_id}")

    async def alerts_page(
        self, *, unacknowledged: bool = False, limit: int | None = None, cursor: str | None = None
    ) -> AlertPage:
        """Your alerts, newest first."""
        params = {"unacknowledged": unacknowledged or None, "limit": limit, "cursor": cursor}
        return self._model(AlertPage, await self._get("/v1/alerts", params))

    async def iter_alerts(self, **kw: Any) -> AsyncIterator[Any]:
        async for page in self._pages(self.alerts_page, kw):
            for alert in page.alerts:
                yield alert

    async def acknowledge_alert(self, alert_id: int) -> None:
        """Idempotent: acknowledging twice keeps the first time."""
        await self._request("POST", f"/v1/alerts/{alert_id}/acknowledge")

    # ------------------------------------------------------------------ paging

    @staticmethod
    async def _pages(
        fetch: Callable[..., Awaitable[Any]], params: dict[str, Any]
    ) -> AsyncIterator[Any]:
        """Pages from ``fetch(**params)``, following ``next_cursor`` until it is None. A fetch
        that returns no cursor (the "latest N" form of candles / trades) is a single page."""
        cursor: str | None = None
        while True:
            page = await fetch(**params, cursor=cursor)
            yield page
            cursor = page.next_cursor
            if cursor is None:
                return
