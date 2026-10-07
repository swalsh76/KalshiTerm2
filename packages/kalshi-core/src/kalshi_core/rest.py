"""Async, read-only REST client for the Kalshi trade API.

Read-only is enforced structurally: no write endpoints exist, and a transport-level guard
rejects any non-GET request. Phase 6 must remove that guard deliberately.
"""

import asyncio
import logging
import random
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from types import TracebackType
from typing import Any, Self
from urllib.parse import urlparse

import httpx

from kalshi_core.auth import KalshiSigner
from kalshi_core.clock import (
    DEFAULT_THRESHOLD,
    ClockCheckError,
    ClockSample,
    ClockSkew,
    estimate_skew,
    parse_http_date,
)
from kalshi_core.config import KalshiSettings
from kalshi_core.models import (
    Event,
    EventsPage,
    ExchangeStatus,
    Market,
    MarketsPage,
    OrderBook,
    Series,
    SeriesList,
    Trade,
    TradesPage,
)
from kalshi_core.ratelimit import TokenBucket, read_bucket

log = logging.getLogger("kalshi_core")

_RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
_BACKOFF_BASE = 0.5
_BACKOFF_MAX = 8.0


class KalshiAPIError(Exception):
    def __init__(self, status: int, body: str) -> None:
        super().__init__(f"Kalshi API error {status}: {body[:200]}")
        self.status = status
        self.body = body


class ReadOnlyViolation(Exception):
    """A non-GET request was attempted while the client is read-only."""


async def _read_only_guard(request: httpx.Request) -> None:
    if request.method != "GET":
        raise ReadOnlyViolation(f"{request.method} {request.url.path} blocked: client is read-only")


class KalshiRestClient:
    def __init__(
        self,
        settings: KalshiSettings,
        *,
        signer: KalshiSigner | None = None,
        bucket: TokenBucket | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        wall_clock: Callable[[], float] = time.time,
        max_retries: int = 5,
        timeout: float = 30.0,
    ) -> None:
        self._base = settings.rest_url
        self._base_path = urlparse(self._base).path
        self._signer = signer
        self._bucket = bucket or read_bucket(settings)
        self._sleep = sleep
        self._wall_clock = wall_clock
        self._max_retries = max_retries
        self._http = httpx.AsyncClient(
            transport=transport,
            timeout=timeout,
            event_hooks={"request": [_read_only_guard]},
        )
        if settings.is_production:
            log.warning("Kalshi client using PRODUCTION environment (read-only)")

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._http.aclose()

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = (await self._get_response(path, params)).json()
        return payload

    async def _get_response(
        self, path: str, params: dict[str, Any] | None = None
    ) -> httpx.Response:
        clean = {k: v for k, v in (params or {}).items() if v is not None}
        for attempt in range(self._max_retries + 1):
            await self._bucket.acquire()
            headers = self._signer.headers("GET", self._base_path + path) if self._signer else {}
            try:
                resp = await self._http.get(self._base + path, params=clean, headers=headers)
            except httpx.TransportError:
                if attempt == self._max_retries:
                    raise
            else:
                if resp.status_code == 200:
                    return resp
                if resp.status_code not in _RETRY_STATUSES or attempt == self._max_retries:
                    raise KalshiAPIError(resp.status_code, resp.text)
            await self._sleep(self._backoff(attempt))
        raise AssertionError("unreachable")

    @staticmethod
    def _backoff(attempt: int) -> float:
        delay = min(_BACKOFF_MAX, _BACKOFF_BASE * (2.0**attempt))
        return delay * random.uniform(0.5, 1.0)  # noqa: S311 - jitter, not security

    async def clock_skew(
        self, *, samples: int = 5, spacing: float = 0.25, threshold: float = DEFAULT_THRESHOLD
    ) -> ClockSkew:
        """Estimate how far the local clock is from Kalshi's (see ``kalshi_core.clock``)."""
        taken: list[ClockSample] = []
        for index in range(samples):
            if index:
                await self._sleep(spacing)
            sent = self._wall_clock()
            response = await self._get_response("/exchange/status")
            received = self._wall_clock()
            date = response.headers.get("date")
            if date is None:
                raise ClockCheckError("response had no Date header")
            age = response.headers.get("age", "0")
            server = parse_http_date(date) + (int(age) if age.isdigit() else 0)
            taken.append(ClockSample(sent, received, server))
        return estimate_skew(taken, threshold)

    async def exchange_status(self) -> ExchangeStatus:
        return ExchangeStatus.model_validate(await self._get("/exchange/status"))

    async def _iter_items(
        self,
        path: str,
        page_model: type[MarketsPage] | type[EventsPage] | type[TradesPage],
        attr: str,
        params: Any,
    ) -> AsyncIterator[Any]:
        """Follow Kalshi's cursor pagination until it returns an empty cursor."""
        cursor: str | None = None
        while True:
            page = page_model.model_validate(await self._get(path, {**params, "cursor": cursor}))
            for item in getattr(page, attr):
                yield item
            if not page.cursor:
                return
            cursor = page.cursor

    async def markets_page(self, **params: Any) -> MarketsPage:
        """One page of markets. Unfiltered results include multivariate (combo) markets;
        pass ``mve_filter="exclude"`` or ``"only"`` to split them."""
        return MarketsPage.model_validate(await self._get("/markets", params))

    async def iter_markets(self, **params: Any) -> AsyncIterator[Market]:
        async for market in self._iter_items("/markets", MarketsPage, "markets", params):
            yield market

    async def iter_trades(self, **params: Any) -> AsyncIterator[Trade]:
        """Executed trades across all markets; filter with ``min_ts`` / ``max_ts`` (epoch
        seconds) and optionally ``ticker``. Used to backfill what a disconnect missed."""
        async for trade in self._iter_items("/markets/trades", TradesPage, "trades", params):
            yield trade

    async def events_page(self, **params: Any) -> EventsPage:
        """One page of ordinary events (Kalshi excludes multivariate events here)."""
        return EventsPage.model_validate(await self._get("/events", params))

    async def iter_events(self, **params: Any) -> AsyncIterator[Event]:
        async for event in self._iter_items("/events", EventsPage, "events", params):
            yield event

    async def multivariate_events_page(self, **params: Any) -> EventsPage:
        """One page of multivariate (combo) events; filter by ``series_ticker`` or
        ``collection_ticker`` (not both)."""
        return EventsPage.model_validate(await self._get("/events/multivariate", params))

    async def iter_multivariate_events(self, **params: Any) -> AsyncIterator[Event]:
        async for event in self._iter_items("/events/multivariate", EventsPage, "events", params):
            yield event

    async def series_list(self, **params: Any) -> list[Series]:
        return SeriesList.model_validate(await self._get("/series", params)).series

    async def orderbook(self, ticker: str, *, depth: int = 0) -> OrderBook:
        payload = await self._get(f"/markets/{ticker}/orderbook", {"depth": depth})
        return OrderBook.from_response(payload)
