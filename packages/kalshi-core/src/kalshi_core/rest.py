"""Async, read-only REST client for the Kalshi trade API.

Read-only is enforced structurally: no write endpoints exist, and a transport-level guard
rejects any non-GET request. Phase 6 must remove that guard deliberately.
"""

import asyncio
import logging
import random
from collections.abc import AsyncIterator, Awaitable, Callable
from types import TracebackType
from typing import Any, Self
from urllib.parse import urlparse

import httpx

from kalshi_core.auth import KalshiSigner
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
        max_retries: int = 5,
        timeout: float = 30.0,
    ) -> None:
        self._base = settings.rest_url
        self._base_path = urlparse(self._base).path
        self._signer = signer
        self._bucket = bucket or read_bucket(settings)
        self._sleep = sleep
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
                    payload: dict[str, Any] = resp.json()
                    return payload
                if resp.status_code not in _RETRY_STATUSES or attempt == self._max_retries:
                    raise KalshiAPIError(resp.status_code, resp.text)
            await self._sleep(self._backoff(attempt))
        raise AssertionError("unreachable")

    @staticmethod
    def _backoff(attempt: int) -> float:
        delay = min(_BACKOFF_MAX, _BACKOFF_BASE * (2.0**attempt))
        return delay * random.uniform(0.5, 1.0)  # noqa: S311 - jitter, not security

    async def exchange_status(self) -> ExchangeStatus:
        return ExchangeStatus.model_validate(await self._get("/exchange/status"))

    async def markets_page(self, **params: Any) -> MarketsPage:
        return MarketsPage.model_validate(await self._get("/markets", params))

    async def iter_markets(self, **params: Any) -> AsyncIterator[Market]:
        cursor: str | None = None
        while True:
            page = await self.markets_page(cursor=cursor, **params)
            for market in page.markets:
                yield market
            if not page.cursor:
                return
            cursor = page.cursor

    async def events_page(self, **params: Any) -> EventsPage:
        return EventsPage.model_validate(await self._get("/events", params))

    async def iter_events(self, **params: Any) -> AsyncIterator[Event]:
        cursor: str | None = None
        while True:
            page = await self.events_page(cursor=cursor, **params)
            for event in page.events:
                yield event
            if not page.cursor:
                return
            cursor = page.cursor

    async def series_list(self, **params: Any) -> list[Series]:
        return SeriesList.model_validate(await self._get("/series", params)).series

    async def orderbook(self, ticker: str, *, depth: int = 0) -> OrderBook:
        payload = await self._get(f"/markets/{ticker}/orderbook", {"depth": depth})
        return OrderBook.from_response(payload)
