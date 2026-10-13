"""The write side of the Kalshi API: orders, cancels, amends, shard transfers.

This is deliberately a separate client from ``KalshiRestClient`` (which stays GET-only, guard
and all). Four layers keep a stray write from happening:

1. It will not exist without a signer made by ``KalshiSigner.for_trading`` (the trading key).
2. It refuses the production environment unless the caller passes ``allow_production=True``
   (the client's order service does that only when ``risk.toml`` enables it).
3. A request hook allows only the routes listed in ``ROUTES``: nothing else can be sent, however
   the code is changed later (no withdrawals, no account changes).
4. In the client, only an ``ApprovedOrder`` from the risk layer reaches ``create_order`` at all.

**A write is never retried when its outcome is ambiguous.** After a timeout, a dropped
connection or a 5xx, the order may or may not exist, so ``create_order`` looks it up by the
``client_order_id`` it always sends (listing orders since just before the attempt). It returns
the order if found and otherwise raises ``WriteOutcomeUnknown``: the caller must reconcile and
must never reuse that id. A 429 means Kalshi rejected the request unprocessed, so it is the one
case retried for every write (bounded, with backoff). Cancels and transfers are idempotent
(a cancel's end state is the same; a transfer carries ``client_transfer_id``), so they are also
retried after ambiguous failures.

Money is exact: prices and counts are ``Decimal`` (a ``float`` is a ``TypeError``) and are sent
as fixed-point strings; more decimal places than Kalshi accepts is a ``ValueError``, never a
silent round.
"""

import asyncio
import re
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from types import TracebackType
from typing import Any, Self
from urllib.parse import urlparse

import httpx

from kalshi_core.auth import KalshiSigner
from kalshi_core.config import KalshiSettings
from kalshi_core.models import KalshiModel
from kalshi_core.portfolio import Order
from kalshi_core.ratelimit import TokenBucket, write_bucket
from kalshi_core.rest import KalshiAPIError

# Token costs on the Basic tier, from GET /account/endpoint_costs (2026-10-13): the default is
# 10; every cancel form is 2 (batch cancel per order). Amend, decrease and transfer use the default.
DEFAULT_COST = 10
CANCEL_COST = 2

ROUTES: tuple[tuple[str, re.Pattern[str]], ...] = tuple(
    (method, re.compile(pattern))
    for method, pattern in (
        ("POST", r"/portfolio/events/orders"),
        ("DELETE", r"/portfolio/events/orders"),
        ("DELETE", r"/portfolio/events/orders/batched"),
        ("DELETE", r"/portfolio/events/orders/[^/]+"),
        ("POST", r"/portfolio/events/orders/[^/]+/amend"),
        ("POST", r"/portfolio/events/orders/[^/]+/decrease"),
        ("POST", r"/portfolio/intra_exchange_instance_transfer"),
        ("GET", r"/portfolio/orders"),  # only to find an order after an ambiguous answer
    )
)
_RESOLVE_DELAYS = (0.5, 1.0, 2.0)
_RATE_LIMIT_DELAYS = (0.5, 1.0, 2.0)
_AMBIGUOUS_STATUSES = frozenset({500, 502, 503, 504})


class TradingRefused(Exception):
    """The trading client was asked to do something it is built never to do."""


class WriteOutcomeUnknown(Exception):
    """A write got no clear answer and could not be resolved by looking. It may have happened.

    ``kind`` says what ("create_order", "amend_order", ...), ``ref`` is the id to reconcile by
    (the ``client_order_id`` or the order id). Never retry with a new id before looking.
    """

    def __init__(self, kind: str, ref: str) -> None:
        super().__init__(f"{kind} {ref}: no clear answer from Kalshi; it may have taken effect")
        self.kind = kind
        self.ref = ref


class Side(StrEnum):
    """The side of the YES book (Create Order V2 has no yes / no field)."""

    BID = "bid"
    ASK = "ask"


class TimeInForce(StrEnum):
    GOOD_TILL_CANCELED = "good_till_canceled"
    IMMEDIATE_OR_CANCEL = "immediate_or_cancel"
    FILL_OR_KILL = "fill_or_kill"


class SelfTradePrevention(StrEnum):
    TAKER_AT_CROSS = "taker_at_cross"
    MAKER = "maker"


def new_client_order_id() -> str:
    return str(uuid.uuid4())


def fixed_point(value: Decimal, places: int, what: str) -> str:
    """``value`` with exactly ``places`` decimals; refuses a float or anything finer."""
    if not isinstance(value, Decimal):
        raise TypeError(f"{what} must be a Decimal, not {type(value).__name__}")
    if not value.is_finite() or value < 0:
        raise ValueError(f"{what} must be a finite, non-negative number")
    quantized = value.quantize(Decimal(1).scaleb(-places))
    if quantized != value:
        raise ValueError(f"{what} {value} has more than {places} decimal places")
    return f"{quantized:.{places}f}"


@dataclass(frozen=True)
class CreateOrder:
    ticker: str
    side: Side
    count: Decimal
    price: Decimal  # dollars on the YES book
    exchange_index: int
    client_order_id: str
    time_in_force: TimeInForce = TimeInForce.GOOD_TILL_CANCELED
    self_trade_prevention: SelfTradePrevention = SelfTradePrevention.TAKER_AT_CROSS
    post_only: bool = False
    reduce_only: bool = False
    expiration_time: int | None = None  # unix seconds, good_till_canceled only

    def payload(self) -> dict[str, Any]:
        if not self.client_order_id:
            raise ValueError("client_order_id is required: it is how a lost answer is resolved")
        if not 0 < self.price < 1:
            raise ValueError("price must be strictly between $0 and $1")
        if self.count <= 0:
            raise ValueError("count must be positive")
        if self.reduce_only and self.time_in_force is not TimeInForce.IMMEDIATE_OR_CANCEL:
            raise ValueError("reduce_only requires immediate_or_cancel")
        if self.expiration_time is not None and (
            self.time_in_force is not TimeInForce.GOOD_TILL_CANCELED
        ):
            raise ValueError("expiration_time only applies to good_till_canceled orders")
        body: dict[str, Any] = {
            "ticker": self.ticker,
            "client_order_id": self.client_order_id,
            "side": self.side.value,
            "count": fixed_point(self.count, 2, "count"),
            "price": fixed_point(self.price, 4, "price"),
            "time_in_force": self.time_in_force.value,
            "self_trade_prevention_type": self.self_trade_prevention.value,
            "exchange_index": self.exchange_index,
        }
        if self.post_only:
            body["post_only"] = True
        if self.reduce_only:
            body["reduce_only"] = True
        if self.expiration_time is not None:
            body["expiration_time"] = self.expiration_time
        return body


class CreatedOrder(KalshiModel):
    order_id: str
    client_order_id: str | None = None
    fill_count: Decimal
    remaining_count: Decimal
    average_fill_price: Decimal | None = None
    average_fee_paid: Decimal | None = None
    ts_ms: int | None = None
    recovered: bool = False  # found by looking after an ambiguous answer


class AmendedOrder(KalshiModel):
    order_id: str
    client_order_id: str | None = None
    remaining_count: Decimal | None = None
    fill_count: Decimal | None = None
    average_fill_price: Decimal | None = None
    average_fee_paid: Decimal | None = None
    ts_ms: int | None = None


class DecreasedOrder(KalshiModel):
    order_id: str
    client_order_id: str | None = None
    remaining_count: Decimal
    ts_ms: int | None = None


class CancelledOrder(KalshiModel):
    order_id: str
    client_order_id: str | None = None
    reduced_by: Decimal | None = None
    ts_ms: int | None = None
    error: dict[str, Any] | None = None  # batch cancel reports failures per order
    already_gone: bool = False  # a retry found it already cancelled / filled


@dataclass(frozen=True)
class CancelTarget:
    order_id: str
    exchange_index: int
    market_ticker: str


class _Ambiguous(Exception):
    pass


class KalshiTradingClient:
    def __init__(
        self,
        settings: KalshiSettings,
        signer: KalshiSigner,
        *,
        allow_production: bool = False,
        bucket: TokenBucket | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        wall_clock: Callable[[], float] = time.time,
        timeout: float = 30.0,
    ) -> None:
        if signer.purpose != "trading":
            raise TradingRefused("orders must be signed with the trading key (for_trading)")
        if settings.is_production and not allow_production:
            raise TradingRefused("production trading is not enabled for this client")
        self._base = settings.rest_url
        self._base_path = urlparse(self._base).path
        self._signer = signer
        self._bucket = bucket or write_bucket(settings)
        self._sleep = sleep
        self._wall_clock = wall_clock
        self._http = httpx.AsyncClient(
            transport=transport, timeout=timeout, event_hooks={"request": [self._route_guard]}
        )

    async def _route_guard(self, request: httpx.Request) -> None:
        path = request.url.path.removeprefix(self._base_path)
        if not any(m == request.method and p.fullmatch(path) for m, p in ROUTES):
            raise TradingRefused(f"{request.method} {path} is not an allowed trading route")

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

    async def _send(
        self,
        method: str,
        path: str,
        *,
        cost: float,
        json: Any = None,
        params: dict[str, Any] | None = None,
        idempotent: bool,
    ) -> httpx.Response:
        """Send one request. 429 is retried; transport errors and 5xx only if ``idempotent``,
        otherwise they raise ``_Ambiguous``. Any other non-2xx raises ``KalshiAPIError``."""
        clean = {k: v for k, v in (params or {}).items() if v is not None}
        rate_limited = 0
        errors = 0
        while True:
            await self._bucket.acquire(cost)
            headers = self._signer.headers(method, self._base_path + path)
            try:
                resp = await self._http.request(
                    method, self._base + path, json=json, params=clean, headers=headers
                )
            except httpx.TransportError:
                if not idempotent:
                    raise _Ambiguous from None
                errors += 1
                if errors > len(_RESOLVE_DELAYS):
                    raise
                await self._sleep(_RESOLVE_DELAYS[errors - 1])
                continue
            if resp.status_code < 300:
                return resp
            if resp.status_code == 429 and rate_limited < len(_RATE_LIMIT_DELAYS):
                await self._sleep(_RATE_LIMIT_DELAYS[rate_limited])
                rate_limited += 1
                continue
            if resp.status_code in _AMBIGUOUS_STATUSES:
                if not idempotent:
                    raise _Ambiguous
                errors += 1
                if errors <= len(_RESOLVE_DELAYS):
                    await self._sleep(_RESOLVE_DELAYS[errors - 1])
                    continue
            raise KalshiAPIError(resp.status_code, resp.text)

    # --- orders -------------------------------------------------------------------------------

    async def create_order(self, order: CreateOrder) -> CreatedOrder:
        body = order.payload()
        since = int(self._wall_clock()) - 5
        try:
            resp = await self._send(
                "POST", "/portfolio/events/orders", cost=DEFAULT_COST, json=body, idempotent=False
            )
        except _Ambiguous:
            found = await self._resolve(order, since)
            if found is None:
                raise WriteOutcomeUnknown("create_order", order.client_order_id) from None
            return found
        except KalshiAPIError as exc:
            if exc.status == 409 and (found := await self._resolve(order, since)):
                return found
            raise
        return CreatedOrder.model_validate(resp.json())

    async def _resolve(self, order: CreateOrder, since: int) -> CreatedOrder | None:
        """Look for the order by ``client_order_id``, a few times with growing pauses."""
        for delay in (0.0, *_RESOLVE_DELAYS):
            if delay:
                await self._sleep(delay)
            found = await self.find_order(order.client_order_id, order.ticker, since)
            if found is not None:
                return CreatedOrder(
                    order_id=found.order_id,
                    client_order_id=found.client_order_id,
                    fill_count=found.fill_count_fp,
                    remaining_count=found.remaining_count_fp,
                    recovered=True,
                )
        return None

    async def find_order(self, client_order_id: str, ticker: str, since: int) -> Order | None:
        """The order with this ``client_order_id``, created at or after unix time ``since``."""
        cursor: str | None = None
        while True:
            resp = await self._send(
                "GET",
                "/portfolio/orders",
                cost=DEFAULT_COST,
                params={"ticker": ticker, "min_ts": since, "limit": 1000, "cursor": cursor},
                idempotent=True,
            )
            payload = resp.json()
            for raw in payload.get("orders", []):
                if raw.get("client_order_id") == client_order_id:
                    return Order.model_validate(raw)
            cursor = payload.get("cursor") or None
            if cursor is None:
                return None

    async def cancel_order(self, target: CancelTarget) -> CancelledOrder:
        """Cancel one order. A 404 after an earlier ambiguous attempt means it is already gone."""
        path = f"/portfolio/events/orders/{target.order_id}"
        params = {"exchange_index": target.exchange_index, "market_ticker": target.market_ticker}
        attempts = 0
        while True:
            attempts += 1
            try:
                resp = await self._send(
                    "DELETE", path, cost=CANCEL_COST, params=params, idempotent=False
                )
            except _Ambiguous:
                if attempts > len(_RESOLVE_DELAYS):
                    raise WriteOutcomeUnknown("cancel_order", target.order_id) from None
                await self._sleep(_RESOLVE_DELAYS[attempts - 1])
                continue
            except KalshiAPIError as exc:
                if exc.status == 404 and attempts > 1:
                    return CancelledOrder(order_id=target.order_id, already_gone=True)
                raise
            return CancelledOrder.model_validate(resp.json())

    async def cancel_orders(self, targets: list[CancelTarget]) -> list[CancelledOrder]:
        """Cancel several orders in one request; each order reports its own error."""
        if not targets:
            return []
        body = {
            "orders": [
                {
                    "order_id": t.order_id,
                    "exchange_index": t.exchange_index,
                    "market_ticker": t.market_ticker,
                }
                for t in targets
            ]
        }
        resp = await self._send(
            "DELETE",
            "/portfolio/events/orders/batched",
            cost=CANCEL_COST * len(targets),
            json=body,
            idempotent=True,
        )
        return [CancelledOrder.model_validate(o) for o in resp.json().get("orders", [])]

    async def cancel_all(self) -> None:
        """Cancel every resting order in the account (all shards). Kalshi also cancels orders
        placed in the minute after the call, so callers must look again afterwards."""
        await self._send("DELETE", "/portfolio/events/orders", cost=CANCEL_COST, idempotent=True)

    async def amend_order(
        self,
        order_id: str,
        *,
        ticker: str,
        side: Side,
        price: Decimal,
        count: Decimal,
        exchange_index: int,
    ) -> AmendedOrder:
        """Set a resting order's price and total count. Changing price or raising the count
        costs its queue position; lowering the count does not."""
        body = {
            "ticker": ticker,
            "side": side.value,
            "price": fixed_point(price, 4, "price"),
            "count": fixed_point(count, 2, "count"),
            "exchange_index": exchange_index,
        }
        try:
            resp = await self._send(
                "POST",
                f"/portfolio/events/orders/{order_id}/amend",
                cost=DEFAULT_COST,
                json=body,
                idempotent=False,
            )
        except _Ambiguous:
            raise WriteOutcomeUnknown("amend_order", order_id) from None
        return AmendedOrder.model_validate(resp.json())

    async def decrease_order(
        self,
        order_id: str,
        *,
        exchange_index: int,
        market_ticker: str,
        reduce_by: Decimal | None = None,
        reduce_to: Decimal | None = None,
    ) -> DecreasedOrder:
        """Shrink a resting order. Give exactly one of ``reduce_by`` / ``reduce_to``."""
        if (reduce_by is None) == (reduce_to is None):
            raise ValueError("give exactly one of reduce_by and reduce_to")
        body: dict[str, Any] = {"exchange_index": exchange_index, "market_ticker": market_ticker}
        if reduce_by is not None:
            body["reduce_by"] = fixed_point(reduce_by, 2, "reduce_by")
        if reduce_to is not None:
            body["reduce_to"] = fixed_point(reduce_to, 2, "reduce_to")
        try:
            resp = await self._send(
                "POST",
                f"/portfolio/events/orders/{order_id}/decrease",
                cost=DEFAULT_COST,
                json=body,
                idempotent=False,
            )
        except _Ambiguous:
            raise WriteOutcomeUnknown("decrease_order", order_id) from None
        return DecreasedOrder.model_validate(resp.json())

    # --- money between shards -----------------------------------------------------------------

    async def transfer(
        self, *, source_shard: int, destination_shard: int, amount_e6: int, transfer_id: str
    ) -> str:
        """Move ``amount_e6`` (millionths of a dollar, a whole number of centicents) between two
        exchange shards of this account. ``transfer_id`` makes a retry safe. Returns Kalshi's id."""
        if source_shard == destination_shard:
            raise ValueError("source and destination shard are the same")
        if amount_e6 <= 0 or amount_e6 % 100:
            raise ValueError("the amount must be positive and a whole number of centicents")
        body = {
            "source": "event_contract",
            "destination": "event_contract",
            "amount": amount_e6 // 100,
            "client_transfer_id": transfer_id,
            "source_exchange_shard": source_shard,
            "destination_exchange_shard": destination_shard,
        }
        resp = await self._send(
            "POST",
            "/portfolio/intra_exchange_instance_transfer",
            cost=DEFAULT_COST,
            json=body,
            idempotent=True,
        )
        return str(resp.json()["transfer_id"])
