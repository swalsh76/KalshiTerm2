"""Fakes shared by the account and order-service tests: Kalshi's REST and private WebSocket."""

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from kalshi_core.models import Event, ExchangeStatus, Market, Series
from kalshi_core.portfolio import Balance, Fill, MarketPosition, Order
from kalshi_core.rest import KalshiAPIError
from kalshi_core.trading import (
    AmendedOrder,
    CancelledOrder,
    CancelTarget,
    CreatedOrder,
    CreateOrder,
)
from kalshi_core.ws_models import WsMessage
from kalshiterm_client.account import AccountTracker
from kalshiterm_client.trading_state import TradingState

T0 = datetime(2026, 10, 13, 12, 0, tzinfo=UTC).timestamp()


def status(transfers: bool | None = True) -> ExchangeStatus:
    rows = []
    for i in range(4):
        row: dict[str, Any] = {
            "exchange_index": i,
            "description": f"s{i}",
            "exchange_active": True,
            "trading_active": i != 2,  # shard 2 is not trading
        }
        if transfers is not None:
            row["intra_exchange_transfers_active"] = transfers
        rows.append(row)
    return ExchangeStatus.model_validate(
        {"exchange_active": True, "trading_active": True, "exchange_index_statuses": rows}
    )


def balance(*amounts: str) -> Balance:
    total = sum(Decimal(a) for a in amounts)
    return Balance.model_validate(
        {
            "balance": int(total * 100),
            "balance_dollars": str(total),
            "balance_breakdown": [
                {"exchange_index": i, "balance": a} for i, a in enumerate(amounts)
            ],
        }
    )


def position(ticker: str, count: str, exposure: str = "0", realized: str = "0") -> MarketPosition:
    return MarketPosition.model_validate(
        {
            "ticker": ticker,
            "position_fp": count,
            "market_exposure_dollars": exposure,
            "realized_pnl_dollars": realized,
        }
    )


def order(
    order_id: str = "o-1",
    ticker: str = "KXA-1",
    *,
    outcome: str = "yes",
    book: str = "bid",
    yes_price: str = "0.4000",
    remaining: str = "2.00",
    status_: str = "resting",
) -> Order:
    no_price = Decimal(1) - Decimal(yes_price)
    return Order.model_validate(
        {
            "order_id": order_id,
            "ticker": ticker,
            "status": status_,
            "action": "buy" if book == "bid" else "sell",
            "outcome_side": outcome,
            "book_side": book,
            "yes_price_dollars": yes_price,
            "no_price_dollars": str(no_price),
            "initial_count_fp": "2.00",
            "fill_count_fp": "0.00",
            "remaining_count_fp": remaining,
            "exchange_index": 0,
        }
    )


def market(ticker: str, shard: int | None, status_: str = "active") -> Market:
    return Market.model_validate(
        {
            "ticker": ticker,
            "event_ticker": f"EV-{ticker}",
            "market_type": "binary",
            "status": status_,
            "exchange_index": shard,
        }
    )


class FakeRest:
    def __init__(self) -> None:
        self.balance_value = balance("10.3490", "0.0000", "0.0000", "2.5000")
        self.status_value = status()
        self.positions: list[MarketPosition] = []
        self.orders: list[Order] = []
        self.fills: list[Fill] = []
        self.balance_calls = 0
        self.fail_next = 0
        self.markets: dict[str, Market] = {
            "KXA-1": market("KXA-1", 0),
            "KXT-1": market("KXT-1", 3),
        }
        self.fee_type: str | None = "quadratic"
        self.fee_multiplier: float | None = 1
        self.market_calls = 0
        self.series_calls = 0

    async def market(self, ticker: str) -> Market:
        self.market_calls += 1
        return self.markets[ticker]

    async def order(self, order_id: str) -> Order:
        for o in self.orders:
            if o.order_id == order_id:
                return o
        raise KalshiAPIError(404, "not found")

    async def event(self, event_ticker: str) -> Event:
        return Event.model_validate(
            {
                "event_ticker": event_ticker,
                "series_ticker": "KXSERIES",
                "title": "t",
                "mutually_exclusive": False,
            }
        )

    async def series(self, series_ticker: str) -> Series:
        self.series_calls += 1
        return Series.model_validate(
            {
                "ticker": series_ticker,
                "title": "t",
                "frequency": "daily",
                "category": "c",
                "fee_type": self.fee_type,
                "fee_multiplier": self.fee_multiplier,
            }
        )

    async def exchange_status(self) -> ExchangeStatus:
        return self.status_value

    async def balance(self, **params: Any) -> Balance:
        self.balance_calls += 1
        if self.fail_next:
            self.fail_next -= 1
            raise RuntimeError("network down")
        return self.balance_value

    async def iter_positions(self, **params: Any) -> AsyncIterator[MarketPosition]:
        for p in self.positions:
            yield p

    async def iter_fills(self, **params: Any) -> AsyncIterator[Fill]:
        for f in self.fills:
            yield f

    async def iter_orders(self, **params: Any) -> AsyncIterator[Order]:
        for o in self.orders:
            if params.get("status") in (None, o.status):
                yield o


class FakeSocket:
    def __init__(self) -> None:
        self.subscribed: list[str] = []
        self.queue: asyncio.Queue[WsMessage | None] = asyncio.Queue()

    async def subscribe(self, channel: str, **params: Any) -> int:
        self.subscribed.append(channel)
        return len(self.subscribed)

    async def messages(self) -> AsyncIterator[WsMessage]:
        while (message := await self.queue.get()) is not None:
            yield message

    def push(self, type_: str, msg: dict[str, Any] | None = None) -> None:
        self.queue.put_nowait(WsMessage(type=type_, msg=msg))


class Clock:
    def __init__(self) -> None:
        self.now = T0

    def __call__(self) -> float:
        return self.now


async def no_sleep(_: float) -> None:
    await asyncio.sleep(0)


def make(
    tmp_path: Path, rest: FakeRest | None = None, **kw: Any
) -> tuple[AccountTracker, FakeRest, FakeSocket, Clock]:
    rest = rest or FakeRest()
    ws = FakeSocket()
    clock = Clock()
    state = kw.pop("state", None) or TradingState(tmp_path / "state.json")
    tracker = AccountTracker(
        rest, ws, state, clock=clock, sleep=no_sleep, balance_interval=kw.pop("interval", 3600.0)
    )
    return tracker, rest, ws, clock


async def settle() -> None:
    for _ in range(20):
        await asyncio.sleep(0)


class FakeTrading:
    """Records every write, and moves money / orders in the fake exchange like Kalshi would."""

    def __init__(self, rest: FakeRest, state: TradingState) -> None:
        self.rest = rest
        self.state = state
        self.calls: list[tuple[str, Any]] = []
        self.transfer_error: Exception | None = None
        self.create_error: Exception | None = None
        self.amend_error: Exception | None = None
        self.cancel_all_error: Exception | None = None
        self.halted_when_cancel_all: bool | None = None
        self.late_orders: list[Any] = []  # appear after the first cancel-all
        self.cancel_all_clears = True

    async def transfer(
        self, *, source_shard: int, destination_shard: int, amount_e6: int, transfer_id: str
    ) -> str:
        self.calls.append(("transfer", (source_shard, destination_shard, amount_e6, transfer_id)))
        if self.transfer_error:
            raise self.transfer_error
        rows = {
            r.exchange_index: Decimal(str(r.balance))
            for r in self.rest.balance_value.balance_breakdown
        }
        rows[source_shard] -= Decimal(amount_e6) / 1_000_000
        rows[destination_shard] += Decimal(amount_e6) / 1_000_000
        self.rest.balance_value = balance(*(f"{rows[i]:.4f}" for i in sorted(rows)))
        return "tr-ok"

    async def create_order(self, order_: CreateOrder) -> CreatedOrder:
        self.calls.append(("create", order_))
        if self.create_error:
            raise self.create_error
        return CreatedOrder(
            order_id="o-new",
            client_order_id=order_.client_order_id,
            fill_count=Decimal(0),
            remaining_count=order_.count,
        )

    async def amend_order(self, order_id: str, **kw: Any) -> AmendedOrder:
        self.calls.append(("amend", (order_id, kw)))
        if self.amend_error:
            raise self.amend_error
        return AmendedOrder(order_id=order_id, remaining_count=kw["count"])

    async def cancel_order(self, target: CancelTarget) -> CancelledOrder:
        self.calls.append(("cancel", target))
        return CancelledOrder(order_id=target.order_id, reduced_by=Decimal(1))

    async def cancel_orders(self, targets: list[CancelTarget]) -> list[CancelledOrder]:
        self.calls.append(("cancel_batch", [t.order_id for t in targets]))
        gone = {t.order_id for t in targets}
        self.rest.orders = [o for o in self.rest.orders if o.order_id not in gone]
        return [CancelledOrder(order_id=t.order_id) for t in targets]

    async def cancel_all(self) -> None:
        self.halted_when_cancel_all = self.state.halted
        self.calls.append(("cancel_all", None))
        if self.cancel_all_error:
            raise self.cancel_all_error
        if self.cancel_all_clears:
            self.rest.orders = []
        self.rest.orders += self.late_orders
        self.late_orders = []

    def names(self) -> list[str]:
        return [name for name, _ in self.calls]


# --- a runtime built from the fakes, for the UI's trading session ---------------------------------


async def instant(_: float) -> None:
    await asyncio.sleep(0)


def fake_runtime_factory(
    rest: FakeRest,
    trading: "FakeTrading",
    state: TradingState,
    settings: Any,
    policy: Any,
    *,
    modes: list[str] | None = None,
    fail: Exception | None = None,
    sockets: list[FakeSocket] | None = None,
) -> Any:
    """``open_runtime``'s stand-in: same shapes, scripted Kalshi, nothing on the network."""
    from contextlib import asynccontextmanager
    from datetime import UTC
    from datetime import datetime as dt

    from kalshiterm_client.orders import OrderService
    from kalshiterm_client.trade_runtime import Runtime

    @asynccontextmanager
    async def factory(mode: str) -> Any:
        if modes is not None:
            modes.append(mode)
        if fail is not None:
            raise fail
        socket = FakeSocket()
        if sockets is not None:
            sockets.append(socket)
        tracker = AccountTracker(rest, socket, state, clock=lambda: T0, balance_interval=3600)
        service = OrderService(
            rest,
            trading,
            tracker,
            policy,
            production=settings.is_production,
            clock=lambda: dt.fromtimestamp(T0, UTC),
            sleep=instant,
        )
        runtime = Runtime(settings, policy, state, rest, service, tracker, trading)  # type: ignore[arg-type]
        if mode in ("trade", "preview"):
            await tracker.start()
        try:
            yield runtime
        finally:
            await tracker.stop()

    return factory
