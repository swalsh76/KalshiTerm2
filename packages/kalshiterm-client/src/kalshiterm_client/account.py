"""A live view of the account for the risk layer: balances, positions, resting orders.

Kalshi's private channels (``fill``, ``user_orders``, ``market_positions``) send **no snapshot and
no sequence numbers**, so a message can be missed without anyone knowing. The tracker therefore
never trusts the stream alone:

* it **re-reads everything over REST** after subscribing and again after every reconnect, and
  until that finishes the snapshot reports the stream as not OK (the risk layer then refuses);
* messages that arrive while it re-reads are applied afterwards, in order, and are idempotent
  (an order message is a full replacement, applied only if not older than what is held; a fill is
  deduplicated by ``trade_id``; a position message replaces the market's position);
* there is **no balance channel**, so the balance is polled (every few seconds, and at once after
  a fill) and the snapshot's age is the age of the last good balance: if polling fails, the
  snapshot goes stale and orders are refused.

Everything is exact integers (``*_e6`` dollars, ``*_e2`` contracts). A figure Kalshi sends with
more precision than that is an error, not a rounding.
"""

import asyncio
import contextlib
import logging
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Protocol

from kalshi_core.models import ExchangeStatus
from kalshi_core.portfolio import Balance, MarketPosition, Order
from kalshi_core.ws_models import FillMsg, MarketPositionMsg, UserOrderMsg, WsMessage

from kalshiterm_client.risk import (
    AccountSnapshot,
    Action,
    MarketState,
    OrderIntent,
    Outcome,
    ShardState,
    collateral_e6,
)
from kalshiterm_client.trading_state import TradingState, utc_day

log = logging.getLogger("kalshiterm_client.account")

E6 = 1_000_000
RECONNECTED = "reconnected"
CHANNELS = ("fill", "user_orders", "market_positions")
BALANCE_INTERVAL = 5.0
RECENT_FILLS = 500


class AccountRest(Protocol):
    """The REST calls the tracker needs (``KalshiRestClient`` provides them)."""

    async def exchange_status(self) -> ExchangeStatus: ...
    async def balance(self, **params: Any) -> Balance: ...
    def iter_positions(self, **params: Any) -> Any: ...
    def iter_orders(self, **params: Any) -> Any: ...


class AccountSocket(Protocol):
    """The WebSocket calls the tracker needs (``KalshiWebSocket`` provides them)."""

    async def subscribe(self, channel: str, **params: Any) -> int: ...
    def messages(self) -> Any: ...


def to_e6(value: Decimal | None, what: str = "amount") -> int:
    """Dollars as integer millionths, refusing anything finer."""
    if value is None:
        return 0
    scaled = value * E6
    if scaled != scaled.to_integral_value():
        raise ValueError(f"{what} {value} has more than six decimal places")
    return int(scaled)


def to_e2(value: Decimal | None, what: str = "count") -> int:
    """Contracts as integer hundredths, refusing anything finer."""
    if value is None:
        return 0
    scaled = value * 100
    if scaled != scaled.to_integral_value():
        raise ValueError(f"{what} {value} has more than two decimal places")
    return int(scaled)


@dataclass
class TrackedOrder:
    """A resting (or finished) order as Kalshi's YES book holds it.

    Only ``book_side`` (bid or ask) and the YES price are used for risk: the orderbook confirmed
    (2026-10-13) that a YES bid is "buy YES" and a YES ask is "sell YES", which is the same thing
    as "buy NO at 1 - price". Kalshi's ``action`` / ``outcome_side`` labels on the same order are
    NOT dependable (a buy of NO is labelled "sell, no, ask"), so they are kept only for display.
    """

    order_id: str
    client_order_id: str
    ticker: str
    exchange_index: int
    status: str
    outcome_side: str  # Kalshi's label, display only
    book_side: str  # bid / ask on the YES book
    yes_price_e6: int
    initial_e2: int
    fill_e2: int
    remaining_e2: int
    updated_ms: int

    @property
    def is_resting(self) -> bool:
        return self.status == "resting"

    @property
    def action(self) -> Action:
        """On the YES book a bid buys YES and an ask sells YES."""
        return Action.BUY if self.book_side == "bid" else Action.SELL

    @property
    def own_price_e6(self) -> int:
        """The price of the YES contract this order trades."""
        return self.yes_price_e6

    @property
    def adds_yes(self) -> bool:
        """Filling a bid moves the net position towards YES, filling an ask towards NO."""
        return self.book_side == "bid"

    @property
    def total_e2(self) -> int:
        """Filled plus still resting (Kalshi leaves ``initial_count`` alone after an amend)."""
        return self.fill_e2 + self.remaining_e2


@dataclass
class TrackedPosition:
    position_e2: int = 0
    exposure_e6: int = 0
    realized_e6: int = 0


def tracked_from_order(order: Order) -> TrackedOrder:
    updated = order.last_update_time or order.created_time
    return TrackedOrder(
        order_id=order.order_id,
        client_order_id=order.client_order_id,
        ticker=order.ticker,
        exchange_index=order.exchange_index,
        status=order.status,
        outcome_side=order.outcome_side,
        book_side=order.book_side,
        yes_price_e6=to_e6(order.yes_price_dollars, "order price"),
        initial_e2=to_e2(order.initial_count_fp),
        fill_e2=to_e2(order.fill_count_fp),
        remaining_e2=to_e2(order.remaining_count_fp),
        updated_ms=int(updated.timestamp() * 1000) if updated else 0,
    )


def tracked_from_message(msg: UserOrderMsg) -> TrackedOrder:
    return TrackedOrder(
        order_id=msg.order_id,
        client_order_id=msg.client_order_id,
        ticker=msg.ticker,
        exchange_index=msg.exchange_index,
        status=msg.status,
        outcome_side=msg.outcome_side,
        book_side=msg.book_side,
        yes_price_e6=to_e6(msg.yes_price_dollars, "order price"),
        initial_e2=to_e2(msg.initial_count_fp),
        fill_e2=to_e2(msg.fill_count_fp),
        remaining_e2=to_e2(msg.remaining_count_fp),
        updated_ms=msg.last_updated_ts_ms or 0,
    )


class AccountTracker:
    def __init__(
        self,
        rest: AccountRest,
        socket: AccountSocket,
        state: TradingState,
        *,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        balance_interval: float = BALANCE_INTERVAL,
        on_change: Callable[[], None] | None = None,
    ) -> None:
        self._rest = rest
        self._ws = socket
        self.state = state
        self._clock = clock
        self._sleep = sleep
        self._balance_interval = balance_interval
        self.on_change = (
            on_change  # called (never raising) whenever what the snapshot shows may differ
        )
        self.balances_e6: dict[int, int] = {}
        self.shard_flags: dict[int, tuple[bool, bool]] = {}  # (trading, transfers)
        self.positions: dict[str, TrackedPosition] = {}
        self.orders: dict[str, TrackedOrder] = {}
        self.fills: deque[FillMsg] = deque(maxlen=RECENT_FILLS)
        self._seen_fills: set[str] = set()
        self.balance_at: float | None = None
        self.synced = False
        self.connected = False
        self.synced_event = asyncio.Event()
        self._balance_dirty = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []

    # --- lifecycle ----------------------------------------------------------------------------

    async def start(self) -> None:
        """Subscribe, read everything, and begin following the stream. Raises if it cannot."""
        for channel in CHANNELS:
            await self._ws.subscribe(channel)
        self.connected = True
        await self.refresh()
        self._tasks = [
            asyncio.create_task(self._follow_stream()),
            asyncio.create_task(self._poll_balance()),
        ]

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._tasks = []
        self.connected = False
        self.synced = False

    # --- reading everything over REST -----------------------------------------------------------

    async def refresh(self) -> None:
        """Re-read balance, shard status, positions and resting orders. Marks the tracker synced
        only when all of it succeeded."""
        self.synced = False
        await self._read_balance()
        positions: dict[str, TrackedPosition] = {}
        async for position in self._rest.iter_positions(settlement_status="all"):
            positions[position.ticker] = self._position_from_rest(position)
        resting: dict[str, TrackedOrder] = {}
        async for order in self._rest.iter_orders(status="resting"):
            resting[order.order_id] = tracked_from_order(order)
        self.positions = positions
        # keep finished orders we already know; replace the resting set with Kalshi's list
        kept = {k: v for k, v in self.orders.items() if not v.is_resting}
        self.orders = {**kept, **resting}
        today = utc_day(datetime.fromtimestamp(self._clock(), UTC))
        self.state.roll(today)
        self.state.set_baseline(self.realized_total_e6())
        self.synced = True
        self.synced_event.set()
        self._changed()

    async def refresh_balance(self) -> None:
        """Re-read the balance and shard status (cheap; used before every order)."""
        await self._read_balance()

    async def _read_balance(self) -> None:
        status = await self._rest.exchange_status()
        balance = await self._rest.balance()
        flags: dict[int, tuple[bool, bool]] = {}
        for row in status.exchange_index_statuses or []:
            flags[row.exchange_index] = (
                row.exchange_active and row.trading_active,
                row.exchange_active and row.intra_exchange_transfers_active,
            )
        self.shard_flags = flags
        self.balances_e6 = {
            row.exchange_index: to_e6(row.balance, "shard balance")
            for row in balance.balance_breakdown
        }
        self.balance_at = self._clock()
        self._changed()

    @staticmethod
    def _position_from_rest(position: MarketPosition) -> TrackedPosition:
        return TrackedPosition(
            position_e2=to_e2(position.position_fp, "position"),
            exposure_e6=to_e6(position.market_exposure_dollars, "exposure"),
            realized_e6=to_e6(position.realized_pnl_dollars, "realized P&L"),
        )

    # --- following the stream -------------------------------------------------------------------

    async def _follow_stream(self) -> None:
        try:
            async for message in self._ws.messages():
                if message.type == RECONNECTED:
                    await self._resync()
                else:
                    self.apply(message)
        finally:
            self.connected = False
            self.synced = False
            self._changed()

    async def _resync(self) -> None:
        delay = 0.5  # ``refresh`` marks the tracker unsynced until it has read everything
        while True:
            try:
                await self.refresh()
                return
            except Exception:
                log.warning("account refresh after reconnect failed; retrying", exc_info=True)
                await self._sleep(delay)
                delay = min(delay * 2, 10.0)

    async def _poll_balance(self) -> None:
        while True:
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._balance_dirty.wait(), self._balance_interval)
            self._balance_dirty.clear()
            try:
                await self._read_balance()
            except Exception:
                log.warning("balance poll failed; the snapshot will go stale", exc_info=True)
                await self._sleep(1.0)

    def _changed(self) -> None:
        if self.on_change is not None:
            try:
                self.on_change()
            except Exception:  # a listener's trouble must never break the account view
                log.exception("account change listener failed")

    def apply(self, message: WsMessage) -> None:
        """Apply one private-channel message. Idempotent, and ignores anything else."""
        if message.type == "user_order":
            self._apply_order(UserOrderMsg.model_validate(message.msg))
        elif message.type == "market_position":
            self._apply_position(MarketPositionMsg.model_validate(message.msg))
        elif message.type == "fill":
            self._apply_fill(FillMsg.model_validate(message.msg))
        else:
            return
        self._changed()

    def _apply_order(self, msg: UserOrderMsg) -> None:
        incoming = tracked_from_message(msg)
        held = self.orders.get(incoming.order_id)
        if held is not None and incoming.updated_ms < held.updated_ms:
            return  # an older message than what we hold
        self.orders[incoming.order_id] = incoming
        if len(self.orders) > 1000:  # keep the finished ones bounded
            for key in [k for k, v in self.orders.items() if not v.is_resting][:-500]:
                del self.orders[key]

    def _apply_position(self, msg: MarketPositionMsg) -> None:
        held = self.positions.get(msg.market_ticker, TrackedPosition())
        self.positions[msg.market_ticker] = TrackedPosition(
            position_e2=to_e2(msg.position_fp, "position"),
            exposure_e6=(
                to_e6(msg.position_cost_dollars, "position cost")
                if msg.position_cost_dollars is not None
                else held.exposure_e6
            ),
            realized_e6=(
                to_e6(msg.realized_pnl_dollars, "realized P&L")
                if msg.realized_pnl_dollars is not None
                else held.realized_e6
            ),
        )

    def _apply_fill(self, msg: FillMsg) -> None:
        if msg.trade_id in self._seen_fills:
            return
        self._seen_fills.add(msg.trade_id)
        self.fills.append(msg)
        if len(self._seen_fills) > 4 * RECENT_FILLS:
            self._seen_fills = {f.trade_id for f in self.fills}
        self._balance_dirty.set()  # a fill moves cash: re-read it now

    # --- what the risk layer sees ---------------------------------------------------------------

    def realized_total_e6(self) -> int:
        return sum(p.realized_e6 for p in self.positions.values())

    def resting_orders(self, exclude: str | None = None) -> list[TrackedOrder]:
        return [o for o in self.orders.values() if o.is_resting and o.order_id != exclude]

    def snapshot(self, exclude_order: str | None = None) -> AccountSnapshot | None:
        """The state for ``risk.check``, or None before the first full read. ``exclude_order``
        leaves one resting order out, so an amend is checked as its replacement without the
        old order's exposure counted twice."""
        if self.balance_at is None or not self.synced_event.is_set():
            return None
        resting = self.resting_orders(exclude_order)
        resting_yes: dict[str, int] = {}
        resting_no: dict[str, int] = {}
        reserved: dict[int, int] = {}
        resting_cost = 0
        for order in resting:
            if order.remaining_e2 <= 0:
                continue
            side = resting_yes if order.adds_yes else resting_no
            side[order.ticker] = side.get(order.ticker, 0) + order.remaining_e2
            position = self.positions.get(order.ticker, TrackedPosition()).position_e2
            cost = collateral_e6(
                OrderIntent(
                    order.ticker,
                    order.exchange_index,
                    order.action,
                    Outcome.YES,
                    order.remaining_e2,
                    min(max(order.yes_price_e6, 1), E6 - 1),
                    None,
                ),
                position,
            )
            resting_cost += cost
            reserved[order.exchange_index] = reserved.get(order.exchange_index, 0) + cost
        markets = {
            ticker: MarketState(
                self.positions.get(ticker, TrackedPosition()).position_e2,
                resting_yes.get(ticker, 0),
                resting_no.get(ticker, 0),
            )
            for ticker in {*self.positions, *resting_yes, *resting_no}
        }
        shards = {}
        for index, balance in self.balances_e6.items():
            trading, transfers = self.shard_flags.get(index, (False, False))
            # Kalshi's balance is gross cash (a resting order does not reduce it, measured live),
            # so what is free to use is the balance less what resting orders may need.
            free = max(balance - reserved.get(index, 0), 0)
            shards[index] = ShardState(index, free, trading, transfers)
        exposure = sum(p.exposure_e6 for p in self.positions.values()) + resting_cost
        return AccountSnapshot(
            as_of=datetime.fromtimestamp(self.balance_at, UTC),
            shards=shards,
            markets=markets,
            open_orders=len(resting),
            exposure_e6=exposure,
            realized_pnl_today_e6=self.state.realized_today_e6(self.realized_total_e6()),
            transferred_today_e6=self.state.transferred_e6,
            stream_ok=self.connected and self.synced,
            halted=self.state.halted,
        )
