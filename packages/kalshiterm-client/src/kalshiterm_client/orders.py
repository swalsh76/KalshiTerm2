"""The order service: the one path from a wish ("buy 2 YES at 40c") to an order at Kalshi.

``prepare`` is read-only: it looks up the market (shard and fee), refreshes the balance, and asks
the risk layer. The result, a ``Prepared``, says exactly what would happen, including any shard
funding, so a person can confirm it. ``execute`` re-checks with fresh data (refusing if the plan
changed since it was shown), funds the shard, re-checks once more, and only then sends the order,
using the ``ApprovedOrder`` that ``risk.check`` produced. Nothing else in the client can place an
order, and nothing here sends one without that proof.

Cancels and the kill switch are never refused by the risk layer.

``kill`` halts first (persisted, so a restart cannot undo it), then cancels everything at Kalshi
and looks again until nothing rests: Kalshi's cancel-all also catches orders placed in the minute
after it, but the list, not the call, is what proves it worked.
"""

import asyncio
import contextlib
import logging
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Protocol

from kalshi_core.fees import QUADRATIC_FAMILY, taker_multiplier
from kalshi_core.models import Event, Market, Series
from kalshi_core.portfolio import Order
from kalshi_core.rest import KalshiAPIError
from kalshi_core.trading import (
    AmendedOrder,
    CancelledOrder,
    CancelTarget,
    CreatedOrder,
    CreateOrder,
    Side,
    TimeInForce,
    WriteOutcomeUnknown,
)

from kalshiterm_client.account import AccountTracker, to_e2, to_e6, tracked_from_order
from kalshiterm_client.risk import (
    Action,
    ApprovedOrder,
    Code,
    OrderIntent,
    Outcome,
    Refusal,
    RiskPolicy,
    Transfer,
    Verdict,
    check,
)

log = logging.getLogger("kalshiterm_client.orders")

E6 = 1_000_000
TRADABLE_STATUSES = frozenset({"active", "open"})
KILL_PASSES = 10
FUNDING_POLLS = 20
FUNDING_POLL_SECONDS = 0.5
GUARD_INTERVAL = 5.0


class OrderRefused(Exception):
    """The risk layer (or the market) said no. ``verdict`` has every reason."""

    def __init__(self, verdict: Verdict) -> None:
        reasons = "; ".join(r.message for r in verdict.refusals)
        super().__init__(f"order refused: {reasons}")
        self.verdict = verdict


class PlanChanged(Exception):
    """What would happen is no longer what was shown; prepare again and confirm again."""

    def __init__(self, before: Verdict, after: Verdict) -> None:
        super().__init__("the order's cost or funding changed since it was shown; prepare it again")
        self.before = before
        self.after = after


class TransferFailed(Exception):
    """Kalshi refused a shard transfer; the order was not placed."""


class TransferUnknown(Exception):
    """A shard transfer got no clear answer. It stays recorded as pending; the order was not
    placed. Look at the balance before trying again."""


class FundingShort(Exception):
    """The transfers finished but the shard still does not hold what the order needs."""


class OrderNotFound(Exception):
    pass


class MarketRest(Protocol):
    async def market(self, ticker: str) -> Market: ...
    async def event(self, event_ticker: str) -> Event: ...
    async def series(self, series_ticker: str) -> Series: ...
    async def order(self, order_id: str) -> Order: ...
    def iter_orders(self, **params: Any) -> Any: ...


class TradingApi(Protocol):
    async def create_order(self, order: CreateOrder) -> CreatedOrder: ...
    async def amend_order(
        self,
        order_id: str,
        *,
        ticker: str,
        side: Side,
        price: Decimal,
        count: Decimal,
        exchange_index: int,
    ) -> AmendedOrder: ...
    async def cancel_order(self, target: CancelTarget) -> CancelledOrder: ...
    async def cancel_orders(self, targets: list[CancelTarget]) -> list[CancelledOrder]: ...
    async def cancel_all(self) -> None: ...
    async def transfer(
        self, *, source_shard: int, destination_shard: int, amount_e6: int, transfer_id: str
    ) -> str: ...


@dataclass(frozen=True)
class OrderRequest:
    """What the user wants. ``price`` is in dollars of the outcome being traded."""

    ticker: str
    action: Action
    outcome: Outcome
    count: Decimal
    price: Decimal
    time_in_force: TimeInForce = TimeInForce.GOOD_TILL_CANCELED
    post_only: bool = False


@dataclass(frozen=True)
class MarketInfo:
    ticker: str
    exchange_index: int
    fee_multiplier_e6: int | None
    status: str

    @property
    def tradable(self) -> bool:
        return self.status in TRADABLE_STATUSES


@dataclass(frozen=True)
class Prepared:
    request: OrderRequest
    market: MarketInfo
    intent: OrderIntent
    verdict: Verdict
    side: Side
    price: Decimal  # on the YES book, as Create Order V2 wants it
    replaces: str | None = None  # an amend: the resting order this replaces
    total_count: Decimal | None = None  # an amend: the order's new total count


@dataclass(frozen=True)
class Amendment:
    result: AmendedOrder
    order_id: str
    approved: ApprovedOrder
    transfers: tuple[Transfer, ...]


@dataclass(frozen=True)
class Placement:
    created: CreatedOrder
    client_order_id: str
    approved: ApprovedOrder
    transfers: tuple[Transfer, ...]


@dataclass
class KillReport:
    cancelled: list[str] = field(default_factory=list)
    remaining: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def clean(self) -> bool:
        return not self.remaining and not self.errors


def to_v2(outcome: Outcome, action: Action, own_price_e6: int) -> tuple[Side, Decimal]:
    """The YES-book side and price for an order in terms of an outcome.

    Create Order V2 has no yes / no field, so the book is the YES book: buying YES is a bid, selling
    YES an ask; buying NO at q is selling YES at 1 - q (an ask), selling NO at q is buying YES at
    1 - q (a bid). This is the documented reading; slice 6.7 proves it on Kalshi with a one-cent
    resting order read back before any other order is sent.
    """
    yes_bid = (action is Action.BUY) == (outcome is Outcome.YES)
    yes_e6 = own_price_e6 if outcome is Outcome.YES else E6 - own_price_e6
    return (Side.BID if yes_bid else Side.ASK, Decimal(yes_e6) / E6)


def multiplier_e6(series: Series) -> int | None:
    """The series' taker fee multiplier in millionths, or None if it cannot be modelled."""
    if series.fee_type not in QUADRATIC_FAMILY or series.fee_multiplier is None:
        return None
    number = Decimal(repr(series.fee_multiplier))  # the JSON number as written
    scaled = number * E6
    if scaled != scaled.to_integral_value():
        return None
    return taker_multiplier(series.fee_type, int(scaled))


def intent_from(request: OrderRequest, market: MarketInfo) -> OrderIntent:
    if isinstance(request.price, float) or isinstance(request.count, float):
        raise TypeError("price and count must be Decimal, not float")
    return OrderIntent(
        ticker=request.ticker,
        exchange_index=market.exchange_index,
        action=request.action,
        outcome=request.outcome,
        count_e2=to_e2(request.count, "count"),
        price_e6=to_e6(request.price, "price"),
        fee_multiplier_e6=market.fee_multiplier_e6,
    )


def describe(prepared: Prepared) -> list[str]:
    """Plain lines saying exactly what will happen, for the confirmation prompt."""
    req, verdict = prepared.request, prepared.verdict
    lines = [
        f"{req.action.value.upper()} {req.count.normalize():f} {req.outcome.value.upper()} "
        f"{req.ticker} at ${req.price:.4f} limit ({req.time_in_force.value}"
        f"{', post-only' if req.post_only else ''}), shard {prepared.market.exchange_index}",
    ]
    if verdict.worst_case_cost_e6 is not None and verdict.fee_e6 is not None:
        lines.append(
            f"worst-case cost ${verdict.worst_case_cost_e6 / E6:.4f} "
            f"(includes up to ${verdict.fee_e6 / E6:.4f} fee)"
        )
    for t in verdict.funding:
        lines.append(
            f"first moves ${t.amount_e6 / E6:.4f} from shard {t.source} to shard {t.destination}"
        )
    for refusal in verdict.refusals:
        lines.append(f"REFUSED: {refusal.message}")
    return lines


class OrderService:
    def __init__(
        self,
        rest: MarketRest,
        trading: TradingApi,
        tracker: AccountTracker,
        policy: RiskPolicy,
        *,
        production: bool,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        new_id: Callable[[], str] = lambda: str(uuid.uuid4()),
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._rest = rest
        self._trading = trading
        self._tracker = tracker
        self._policy = policy
        self._production = production
        self._clock = clock
        self._new_id = new_id
        self._sleep = sleep
        self._multipliers: dict[str, int | None] = {}
        self._guard: asyncio.Task[None] | None = None

    # --- looking before acting ----------------------------------------------------------------

    async def market_info(self, ticker: str) -> MarketInfo:
        """Shard, status and fee of a market, straight from Kalshi (not from the server)."""
        market = await self._rest.market(ticker)
        if market.exchange_index is None:
            raise OrderRefused(
                Verdict(
                    None,
                    (Refusal(Code.UNKNOWN_SHARD, f"{ticker}: Kalshi did not say which shard"),),
                    None,
                    None,
                    (),
                )
            )
        if market.event_ticker not in self._multipliers:
            event = await self._rest.event(market.event_ticker)
            self._multipliers[market.event_ticker] = multiplier_e6(
                await self._rest.series(event.series_ticker)
            )
        return MarketInfo(
            ticker, market.exchange_index, self._multipliers[market.event_ticker], market.status
        )

    async def _verdict(
        self, intent: OrderIntent, market: MarketInfo, replaces: str | None = None
    ) -> Verdict:
        await self._tracker.refresh_balance()
        verdict = check(
            intent,
            self._tracker.snapshot(replaces),
            self._policy,
            now=self._clock(),
            production=self._production,
        )
        if market.tradable:
            return verdict
        refusal = Refusal(Code.MARKET_NOT_TRADABLE, f"{market.ticker} is {market.status}")
        return replace(verdict, approved=None, refusals=(*verdict.refusals, refusal), funding=())

    async def prepare(self, request: OrderRequest) -> Prepared:
        """Read-only. Never sends anything; the verdict may be a refusal."""
        market = await self.market_info(request.ticker)
        intent = intent_from(request, market)
        side, price = to_v2(request.outcome, request.action, intent.price_e6)
        # Build the wire body now: a price finer than Kalshi takes must fail here, before any
        # shard transfer has moved money.
        self._create_order(request, intent, side, price, "preview").payload()
        verdict = await self._verdict(intent, market)
        return Prepared(request, market, intent, verdict, side, price)

    async def prepare_amend(
        self, order_id: str, *, price: Decimal | None = None, count: Decimal | None = None
    ) -> Prepared:
        """Read-only. Checks the order as it would be after the amend, with the old one left out
        of the account so its exposure is not counted twice. ``count`` is the new TOTAL count (what
        Kalshi's amend takes); the risk check uses what would still be unfilled."""
        if price is None and count is None:
            raise ValueError("give a new price, a new count, or both")
        if isinstance(price, float) or isinstance(count, float):
            raise TypeError("price and count must be Decimal, not float")
        current = tracked_from_order(await self._rest.order(order_id))
        if not current.is_resting:
            raise OrderNotFound(f"order {order_id} is {current.status}, not resting")
        if count is not None and current.fill_e2 > 0:
            raise ValueError(
                "this order has partly filled, and whether Kalshi's count includes the filled "
                "part is unverified: change the price only, or cancel and place a new order"
            )
        total = count if count is not None else Decimal(current.total_e2) / 100
        remaining_e2 = to_e2(total, "count") - current.fill_e2
        if remaining_e2 <= 0:
            raise ValueError("the new count must be more than what has already filled; cancel it")
        # The price is on the YES book, as `kterm orders` shows it: a bid is a buy of YES, an
        # ask a sell of YES (the same as a buy of NO at 1 - price).
        yes_price_e6 = to_e6(price, "price") if price is not None else current.yes_price_e6
        request = OrderRequest(
            current.ticker,
            current.action,
            Outcome.YES,
            Decimal(remaining_e2) / 100,
            Decimal(yes_price_e6) / E6,
        )
        market = await self.market_info(current.ticker)
        intent = intent_from(request, market)
        side, yes_price = to_v2(Outcome.YES, current.action, yes_price_e6)
        verdict = await self._verdict(intent, market, order_id)
        return Prepared(request, market, intent, verdict, side, yes_price, order_id, total)

    # --- acting -------------------------------------------------------------------------------

    async def _authorize(self, prepared: Prepared) -> tuple[ApprovedOrder, tuple[Transfer, ...]]:
        """Re-check with fresh data and fund the shard. Returns the proof and the transfers made."""
        if not prepared.verdict.ok:
            raise OrderRefused(prepared.verdict)
        fresh = await self._verdict(prepared.intent, prepared.market, prepared.replaces)
        await self._trip_if_needed(fresh)
        if not fresh.ok:
            raise OrderRefused(fresh)
        if fresh.funding != prepared.verdict.funding or (fresh.worst_case_cost_e6 or 0) > (
            prepared.verdict.worst_case_cost_e6 or 0
        ):
            raise PlanChanged(prepared.verdict, fresh)
        for transfer in fresh.funding:
            await self._move(transfer)
        approved = fresh.approved
        assert approved is not None
        if fresh.funding:
            approved = await self._await_funding(prepared)
        return approved, fresh.funding

    async def _await_funding(self, prepared: Prepared) -> ApprovedOrder:
        """Wait for the transfers to show in the balance, then check once more.

        Kalshi's balance lags a transfer by a moment (seen live: the very next read still showed
        the old shard balance). An order is refused at once for any real reason (a halt, a
        limit); only "the shard still needs funding" is waited on, for about ten seconds.
        """
        for _ in range(FUNDING_POLLS):
            after = await self._verdict(prepared.intent, prepared.market, prepared.replaces)
            if not after.ok:
                raise FundingShort(
                    "the order is no longer allowed after the transfers: "
                    + "; ".join(r.message for r in after.refusals)
                )
            if not after.funding:
                assert after.approved is not None
                return after.approved
            await self._sleep(FUNDING_POLL_SECONDS)
        raise FundingShort(
            "the transfer has not shown up in the shard balance after "
            f"{FUNDING_POLLS * FUNDING_POLL_SECONDS:.0f} seconds. It is recorded as made: check "
            "`kterm balance`, then run the order again."
        )

    async def execute(self, prepared: Prepared) -> Placement:
        if prepared.replaces is not None:
            raise ValueError("this is an amend: use execute_amend")
        approved, funding = await self._authorize(prepared)
        return await self._place(prepared, approved, funding)

    async def execute_amend(self, prepared: Prepared) -> Amendment:
        if prepared.replaces is None or prepared.total_count is None:
            raise ValueError("this is not an amend: use execute")
        approved, funding = await self._authorize(prepared)
        result = await self._trading.amend_order(
            prepared.replaces,
            ticker=prepared.request.ticker,
            side=prepared.side,
            price=prepared.price,
            count=prepared.total_count,
            exchange_index=prepared.intent.exchange_index,
        )
        return Amendment(result, prepared.replaces, approved, funding)

    async def _move(self, transfer: Transfer) -> None:
        transfer_id = self._new_id()
        state = self._tracker.state
        state.begin_transfer(transfer_id, transfer.source, transfer.destination, transfer.amount_e6)
        try:
            await self._trading.transfer(
                source_shard=transfer.source,
                destination_shard=transfer.destination,
                amount_e6=transfer.amount_e6,
                transfer_id=transfer_id,
            )
        except KalshiAPIError as exc:
            state.abort_transfer(transfer_id)
            raise TransferFailed(f"Kalshi refused the shard transfer: {exc}") from exc
        except Exception as exc:
            raise TransferUnknown(
                f"no clear answer to the shard transfer {transfer_id} ({type(exc).__name__}); "
                "it stays recorded as pending. Check the balance before trying again."
            ) from exc
        state.finish_transfer(transfer_id)

    @staticmethod
    def _create_order(
        request: OrderRequest, intent: OrderIntent, side: Side, price: Decimal, coid: str
    ) -> CreateOrder:
        return CreateOrder(
            ticker=request.ticker,
            side=side,
            count=Decimal(intent.count_e2) / 100,
            price=price,
            exchange_index=intent.exchange_index,
            client_order_id=coid,
            time_in_force=request.time_in_force,
            post_only=request.post_only,
        )

    async def _place(
        self, prepared: Prepared, approved: ApprovedOrder, funding: tuple[Transfer, ...]
    ) -> Placement:
        client_order_id = self._new_id()
        request = self._create_order(
            prepared.request, prepared.intent, prepared.side, prepared.price, client_order_id
        )
        try:
            created = await self._trading.create_order(request)
        except WriteOutcomeUnknown:
            self._tracker.state.note_unknown_order(client_order_id)
            raise
        return Placement(created, client_order_id, approved, funding)

    # --- cancelling (never refused) -----------------------------------------------------------

    async def cancel(self, order_id: str) -> CancelledOrder:
        order = self._tracker.orders.get(order_id)
        if order is None:
            raise OrderNotFound(f"no order {order_id} is known; try refreshing")
        return await self._trading.cancel_order(
            CancelTarget(order_id, order.exchange_index, order.ticker)
        )

    async def kill(self, reason: str = "kill switch") -> KillReport:
        """Halt, cancel everything, and look until nothing rests."""
        self._tracker.state.halt(reason)
        report = KillReport()
        try:
            await self._trading.cancel_all()
        except Exception as exc:
            report.errors.append(f"cancel-all: {type(exc).__name__}: {exc}")
        quiet = 0
        for _ in range(KILL_PASSES):
            await self._sleep(1.0)
            try:
                resting = [o async for o in self._rest.iter_orders(status="resting")]
            except Exception as exc:
                report.errors.append(f"listing orders: {type(exc).__name__}: {exc}")
                continue
            if not resting:
                quiet += 1
                if quiet >= 2:  # nothing on two looks, a second apart
                    report.errors = [e for e in report.errors if not e.startswith("listing")]
                    break
                continue
            quiet = 0
            targets = [CancelTarget(o.order_id, o.exchange_index, o.ticker) for o in resting]
            try:
                results = await self._trading.cancel_orders(targets)
            except Exception as exc:
                report.errors.append(f"cancel: {type(exc).__name__}: {exc}")
            else:
                report.cancelled += [r.order_id for r in results if r.error is None]
            report.remaining = [o.order_id for o in resting]
        else:
            report.errors.append("orders were still resting after the last pass")
            return report
        report.remaining = []
        return report

    # --- the daily-loss guard -----------------------------------------------------------------

    async def _trip_if_needed(self, verdict: Verdict) -> None:
        if verdict.trip_kill and not self._tracker.state.halted:
            await self.kill("daily loss limit reached")

    async def guard_once(self) -> bool:
        """Fire the kill switch if the day's realized loss has reached the limit. True if fired."""
        snapshot = self._tracker.snapshot()
        if snapshot is None or self._tracker.state.halted:
            return False
        if snapshot.realized_pnl_today_e6 <= -self._policy.max_daily_loss_e6:
            await self.kill("daily loss limit reached")
            return True
        return False

    def start_guard(self, interval: float = GUARD_INTERVAL) -> None:
        async def loop() -> None:
            while True:
                await self._sleep(interval)
                with contextlib.suppress(Exception):
                    await self.guard_once()

        self._guard = asyncio.create_task(loop())

    async def stop_guard(self) -> None:
        if self._guard is not None:
            self._guard.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._guard
            self._guard = None


@dataclass(frozen=True)
class CancelOutcome:
    order_id: str
    ok: bool
    message: str


async def cancel_by_ids(
    rest: MarketRest, trading: TradingApi, order_ids: list[str]
) -> list[CancelOutcome]:
    """Cancel named orders using Kalshi's own record of each (shard and market), so it needs no
    live tracker and works while halted. One failing id does not stop the others."""
    outcomes: list[CancelOutcome] = []
    for order_id in order_ids:
        try:
            order = await rest.order(order_id)
            if order.status != "resting":
                outcomes.append(CancelOutcome(order_id, False, f"already {order.status}"))
                continue
            result = await trading.cancel_order(
                CancelTarget(order_id, order.exchange_index, order.ticker)
            )
        except KalshiAPIError as exc:
            outcomes.append(CancelOutcome(order_id, False, f"Kalshi said {exc.status}"))
        except WriteOutcomeUnknown:
            outcomes.append(
                CancelOutcome(order_id, False, "no clear answer; look at `kterm orders`")
            )
        else:
            outcomes.append(CancelOutcome(order_id, True, f"cancelled {result.reduced_by} left"))
    return outcomes
