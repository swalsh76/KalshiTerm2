import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from kalshi_core.models import Series
from kalshi_core.rest import KalshiAPIError
from kalshi_core.trading import (
    CancelledOrder,
    CancelTarget,
    CreatedOrder,
    CreateOrder,
    Side,
    TimeInForce,
    WriteOutcomeUnknown,
)
from kalshiterm_client.orders import (
    FundingShort,
    OrderNotFound,
    OrderRefused,
    OrderRequest,
    OrderService,
    PlanChanged,
    TransferFailed,
    TransferUnknown,
    describe,
    multiplier_e6,
    to_v2,
)
from kalshiterm_client.risk import Action, Code, Outcome, RiskPolicy
from kalshiterm_client.trading_state import TradingState
from trading_fakes import (
    T0,
    FakeRest,
    balance,
    make,
    market,
    order,
    position,
)

NOW = datetime.fromtimestamp(T0, UTC)


class FakeTrading:
    """Records every write, and moves money / orders in the fake exchange like Kalshi would."""

    def __init__(self, rest: FakeRest, state: TradingState) -> None:
        self.rest = rest
        self.state = state
        self.calls: list[tuple[str, Any]] = []
        self.transfer_error: Exception | None = None
        self.create_error: Exception | None = None
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


async def no_sleep(_: float) -> None:
    await asyncio.sleep(0)


async def build(
    tmp_path: Path,
    *,
    policy: RiskPolicy | None = None,
    production: bool = False,
    rest: FakeRest | None = None,
) -> tuple[OrderService, FakeRest, FakeTrading, TradingState]:
    if rest is None:
        rest = FakeRest()
        rest.balance_value = balance("10.3490", "0.0000", "0.0000", "0.0000")
    tracker, _, _, _ = make(tmp_path, rest)
    state = tracker.state
    ids = iter(f"id-{n}" for n in range(1000))
    trading = FakeTrading(rest, state)
    service = OrderService(
        rest,
        trading,
        tracker,
        policy or RiskPolicy(),
        production=production,
        clock=lambda: NOW,
        new_id=lambda: next(ids),
        sleep=no_sleep,
    )
    await tracker.start()
    return service, rest, trading, state


def buy(ticker: str = "KXA-1", count: str = "2", price: str = "0.40", **kw: Any) -> OrderRequest:
    return OrderRequest(
        ticker, Action.BUY, kw.pop("outcome", Outcome.YES), Decimal(count), Decimal(price), **kw
    )


# --- the mapping to the YES book --------------------------------------------------------------


@pytest.mark.parametrize(
    ("outcome", "action", "own", "side", "yes_price"),
    [
        (Outcome.YES, Action.BUY, 400_000, Side.BID, "0.4"),
        (Outcome.YES, Action.SELL, 400_000, Side.ASK, "0.4"),
        (Outcome.NO, Action.BUY, 300_000, Side.ASK, "0.7"),  # buy NO at 30c = sell YES at 70c
        (Outcome.NO, Action.SELL, 300_000, Side.BID, "0.7"),  # sell NO at 30c = buy YES at 70c
    ],
)
def test_orders_are_expressed_on_the_yes_book(
    outcome: Outcome, action: Action, own: int, side: Side, yes_price: str
) -> None:
    assert to_v2(outcome, action, own) == (side, Decimal(yes_price))


def test_fee_multipliers_come_from_the_series_and_unmodellable_ones_are_unknown() -> None:
    def series(fee_type: str | None, mult: float | None) -> Series:
        return Series.model_validate(
            {
                "ticker": "S",
                "title": "t",
                "frequency": "f",
                "category": "c",
                "fee_type": fee_type,
                "fee_multiplier": mult,
            }
        )

    assert multiplier_e6(series("quadratic", 1)) == 1_000_000
    assert multiplier_e6(series("quadratic", 0.5)) == 500_000
    assert multiplier_e6(series("quadratic_with_maker_fees", 0.07)) == 70_000
    assert multiplier_e6(series("flat", 1)) is None
    assert multiplier_e6(series(None, None)) is None
    assert multiplier_e6(series("quadratic", None)) is None
    assert multiplier_e6(series("quadratic", 0.0000001)) is None  # finer than we can hold exactly


# --- looking ----------------------------------------------------------------------------------


async def test_prepare_is_read_only_and_says_what_would_happen(tmp_path: Path) -> None:
    service, rest, trading, _ = await build(tmp_path)
    try:
        prepared = await service.prepare(buy())
        assert prepared.verdict.ok and prepared.market.exchange_index == 0
        assert prepared.intent.fee_multiplier_e6 == 1_000_000
        assert (prepared.side, prepared.price) == (Side.BID, Decimal("0.4"))
        assert trading.calls == []
        lines = describe(prepared)
        assert lines[0] == "BUY 2 YES KXA-1 at $0.4000 limit (good_till_canceled), shard 0"
        assert lines[1].startswith("worst-case cost $0.8")  # 2 x 0.40 plus the fee
        assert len(lines) == 2  # shard 0 holds the money: no transfer
    finally:
        await service._tracker.stop()


async def test_the_market_and_its_fee_are_looked_up_once(tmp_path: Path) -> None:
    service, rest, _, _ = await build(tmp_path)
    try:
        await service.prepare(buy())
        await service.prepare(buy(count="1"))
        assert rest.series_calls == 1
    finally:
        await service._tracker.stop()


async def test_a_shard_without_money_shows_the_transfer_it_would_make(tmp_path: Path) -> None:
    service, rest, trading, _ = await build(tmp_path)
    try:
        prepared = await service.prepare(buy("KXT-1"))
        assert prepared.verdict.ok and prepared.market.exchange_index == 3
        (transfer,) = prepared.verdict.funding
        assert (transfer.source, transfer.destination) == (0, 3)
        lines = describe(prepared)
        assert any(
            line.startswith("first moves $") and "from shard 0 to shard 3" in line for line in lines
        )
        assert trading.calls == []
    finally:
        await service._tracker.stop()


def closed(rest: FakeRest) -> None:
    rest.markets["KXA-1"] = market("KXA-1", 0, "closed")


def flat_fee(rest: FakeRest) -> None:
    rest.fee_type = "flat"


def no_shard(rest: FakeRest) -> None:
    rest.markets["KXA-1"] = market("KXA-1", None)


@pytest.mark.parametrize(
    ("setup", "code"),
    [
        (closed, Code.MARKET_NOT_TRADABLE),
        (flat_fee, Code.UNKNOWN_FEE),
        (no_shard, Code.UNKNOWN_SHARD),
    ],
)
async def test_a_market_that_cannot_be_traded_or_priced_is_refused_before_anything_moves(
    tmp_path: Path, setup: Any, code: Code
) -> None:
    rest = FakeRest()
    setup(rest)
    service, _, trading, _ = await build(tmp_path, rest=rest)
    try:
        with pytest.raises(OrderRefused) as info:
            await service.execute(await service.prepare(buy()))
        assert code in {r.code for r in info.value.verdict.refusals}
        assert trading.calls == []
    finally:
        await service._tracker.stop()


# --- acting -----------------------------------------------------------------------------------


async def test_execute_places_exactly_the_order_that_was_shown(tmp_path: Path) -> None:
    service, rest, trading, _ = await build(tmp_path)
    try:
        prepared = await service.prepare(buy(price="0.40", post_only=True))
        placement = await service.execute(prepared)
        assert trading.names() == ["create"]
        sent = trading.calls[0][1]
        assert (sent.ticker, sent.side, sent.count, sent.price) == (
            "KXA-1",
            Side.BID,
            Decimal("2"),
            Decimal("0.4"),
        )
        assert sent.exchange_index == 0 and sent.post_only and sent.client_order_id == "id-0"
        assert sent.time_in_force is TimeInForce.GOOD_TILL_CANCELED
        assert placement.created.order_id == "o-new" and placement.client_order_id == "id-0"
        assert placement.approved.intent == prepared.intent and placement.transfers == ()
    finally:
        await service._tracker.stop()


async def test_a_no_order_is_sent_as_the_opposite_side_of_the_yes_book(tmp_path: Path) -> None:
    service, _, trading, _ = await build(tmp_path)
    try:
        await service.execute(await service.prepare(buy(outcome=Outcome.NO, price="0.30")))
        sent = trading.calls[0][1]
        assert (sent.side, sent.price) == (Side.ASK, Decimal("0.7"))  # buy NO 30c = sell YES 70c
    finally:
        await service._tracker.stop()


async def test_funding_happens_before_the_order_and_is_counted_and_recorded(tmp_path: Path) -> None:
    service, rest, trading, state = await build(tmp_path)
    try:
        prepared = await service.prepare(buy("KXT-1", count="2", price="0.40"))
        placement = await service.execute(prepared)
        assert trading.names() == ["transfer", "create"]
        source, destination, amount, transfer_id = trading.calls[0][1]
        assert (source, destination) == (0, 3) and transfer_id == "id-0"
        assert (
            amount
            == placement.transfers[0].amount_e6
            >= (prepared.verdict.worst_case_cost_e6 or 0) - 2_500_000
        )
        assert (
            trading.calls[1][1].client_order_id == "id-1"
            and trading.calls[1][1].exchange_index == 3
        )
        assert state.transferred_e6 == amount and state.pending_transfers == []
        assert TradingState.load(state.path).transferred_e6 == amount
    finally:
        await service._tracker.stop()


async def test_a_refused_order_sends_nothing_at_all(tmp_path: Path) -> None:
    service, _, trading, _ = await build(tmp_path)
    try:
        prepared = await service.prepare(buy(count="50", price="0.90"))  # far over every limit
        assert not prepared.verdict.ok
        with pytest.raises(OrderRefused) as info:
            await service.execute(prepared)
        assert Code.ORDER_TOO_LARGE in {r.code for r in info.value.verdict.refusals}
        assert trading.calls == []
    finally:
        await service._tracker.stop()


async def test_a_price_kalshi_cannot_take_fails_before_any_money_moves(tmp_path: Path) -> None:
    service, _, trading, state = await build(tmp_path)
    try:
        with pytest.raises(ValueError, match="decimal places"):
            await service.prepare(buy("KXT-1", price="0.123456"))  # would need a transfer
        assert trading.calls == [] and state.transferred_e6 == 0
    finally:
        await service._tracker.stop()


async def test_floats_are_refused(tmp_path: Path) -> None:
    service, *_ = await build(tmp_path)
    try:
        two: Any = 2.0
        with pytest.raises(TypeError):
            await service.prepare(
                OrderRequest("KXA-1", Action.BUY, Outcome.YES, two, Decimal("0.4"))
            )
    finally:
        await service._tracker.stop()


async def test_if_the_plan_changed_since_it_was_shown_nothing_is_sent(tmp_path: Path) -> None:
    service, rest, trading, _ = await build(tmp_path)
    try:
        prepared = await service.prepare(buy("KXT-1"))
        assert prepared.verdict.funding
        # while the person was reading the prompt, the money moved to shard 1
        rest.balance_value = balance("0.0000", "10.3490", "0.0000", "0.0000")
        with pytest.raises(PlanChanged):
            await service.execute(prepared)
        assert trading.calls == []
    finally:
        await service._tracker.stop()


async def test_a_refused_transfer_stops_the_order_and_is_not_counted(tmp_path: Path) -> None:
    service, rest, trading, state = await build(tmp_path)
    try:
        trading.transfer_error = KalshiAPIError(400, "no")
        prepared = await service.prepare(buy("KXT-1"))
        with pytest.raises(TransferFailed):
            await service.execute(prepared)
        assert trading.names() == ["transfer"]
        assert state.transferred_e6 == 0 and state.pending_transfers == []
    finally:
        await service._tracker.stop()


async def test_a_transfer_with_no_clear_answer_stays_pending_and_stops_the_order(
    tmp_path: Path,
) -> None:
    service, rest, trading, state = await build(tmp_path)
    try:
        trading.transfer_error = TimeoutError("slow")
        prepared = await service.prepare(buy("KXT-1"))
        with pytest.raises(TransferUnknown):
            await service.execute(prepared)
        assert trading.names() == ["transfer"]
        assert state.transferred_e6 > 0 and len(state.pending_transfers) == 1
    finally:
        await service._tracker.stop()


async def test_transfers_that_do_not_arrive_stop_the_order(tmp_path: Path) -> None:
    service, rest, trading, _ = await build(tmp_path)
    try:
        prepared = await service.prepare(buy("KXT-1"))

        async def lying_transfer(**kw: Any) -> str:
            trading.calls.append(("transfer", kw))
            return "ok"  # Kalshi said yes but the balance did not move

        trading.transfer = lying_transfer  # type: ignore[method-assign]
        with pytest.raises(FundingShort):
            await service.execute(prepared)
        assert trading.names() == ["transfer"]
    finally:
        await service._tracker.stop()


async def test_an_order_with_no_clear_answer_is_remembered_by_its_id(tmp_path: Path) -> None:
    service, rest, trading, state = await build(tmp_path)
    try:
        trading.create_error = WriteOutcomeUnknown("create_order", "id-0")
        prepared = await service.prepare(buy())
        with pytest.raises(WriteOutcomeUnknown):
            await service.execute(prepared)
        assert state.unknown_orders == ["id-0"]
        assert TradingState.load(state.path).unknown_orders == ["id-0"]
    finally:
        await service._tracker.stop()


async def test_production_is_refused_unless_the_policy_allows_it(tmp_path: Path) -> None:
    service, _, trading, _ = await build(tmp_path, production=True)
    try:
        prepared = await service.prepare(buy())
        assert Code.PRODUCTION_DISABLED in {r.code for r in prepared.verdict.refusals}
        with pytest.raises(OrderRefused):
            await service.execute(prepared)
        assert trading.calls == []
    finally:
        await service._tracker.stop()
    service, _, trading, _ = await build(
        tmp_path / "b", production=True, policy=RiskPolicy(production_enabled=True)
    )
    try:
        await service.execute(await service.prepare(buy()))
        assert trading.names() == ["create"]
    finally:
        await service._tracker.stop()


async def test_a_halted_account_refuses_new_orders_but_not_cancels(tmp_path: Path) -> None:
    rest = FakeRest()
    rest.orders = [order("o-1", "KXA-1", outcome="yes", book="bid")]
    service, _, trading, state = await build(tmp_path, rest=rest)
    try:
        state.halt("kill switch")
        prepared = await service.prepare(buy())
        assert Code.HALTED in {r.code for r in prepared.verdict.refusals}
        with pytest.raises(OrderRefused):
            await service.execute(prepared)
        await service.cancel("o-1")  # still allowed
        assert trading.names() == ["cancel"]
        assert trading.calls[0][1] == CancelTarget("o-1", 0, "KXA-1")
    finally:
        await service._tracker.stop()


async def test_cancelling_an_unknown_order_is_an_error(tmp_path: Path) -> None:
    service, _, trading, _ = await build(tmp_path)
    try:
        with pytest.raises(OrderNotFound):
            await service.cancel("nope")
        assert trading.calls == []
    finally:
        await service._tracker.stop()


# --- the kill switch --------------------------------------------------------------------------


def resting(*ids: str) -> list[Any]:
    return [order(i, "KXA-1") for i in ids]


async def test_kill_halts_first_then_cancels_and_checks_until_nothing_rests(
    tmp_path: Path,
) -> None:
    rest = FakeRest()
    rest.orders = resting("o-1", "o-2")
    service, _, trading, state = await build(tmp_path, rest=rest)
    try:
        report = await service.kill("test")
        assert trading.halted_when_cancel_all is True  # the halt was already saved
        assert TradingState.load(state.path).halted
        assert report.clean and trading.names() == ["cancel_all"]
        assert rest.orders == []
    finally:
        await service._tracker.stop()


async def test_kill_cancels_an_order_that_appears_after_the_cancel_all(tmp_path: Path) -> None:
    rest = FakeRest()
    rest.orders = resting("o-1")
    service, _, trading, _ = await build(tmp_path, rest=rest)
    try:
        trading.late_orders = resting("late-1")
        report = await service.kill()
        assert trading.names() == ["cancel_all", "cancel_batch"]
        assert trading.calls[1][1] == ["late-1"]
        assert report.clean and report.cancelled == ["late-1"]
    finally:
        await service._tracker.stop()


async def test_kill_needs_two_empty_looks_before_it_calls_the_job_done(tmp_path: Path) -> None:
    rest = FakeRest()
    service, _, trading, _ = await build(tmp_path, rest=rest)
    try:
        looks = 0
        original = rest.iter_orders

        async def listing(**params: Any) -> Any:
            nonlocal looks
            looks += 1
            if looks == 2:  # an order appears right after a look that found nothing
                rest.orders = resting("sneaky")
            async for o in original(**params):
                yield o

        rest.iter_orders = listing  # type: ignore[method-assign]
        report = await service.kill()
        assert trading.calls[-1] == ("cancel_batch", ["sneaky"])
        assert report.clean and looks >= 4
    finally:
        await service._tracker.stop()


async def test_kill_still_halts_and_cancels_when_cancel_all_fails(tmp_path: Path) -> None:
    rest = FakeRest()
    rest.orders = resting("o-1", "o-2")
    service, _, trading, state = await build(tmp_path, rest=rest)
    try:
        trading.cancel_all_error = TimeoutError("down")
        trading.cancel_all_clears = False
        report = await service.kill()
        assert state.halted
        assert trading.names() == ["cancel_all", "cancel_batch"]
        assert rest.orders == [] and report.cancelled == ["o-1", "o-2"]
        assert not report.clean and "cancel-all" in report.errors[0]  # reported honestly
    finally:
        await service._tracker.stop()


async def test_kill_reports_orders_that_will_not_cancel(tmp_path: Path) -> None:
    rest = FakeRest()
    rest.orders = resting("stuck")
    service, _, trading, _ = await build(tmp_path, rest=rest)
    try:
        trading.cancel_all_clears = False

        async def cancel_nothing(targets: list[CancelTarget]) -> list[CancelledOrder]:
            trading.calls.append(("cancel_batch", [t.order_id for t in targets]))
            return []

        trading.cancel_orders = cancel_nothing  # type: ignore[method-assign]
        report = await service.kill()
        assert not report.clean and report.remaining == ["stuck"]
        assert trading.names().count("cancel_batch") == 10
    finally:
        await service._tracker.stop()


# --- the loss guard ---------------------------------------------------------------------------


async def test_the_guard_fires_the_kill_switch_once_the_loss_limit_is_reached(
    tmp_path: Path,
) -> None:
    rest = FakeRest()
    rest.positions = [position("KXA-1", "0", realized="0")]
    service, _, trading, state = await build(tmp_path, rest=rest)
    try:
        assert await service.guard_once() is False
        service._tracker.positions["KXA-1"].realized_e6 = -1_999_999
        assert await service.guard_once() is False
        service._tracker.positions["KXA-1"].realized_e6 = -2_000_000
        assert await service.guard_once() is True
        assert state.halted and "daily loss" in state.halt_reason
        assert await service.guard_once() is False  # already halted: not fired again
        assert trading.names().count("cancel_all") == 1
    finally:
        await service._tracker.stop()


async def test_an_order_attempted_over_the_loss_limit_trips_the_kill_switch(tmp_path: Path) -> None:
    rest = FakeRest()
    rest.positions = [position("KXA-1", "0", realized="0")]
    service, _, trading, state = await build(tmp_path, rest=rest)
    try:
        prepared = await service.prepare(buy())
        assert prepared.verdict.ok
        service._tracker.positions["KXA-1"].realized_e6 = -2_500_000
        with pytest.raises(OrderRefused):
            await service.execute(prepared)
        assert state.halted and "cancel_all" in trading.names() and "create" not in trading.names()
    finally:
        await service._tracker.stop()


async def test_the_background_guard_can_be_started_and_stopped(tmp_path: Path) -> None:
    service, *_ = await build(tmp_path)
    try:
        service.start_guard(interval=0.0)
        await asyncio.sleep(0.01)
        await service.stop_guard()
        await service.stop_guard()  # harmless twice
    finally:
        await service._tracker.stop()
