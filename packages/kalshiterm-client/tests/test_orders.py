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
    Side,
    TimeInForce,
    WriteOutcomeUnknown,
)
from kalshiterm_client.orders import (
    KILL_PASSES,
    FundingShort,
    OrderNotFound,
    OrderRefused,
    OrderRequest,
    OrderService,
    PlanChanged,
    TransferFailed,
    TransferUnknown,
    cancel_by_ids,
    describe,
    multiplier_e6,
    to_v2,
)
from kalshiterm_client.risk import Action, Code, Outcome, RiskPolicy
from kalshiterm_client.trading_state import TradingState
from trading_fakes import (
    T0,
    FakeRest,
    FakeTrading,
    balance,
    make,
    market,
    order,
    position,
)

NOW = datetime.fromtimestamp(T0, UTC)


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


async def test_a_balance_that_catches_up_slowly_after_a_transfer_still_gets_the_order_placed(
    tmp_path: Path,
) -> None:
    """Seen live 2026-10-13: the read right after a transfer still showed the old shard balance."""
    service, rest, trading, state = await build(tmp_path)
    try:
        prepared = await service.prepare(buy("KXT-1", count="1", price="0.01"))
        moved: dict[str, int | None] = {"reads_since": None}
        real_balance = rest.balance
        new_balance: dict[str, Any] = {}

        async def lagging_transfer(**kw: Any) -> str:
            trading.calls.append(("transfer", kw))
            new_balance["value"] = balance("10.3290", "0.0000", "0.0000", "0.0200")
            moved["reads_since"] = 0
            return "ok"

        async def lagging_read(**params: Any) -> Any:
            if moved["reads_since"] is not None:
                moved["reads_since"] += 1
                if moved["reads_since"] > 3:  # three stale reads, then the truth
                    rest.balance_value = new_balance["value"]
            return await real_balance(**params)

        trading.transfer = lagging_transfer  # type: ignore[method-assign]
        rest.balance = lagging_read  # type: ignore[method-assign]
        pauses: list[float] = []

        async def record(seconds: float) -> None:
            pauses.append(seconds)

        service._sleep = record
        placement = await service.execute(prepared)
        assert trading.names() == ["transfer", "create"]
        assert (moved["reads_since"] or 0) > 3  # it really did wait out stale reads
        assert pauses == [0.5, 0.5, 0.5]  # a pause between polls, not a spin
        assert placement.transfers and state.transferred_e6 > 0
    finally:
        await service._tracker.stop()


async def test_a_transfer_that_never_shows_in_the_balance_stops_the_order_with_advice(
    tmp_path: Path,
) -> None:
    service, rest, trading, state = await build(tmp_path)
    try:
        prepared = await service.prepare(buy("KXT-1"))

        async def never_arrives(**kw: Any) -> str:
            trading.calls.append(("transfer", kw))
            return "ok"

        trading.transfer = never_arrives  # type: ignore[method-assign]
        with pytest.raises(FundingShort, match="has not shown up.*kterm balance"):
            await service.execute(prepared)
        assert trading.names() == ["transfer"] and state.transferred_e6 > 0  # still counted
    finally:
        await service._tracker.stop()


async def test_an_order_that_became_disallowed_while_waiting_is_refused_not_waited_on(
    tmp_path: Path,
) -> None:
    service, rest, trading, state = await build(tmp_path)
    try:
        prepared = await service.prepare(buy("KXT-1"))

        async def transfer_then_halt(**kw: Any) -> str:
            trading.calls.append(("transfer", kw))
            state.halt("kill switch")
            return "ok"

        trading.transfer = transfer_then_halt  # type: ignore[method-assign]
        with pytest.raises(FundingShort, match="no longer allowed.*halted"):
            await service.execute(prepared)
        assert "create" not in trading.names()
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


async def test_kill_forgets_a_listing_error_once_later_looks_are_clean(tmp_path: Path) -> None:
    rest = FakeRest()
    service, _, trading, _ = await build(tmp_path, rest=rest)
    real, looks = rest.iter_orders, []

    async def flaky(**params: Any) -> Any:
        looks.append(params)
        if len(looks) == 1:
            raise TimeoutError("listing down")
        async for o in real(**params):
            yield o

    rest.iter_orders = flaky  # type: ignore[method-assign]
    try:
        report = await service.kill()
        assert len(looks) == 3  # the failed look, then two empty ones a second apart
        assert report.clean and report.errors == []  # the later clean looks settle it
    finally:
        await service._tracker.stop()


async def test_kill_is_never_clean_when_it_cannot_see_the_orders(tmp_path: Path) -> None:
    rest = FakeRest()
    service, _, trading, _ = await build(tmp_path, rest=rest)
    looks: list[int] = []

    async def down(**params: Any) -> Any:
        looks.append(1)
        raise TimeoutError("listing down")
        yield  # pragma: no cover  (makes this an async generator)

    rest.iter_orders = down  # type: ignore[method-assign]
    try:
        report = await service.kill()
        assert len(looks) == KILL_PASSES  # it keeps trying, then stops
        assert not report.clean and report.cancelled == []
        assert sum("listing orders" in e for e in report.errors) == KILL_PASSES
    finally:
        await service._tracker.stop()


async def test_kill_names_what_still_rests_when_the_batch_cancel_keeps_failing(
    tmp_path: Path,
) -> None:
    rest = FakeRest()
    rest.orders = resting("stuck-1", "stuck-2")
    service, _, trading, _ = await build(tmp_path, rest=rest)
    try:
        trading.cancel_all_clears = False

        async def broken(targets: list[CancelTarget]) -> list[CancelledOrder]:
            raise RuntimeError("no route")

        trading.cancel_orders = broken  # type: ignore[method-assign]
        report = await service.kill()
        assert not report.clean
        assert report.remaining == ["stuck-1", "stuck-2"] and report.cancelled == []
        assert sum(e.startswith("cancel: RuntimeError") for e in report.errors) == KILL_PASSES
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


# --- amend ------------------------------------------------------------------------------------


def one_resting(
    ticker: str = "KXA-1", *, outcome: str = "yes", book: str = "bid", yes_price: str = "0.4000"
) -> FakeRest:
    rest = FakeRest()
    rest.balance_value = balance("10.3490", "0.0000", "0.0000", "0.0000")
    rest.orders = [order("o-1", ticker, outcome=outcome, book=book, yes_price=yes_price)]
    return rest


async def test_an_amend_is_checked_as_the_replacement_without_counting_the_old_order(
    tmp_path: Path,
) -> None:
    rest = one_resting()
    service, _, trading, _ = await build(tmp_path, rest=rest, policy=RiskPolicy(max_open_orders=1))
    try:
        fresh_order = await service.prepare(buy())
        assert Code.TOO_MANY_ORDERS in {r.code for r in fresh_order.verdict.refusals}
        prepared = await service.prepare_amend("o-1", price=Decimal("0.45"))
        assert prepared.verdict.ok  # the one open order is the one being replaced
        assert (prepared.replaces, prepared.total_count) == ("o-1", Decimal("2"))
        assert prepared.intent.count_e2 == 200 and prepared.intent.price_e6 == 450_000
        amendment = await service.execute_amend(prepared)
        assert trading.names() == ["amend"]
        assert trading.calls[0][1] == (
            "o-1",
            {
                "ticker": "KXA-1",
                "side": Side.BID,
                "price": Decimal("0.45"),
                "count": Decimal("2"),
                "exchange_index": 0,
            },
        )
        assert amendment.order_id == "o-1" and amendment.transfers == ()
    finally:
        await service._tracker.stop()


async def test_an_amend_that_would_break_a_limit_is_refused_and_sends_nothing(
    tmp_path: Path,
) -> None:
    service, _, trading, _ = await build(tmp_path, rest=one_resting())
    try:
        prepared = await service.prepare_amend("o-1", price=Decimal("0.90"), count=Decimal("5"))
        assert Code.ORDER_TOO_LARGE in {r.code for r in prepared.verdict.refusals} or (
            Code.NOTIONAL_TOO_LARGE in {r.code for r in prepared.verdict.refusals}
        )
        with pytest.raises(OrderRefused):
            await service.execute_amend(prepared)
        assert trading.calls == []
    finally:
        await service._tracker.stop()


async def test_amending_a_buy_of_no_stays_an_ask_and_never_becomes_a_bid(tmp_path: Path) -> None:
    """Seen live 2026-10-13: a buy of NO at 1c is held as 'sell, no, ask, yes 0.99' (a NO bid).
    Rebuilding it from those labels sent a BID at 0.99, which crosses the book and buys YES."""
    rest = one_resting(outcome="no", book="ask", yes_price="0.9900")
    service, _, trading, _ = await build(tmp_path, rest=rest)
    try:
        prepared = await service.prepare_amend("o-1", price=Decimal("0.98"))
        assert (prepared.side, prepared.price) == (Side.ASK, Decimal("0.98"))
        assert prepared.total_count == Decimal("2")
        assert prepared.request.outcome is Outcome.YES and prepared.intent.outcome is Outcome.YES
        assert prepared.request.action is Action.SELL  # an ask sells YES, whatever Kalshi labels it
        await service.execute_amend(prepared)
        sent = trading.calls[0][1][1]
        assert (sent["side"], sent["price"], sent["count"]) == (Side.ASK, Decimal("0.98"), 2)
    finally:
        await service._tracker.stop()


async def test_an_amended_orders_current_total_is_filled_plus_remaining(tmp_path: Path) -> None:
    rest = one_resting()
    # amended from 1 to 2 contracts: Kalshi leaves initial_count at 1
    rest.orders[0] = rest.orders[0].model_copy(
        update={"initial_count_fp": Decimal("1.00"), "remaining_count_fp": Decimal("2.00")}
    )
    service, *_ = await build(tmp_path, rest=rest)
    try:
        prepared = await service.prepare_amend("o-1", price=Decimal("0.45"))
        assert prepared.total_count == Decimal("2") and prepared.intent.count_e2 == 200
    finally:
        await service._tracker.stop()


async def test_a_partly_filled_order_may_change_price_but_not_count(tmp_path: Path) -> None:
    rest = one_resting()
    rest.orders[0] = rest.orders[0].model_copy(
        update={"fill_count_fp": Decimal("1.00"), "remaining_count_fp": Decimal("1.00")}
    )
    service, _, trading, _ = await build(tmp_path, rest=rest)
    try:
        with pytest.raises(ValueError, match="partly filled"):
            await service.prepare_amend("o-1", count=Decimal("3"))
        prepared = await service.prepare_amend("o-1", price=Decimal("0.45"))
        assert prepared.total_count == Decimal("2") and prepared.intent.count_e2 == 100
        assert trading.calls == []
    finally:
        await service._tracker.stop()


async def test_an_amend_on_an_unfunded_shard_funds_it_first(tmp_path: Path) -> None:
    rest = one_resting("KXT-1")
    service, _, trading, _ = await build(tmp_path, rest=rest)
    try:
        prepared = await service.prepare_amend("o-1", count=Decimal("2"), price=Decimal("0.45"))
        assert prepared.verdict.funding
        await service.execute_amend(prepared)
        assert trading.names() == ["transfer", "amend"]
    finally:
        await service._tracker.stop()


async def test_bad_amends_are_refused_before_anything_is_sent(tmp_path: Path) -> None:
    rest = one_resting()
    rest.orders.append(order("o-done", "KXA-1", status_="executed"))
    service, _, trading, _ = await build(tmp_path, rest=rest)
    try:
        with pytest.raises(ValueError, match="new price"):
            await service.prepare_amend("o-1")
        with pytest.raises(OrderNotFound):
            await service.prepare_amend("o-done", price=Decimal("0.5"))
        with pytest.raises(ValueError, match="cancel"):
            await service.prepare_amend("o-1", count=Decimal("0"))
        bad: Any = 0.5
        with pytest.raises(TypeError):
            await service.prepare_amend("o-1", price=bad)
        plain = await service.prepare(buy())
        with pytest.raises(ValueError, match="not an amend"):
            await service.execute_amend(plain)
        amend = await service.prepare_amend("o-1", count=Decimal("1"))
        with pytest.raises(ValueError, match="use execute_amend"):
            await service.execute(amend)
        assert trading.calls == []
    finally:
        await service._tracker.stop()


async def test_an_amend_with_no_clear_answer_is_reported_unknown(tmp_path: Path) -> None:
    service, _, trading, _ = await build(tmp_path, rest=one_resting())
    try:
        trading.amend_error = WriteOutcomeUnknown("amend_order", "o-1")
        prepared = await service.prepare_amend("o-1", count=Decimal("1"))
        with pytest.raises(WriteOutcomeUnknown):
            await service.execute_amend(prepared)
    finally:
        await service._tracker.stop()


# --- cancelling by id -------------------------------------------------------------------------


async def test_cancel_by_ids_uses_kalshis_record_and_one_failure_does_not_stop_the_rest(
    tmp_path: Path,
) -> None:
    rest = FakeRest()
    rest.orders = [
        order("o-1", "KXT-1"),
        order("o-done", "KXA-1", status_="executed"),
        order("o-2", "KXA-1"),
    ]
    trading = FakeTrading(rest, TradingState(tmp_path / "s.json"))
    outcomes = await cancel_by_ids(rest, trading, ["o-1", "o-done", "o-missing", "o-2"])
    assert [(o.order_id, o.ok) for o in outcomes] == [
        ("o-1", True),
        ("o-done", False),
        ("o-missing", False),
        ("o-2", True),
    ]
    assert outcomes[1].message == "already executed" and "404" in outcomes[2].message
    assert [c[1] for c in trading.calls] == [
        CancelTarget("o-1", 0, "KXT-1"),
        CancelTarget("o-2", 0, "KXA-1"),
    ]


async def test_cancel_by_ids_reports_an_unknown_outcome_and_carries_on(tmp_path: Path) -> None:
    rest = FakeRest()
    rest.orders = [order("o-1", "KXT-1"), order("o-2", "KXA-1")]
    trading = FakeTrading(rest, TradingState(tmp_path / "s.json"))
    real = trading.cancel_order

    async def unsure(target: CancelTarget) -> CancelledOrder:
        if target.order_id == "o-1":
            raise WriteOutcomeUnknown("cancel_order", "o-1")
        return await real(target)

    trading.cancel_order = unsure  # type: ignore[method-assign]
    outcomes = await cancel_by_ids(rest, trading, ["o-1", "o-2"])
    assert [(o.order_id, o.ok) for o in outcomes] == [("o-1", False), ("o-2", True)]
    assert outcomes[0].message == "no clear answer; look at `kterm orders`"


# --- cancelling everything without halting; the typed order phrase -------------------------------


async def test_cancel_everything_clears_the_orders_but_does_not_halt(tmp_path: Path) -> None:
    rest = FakeRest()
    rest.orders = resting("o-1", "o-2")
    service, _, trading, state = await build(tmp_path, rest=rest)
    try:
        report = await service.cancel_everything()
        assert report.clean and rest.orders == [] and trading.names() == ["cancel_all"]
        assert not state.halted and not TradingState.load(state.path).halted
        assert (await service.prepare(buy())).verdict.ok  # orders are still allowed afterwards
    finally:
        await service._tracker.stop()


@pytest.mark.parametrize(
    ("typed", "ok"),
    [
        ("KXA-1 buy yes 2 0.4", True),
        ("KXA-1 BUY YES 2.00 0.40", True),
        ("  KXA-1   buy  yes  2  0.4  ", True),
        ("kxa-1 buy yes 2 0.4", False),  # the ticker is case-sensitive
        ("KXA-1 sell yes 2 0.4", False),
        ("KXA-1 buy no 2 0.4", False),
        ("KXA-1 buy yes 3 0.4", False),
        ("KXA-1 buy yes 2 0.41", False),
        ("KXA-1 buy yes 2", False),
        ("KXA-1 buy yes 2 0.4 now", False),
        ("KXA-1 buy yes two 0.4", False),
        ("", False),
    ],
)
def test_the_typed_order_phrase(typed: str, ok: bool) -> None:
    from kalshiterm_client.orders import typed_phrase_matches

    assert typed_phrase_matches(typed, buy()) is ok
