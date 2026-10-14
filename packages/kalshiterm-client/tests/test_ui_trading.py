"""The UI's trading session: when trading is on, what it needs to be switched on, how it lapses,
and the whole life of an order through the two-step flow."""

import asyncio
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pytest
from kalshi_core.auth import AuthError
from kalshi_core.trading import TradingRefused
from kalshiterm_client.orders import OrderRefused, PlanChanged
from kalshiterm_client.profiles import ProfileError
from kalshiterm_client.risk import RiskPolicy
from kalshiterm_client.trading_state import TradingState
from kalshiterm_client.ui import trading_models as tm
from kalshiterm_client.ui.trading import (
    PreparedGone,
    SwitchRefused,
    TradingOff,
    WrongOrderPhrase,
    contracts,
    money,
)
from trading_fakes import (
    FakeRest,
    balance,
    order,
    position,
)
from ui_trading_fixtures import PHRASE_DEMO, PHRASE_PROD, buy_request, make, on, settle

# --- switching on --------------------------------------------------------------------------------


async def test_a_new_session_is_read_only_and_has_loaded_nothing(tmp_path: Path) -> None:
    h = make(tmp_path)
    assert h.session.mode == "read-only" and h.modes == []
    info = h.session.info()
    assert info.mode == "read-only" and info.production and info.permitted
    assert info.enable_phrase == PHRASE_PROD and info.seconds_until_idle is None
    assert info.limits.max_order_notional == "1.000000" and info.limits.max_order_count == "5.00"
    assert info.limits.max_open_orders == 3 and info.halted is False
    assert make(tmp_path / "d", production=False).session.info().enable_phrase == PHRASE_DEMO


async def test_the_phrase_must_be_typed_exactly_before_anything_is_loaded(tmp_path: Path) -> None:
    h = make(tmp_path)
    for wrong in (
        "",
        "enable production trading",
        "ENABLE DEMO TRADING",
        "ENABLE PRODUCTION",
        "yes",
    ):
        with pytest.raises(SwitchRefused) as info:
            await h.session.enable(wrong)
        assert info.value.code == "wrong_phrase" and info.value.status == 422
    assert h.modes == [] and h.session.mode == "read-only"
    await h.session.enable("  ENABLE PRODUCTION TRADING \n")  # surrounding whitespace is fine
    assert h.session.mode == "trading" and h.modes == ["trade"]
    await h.session.close()


async def test_risk_toml_must_permit_production_and_the_page_cannot_grant_it(
    tmp_path: Path,
) -> None:
    h = make(tmp_path, policy=RiskPolicy(production_enabled=False))
    info = h.session.info()
    assert info.permitted is False and "risk.toml" in (info.permission_help or "")
    with pytest.raises(SwitchRefused) as refused:
        await h.session.enable(PHRASE_PROD)  # even with the right phrase
    assert refused.value.code == "production_not_permitted" and refused.value.status == 403
    assert "risk.toml" in refused.value.message and h.modes == []
    demo = make(tmp_path / "d", production=False, policy=RiskPolicy(production_enabled=False))
    assert demo.session.info().permitted is True  # the switch governs production only
    await demo.session.enable(PHRASE_DEMO)
    assert demo.session.mode == "trading"
    await demo.session.close()


@pytest.mark.parametrize(
    ("failure", "status", "code"),
    [
        (AuthError("KALSHI_TRADING_KEY_ID is not set"), 409, "no_trading_key"),
        (ProfileError("risk.toml is wrong"), 409, "config_problem"),
        (TradingRefused("production orders are not enabled"), 403, "trading_refused"),
        (httpx.ConnectError("down"), 502, "kalshi_unreachable"),
    ],
)
async def test_a_failure_while_switching_on_leaves_trading_off_and_can_be_retried(
    tmp_path: Path, failure: Exception, status: int, code: str
) -> None:
    h = make(tmp_path)
    h.fail = failure
    with pytest.raises(SwitchRefused) as info:
        await h.session.enable(PHRASE_PROD)
    assert (info.value.status, info.value.code) == (status, code)
    assert h.session.mode == "read-only" and h.mode_messages == []
    h.fail = None
    await h.session.enable(PHRASE_PROD)  # nothing was left half-built
    assert h.session.mode == "trading"
    await h.session.close()


async def test_a_damaged_state_file_refuses_before_the_key_is_loaded(tmp_path: Path) -> None:
    h = make(tmp_path)
    bad = ProfileError("trading_state.json is damaged")

    def broken() -> TradingState:
        raise bad

    h.session._state_loader = broken
    with pytest.raises(SwitchRefused) as info:
        await h.session.enable(PHRASE_PROD)
    assert info.value.code == "config_problem" and h.modes == []


async def test_switching_on_loads_the_runtime_announces_it_and_is_idempotent(
    tmp_path: Path,
) -> None:
    h = make(tmp_path)
    await on(h)
    assert h.modes == ["trade"] and h.session.mode == "trading"
    assert h.mode_messages == [{"type": "mode", "mode": "trading", "reason": "user"}]
    assert h.session._runtime is not None and h.session._runtime.service is not None
    assert h.session._runtime.service._guard is not None  # the daily-loss guard is running
    again = await h.session.enable(PHRASE_PROD)
    assert again.mode == "trading" and h.modes == ["trade"]  # no second runtime
    await h.session.close()


# --- switching off, and lapsing ------------------------------------------------------------------


async def test_switching_off_drops_everything_and_forgets_prepared_orders(tmp_path: Path) -> None:
    h = make(tmp_path)
    await on(h)
    prepared = await h.session.prepare(buy_request())
    assert prepared.prepare_id
    change = await h.session.disable()
    assert change.mode == "read-only" and h.session._runtime is None and h.session._stack is None
    assert h.session._prepared == {}
    assert h.mode_messages[-1] == {"type": "mode", "mode": "read-only", "reason": "user"}
    assert h.account_messages[-1] is None
    with pytest.raises(TradingOff):
        await h.session.execute(tm.ExecuteRequest(prepare_id=prepared.prepare_id, phrase="x"))
    assert (await h.session.disable()).mode == "read-only"  # and again is harmless


async def test_switching_off_can_also_cancel_the_open_orders_without_halting(
    tmp_path: Path,
) -> None:
    rest = FakeRest()
    rest.balance_value = balance("10.3490", "0", "0", "0")
    rest.orders = [order("o-1", "KXA-1"), order("o-2", "KXA-1")]
    h = make(tmp_path, rest=rest)
    await on(h)
    change = await h.session.disable(cancel_open_orders=True)
    assert rest.orders == [] and "cancel_all" in h.trading.names() and change.problems == []
    assert not h.state.halted  # switching off is not the kill switch
    await h.session.enable(PHRASE_PROD)  # and trading can come straight back
    await h.session.close()


async def test_it_switches_itself_off_after_the_idle_time_and_a_touch_postpones_it(
    tmp_path: Path,
) -> None:
    h = make(tmp_path, policy=RiskPolicy(production_enabled=True, ui_idle_minutes=1))
    await on(h)
    h.clock.now += 50
    await settle()
    assert h.session.mode == "trading"
    assert h.session.info().seconds_until_idle == 10
    h.session.touch()  # the user did something
    h.clock.now += 50
    await settle()
    assert h.session.mode == "trading"
    h.clock.now += 11
    await settle()
    assert h.session.mode == "read-only"
    assert h.mode_messages[-1] == {"type": "mode", "mode": "read-only", "reason": "idle"}


async def test_deliberate_actions_count_as_activity_but_reading_the_account_does_not(
    tmp_path: Path,
) -> None:
    h = make(tmp_path, policy=RiskPolicy(production_enabled=True, ui_idle_minutes=1))
    await on(h)
    h.clock.now += 40
    h.session.account()  # a page polling the view must not keep trading alive
    assert h.session.info().seconds_until_idle == 20
    await h.session.prepare(buy_request())
    assert h.session.info().seconds_until_idle == 60  # preparing an order is activity
    await h.session.close()


async def test_it_switches_itself_off_when_no_browser_is_connected(tmp_path: Path) -> None:
    h = make(tmp_path, policy=RiskPolicy(production_enabled=True, ui_disconnect_seconds=60))
    await on(h)
    h.connections = 0
    await settle()  # the watcher notices there is no browser: the 60 seconds start here
    h.clock.now += 40
    await settle()
    assert h.session.mode == "trading"  # not long enough yet
    h.connections = 1  # a browser came back: the countdown is cancelled...
    await settle()
    h.connections = 0
    await settle()  # ...and starts again from here
    h.clock.now += 50  # 90 s since the first absence, but only 50 since the second
    await settle()
    assert h.session.mode == "trading"
    h.clock.now += 11
    await settle()
    assert h.session.mode == "read-only"
    assert h.mode_messages[-1]["reason"] == "no browser"


async def test_shutdown_switches_trading_off(tmp_path: Path) -> None:
    h = make(tmp_path)
    await on(h)
    await h.session.close()
    assert h.session.mode == "read-only" and h.mode_messages[-1]["reason"] == "shutdown"


# --- while off, nothing trades --------------------------------------------------------------------


async def test_every_trading_action_refuses_while_trading_is_off(tmp_path: Path) -> None:
    h = make(tmp_path)
    calls = [
        h.session.prepare(buy_request()),
        h.session.prepare_amend("o-1", tm.AmendPrepareRequest(price="0.5")),
        h.session.execute(tm.ExecuteRequest(prepare_id="x" * 16, phrase="p")),
        h.session.cancel("o-1"),
    ]
    for call in calls:
        with pytest.raises(TradingOff):
            await call
    for plain in (h.session.account, h.session.resume):
        with pytest.raises(TradingOff):
            plain()
    assert h.modes == [] and h.trading.calls == []


# --- an order's whole life ---------------------------------------------------------------------


async def test_prepare_shows_everything_and_stores_a_one_time_id(tmp_path: Path) -> None:
    h = make(tmp_path)
    await on(h)
    view = await h.session.prepare(buy_request())
    assert view.approved and view.prepare_id and view.expires_in_seconds == 60
    assert view.kind == "order" and view.shard == 0 and view.refusals == []
    assert view.lines[0].startswith("BUY 2 YES KXA-1 at $0.4000")
    assert view.worst_case_cost is not None and view.fee is not None
    assert view.requires_phrase and view.phrase_format == "TICKER buy|sell yes|no COUNT PRICE"
    assert h.trading.calls == []  # nothing was sent
    await h.session.close()


async def test_a_refused_order_has_no_id_to_execute(tmp_path: Path) -> None:
    h = make(tmp_path)
    await on(h)
    view = await h.session.prepare(buy_request(count="50", price="0.90"))
    assert not view.approved and view.prepare_id is None and view.expires_in_seconds == 0
    assert {r.code for r in view.refusals} >= {"order_too_large", "notional_too_large"}
    assert not view.requires_phrase and h.session._prepared == {}
    await h.session.close()


async def test_a_price_kalshi_cannot_take_is_an_error_not_a_prepared_order(tmp_path: Path) -> None:
    h = make(tmp_path)
    await on(h)
    with pytest.raises(ValueError, match="decimal places"):
        await h.session.prepare(buy_request(price="0.123456"))
    await h.session.close()


async def test_in_production_the_order_must_be_typed_and_a_wrong_try_does_not_spend_the_id(
    tmp_path: Path,
) -> None:
    h = make(tmp_path)
    await on(h)
    view = await h.session.prepare(buy_request())
    pid = view.prepare_id or ""
    for wrong in (
        None,
        "",
        "yes",
        "KXA-1 buy yes 2 0.45",
        "KXA-1 sell yes 2 0.4",
        "KXA-2 buy yes 2 0.4",
    ):
        with pytest.raises(WrongOrderPhrase):
            await h.session.execute(tm.ExecuteRequest(prepare_id=pid, phrase=wrong))
    assert h.trading.calls == [] and pid in h.session._prepared  # still there to try again
    done = await h.session.execute(
        tm.ExecuteRequest(prepare_id=pid, phrase="KXA-1 BUY yes 2.00 0.4")
    )
    assert done.kind == "order" and done.order_id == "o-new" and h.trading.names() == ["create"]
    assert (
        done.client_order_id
        and Decimal(done.filled or "x") == 0
        and Decimal(done.remaining or "x") == 2
    )
    await h.session.close()


async def test_in_the_demo_no_phrase_is_needed(tmp_path: Path) -> None:
    h = make(tmp_path, production=False)
    await on(h)
    view = await h.session.prepare(buy_request())
    assert not view.requires_phrase and view.phrase_format is None
    done = await h.session.execute(tm.ExecuteRequest(prepare_id=view.prepare_id or ""))
    assert done.order_id == "o-new"
    await h.session.close()


async def test_a_prepared_order_can_be_executed_once_only(tmp_path: Path) -> None:
    h = make(tmp_path, production=False)
    await on(h)
    pid = (await h.session.prepare(buy_request())).prepare_id or ""
    await h.session.execute(tm.ExecuteRequest(prepare_id=pid))
    with pytest.raises(PreparedGone):
        await h.session.execute(tm.ExecuteRequest(prepare_id=pid))
    assert h.trading.names() == ["create"]  # one order, however often the button is pressed
    await h.session.close()


async def test_a_prepared_order_expires_after_a_minute_and_unknown_ids_are_refused(
    tmp_path: Path,
) -> None:
    h = make(tmp_path, production=False)
    await on(h)
    pid = (await h.session.prepare(buy_request())).prepare_id or ""
    h.clock.now += 59
    assert pid in h.session._prepared
    h.clock.now += 2
    with pytest.raises(PreparedGone):
        await h.session.execute(tm.ExecuteRequest(prepare_id=pid))
    with pytest.raises(PreparedGone):
        await h.session.execute(tm.ExecuteRequest(prepare_id="never-issued-id-1"))
    assert h.trading.calls == []
    await h.session.close()


async def test_a_failed_execution_still_spends_the_id(tmp_path: Path) -> None:
    h = make(tmp_path, production=False)
    await on(h)
    view = await h.session.prepare(buy_request(ticker="KXT-1"))  # needs a transfer to shard 3
    assert view.funding and view.funding[0].source == 0
    h.rest.balance_value = balance("0.0000", "10.3490", "0.0000", "0.0000")  # the money moved
    with pytest.raises(PlanChanged):
        await h.session.execute(tm.ExecuteRequest(prepare_id=view.prepare_id or ""))
    assert view.prepare_id not in h.session._prepared and h.trading.calls == []
    await h.session.close()


async def test_a_refusal_at_execution_time_surfaces_and_sends_nothing(tmp_path: Path) -> None:
    h = make(tmp_path, production=False)
    await on(h)
    pid = (await h.session.prepare(buy_request())).prepare_id or ""
    h.state.halt("kill switch")  # the kill switch fired while the user was reading
    with pytest.raises(OrderRefused):
        await h.session.execute(tm.ExecuteRequest(prepare_id=pid))
    assert h.trading.calls == []
    await h.session.close()


async def test_an_amend_goes_through_the_same_two_steps(tmp_path: Path) -> None:
    rest = FakeRest()
    rest.balance_value = balance("10.3490", "0", "0", "0")
    rest.orders = [order("o-1", "KXA-1", yes_price="0.4000")]
    h = make(tmp_path, production=False, rest=rest)
    await on(h)
    view = await h.session.prepare_amend("o-1", tm.AmendPrepareRequest(price="0.45"))
    assert view.kind == "amend" and view.approved and view.prepare_id
    assert h.trading.calls == []
    done = await h.session.execute(tm.ExecuteRequest(prepare_id=view.prepare_id))
    assert done.kind == "amend" and done.order_id == "o-1" and h.trading.names() == ["amend"]
    await h.session.close()


async def test_two_executions_never_run_at_the_same_time(tmp_path: Path) -> None:
    h = make(tmp_path, production=False)
    await on(h)
    running = 0
    peak = 0
    original = h.trading.create_order

    async def slow(order_: Any) -> Any:
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        await asyncio.sleep(0.05)
        running -= 1
        return await original(order_)

    h.trading.create_order = slow  # type: ignore[method-assign]
    a = (await h.session.prepare(buy_request(count="1", price="0.30"))).prepare_id or ""
    b = (await h.session.prepare(buy_request(count="1", price="0.31"))).prepare_id or ""
    await asyncio.gather(
        h.session.execute(tm.ExecuteRequest(prepare_id=a)),
        h.session.execute(tm.ExecuteRequest(prepare_id=b)),
    )
    assert peak == 1  # the second waited for the first (and was re-checked with fresh data)
    await h.session.close()


async def test_switching_off_waits_for_an_order_in_flight(tmp_path: Path) -> None:
    h = make(tmp_path, production=False)
    await on(h)
    finished = asyncio.Event()
    original = h.trading.create_order

    async def slow(order_: Any) -> Any:
        await asyncio.sleep(0.1)
        finished.set()
        return await original(order_)

    h.trading.create_order = slow  # type: ignore[method-assign]
    pid = (await h.session.prepare(buy_request())).prepare_id or ""
    executing = asyncio.create_task(h.session.execute(tm.ExecuteRequest(prepare_id=pid)))
    await asyncio.sleep(0.02)
    await h.session.disable()
    assert finished.is_set()  # the order completed before the runtime was torn down
    assert (await executing).order_id == "o-new"


async def test_cancel_one_order(tmp_path: Path) -> None:
    rest = FakeRest()
    rest.orders = [order("o-1", "KXA-1"), order("o-done", "KXA-1", status_="executed")]
    h = make(tmp_path, production=False, rest=rest)
    await on(h)
    ok = await h.session.cancel("o-1")
    assert ok.ok and ok.order_id == "o-1"
    gone = await h.session.cancel("o-done")
    assert not gone.ok and gone.message == "already executed"
    await h.session.close()


# --- the kill switch and resume -----------------------------------------------------------------


async def test_the_kill_switch_halts_and_cancels_while_trading(tmp_path: Path) -> None:
    rest = FakeRest()
    rest.orders = [order("o-1", "KXA-1")]
    h = make(tmp_path, production=False, rest=rest)
    await on(h)
    report = await h.session.kill()
    assert report.clean and report.halted and rest.orders == []
    assert h.state.halted and "kill switch" in h.state.halt_reason
    assert h.modes == ["trade"]  # the live runtime was used, not a second one opened
    assert h.session.account().halted  # (Kalshi's own cancel messages update the order list)
    await h.session.close()


async def test_the_kill_switch_works_with_trading_off_by_opening_a_minimal_runtime(
    tmp_path: Path,
) -> None:
    rest = FakeRest()
    rest.orders = [order("o-1", "KXA-1"), order("o-2", "KXA-1")]
    h = make(tmp_path, rest=rest)
    assert h.session.mode == "read-only"
    report = await h.session.kill()
    assert h.modes == ["exit"] and report.clean and rest.orders == []
    assert h.state.halted and h.session.mode == "read-only"  # and trading did not switch on


async def test_the_kill_switch_with_trading_off_and_no_key_says_so(tmp_path: Path) -> None:
    h = make(tmp_path)
    h.fail = AuthError("KALSHI_TRADING_KEY_ID is not set")
    with pytest.raises(SwitchRefused) as info:
        await h.session.kill()
    assert info.value.code == "no_trading_key"


async def test_resume_clears_the_halt_but_only_while_trading(tmp_path: Path) -> None:
    h = make(tmp_path, production=False)
    h.state.halt("kill switch")
    with pytest.raises(TradingOff):
        h.session.resume()
    assert h.session.info().halted is True  # visible before switching on
    await on(h)
    pid = (await h.session.prepare(buy_request())).prepare_id
    assert pid is None  # halted: nothing can be prepared for execution
    assert h.session.resume().halted is False and not h.state.halted
    assert (await h.session.prepare(buy_request())).approved
    await h.session.close()


# --- the account view ----------------------------------------------------------------------------


async def test_the_account_view_is_exact_and_covers_what_the_page_needs(tmp_path: Path) -> None:
    rest = FakeRest()
    rest.balance_value = balance("10.3490", "0.0000", "0.0000", "2.5000")
    rest.positions = [position("KXA-1", "3.00", "1.20", "-0.25"), position("KXFLAT-1", "0.00")]
    rest.orders = [order("o-1", "KXA-1", yes_price="0.4000", remaining="2.00")]
    h = make(tmp_path, production=False, rest=rest)
    await on(h)
    view = h.session.account()
    assert view.synced and not view.halted and view.open_orders == 1
    cash = {s.index: (s.cash, s.free) for s in view.shards}
    assert cash[0] == ("10.349000", "9.549000")  # 0.80 reserved by the resting bid
    assert cash[3] == ("2.500000", "2.500000")
    assert [(p.ticker, p.position, p.exposure, p.realized) for p in view.positions] == [
        ("KXA-1", "3.00", "1.200000", "-0.250000")
    ]  # flat markets are left out
    (resting,) = view.orders
    assert (resting.order_id, resting.book_side, resting.yes_price, resting.no_price) == (
        "o-1", "bid", "0.400000", "0.600000",
    )  # fmt: skip
    assert (resting.filled, resting.remaining, resting.total) == ("0.00", "2.00", "2.00")
    assert view.realized_today == "0.000000" and view.transferred_today == "0.000000"
    await h.session.close()


async def test_fills_are_listed_newest_first_with_exact_figures(tmp_path: Path) -> None:
    h = make(tmp_path, production=False)
    await on(h)
    socket = h.sockets[0]
    for n, (price, count, fee) in enumerate(
        (("0.4000", "1.00", "0.0014"), ("0.5500", "2.50", "0.0200"))
    ):
        socket.push(
            "fill",
            {"trade_id": f"t-{n}", "order_id": "o-1", "market_ticker": "KXA-1", "book_side": "bid",
             "yes_price_dollars": price, "count_fp": count, "fee_cost": fee, "is_taker": n == 1,
             "ts_ms": 1_000 + n},
        )  # fmt: skip
    await settle(0.1)
    fills = h.session.account().fills
    assert [f.trade_id for f in fills] == ["t-1", "t-0"]  # newest first
    assert (fills[0].yes_price, fills[0].count, fills[0].fee) == ("0.550000", "2.50", "0.020000")
    assert (fills[0].is_taker, fills[0].time_ms, fills[0].book_side) == (True, 1001, "bid")
    await h.session.close()


async def test_account_pushes_follow_changes_and_are_coalesced(tmp_path: Path) -> None:
    h = make(tmp_path, production=False)
    await on(h)
    await settle(0.05)
    start = len(h.account_messages)
    assert start >= 1 and h.account_messages[-1] is not None
    runtime = h.session._runtime
    assert runtime is not None and runtime.tracker is not None
    for _ in range(20):  # a burst of changes
        runtime.tracker._changed()
    await settle(0.1)
    pushed = len(h.account_messages) - start
    assert 1 <= pushed <= 2  # one view, not twenty
    last = h.account_messages[-1]
    assert last is not None and last["shards"][0]["cash"] == "10.349000"
    await h.session.close()
    assert h.account_messages[-1] is None  # switching off tells the page the feed is gone


async def test_formatting_is_exact(tmp_path: Path) -> None:
    assert money(0) == "0.000000" and money(10_349_000) == "10.349000" and money(-1) == "-0.000001"
    assert (
        money(123_456_789) == "123.456789" and contracts(-5) == "-0.05" and contracts(250) == "2.50"
    )
