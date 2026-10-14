import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from kalshiterm_client.account import (
    TrackedOrder,
    to_e2,
    to_e6,
)
from kalshiterm_client.risk import (
    Action,
    Code,
    OrderIntent,
    Outcome,
    RiskPolicy,
    check,
)
from kalshiterm_client.trading_state import TradingState
from trading_fakes import (
    T0,
    FakeRest,
    make,
    order,
    position,
    settle,
    status,
)


def user_order(**kw: Any) -> dict[str, Any]:
    base = {
        "order_id": "o-1",
        "client_order_id": "c-1",
        "ticker": "KXA-1",
        "exchange_index": 0,
        "status": "resting",
        "side": "yes",
        "outcome_side": "yes",
        "book_side": "bid",
        "yes_price_dollars": "0.4000",
        "initial_count_fp": "2.00",
        "fill_count_fp": "0.00",
        "remaining_count_fp": "2.00",
        "last_updated_ts_ms": 1000,
    }
    return {**base, **kw}


def test_exact_conversions_refuse_finer_precision() -> None:
    assert to_e6(Decimal("10.3490")) == 10_349_000 and to_e6(None) == 0
    assert to_e2(Decimal("2.50")) == 250
    with pytest.raises(ValueError):
        to_e6(Decimal("0.0000001"))
    with pytest.raises(ValueError):
        to_e2(Decimal("0.001"))


@pytest.mark.parametrize(
    ("book", "label", "adds_yes", "action"),
    [
        ("bid", "yes", True, Action.BUY),
        ("bid", "no", True, Action.BUY),  # Kalshi's outcome label is ignored: a bid is a bid
        ("ask", "yes", False, Action.SELL),
        # Observed live 2026-10-13: a BUY of NO at 1c is reported as "sell, no, ask, yes 0.99"
        # and sits in the book as a NO bid, so it pushes the position towards NO.
        ("ask", "no", False, Action.SELL),
    ],
)
def test_which_way_a_resting_order_pushes_the_position(
    book: str, label: str, adds_yes: bool, action: Action
) -> None:
    t = TrackedOrder("o", "c", "T", 0, "resting", label, book, 400_000, 200, 0, 200, 0)
    assert t.adds_yes is adds_yes and t.action is action
    assert t.own_price_e6 == 400_000  # always the YES price
    assert t.total_e2 == 200


def test_an_amended_orders_total_is_filled_plus_remaining_not_the_original_count() -> None:
    t = TrackedOrder("o", "c", "T", 0, "resting", "yes", "bid", 20_000, 100, 0, 200, 0)
    assert t.total_e2 == 200  # Kalshi leaves the initial count (1) alone after an amend to 2
    partly = TrackedOrder("o", "c", "T", 0, "resting", "yes", "bid", 20_000, 300, 100, 200, 0)
    assert partly.total_e2 == 300


async def test_nothing_is_reported_before_the_first_full_read(tmp_path: Path) -> None:
    tracker, *_ = make(tmp_path)
    assert tracker.snapshot() is None


async def test_start_subscribes_reads_everything_and_builds_an_exact_snapshot(
    tmp_path: Path,
) -> None:
    rest = FakeRest()
    rest.positions = [
        position("KXA-1", "3.00", "1.20", "-0.25"),
        position("KXB-1", "-1.00", "0.40"),
    ]
    rest.orders = [
        order("o-1", "KXA-1", outcome="yes", book="bid", yes_price="0.4000", remaining="2.00"),
        order("o-2", "KXB-1", outcome="no", book="ask", yes_price="0.3000", remaining="1.00"),
        order("o-3", "KXA-1", status_="canceled"),
    ]
    tracker, _, ws, _ = make(tmp_path, rest)
    await tracker.start()
    try:
        assert ws.subscribed == ["fill", "user_orders", "market_positions"]
        snap = tracker.snapshot()
        assert snap is not None and snap.stream_ok and not snap.halted
        # shard 0 holds 10.349 in cash, of which 0.80 + 0.70 may be needed by the resting orders
        assert {i: s.balance_e6 for i, s in snap.shards.items()} == {
            0: 8_849_000,
            1: 0,
            2: 0,
            3: 2_500_000,
        }
        assert snap.shards[2].trading_active is False and snap.shards[0].trading_active
        assert all(s.transfers_active for s in snap.shards.values())
        assert snap.open_orders == 2  # the cancelled one is not counted
        assert snap.markets["KXA-1"].position_e2 == 300
        assert snap.markets["KXA-1"].resting_yes_e2 == 200  # a bid on YES
        assert snap.markets["KXB-1"].position_e2 == -100
        assert snap.markets["KXB-1"].resting_yes_e2 == 0
        assert snap.markets["KXB-1"].resting_no_e2 == 100  # an ask pushes towards NO
        # exposure: positions 1.20 + 0.40, the bid 2 x 0.40, and the ask at 0.30 (selling YES that
        # is not held, so (1 - 0.30) of collateral)
        assert snap.exposure_e6 == 1_200_000 + 400_000 + 800_000 + 700_000
        assert snap.as_of == datetime.fromtimestamp(T0, UTC)
    finally:
        await tracker.stop()


async def test_missing_transfer_flags_mean_funding_is_not_allowed(tmp_path: Path) -> None:
    rest = FakeRest()
    rest.status_value = status(transfers=None)
    tracker, *_ = make(tmp_path, rest)
    await tracker.start()
    try:
        snap = tracker.snapshot()
        assert snap is not None and not any(s.transfers_active for s in snap.shards.values())
    finally:
        await tracker.stop()


async def test_order_messages_replace_the_order_and_older_ones_are_ignored(tmp_path: Path) -> None:
    tracker, rest, ws, _ = make(tmp_path)
    await tracker.start()
    try:
        ws.push("user_order", user_order(last_updated_ts_ms=1000))
        await settle()
        assert [o.remaining_e2 for o in tracker.resting_orders()] == [200]
        ws.push(
            "user_order",
            user_order(remaining_count_fp="1.00", fill_count_fp="1.00", last_updated_ts_ms=2000),
        )
        ws.push("user_order", user_order(last_updated_ts_ms=1500))  # stale: arrived late
        await settle()
        assert [(o.remaining_e2, o.fill_e2) for o in tracker.resting_orders()] == [(100, 100)]
        ws.push("user_order", user_order(status="canceled", last_updated_ts_ms=3000))
        await settle()
        assert tracker.resting_orders() == [] and tracker.snapshot().open_orders == 0  # type: ignore[union-attr]
    finally:
        await tracker.stop()


async def test_position_messages_replace_the_markets_position(tmp_path: Path) -> None:
    rest = FakeRest()
    rest.positions = [position("KXA-1", "1.00", "0.40", "0.10")]
    tracker, _, ws, _ = make(tmp_path, rest)
    await tracker.start()
    try:
        ws.push(
            "market_position",
            {"market_ticker": "KXA-1", "position_fp": "-4.00", "position_cost_dollars": "1.9"},
        )
        await settle()
        held = tracker.positions["KXA-1"]
        assert (held.position_e2, held.exposure_e6, held.realized_e6) == (-400, 1_900_000, 100_000)
        ws.push(
            "market_position",
            {"market_ticker": "KXNEW-1", "position_fp": "2.00", "realized_pnl_dollars": "-0.5"},
        )
        await settle()
        assert tracker.positions["KXNEW-1"].realized_e6 == -500_000
    finally:
        await tracker.stop()


async def test_a_fill_is_counted_once_and_makes_the_balance_be_reread_at_once(
    tmp_path: Path,
) -> None:
    tracker, rest, ws, _ = make(tmp_path)
    await tracker.start()
    try:
        before = rest.balance_calls
        fill = {
            "trade_id": "t-1",
            "order_id": "o-1",
            "market_ticker": "KXA-1",
            "yes_price_dollars": "0.4000",
            "count_fp": "1.00",
            "fee_cost": "0.0200",
        }
        ws.push("fill", fill)
        ws.push("fill", fill)  # duplicate delivery
        await settle()
        assert [f.trade_id for f in tracker.fills] == ["t-1"]
        assert rest.balance_calls > before
    finally:
        await tracker.stop()


async def test_after_a_reconnect_the_stream_is_not_ok_until_everything_is_read_again(
    tmp_path: Path,
) -> None:
    rest = FakeRest()
    rest.positions = [position("KXA-1", "1.00")]
    tracker, _, ws, _ = make(tmp_path, rest)
    await tracker.start()
    try:
        assert tracker.snapshot().stream_ok  # type: ignore[union-attr]
        # while the link was down the world changed: a fill moved the position, an order went
        rest.positions = [position("KXA-1", "5.00")]
        rest.orders = [order("o-9", "KXA-1")]
        rest.fail_next = 10**6  # the re-reads fail for a while; the tracker keeps trying
        ws.push("reconnected")
        for _ in range(20):
            await asyncio.sleep(0)
        assert not tracker.synced
        down = tracker.snapshot()
        assert down is not None and not down.stream_ok  # the risk layer would refuse now
        rest.fail_next = 0
        # a message sent right after the reconnect event must be applied after the re-read
        ws.push(
            "user_order",
            user_order(order_id="o-9", remaining_count_fp="1.00", last_updated_ts_ms=5000),
        )
        for _ in range(200):
            await asyncio.sleep(0)
            if tracker.synced:
                break
        assert tracker.synced
        snap = tracker.snapshot()
        assert snap is not None and snap.stream_ok
        assert snap.markets["KXA-1"].position_e2 == 500
        await settle()
        assert [(o.order_id, o.remaining_e2) for o in tracker.resting_orders()] == [("o-9", 100)]
    finally:
        await tracker.stop()


async def test_the_snapshot_says_the_stream_is_down_when_the_socket_closes(tmp_path: Path) -> None:
    tracker, _, ws, _ = make(tmp_path)
    await tracker.start()
    ws.queue.put_nowait(None)  # the socket is gone for good
    await settle()
    snap = tracker.snapshot()
    assert snap is not None and not snap.stream_ok
    await tracker.stop()


async def test_a_balance_that_cannot_be_refreshed_goes_stale_and_the_risk_layer_refuses(
    tmp_path: Path,
) -> None:
    tracker, rest, ws, clock = make(tmp_path, interval=0.01)
    await tracker.start()
    try:
        rest.fail_next = 10**6  # every balance poll fails from now on
        clock.now += 30
        await asyncio.sleep(0.05)
        snap = tracker.snapshot()
        assert snap is not None
        assert snap.as_of == datetime.fromtimestamp(T0, UTC)  # the last good read
        intent = OrderIntent("KXA-1", 0, Action.BUY, Outcome.YES, 100, 500_000, 1_000_000)
        verdict = check(
            intent, snap, RiskPolicy(), now=datetime.fromtimestamp(clock.now, UTC), production=False
        )
        assert Code.STALE_SNAPSHOT in {r.code for r in verdict.refusals}
    finally:
        await tracker.stop()


async def test_the_days_loss_is_measured_from_the_baseline_and_survives_a_restart(
    tmp_path: Path,
) -> None:
    rest = FakeRest()
    rest.positions = [position("KXA-1", "2.00", realized="1.00")]
    state = TradingState(tmp_path / "state.json")
    tracker, _, ws, _ = make(tmp_path, rest, state=state)
    await tracker.start()
    try:
        assert tracker.snapshot().realized_pnl_today_e6 == 0  # type: ignore[union-attr]
        ws.push(
            "market_position",
            {"market_ticker": "KXA-1", "position_fp": "0", "realized_pnl_dollars": "-0.75"},
        )
        await settle()
        assert tracker.snapshot().realized_pnl_today_e6 == -1_750_000  # type: ignore[union-attr]
    finally:
        await tracker.stop()

    # the program is restarted later the same day: the baseline is not re-taken from the new total
    rest.positions = [position("KXA-1", "0.00", realized="-0.75")]
    again, *_ = make(tmp_path, rest, state=TradingState.load(state.path))
    await again.start()
    try:
        assert again.snapshot().realized_pnl_today_e6 == -1_750_000  # type: ignore[union-attr]
    finally:
        await again.stop()


async def test_a_new_day_starts_from_zero(tmp_path: Path) -> None:
    rest = FakeRest()
    rest.positions = [position("KXA-1", "0.00", realized="-3.00")]
    state = TradingState(tmp_path / "state.json")
    state.roll("2026-10-12")
    state.set_baseline(0)
    state.begin_transfer("t", 0, 1, 1_000_000)
    tracker, *_ = make(tmp_path, rest, state=state)
    await tracker.start()
    try:
        snap = tracker.snapshot()
        assert snap is not None and snap.realized_pnl_today_e6 == 0
        assert snap.transferred_today_e6 == 0  # yesterday's transfer no longer counts
    finally:
        await tracker.stop()


async def test_the_halt_and_the_days_transfers_reach_the_snapshot(tmp_path: Path) -> None:
    state = TradingState(tmp_path / "state.json")
    tracker, *_ = make(tmp_path, state=state)
    await tracker.start()
    try:
        state.halt("test")
        state.begin_transfer("t", 0, 3, 700_000)
        snap = tracker.snapshot()
        assert snap is not None and snap.halted and snap.transferred_today_e6 == 700_000
    finally:
        await tracker.stop()


async def test_a_failed_first_read_raises_and_leaves_nothing_synced(tmp_path: Path) -> None:
    rest = FakeRest()
    rest.fail_next = 1
    tracker, *_ = make(tmp_path, rest)
    with pytest.raises(RuntimeError):
        await tracker.start()
    assert tracker.snapshot() is None


# --- the change hook the UI listens to ------------------------------------------------------------


async def test_the_change_hook_fires_when_the_view_may_differ_and_a_broken_listener_is_harmless(
    tmp_path: Path,
) -> None:
    rest = FakeRest()
    rest.positions = [position("KXA-1", "1.00")]
    tracker, _, ws, _ = make(tmp_path, rest)
    calls: list[int] = []
    tracker.on_change = lambda: calls.append(1)
    await tracker.start()
    try:
        after_start = len(calls)
        assert after_start >= 2  # the balance read and the full read
        ws.push("user_order", user_order())
        ws.push("market_position", {"market_ticker": "KXA-1", "position_fp": "2.00"})
        ws.push(
            "fill",
            {"trade_id": "t-1", "order_id": "o-1", "market_ticker": "KXA-1",
             "yes_price_dollars": "0.4", "count_fp": "1.00"},
        )  # fmt: skip
        await settle()
        assert len(calls) >= after_start + 3
        before_noise = len(calls)
        ws.push("subscribed", {})  # not an account message: nothing changed
        await settle()
        assert len(calls) == before_noise

        def broken() -> None:
            raise RuntimeError("a listener's own bug")

        tracker.on_change = broken
        ws.push("user_order", user_order(order_id="o-2", last_updated_ts_ms=5000))
        await settle()
        assert "o-2" in tracker.orders  # the state still updated
    finally:
        await tracker.stop()
