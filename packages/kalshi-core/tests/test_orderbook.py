from decimal import Decimal as D
from typing import Any

import pytest
from kalshi_core.models import OrderBook, PriceLevel
from kalshi_core.orderbook import (
    DELTA,
    GAP,
    MESSAGE,
    RESET,
    SNAPSHOT,
    BookTracker,
    LocalOrderBook,
)
from kalshi_core.ws import RECONNECTED
from kalshi_core.ws_models import OrderbookDeltaMsg, WsMessage


def snap(sid: int, seq: int, ticker: str, yes: Any = (), no: Any = ()) -> WsMessage:
    msg = {"market_ticker": ticker, "yes_dollars_fp": list(yes), "no_dollars_fp": list(no)}
    return WsMessage(type="orderbook_snapshot", sid=sid, seq=seq, msg=msg)


def delta(sid: int, seq: int, ticker: str, side: str, price: str, change: str) -> WsMessage:
    msg = {"market_ticker": ticker, "side": side, "price_dollars": price, "delta_fp": change}
    return WsMessage(type="orderbook_delta", sid=sid, seq=seq, msg=msg)


def kinds(events: list[Any]) -> list[str]:
    return [e.kind for e in events]


def test_snapshot_then_signed_deltas_and_removal_at_zero() -> None:
    t = BookTracker()
    t.process(
        snap(1, 1, "M", yes=[("0.4000", "10.00"), ("0.4100", "5.00")], no=[("0.5500", "7.00")])
    )
    t.process(delta(1, 2, "M", "yes", "0.4100", "-2.00"))
    t.process(delta(1, 3, "M", "yes", "0.4000", "-10.00"))  # removes the level
    t.process(delta(1, 4, "M", "no", "0.5600", "3.00"))  # new level
    book = t.books["M"]
    assert book.yes == {D("0.4100"): D("3.00")}
    assert book.no == {D("0.5500"): D("7.00"), D("0.5600"): D("3.00")}


def test_best_prices_and_derived_asks() -> None:
    book = LocalOrderBook(
        "M", {D("0.40"): D(1), D("0.42"): D(2)}, {D("0.55"): D(3), D("0.57"): D(4)}
    )
    assert book.best_yes_bid == D("0.42") and book.best_no_bid == D("0.57")
    assert book.best_yes_ask == D("0.43")  # 1 - best NO bid
    assert book.best_no_ask == D("0.58")  # 1 - best YES bid
    assert [p for p, _ in book.bids("yes")] == [D("0.42"), D("0.40")]
    assert LocalOrderBook("E").best_yes_bid is None
    assert LocalOrderBook("E").best_yes_ask is None


def test_seq_is_one_counter_shared_by_all_markets_in_a_subscription() -> None:
    t = BookTracker()
    t.begin_subscription(1, ["A", "B"])
    events = [
        *t.process(snap(1, 1, "A", yes=[("0.40", "1.00")])),
        *t.process(snap(1, 2, "B", yes=[("0.30", "1.00")])),
        *t.process(delta(1, 3, "A", "yes", "0.40", "1.00")),
        *t.process(delta(1, 4, "B", "yes", "0.30", "1.00")),
    ]
    assert GAP not in kinds(events)
    assert t.stale == set()
    assert t.books["A"].yes == {D("0.40"): D("2.00")}


def test_gap_marks_every_book_in_the_subscription_stale() -> None:
    t = BookTracker()
    t.begin_subscription(1, ["A", "B"])
    t.process(snap(1, 1, "A", yes=[("0.40", "1.00")]))
    t.process(snap(1, 2, "B", yes=[("0.30", "1.00")]))
    events = t.process(delta(1, 4, "A", "yes", "0.40", "1.00"))  # seq 3 was lost
    assert kinds(events) == [GAP]
    assert events[0].tickers == ("A", "B")
    assert "expected 3" in events[0].detail
    assert t.stale == {"A", "B"}
    assert t.books["A"].yes == {D("0.40"): D("1.00")}  # the post-gap delta was not applied


def test_stale_book_ignores_deltas_until_a_snapshot_then_recovers() -> None:
    t = BookTracker()
    t.begin_subscription(1, ["A"])
    t.process(snap(1, 1, "A", yes=[("0.40", "1.00")]))
    t.process(delta(1, 3, "A", "yes", "0.40", "9.00"))  # gap, ignored
    assert kinds(t.process(delta(1, 4, "A", "yes", "0.40", "9.00"))) == []  # no repeat gap
    events = t.process(snap(1, 5, "A", yes=[("0.40", "4.00")]))  # in-stream resync
    assert kinds(events) == [SNAPSHOT]
    assert t.stale == set()
    events = t.process(delta(1, 6, "A", "yes", "0.40", "1.00"))
    assert kinds(events) == [DELTA]
    assert t.books["A"].yes == {D("0.40"): D("5.00")}


def test_snapshot_that_arrives_with_a_gap_is_applied_and_still_reports_the_gap() -> None:
    t = BookTracker()
    t.begin_subscription(1, ["A"])
    t.process(snap(1, 1, "A"))
    events = t.process(snap(1, 7, "A", yes=[("0.40", "1.00")]))
    assert kinds(events) == [GAP, SNAPSHOT]
    assert t.stale == set()  # the snapshot itself is fresh, so the book is usable


def test_first_message_not_seq_1_is_a_gap_when_subscription_was_declared() -> None:
    t = BookTracker()
    t.begin_subscription(1, ["A", "B"])
    events = t.process(snap(1, 2, "B"))  # the snapshot for A (seq 1) was lost
    assert kinds(events) == [GAP, SNAPSHOT]
    assert events[0].tickers == ("A", "B")
    assert t.stale == {"A"}


def test_delta_before_any_snapshot_is_ignored() -> None:
    t = BookTracker()
    t.begin_subscription(1, ["A"])
    assert kinds(t.process(delta(1, 1, "A", "yes", "0.4", "1.00"))) == []
    assert "A" not in t.books


def test_negative_quantity_marks_book_stale_and_reports_a_gap() -> None:
    t = BookTracker()
    t.begin_subscription(1, ["A"])
    t.process(snap(1, 1, "A", yes=[("0.40", "1.00")]))
    events = t.process(delta(1, 2, "A", "yes", "0.40", "-5.00"))
    assert kinds(events) == [GAP]
    assert events[0].detail == "negative quantity"
    assert t.stale == {"A"}


def test_reconnect_clears_books_and_seq_restarting_at_1_is_not_a_gap() -> None:
    t = BookTracker()
    t.begin_subscription(1, ["A"])
    t.process(snap(1, 1, "A", yes=[("0.40", "1.00")]))
    t.process(delta(1, 2, "A", "yes", "0.40", "1.00"))
    events = t.process(WsMessage(type=RECONNECTED, msg={"resubscribed": [], "failed": []}))
    assert kinds(events) == [RESET]
    assert t.books == {} and t.stale == set()
    t.begin_subscription(1, ["A"])  # same sid, fresh seq
    events = t.process(snap(1, 1, "A", yes=[("0.50", "2.00")]))
    assert kinds(events) == [SNAPSHOT]
    assert t.books["A"].yes == {D("0.50"): D("2.00")}


def test_other_messages_pass_through() -> None:
    t = BookTracker()
    trade = WsMessage(type="trade", sid=2, seq=1, msg={})
    events = t.process(trade)
    assert kinds(events) == [MESSAGE] and events[0].message is trade


def test_rest_snapshot_is_flagged_approximate_and_clears_stale() -> None:
    t = BookTracker()
    t.begin_subscription(1, ["A"])
    rest = OrderBook(
        yes=[PriceLevel(price=D("0.40"), quantity=D("3.00"))],
        no=[PriceLevel(price=D("0.55"), quantity=D("0.00"))],
    )
    event = t.apply_rest_snapshot("A", rest)
    assert event.detail == "rest" and event.book is not None and event.book.approximate
    assert t.books["A"].yes == {D("0.40"): D("3.00")} and t.books["A"].no == {}
    assert t.stale == set()


def test_apply_delta_rejects_negative_without_mutating() -> None:
    book = LocalOrderBook("A", {D("0.40"): D("1.00")})
    ok = book.apply_delta(
        OrderbookDeltaMsg(market_ticker="A", side="yes", price_dollars=D("0.40"), delta_fp=D("-2"))
    )
    assert ok is False and book.yes == {D("0.40"): D("1.00")}


def test_copy_is_independent() -> None:
    book = LocalOrderBook("A", {D("0.40"): D("1.00")})
    clone = book.copy()
    clone.yes[D("0.40")] = D("9")
    assert book.yes[D("0.40")] == D("1.00")


@pytest.mark.parametrize("side", ["yes", "no"])
def test_zero_quantity_levels_in_snapshot_are_dropped(side: str) -> None:
    t = BookTracker()
    t.begin_subscription(1, ["A"])
    levels = [("0.40", "0.00"), ("0.41", "2.00")]
    t.process(snap(1, 1, "A", **{side: levels}))
    assert list(getattr(t.books["A"], side)) == [D("0.41")]


def test_a_snapshot_event_keeps_its_moment_while_later_deltas_change_the_live_book() -> None:
    """Consumers read events later (through queues); the stored checkpoint must not drift."""
    t = BookTracker()
    t.begin_subscription(1, ["M"])
    [event] = t.process(snap(1, 1, "M", yes=[("0.40", "10.00")]))
    t.process(delta(1, 2, "M", "yes", "0.40", "5.00"))
    t.process(delta(1, 3, "M", "yes", "0.41", "1.00"))
    assert event.book is not None
    assert event.book.yes == {D("0.40"): D("10.00")}  # as it was when the snapshot arrived
    assert t.books["M"].yes == {D("0.40"): D("15.00"), D("0.41"): D("1.00")}  # the live book moved


def test_a_rest_snapshot_event_is_also_a_copy() -> None:
    t = BookTracker()
    t.begin_subscription(1, ["M"])
    rest = OrderBook(yes=[PriceLevel(price=D("0.40"), quantity=D("3.00"))], no=[])
    event = t.apply_rest_snapshot("M", rest)
    t.books["M"].yes[D("0.99")] = D("1.00")
    assert event.book is not None and D("0.99") not in event.book.yes
