from decimal import Decimal

from kalshiterm_client.book import BookTracker
from kalshiterm_client.stream_events import BookDeltaEvent, BookSnapshotEvent

D = Decimal
WHEN = "2026-10-12T12:00:00.000000+00:00"


def snapshot(
    yes: list[tuple[str, str]],
    no: list[tuple[str, str]],
    *,
    sid: int | None = 1,
    seq: int | None = 10,
    ticker: str = "T",
) -> BookSnapshotEvent:
    return BookSnapshotEvent.model_validate(
        {
            "ticker": ticker,
            "time": WHEN,
            "sid": sid,
            "seq": seq,
            "replay": True,
            "yes": [{"price": p, "size": s} for p, s in yes],
            "no": [{"price": p, "size": s} for p, s in no],
        }
    )


def delta(
    side: str,
    price: str,
    change: str,
    *,
    seq: int | None = 11,
    sid: int | None = 1,
    ticker: str = "T",
) -> BookDeltaEvent:
    return BookDeltaEvent.model_validate(
        {
            "ticker": ticker,
            "time": WHEN,
            "cursor": 5,
            "sid": sid,
            "seq": seq,
            "side": side,
            "price": price,
            "delta": change,
        }
    )


def test_a_snapshot_then_changes_build_the_book_exactly() -> None:
    tracker = BookTracker()
    assert tracker.apply(
        snapshot([("0.400000", "10.00"), ("0.390000", "5.00")], [("0.580000", "7.00")])
    )
    assert tracker.apply(delta("yes", "0.400000", "2.50", seq=11))
    assert tracker.apply(delta("yes", "0.390000", "-5.00", seq=12))  # emptied: it disappears
    assert tracker.apply(delta("no", "0.570000", "3.00", seq=13))  # a new level
    book = tracker.book("T")
    assert book is not None and book.yes == {D("0.400000"): D("12.50")}
    assert book.no == {D("0.580000"): D("7.00"), D("0.570000"): D("3.00")}
    assert book.best_yes_bid == D("0.400000") and book.best_no_bid == D("0.580000")
    assert book.best_yes_ask == D("0.420000") and book.best_no_ask == D("0.600000")
    assert book.levels("no") == [(D("0.580000"), D("7.00")), (D("0.570000"), D("3.00"))]
    assert book.levels("no", depth=1) == [(D("0.580000"), D("7.00"))]
    assert book.inconsistent is False


def test_an_empty_side_has_no_best_prices() -> None:
    tracker = BookTracker()
    tracker.apply(snapshot([("0.400000", "1.00")], []))
    book = tracker.book("T")
    assert book is not None and book.best_no_bid is None and book.best_yes_ask is None
    assert book.best_no_ask == D("0.600000")


def test_changes_already_in_the_snapshot_are_not_applied_twice() -> None:
    tracker = BookTracker()
    tracker.apply(snapshot([("0.400000", "10.00")], [], seq=10))
    assert tracker.apply(delta("yes", "0.400000", "1.00", seq=10)) is False  # same seq: included
    assert tracker.apply(delta("yes", "0.400000", "1.00", seq=9)) is False  # older: included
    assert tracker.apply(delta("yes", "0.400000", "1.00", seq=11)) is True
    book = tracker.book("T")
    assert book is not None and book.yes[D("0.400000")] == D("11.00") and book.seq == 11


def test_removing_more_than_the_book_holds_flags_a_missed_change_and_never_goes_negative() -> None:
    tracker = BookTracker()
    tracker.apply(snapshot([("0.400000", "1.00")], []))
    tracker.apply(delta("yes", "0.400000", "-3.00"))
    book = tracker.book("T")
    assert book is not None and book.inconsistent is True and book.yes == {}


def test_a_change_for_a_book_we_never_saw_is_ignored_and_books_are_per_market() -> None:
    tracker = BookTracker()
    assert tracker.apply(delta("yes", "0.400000", "1.00", ticker="NEW")) is False
    tracker.apply(snapshot([("0.400000", "1.00")], [], ticker="A"))
    tracker.apply(snapshot([("0.700000", "9.00")], [], ticker="B"))
    tracker.apply(delta("yes", "0.400000", "1.00", ticker="A"))
    a, b = tracker.book("A"), tracker.book("B")
    assert a is not None and b is not None
    assert a.yes == {D("0.400000"): D("2.00")} and b.yes == {D("0.700000"): D("9.00")}
    assert tracker.book("NEW") is None


def test_a_new_snapshot_replaces_the_book_and_reset_forgets_everything() -> None:
    tracker = BookTracker()
    tracker.apply(snapshot([("0.400000", "1.00")], [("0.500000", "1.00")], seq=10))
    tracker.apply(snapshot([("0.450000", "2.00")], [], seq=20))
    book = tracker.book("T")
    assert book is not None and book.yes == {D("0.450000"): D("2.00")} and book.no == {}
    tracker.reset()
    assert tracker.book("T") is None and tracker.books == {}


def test_levels_of_size_zero_in_a_snapshot_are_dropped_and_prices_stay_exact() -> None:
    tracker = BookTracker()
    tracker.apply(snapshot([("0.123456", "0.01"), ("0.100000", "0.00")], []))
    book = tracker.book("T")
    assert book is not None and book.yes == {D("0.123456"): D("0.01")}
    assert all(isinstance(price, Decimal) for price in book.yes)
