"""An orderbook kept from stream events.

Feed it every ``BookSnapshotEvent`` and ``BookDeltaEvent`` (including the ``replay`` ones sent at
subscribe time) and call ``reset()`` on a ``Resync``. A YES bid at X is a NO ask at 1 - X, so the
book holds bids only, like Kalshi's. Sizes are exact ``Decimal`` contracts.
"""

from dataclasses import dataclass, field
from decimal import Decimal

from kalshiterm_client.stream_events import BookDeltaEvent, BookSnapshotEvent

ONE = Decimal(1)


@dataclass
class Book:
    yes: dict[Decimal, Decimal] = field(default_factory=dict)  # price -> contracts
    no: dict[Decimal, Decimal] = field(default_factory=dict)
    sid: int | None = None
    seq: int | None = None
    inconsistent: bool = False  # a change removed more than the book held: a change was missed

    @property
    def best_yes_bid(self) -> Decimal | None:
        return max(self.yes, default=None)

    @property
    def best_no_bid(self) -> Decimal | None:
        return max(self.no, default=None)

    @property
    def best_yes_ask(self) -> Decimal | None:
        bid = self.best_no_bid
        return None if bid is None else ONE - bid

    @property
    def best_no_ask(self) -> Decimal | None:
        bid = self.best_yes_bid
        return None if bid is None else ONE - bid

    def levels(self, side: str, depth: int | None = None) -> list[tuple[Decimal, Decimal]]:
        """Best price first."""
        book = self.yes if side == "yes" else self.no
        ranked = sorted(book.items(), key=lambda kv: kv[0], reverse=True)
        return ranked if depth is None else ranked[:depth]


class BookTracker:
    def __init__(self) -> None:
        self.books: dict[str, Book] = {}

    def reset(self) -> None:
        """Forget everything (after a ``Resync``); fresh snapshots follow."""
        self.books.clear()

    def book(self, ticker: str) -> Book | None:
        return self.books.get(ticker)

    def apply(self, event: BookSnapshotEvent | BookDeltaEvent) -> bool:
        """Apply one event; True if it changed the book."""
        if isinstance(event, BookSnapshotEvent):
            self.books[event.ticker] = Book(
                yes={level.price: level.size for level in event.yes if level.size > 0},
                no={level.price: level.size for level in event.no if level.size > 0},
                sid=event.sid,
                seq=event.seq,
            )
            return True
        book = self.books.get(event.ticker)
        if book is None:
            return False  # a change for a book we never saw a snapshot of
        if book.sid is not None and event.sid == book.sid and event.seq is not None:
            if book.seq is not None and event.seq <= book.seq:
                return False  # already included in what we hold
            book.seq = event.seq
        side = book.yes if event.side == "yes" else book.no
        size = side.get(event.price, Decimal(0)) + event.delta
        if size < 0:
            book.inconsistent = True
            size = Decimal(0)
        if size == 0:
            side.pop(event.price, None)
        else:
            side[event.price] = size
        return True
