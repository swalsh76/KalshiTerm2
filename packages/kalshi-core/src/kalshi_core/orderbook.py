"""Local orderbooks built from Kalshi's ``orderbook_delta`` channel.

Facts established against the live API (the docs are silent on them):

* ``seq`` is one counter per *subscription*, shared by every market in it, starting at 1 and
  rising by one per message. Snapshots (one per market, sent on subscribe) consume numbers too.
* A delta is a signed change in contracts at a price on one side; a level is removed when its
  quantity reaches zero. Snapshot + deltas reproduced the REST book exactly.
* An ``update_subscription`` ``get_snapshot`` command is answered by an in-stream snapshot with
  the next ``seq``, so it aligns exactly with the delta stream; REST snapshots carry no ``seq``.

A sequence gap therefore invalidates every book in the subscription.
"""

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from decimal import Decimal
from types import TracebackType
from typing import Self

from kalshi_core.models import OrderBook
from kalshi_core.rest import KalshiAPIError, KalshiRestClient
from kalshi_core.ws import RECONNECTED, KalshiWebSocket, KalshiWSError
from kalshi_core.ws_models import OrderbookDeltaMsg, OrderbookSnapshotMsg, WsMessage

log = logging.getLogger("kalshi_core")

ONE = Decimal(1)
CHANNEL = "orderbook_delta"

# Event kinds
SNAPSHOT = "snapshot"
DELTA = "delta"
GAP = "gap"
RESET = "reset"
RESUBSCRIBE_FAILED = "resubscribe_failed"
RESYNC_FAILED = "resync_failed"
MESSAGE = "message"


class LocalOrderBook:
    """Bids on both sides; asks are derived (a YES bid at X is a NO ask at 1 - X)."""

    def __init__(
        self,
        ticker: str,
        yes: dict[Decimal, Decimal] | None = None,
        no: dict[Decimal, Decimal] | None = None,
        *,
        approximate: bool = False,
    ) -> None:
        self.ticker = ticker
        self.yes: dict[Decimal, Decimal] = yes or {}
        self.no: dict[Decimal, Decimal] = no or {}
        self.approximate = approximate  # True if built from a REST snapshot (no seq alignment)

    @classmethod
    def from_snapshot(cls, snap: OrderbookSnapshotMsg) -> Self:
        return cls(
            snap.market_ticker,
            {p: q for p, q in snap.yes_dollars_fp if q > 0},
            {p: q for p, q in snap.no_dollars_fp if q > 0},
        )

    @classmethod
    def from_rest(cls, ticker: str, book: OrderBook) -> Self:
        return cls(
            ticker,
            {lv.price: lv.quantity for lv in book.yes if lv.quantity > 0},
            {lv.price: lv.quantity for lv in book.no if lv.quantity > 0},
            approximate=True,
        )

    def apply_delta(self, delta: OrderbookDeltaMsg) -> bool:
        """Apply a signed change. Returns False if it would make a quantity negative."""
        levels = self.yes if delta.side == "yes" else self.no
        quantity = levels.get(delta.price_dollars, Decimal(0)) + delta.delta_fp
        if quantity < 0:
            return False
        if quantity == 0:
            levels.pop(delta.price_dollars, None)
        else:
            levels[delta.price_dollars] = quantity
        return True

    def bids(self, side: str) -> list[tuple[Decimal, Decimal]]:
        """Levels for ``yes`` or ``no``, best (highest) price first."""
        levels = self.yes if side == "yes" else self.no
        return sorted(levels.items(), reverse=True)

    @property
    def best_yes_bid(self) -> Decimal | None:
        return max(self.yes) if self.yes else None

    @property
    def best_no_bid(self) -> Decimal | None:
        return max(self.no) if self.no else None

    @property
    def best_yes_ask(self) -> Decimal | None:
        return ONE - max(self.no) if self.no else None

    @property
    def best_no_ask(self) -> Decimal | None:
        return ONE - max(self.yes) if self.yes else None

    def copy(self) -> "LocalOrderBook":
        return LocalOrderBook(
            self.ticker, dict(self.yes), dict(self.no), approximate=self.approximate
        )


@dataclass(slots=True)
class BookEvent:
    kind: str
    ticker: str | None = None
    book: LocalOrderBook | None = None
    tickers: tuple[str, ...] = ()
    message: WsMessage | None = None
    detail: str = ""


@dataclass
class _SubState:
    expected: int = 1
    tickers: set[str] = field(default_factory=set)


class BookTracker:
    """Pure state machine: feed it messages, it applies them and reports what happened."""

    def __init__(self) -> None:
        self.books: dict[str, LocalOrderBook] = {}
        self.stale: set[str] = set()
        self._subs: dict[int, _SubState] = {}

    def begin_subscription(self, sid: int, tickers: list[str]) -> None:
        """Declare a new subscription: no books yet, and its first message must be seq 1."""
        self._subs[sid] = _SubState(1, set(tickers))
        self.stale.update(tickers)

    def process(self, message: WsMessage) -> list[BookEvent]:
        if message.type == RECONNECTED:
            self.books.clear()
            self.stale.clear()
            self._subs.clear()  # seq and sid restart on a new connection
            return [BookEvent(RESET, message=message)]
        if message.type not in ("orderbook_snapshot", "orderbook_delta"):
            return [BookEvent(MESSAGE, message=message)]
        payload = message.payload()
        assert isinstance(payload, OrderbookSnapshotMsg | OrderbookDeltaMsg)
        ticker = payload.market_ticker
        events: list[BookEvent] = []
        sub = self._subs.setdefault(message.sid or 0, _SubState(message.seq or 1))
        sub.tickers.add(ticker)
        if message.seq != sub.expected:
            affected = tuple(sorted(sub.tickers | {ticker}))
            self.stale.update(affected)
            reason = f"seq {message.seq}, expected {sub.expected}"
            events.append(BookEvent(GAP, tickers=affected, detail=reason))
        if message.seq is not None:
            sub.expected = message.seq + 1
        if isinstance(payload, OrderbookSnapshotMsg):
            book = LocalOrderBook.from_snapshot(payload)
            self.books[ticker] = book
            self.stale.discard(ticker)
            events.append(BookEvent(SNAPSHOT, ticker, book))
        elif ticker not in self.stale and ticker in self.books:
            book = self.books[ticker]
            if book.apply_delta(payload):
                events.append(BookEvent(DELTA, ticker, book))
            else:
                self.stale.add(ticker)
                events.append(BookEvent(GAP, tickers=(ticker,), detail="negative quantity"))
        return events

    def apply_rest_snapshot(self, ticker: str, rest_book: OrderBook) -> BookEvent:
        """Fallback when an in-stream snapshot is unavailable: approximate, not seq-aligned."""
        book = LocalOrderBook.from_rest(ticker, rest_book)
        self.books[ticker] = book
        self.stale.discard(ticker)
        return BookEvent(SNAPSHOT, ticker, book, detail="rest")


class OrderBookFeed:
    """Subscribes to orderbooks and keeps them consistent, recovering from gaps.

    ``events()`` is the single stream: orderbook events plus ``message`` events wrapping
    anything else received on the connection (trades, tickers, ...).
    """

    def __init__(
        self,
        ws: KalshiWebSocket,
        rest: KalshiRestClient,
        tickers: list[str],
        *,
        snapshot_timeout: float = 5.0,
    ) -> None:
        self._ws = ws
        self._rest = rest
        self._tickers = list(tickers)
        self._snapshot_timeout = snapshot_timeout
        self.tracker = BookTracker()
        self._sid: int | None = None
        self._out: asyncio.Queue[BookEvent | Exception | None] = asyncio.Queue()
        self._requests: dict[int, str] = {}
        self._fresh: dict[str, asyncio.Event] = {}
        self._failed: set[str] = set()
        self._resyncing: set[str] = set()
        self._tasks: set[asyncio.Task[None]] = set()
        self._pump: asyncio.Task[None] | None = None

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    async def start(self) -> None:
        self._sid = await self._ws.subscribe(CHANNEL, market_tickers=self._tickers)
        self.tracker.begin_subscription(self._sid, self._tickers)
        self._pump = asyncio.create_task(self._run_pump())

    async def close(self) -> None:
        for task in [self._pump, *self._tasks]:
            if task is not None:
                task.cancel()
        for task in [self._pump, *self._tasks]:
            if task is not None:
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    async def events(self) -> AsyncIterator[BookEvent]:
        while True:
            item = await self._out.get()
            if item is None:
                return
            if isinstance(item, Exception):
                raise item
            yield item

    async def _run_pump(self) -> None:
        try:
            async for message in self._ws.messages():
                self._handle(message)
        except KalshiWSError as exc:
            self._out.put_nowait(exc)
        else:
            self._out.put_nowait(None)

    def _handle(self, message: WsMessage) -> None:
        if message.type == "error" and message.id in self._requests:
            ticker = self._requests.pop(message.id)
            log.warning("get_snapshot for %s rejected: %s", ticker, message.msg)
            self._failed.add(ticker)
            self._fresh[ticker].set()
            return
        if message.type == RECONNECTED:
            self._on_reconnect(message)
            return
        for event in self.tracker.process(message):
            self._out.put_nowait(event)
            if event.kind == SNAPSHOT and event.ticker in self._fresh and not event.detail:
                self._fresh[event.ticker].set()
            elif event.kind == GAP:
                self._start_resync(event.tickers)

    def _on_reconnect(self, message: WsMessage) -> None:
        for task in self._tasks:
            task.cancel()
        self._requests.clear()
        self._fresh.clear()
        self._resyncing.clear()
        for event in self.tracker.process(message):
            self._out.put_nowait(event)
        restored = [r for r in message.msg["resubscribed"] if r["channel"] == CHANNEL]
        if restored:
            self._sid = restored[0]["sid"]
            self.tracker.begin_subscription(restored[0]["sid"], self._tickers)
        else:
            self._sid = None
            detail = "orderbook subscription was not restored"
            self._out.put_nowait(
                BookEvent(RESUBSCRIBE_FAILED, tickers=tuple(self._tickers), detail=detail)
            )

    def _start_resync(self, tickers: tuple[str, ...]) -> None:
        todo = [t for t in tickers if t not in self._resyncing]
        self._resyncing.update(todo)
        for ticker in todo:
            task = asyncio.create_task(self._resync(ticker))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    async def _resync(self, ticker: str) -> None:
        """Recover one book: in-stream snapshot first, REST if that fails or times out."""
        try:
            fresh = self._fresh[ticker] = asyncio.Event()
            self._failed.discard(ticker)
            try:
                if self._sid is None:
                    raise KalshiWSError("no orderbook subscription")
                command_id = await self._ws.request_snapshot(self._sid, ticker)
                self._requests[command_id] = ticker
                await asyncio.wait_for(fresh.wait(), self._snapshot_timeout)
                ok = ticker not in self._failed
            except (KalshiWSError, TimeoutError):
                ok = False
            if ok:
                return
            log.warning("falling back to REST snapshot for %s", ticker)
            try:
                rest_book = await self._rest.orderbook(ticker)
            except (KalshiAPIError, OSError) as exc:
                self._out.put_nowait(BookEvent(RESYNC_FAILED, ticker, detail=str(exc)))
                return
            self._out.put_nowait(self.tracker.apply_rest_snapshot(ticker, rest_book))
        finally:
            self._resyncing.discard(ticker)
            self._fresh.pop(ticker, None)
