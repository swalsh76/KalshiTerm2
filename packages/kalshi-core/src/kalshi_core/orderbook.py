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
    """One thing that happened to the books.

    ``book`` on a SNAPSHOT event is an immutable-by-convention *copy* taken when the snapshot
    was applied, so a consumer reading it later still sees that moment. On a DELTA event it is
    the *live* book, which may already include later deltas; use the event's ``message`` for
    the change itself.
    """

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
        self._removed: set[str] = set()  # markets dropped live; their late messages are ignored

    def begin_subscription(self, sid: int, tickers: list[str]) -> None:
        """Declare a new subscription: no books yet, and its first message must be seq 1."""
        self._subs[sid] = _SubState(1, set(tickers))
        self.stale.update(tickers)
        self._removed.difference_update(tickers)

    def end_subscription(self, sid: int) -> None:
        self._subs.pop(sid, None)

    def begin_markets(self, sid: int, tickers: list[str]) -> None:
        """Markets added to a live subscription: no book until a snapshot arrives."""
        if sid in self._subs:
            self._subs[sid].tickers.update(tickers)
        self.stale.update(tickers)
        self._removed.difference_update(tickers)

    def forget_markets(self, sid: int, tickers: list[str]) -> None:
        """Markets removed from a live subscription: drop their books, ignore stragglers."""
        if sid in self._subs:
            self._subs[sid].tickers.difference_update(tickers)
        for ticker in tickers:
            self.books.pop(ticker, None)
            self.stale.discard(ticker)
        self._removed.update(tickers)

    def _count_control(self, message: WsMessage) -> list[BookEvent]:
        """A command reply consumed a sequence number: count it, flag a gap if it is out of step."""
        sub = self._subs.get(message.sid or 0)
        if sub is None or message.seq is None:
            return []
        events: list[BookEvent] = []
        if message.seq != sub.expected:
            affected = tuple(sorted(sub.tickers))
            self.stale.update(affected)
            reason = f"seq {message.seq}, expected {sub.expected}"
            events.append(BookEvent(GAP, tickers=affected, detail=reason))
        sub.expected = message.seq + 1
        return events

    def process(self, message: WsMessage) -> list[BookEvent]:
        if message.type == RECONNECTED:
            self.books.clear()
            self.stale.clear()
            self._subs.clear()  # seq and sid restart on a new connection
            self._removed.clear()
            return [BookEvent(RESET, message=message)]
        if message.type == "control":
            return self._count_control(message)
        if message.type not in ("orderbook_snapshot", "orderbook_delta"):
            return [BookEvent(MESSAGE, message=message)]
        payload = message.payload()
        assert isinstance(payload, OrderbookSnapshotMsg | OrderbookDeltaMsg)
        ticker = payload.market_ticker
        events: list[BookEvent] = []
        sub = self._subs.setdefault(message.sid or 0, _SubState(message.seq or 1))
        removed = ticker in self._removed
        if not removed:
            sub.tickers.add(ticker)
        if message.seq != sub.expected:
            affected = tuple(sorted(sub.tickers | ({ticker} - self._removed)))
            self.stale.update(affected)
            reason = f"seq {message.seq}, expected {sub.expected}"
            events.append(BookEvent(GAP, tickers=affected, detail=reason))
        if message.seq is not None:
            sub.expected = message.seq + 1
        if removed:
            return events  # a late message for a market we dropped: counted in sequence, ignored
        if isinstance(payload, OrderbookSnapshotMsg):
            book = LocalOrderBook.from_snapshot(payload)
            self.books[ticker] = book
            self.stale.discard(ticker)
            events.append(BookEvent(SNAPSHOT, ticker, book.copy(), message=message))
        elif ticker not in self.stale and ticker in self.books:
            book = self.books[ticker]
            if book.apply_delta(payload):
                events.append(BookEvent(DELTA, ticker, book, message=message))
            else:
                self.stale.add(ticker)
                events.append(BookEvent(GAP, tickers=(ticker,), detail="negative quantity"))
        return events

    def apply_rest_snapshot(self, ticker: str, rest_book: OrderBook) -> BookEvent:
        """Fallback when an in-stream snapshot is unavailable: approximate, not seq-aligned."""
        book = LocalOrderBook.from_rest(ticker, rest_book)
        self.books[ticker] = book
        self.stale.discard(ticker)
        return BookEvent(SNAPSHOT, ticker, book.copy(), detail="rest")


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
        out_limit: int = 1_000,
        periodic_snapshot_interval: float | None = None,
    ) -> None:
        self._ws = ws
        self._rest = rest
        self._tickers = list(tickers)
        self._snapshot_timeout = snapshot_timeout
        self._out_limit = out_limit
        self._periodic_interval = periodic_snapshot_interval
        self._periodic: asyncio.Task[None] | None = None
        self._space = asyncio.Event()  # set whenever the consumer has taken an event
        self._space.set()
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
        if self._periodic_interval:
            self._periodic = asyncio.create_task(self._periodic_snapshots(self._periodic_interval))

    @property
    def tickers(self) -> list[str]:
        """The markets currently subscribed."""
        return list(self._tickers)

    async def add_markets(self, tickers: list[str]) -> None:
        """Start watching more markets on the live subscription.

        Kalshi sends no snapshot for added markets, so one is requested for each; until it
        arrives the market's book is stale and its deltas are ignored.
        """
        new = [t for t in dict.fromkeys(tickers) if t not in self._tickers]
        if not new:
            return
        if self._sid is None:  # nothing subscribed (never started, or everything was removed)
            self._tickers = [*self._tickers, *new]
            self._sid = await self._ws.subscribe(CHANNEL, market_tickers=self._tickers)
            self.tracker.begin_subscription(
                self._sid, self._tickers
            )  # snapshots arrive by themselves
            return
        current = await self._ws.update_subscription(self._sid, "add_markets", new)
        self._tickers = current
        self.tracker.begin_markets(self._sid, new)
        self._start_resync(tuple(new))

    async def remove_markets(self, tickers: list[str]) -> None:
        """Stop watching markets; their books are dropped and late messages ignored."""
        gone = [t for t in dict.fromkeys(tickers) if t in self._tickers]
        if not gone or self._sid is None:
            return
        sid = self._sid
        remaining = [t for t in self._tickers if t not in gone]
        if not remaining:
            # An empty market list could be read by Kalshi as "all markets": unsubscribe instead.
            await self._ws.unsubscribe(sid)
            self._sid = None
            self.tracker.forget_markets(sid, gone)
            self.tracker.end_subscription(sid)
        else:
            remaining = await self._ws.update_subscription(sid, "delete_markets", gone)
            self.tracker.forget_markets(sid, gone)
        self._tickers = remaining

    async def _periodic_snapshots(self, interval: float) -> None:
        """Ask for an authoritative snapshot of every market once per ``interval``.

        Requests are spread evenly (not sent in a burst) and round-robin over the markets
        currently watched. These snapshots are the checkpoints deltas are replayed from.
        """
        index = 0
        while True:
            watched = self._tickers
            if not watched or self._sid is None:
                await asyncio.sleep(1.0)
                continue
            await asyncio.sleep(max(0.01, interval / len(watched)))
            watched = self._tickers
            if not watched or self._sid is None:
                continue
            ticker = watched[index % len(watched)]
            index += 1
            with contextlib.suppress(KalshiWSError):
                await self._ws.request_snapshot(self._sid, ticker)

    async def close(self) -> None:
        for task in [self._pump, self._periodic, *self._tasks]:
            if task is not None:
                task.cancel()
        for task in [self._pump, self._periodic, *self._tasks]:
            if task is not None:
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    def stats(self) -> dict[str, int]:
        """WebSocket queue stats plus this feed's own output backlog."""
        return {**self._ws.stats(), "feed_depth": self._out.qsize()}

    async def events(self) -> AsyncIterator[BookEvent]:
        while True:
            item = await self._out.get()
            self._space.set()
            if item is None:
                return
            if isinstance(item, Exception):
                raise item
            yield item

    async def _run_pump(self) -> None:
        try:
            async for message in self._ws.messages():
                # Backpressure: leave the backlog in the WebSocket client's bounded queue
                # instead of piling it up here.
                while self._out.qsize() >= self._out_limit:
                    self._space.clear()
                    await self._space.wait()
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
