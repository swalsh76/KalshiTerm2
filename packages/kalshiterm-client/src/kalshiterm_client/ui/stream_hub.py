"""One live feed from the KalshiTerm server, shared by every browser connection.

The browser asks for (channel, ticker) pairs; the hub keeps a single ``StreamClient`` open for the
union of what all connections want and fans the events out:

* **The backend keeps the order books** (``BookTracker``) and sends the browser whole books
  (``BookMessage``), at most ``flush_interval`` apart per market, so the page never replays deltas
  and a burst of book changes costs one message, not hundreds.
* **Nothing is lost on a dropped link**: ``StreamClient`` resumes by cursor on its own; the hub
  only reports the state (``Link``) and relays a ``Resync`` (books reset, fresh ones follow).
* **Changing the set of markets** restarts the stream (after a short debounce) with the new union.
  A connection that subscribes to a market already streaming gets the current book and last
  ticker at once, without a restart.
* **Every connection has a bounded queue.** A page that cannot keep up is dropped (close code
  1013, "try again later"); it reconnects and resubscribes. Memory never grows without bound.
* Limits: 100 markets per connection, 200 across the hub.

Nothing here talks to Kalshi; the stream is the KalshiTerm server's.
"""

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator, Callable, Sequence
from datetime import UTC, datetime
from typing import Any, Protocol

from pydantic import BaseModel

from kalshiterm_client.book import BookTracker
from kalshiterm_client.errors import ConnectionProblem
from kalshiterm_client.stream_events import (
    AlertEvent,
    BookDeltaEvent,
    BookSnapshotEvent,
    Connected,
    Disconnected,
    Resync,
    StreamEvent,
    TickerEvent,
    TradeEvent,
)
from kalshiterm_client.ui import messages as m

log = logging.getLogger(__name__)

MAX_BOOK_DEPTH = 25
QUEUE_LIMIT = 2000
MAX_PER_CONNECTION = 100
MAX_TOTAL = 200
SERVER_CHANNEL = {"ticker": "ticker", "trades": "trades", "book": "orderbook"}


class StreamLike(Protocol):
    def __aiter__(self) -> AsyncIterator[StreamEvent]: ...
    async def aclose(self) -> None: ...


# (server channels, tickers) -> a stream; the real one is ``StreamClient.for_profile(...)``
StreamFactory = Callable[[Sequence[str], Sequence[str]], StreamLike]


class Connection:
    """One browser WebSocket's state inside the hub."""

    def __init__(self, limit: int | None = None) -> None:
        self.queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.limit = QUEUE_LIMIT if limit is None else limit
        self.subs: dict[str, set[str]] = {"ticker": set(), "trades": set(), "book": set()}
        self.alerts = True
        self.overflowed = False
        self.dropped = asyncio.Event()  # set when the page fell too far behind

    def send(self, message: BaseModel | dict[str, Any]) -> None:
        if self.overflowed:
            return
        if self.queue.qsize() >= self.limit:
            self.overflowed = True
            self.dropped.set()  # the connection's owner closes the socket with 1013
            return
        payload = message if isinstance(message, dict) else message.model_dump(mode="json")
        self.queue.put_nowait(payload)

    def tickers(self) -> set[str]:
        return set().union(*self.subs.values())


class StreamHub:
    def __init__(
        self,
        factory: StreamFactory | None,
        *,
        flush_interval: float = 0.1,
        restart_delay: float = 0.2,
        depth: int = MAX_BOOK_DEPTH,
    ) -> None:
        self._factory = factory
        self._flush_interval = flush_interval
        self._restart_delay = restart_delay
        self._depth = depth
        self._connections: set[Connection] = set()
        self._tracker = BookTracker()
        self._book_time: dict[str, datetime] = {}
        self._last_ticker: dict[str, dict[str, Any]] = {}
        self._dirty: set[str] = set()
        self._connected: Connected | None = None
        self._link: m.Link | None = None  # unknown until a stream is wanted
        self._changed = asyncio.Event()
        self._current: tuple[tuple[str, ...], tuple[str, ...]] | None = None
        self._failed = False  # the stream ended with a fatal error; a new subscribe retries once
        self._consumer: asyncio.Task[None] | None = None
        self._tasks: list[asyncio.Task[None]] = []

    # ------------------------------------------------------------------ connections

    def attach(self, conn: Connection) -> None:
        self._connections.add(conn)
        if not self._tasks:
            self._tasks = [
                asyncio.create_task(self._supervise()),
                asyncio.create_task(self._flush_loop()),
            ]
        if self._link is not None:
            conn.send(self._link)
        self._changed.set()

    def detach(self, conn: Connection) -> None:
        self._connections.discard(conn)
        self._changed.set()

    def subscribe(self, conn: Connection, channels: Sequence[str], tickers: Sequence[str]) -> None:
        if self._factory is None:
            raise ValueError("the KalshiTerm server is not configured")
        wanted = set(conn.tickers()) | set(tickers)
        if len(wanted) > MAX_PER_CONNECTION:
            raise ValueError(f"at most {MAX_PER_CONNECTION} markets per connection")
        total = set().union(*(c.tickers() for c in self._connections), set(tickers))
        if len(total) > MAX_TOTAL:
            raise ValueError(f"at most {MAX_TOTAL} markets across the UI")
        for channel in channels:
            conn.subs[channel].update(tickers)
        if self._failed:  # the user is asking again: try once more (never on our own)
            self._failed = False
            self._current = None
        self._catch_up(conn, channels, tickers)
        self._changed.set()

    def unsubscribe(
        self, conn: Connection, channels: Sequence[str], tickers: Sequence[str]
    ) -> None:
        for channel in channels:
            conn.subs[channel].difference_update(tickers)
        self._changed.set()

    def set_alerts(self, conn: Connection, enabled: bool) -> None:
        conn.alerts = enabled
        self._changed.set()

    def _catch_up(self, conn: Connection, channels: Sequence[str], tickers: Sequence[str]) -> None:
        """What is already known about markets that are already streaming."""
        for ticker in tickers:
            book = self._book_message(ticker) if "book" in channels else None
            if book is not None:
                conn.send(book)
            if "ticker" in channels and ticker in self._last_ticker:
                conn.send(self._last_ticker[ticker])
        if self._connected is not None:
            asked = set(tickers)
            conn.send(
                m.Subscribed(
                    unknown=[t for t in self._connected.unknown if t in asked],
                    no_orderbook=[t for t in self._connected.no_orderbook if t in asked],
                    resumed=self._connected.resumed,
                )
            )

    # ------------------------------------------------------------------ what to stream

    def _desired(self) -> tuple[tuple[str, ...], tuple[str, ...]] | None:
        channels: set[str] = set()
        tickers: set[str] = set()
        for conn in self._connections:
            for channel, symbols in conn.subs.items():
                if symbols:
                    channels.add(SERVER_CHANNEL[channel])
                    tickers.update(symbols)
            if conn.alerts:
                channels.add("alerts")
        return (tuple(sorted(channels)), tuple(sorted(tickers))) if channels else None

    async def _supervise(self) -> None:
        while True:
            await self._changed.wait()
            self._changed.clear()
            await asyncio.sleep(self._restart_delay)  # several changes at once -> one restart
            self._changed.clear()
            desired = self._desired()
            if desired == self._current:
                continue
            await self._stop_consumer()
            self._current = desired
            self._connected = None
            if desired is not None and self._factory is not None:
                self._set_link(m.Link(state="down"))  # connecting
                channels, tickers = desired
                self._consumer = asyncio.create_task(
                    self._consume(self._factory(channels, tickers), desired)
                )

    async def _stop_consumer(self) -> None:
        if self._consumer is not None:
            self._consumer.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._consumer
            self._consumer = None

    async def _consume(
        self, stream: StreamLike, desired: tuple[tuple[str, ...], tuple[str, ...]]
    ) -> None:
        try:
            async for event in stream:
                self._dispatch(event)
        except asyncio.CancelledError:
            raise
        except ConnectionProblem as exc:
            self._failed = True
            self._set_link(m.Link(state="failed", error=str(exc)))
        except Exception:
            log.exception("the live stream ended unexpectedly")
            self._failed = True
            self._set_link(m.Link(state="failed", error="the live stream failed"))
        finally:
            with contextlib.suppress(Exception):
                await stream.aclose()

    # ------------------------------------------------------------------ events out

    def _set_link(self, link: m.Link) -> None:
        self._link = link
        self._broadcast(lambda c: True, link)

    def _broadcast(self, wanted: Callable[[Connection], bool], message: Any) -> None:
        for conn in list(self._connections):
            if wanted(conn):
                conn.send(message)

    def _broadcast_book(self, ticker: str, message: m.BookMessage) -> None:
        self._broadcast(lambda c: ticker in c.subs["book"], message)

    def _dispatch(self, event: StreamEvent) -> None:
        if isinstance(event, TickerEvent):
            payload = event.model_dump(mode="json")
            self._last_ticker[event.ticker] = payload
            self._broadcast(lambda c: event.ticker in c.subs["ticker"], payload)
        elif isinstance(event, TradeEvent):
            self._broadcast(lambda c: event.ticker in c.subs["trades"], event)
        elif isinstance(event, BookSnapshotEvent | BookDeltaEvent):
            if self._tracker.apply(event):
                self._book_time[event.ticker] = event.time
                self._dirty.add(event.ticker)
        elif isinstance(event, AlertEvent):
            self._broadcast(lambda c: c.alerts, event)
        elif isinstance(event, Connected):
            self._connected = event
            self._set_link(m.Link(state="up"))
            for conn in list(self._connections):
                mine = conn.tickers()
                conn.send(
                    m.Subscribed(
                        unknown=[t for t in event.unknown if t in mine],
                        no_orderbook=[t for t in event.no_orderbook if t in mine],
                        resumed=event.resumed,
                    )
                )
        elif isinstance(event, Resync):
            self._tracker.reset()
            self._book_time.clear()
            self._dirty.clear()
            self._broadcast(lambda c: True, m.Resync(reason=event.reason))
        elif isinstance(event, Disconnected):
            self._set_link(m.Link(state="down", retry_in=event.retry_in))
        # CaughtUp, WatchlistChanged and unknown events carry nothing the page needs

    # ------------------------------------------------------------------ books

    def _book_message(self, ticker: str) -> m.BookMessage | None:
        book = self._tracker.book(ticker)
        if book is None:
            return None
        return m.BookMessage(
            ticker=ticker,
            time=self._book_time.get(ticker, datetime.now(UTC)),
            yes=book.levels("yes", self._depth),
            no=book.levels("no", self._depth),
            inconsistent=book.inconsistent,
        )

    async def _flush_loop(self) -> None:
        while True:
            await asyncio.sleep(self._flush_interval)
            if not self._dirty:
                continue
            dirty, self._dirty = self._dirty, set()
            for ticker in dirty:
                message = self._book_message(ticker)
                if message is not None:
                    self._broadcast_book(ticker, message)

    # ------------------------------------------------------------------ shutdown

    async def close(self) -> None:
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._tasks = []
        await self._stop_consumer()
