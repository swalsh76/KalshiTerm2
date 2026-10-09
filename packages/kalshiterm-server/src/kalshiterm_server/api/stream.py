"""Live push: ``/v1/stream`` (WebSocket).

Delivery is exactly-once and ordered by ``ingest_seq`` (migration 0016): one total order across
trades, tickers and orderbook rows, committed together with a watermark (``ingest_progress``).
Each connection *pulls* from the database up to that watermark, page by page, only after the
previous page was sent: no unbounded queues, and a client that stops reading is dropped
(close code 1013) and catches up later with ``since``. ``NOTIFY`` just wakes the pull early.

Protocol, client -> server (JSON text frames):
  {"op": "auth", "token": "..."}            first frame, if no Authorization header was sent
  {"op": "subscribe", "channels": ["trades", "ticker", "orderbook"], "markets": ["T", ...],
   "watchlist": true, "since": "<cursor>"}   replaces the subscription
  {"op": "unsubscribe"} / {"op": "ping"}

server -> client: ``subscribed`` (with the ``cursor`` live delivery starts after), then
``trade`` / ``ticker`` / ``book_snapshot`` / ``book_delta`` events each carrying its ``cursor``,
``caught_up`` once a ``since`` replay has finished, ``heartbeat`` (with the current cursor),
``gap`` when the server cannot replay (resubscribe without ``since`` after refetching over
REST), and ``error``. Without ``since`` an orderbook subscription first receives the latest
stored snapshot of each market plus the stored changes after it, flagged ``"replay": true``
and without a cursor (the book state, not part of the live sequence).

Cursors follow arrival order across all four tables (one block of numbers per committed batch,
handed out in the order messages arrived), so a snapshot always carries a higher cursor than the
deltas that preceded it. Orderbook deltas the client's book already includes (same ``sid``,
``seq`` not above the last snapshot sent) are not repeated.
"""

import asyncio
import contextlib
import json
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any

import asyncpg  # type: ignore[import-untyped]
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine

from kalshiterm_server import auth
from kalshiterm_server.api import format as fmt

log = logging.getLogger("kalshiterm_server")
router = APIRouter()

CHANNELS = ("trades", "ticker", "orderbook")
TICKER_RE = re.compile(r"^[A-Za-z0-9._-]{1,128}$")
MAX_FRAME = 64 * 1024
SEND_TIMEOUT = 10.0
WATCHLIST_REFRESH = 15.0
ONE_DOLLAR_E6 = 1_000_000

# (table, columns beyond the common ones); every table also has ingest_seq, ts, market_id
TABLE_COLUMNS = {
    "trades": "trade_id, yes_price_e6, count_e2, taker_side, is_block_trade",
    "tickers": (
        "price_e6, yes_bid_e6, yes_ask_e6, yes_bid_size_e2, yes_ask_size_e2, volume_e2, "
        "open_interest_e2, last_trade_size_e2"
    ),
    "orderbook_snapshots": (
        "sid, seq, approximate, yes_prices_e6, yes_sizes_e2, no_prices_e6, no_sizes_e2"
    ),
    "orderbook_deltas": "sid, seq, is_yes, price_e6, delta_e2",
}
SNAPSHOT_SELECT = f"SELECT ingest_seq, ts, market_id, {TABLE_COLUMNS['orderbook_snapshots']} "
DELTA_SELECT = f"SELECT ingest_seq, ts, market_id, {TABLE_COLUMNS['orderbook_deltas']} "
CHANNEL_TABLES = {
    "trades": ("trades",),
    "ticker": ("tickers",),
    "orderbook": ("orderbook_snapshots", "orderbook_deltas"),
}


class Hub:
    """Shared by all connections: the NOTIFY listener, wake-ups and the per-user count."""

    def __init__(self, db_url: str) -> None:
        self._dsn = make_url(db_url).set(drivername="postgresql").render_as_string(False)
        self._wakers: set[asyncio.Event] = set()
        self._task: asyncio.Task[None] | None = None
        self.connections: dict[int, int] = {}

    def add(self, waker: asyncio.Event) -> None:
        self._wakers.add(waker)

    def discard(self, waker: asyncio.Event) -> None:
        self._wakers.discard(waker)

    def wake_all(self) -> None:
        for waker in self._wakers:
            waker.set()

    async def start(self) -> None:
        self._task = asyncio.create_task(self._listen())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    async def _listen(self) -> None:
        """Stay connected to LISTEN; if it cannot be, connections still poll every second."""
        delay = 1.0
        while True:
            try:
                connection = await asyncpg.connect(self._dsn)
            except Exception as exc:
                log.debug("stream: cannot LISTEN yet (%s); sessions will poll", type(exc).__name__)
                await asyncio.sleep(delay)
                delay = min(30.0, delay * 2)
                continue
            delay = 1.0
            closed = asyncio.Event()
            connection.add_termination_listener(lambda _c, done=closed: done.set())
            await connection.add_listener("kterm_ingest", lambda *_: self.wake_all())
            self.wake_all()
            try:
                await closed.wait()
            finally:
                with contextlib.suppress(Exception):
                    await connection.close()


@dataclass(slots=True)
class BookRef:
    """Where a market's orderbook stands: deltas after it are the ones to apply."""

    sid: int | None
    seq: int | None
    ingest_seq: int


@dataclass(slots=True)
class Subscription:
    channels: tuple[str, ...] = ()
    explicit: dict[str, int] = field(default_factory=dict)  # ticker -> market id
    use_watchlist: bool = False
    watched: dict[str, int] = field(default_factory=dict)
    tickers: dict[int, str] = field(default_factory=dict)  # market id -> ticker (everything)
    refs: dict[int, BookRef] = field(default_factory=dict)
    pos: int = 0
    catching_up: bool = False

    @property
    def market_ids(self) -> list[int]:
        return list(self.tickers)


def book_side(prices: list[int], sizes: list[int]) -> list[dict[str, str | None]]:
    return [
        {"price": fmt.dollars(p), "size": fmt.count(s)}
        for p, s in sorted(zip(prices, sizes, strict=True), reverse=True)
    ]


def event_for(table: str, row: Any, ticker: str, replay: bool = False) -> dict[str, Any]:
    base: dict[str, Any] = {"ticker": ticker, "time": fmt.moment(row.ts)}
    if table == "trades":
        base |= {
            "type": "trade",
            "trade_id": str(row.trade_id),
            "yes_price": fmt.dollars(row.yes_price_e6),
            "no_price": fmt.dollars(ONE_DOLLAR_E6 - row.yes_price_e6),
            "count": fmt.count(row.count_e2),
            "taker_side": row.taker_side,
            "is_block_trade": row.is_block_trade,
        }
    elif table == "tickers":
        base |= {
            "type": "ticker",
            "price": fmt.dollars(row.price_e6),
            "yes_bid": fmt.dollars(row.yes_bid_e6),
            "yes_ask": fmt.dollars(row.yes_ask_e6),
            "yes_bid_size": fmt.count(row.yes_bid_size_e2),
            "yes_ask_size": fmt.count(row.yes_ask_size_e2),
            "volume": fmt.count(row.volume_e2),
            "open_interest": fmt.count(row.open_interest_e2),
            "last_trade_size": fmt.count(row.last_trade_size_e2),
        }
    elif table == "orderbook_snapshots":
        base |= {
            "type": "book_snapshot",
            "sid": row.sid,
            "seq": row.seq,
            "approximate": bool(row.approximate),
            "yes": book_side(row.yes_prices_e6, row.yes_sizes_e2),
            "no": book_side(row.no_prices_e6, row.no_sizes_e2),
        }
    else:
        base |= {
            "type": "book_delta",
            "sid": row.sid,
            "seq": row.seq,
            "side": "yes" if row.is_yes else "no",
            "price": fmt.dollars(row.price_e6),
            "delta": fmt.count(row.delta_e2),
        }
    if replay:
        base["replay"] = True
    else:
        base["cursor"] = str(row.ingest_seq)
    return base


class Session:
    def __init__(self, ws: WebSocket, who: auth.Principal, hub: Hub) -> None:
        self.ws = ws
        self.who = who
        self.hub = hub
        self.engine: AsyncEngine = ws.app.state.engine
        self.config = ws.app.state.settings
        self.sub = Subscription()
        self.commands: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.wake = asyncio.Event()
        self._initial: list[dict[str, Any]] = []

    # ------------------------------------------------------------ sending
    async def send(self, event: dict[str, Any]) -> None:
        try:
            await asyncio.wait_for(self.ws.send_text(json.dumps(event)), SEND_TIMEOUT)
        except TimeoutError:
            with contextlib.suppress(Exception):
                await self.ws.close(code=1013)  # too slow: reconnect and catch up with `since`
            raise WebSocketDisconnect(1013) from None

    async def error(self, code: str, **extra: Any) -> None:
        await self.send({"type": "error", "error": code, **extra})

    # ------------------------------------------------------------ receiving
    async def receive_loop(self) -> None:
        bad = 0
        while True:
            raw = await self.ws.receive_text()
            if len(raw) > MAX_FRAME:
                await self.error("frame_too_large")
                await self.ws.close(code=1009)
                return
            try:
                message = json.loads(raw)
                if not isinstance(message, dict):
                    raise ValueError
            except ValueError:
                bad += 1
                await self.error("invalid_message")
                if bad >= 5:
                    await self.ws.close(code=4400)
                    return
                continue
            if message.get("op") == "ping":
                await self.send({"type": "pong"})
            else:
                await self.commands.put(message)

    # ------------------------------------------------------------ database
    async def watermark(self) -> int:
        async with self.engine.connect() as conn:
            return int(
                (await conn.execute(text("SELECT last_seq FROM ingest_progress"))).scalar_one()
            )

    async def resolve(self, tickers: list[str]) -> dict[str, int]:
        if not tickers:
            return {}
        async with self.engine.connect() as conn:
            found = await conn.execute(
                text("SELECT ticker, id FROM markets WHERE ticker = ANY(:t)"), {"t": tickers}
            )
            return {ticker: mid for ticker, mid in found}

    async def watchlist_markets(self) -> dict[str, int]:
        async with self.engine.connect() as conn:
            found = await conn.execute(
                text(
                    "SELECT m.ticker, m.id FROM user_watchlists w JOIN markets m ON m.id = "
                    "w.market_id WHERE w.user_id = :u"
                ),
                {"u": self.who.user_id},
            )
            return {ticker: mid for ticker, mid in found}

    async def rows(self, table: str, ids: list[int], low: int, high: int, limit: int) -> list[Any]:
        async with self.engine.connect() as conn:
            found = await conn.execute(
                text(
                    f"SELECT ingest_seq, ts, market_id, {TABLE_COLUMNS[table]} FROM {table} "
                    "WHERE market_id = ANY(:ids) AND ingest_seq > :low AND ingest_seq <= :high "
                    "ORDER BY ingest_seq LIMIT :n"
                ),
                {"ids": ids, "low": low, "high": high, "n": limit},
            )
            return list(found)

    # ------------------------------------------------------------ the subscription
    async def subscribe(self, message: dict[str, Any]) -> None:
        channels: Any = message.get("channels")
        markets: Any = message.get("markets", [])
        since = message.get("since")
        bad_channels = (
            not isinstance(channels, list)
            or not channels
            or any(c not in CHANNELS for c in channels)
        )
        bad_markets = (
            not isinstance(markets, list)
            or any(not isinstance(m, str) or not TICKER_RE.match(m) for m in markets)
            or len(markets) > self.config.stream_max_markets
        )
        if bad_channels:
            return await self.error("invalid_parameter", fields=["channels"])
        if bad_markets:
            return await self.error(
                "invalid_parameter", fields=["markets"], limit=self.config.stream_max_markets
            )
        cursor: int | None = None
        if since is not None:
            if not isinstance(since, str) or not since.isascii() or not since.isdigit():
                return await self.error("invalid_parameter", fields=["since"])
            cursor = int(since)
        use_watchlist = bool(message.get("watchlist", False))
        explicit = await self.resolve(list(dict.fromkeys(markets)))
        watched = await self.watchlist_markets() if use_watchlist else {}
        if len(explicit) + len(watched) > self.config.stream_max_markets * 2:
            return await self.error("invalid_parameter", fields=["markets"], reason="too_many")
        head = await self.watermark()
        if cursor is not None and cursor > head:
            return await self.error("invalid_parameter", fields=["since"], reason="in_the_future")

        sub = Subscription(
            channels=tuple(dict.fromkeys(channels)),
            explicit=explicit,
            use_watchlist=use_watchlist,
            watched=watched,
        )
        sub.tickers = {mid: t for t, mid in (explicit | watched).items()}
        self.sub = sub
        unknown = [m for m in dict.fromkeys(markets) if m not in explicit]
        no_book: list[str] = []
        if "orderbook" in sub.channels and cursor is None:
            no_book = await self.initial_books(sub.market_ids, head)
        await self.send(
            {
                "type": "subscribed",
                "channels": list(sub.channels),
                "markets": sorted(sub.tickers.values()),
                "unknown": unknown,
                "no_orderbook": no_book,
                "cursor": str(head if cursor is None else cursor),
            }
        )
        await self.flush_initial()
        if cursor is not None and head - cursor > self.config.stream_catchup_horizon:
            await self.send({"type": "gap", "reason": "cursor_too_old", "cursor": str(head)})
            cursor = None
            sub.pos = head
        elif cursor is not None:
            sub.pos = cursor
            sub.catching_up = True
        else:
            sub.pos = head

    async def initial_books(self, ids: list[int], head: int) -> list[str]:
        """Prepare the book state: latest snapshot of each market plus the stored changes after
        it (as of ``head``). Returns the tickers that have no book at all."""
        self._initial = []
        missing: list[str] = []
        async with self.engine.connect() as conn:
            for mid in ids:
                snap = (
                    await conn.execute(
                        text(
                            SNAPSHOT_SELECT
                            + "FROM orderbook_snapshots WHERE market_id = :m AND ingest_seq <= :h "
                            "ORDER BY ingest_seq DESC LIMIT 1"
                        ),
                        {"m": mid, "h": head},
                    )
                ).first()
                if snap is None:
                    missing.append(self.sub.tickers[mid])
                    continue
                ref = BookRef(snap.sid, snap.seq, snap.ingest_seq)
                self.sub.refs[mid] = ref
                ticker = self.sub.tickers[mid]
                self._initial.append(event_for("orderbook_snapshots", snap, ticker, replay=True))
                if ref.sid is not None:
                    changes = await conn.execute(
                        text(
                            DELTA_SELECT
                            + "FROM orderbook_deltas WHERE market_id = :m AND sid = :sid "
                            "AND seq > :seq AND ingest_seq <= :h ORDER BY seq"
                        ),
                        {"m": mid, "sid": ref.sid, "seq": ref.seq, "h": head},
                    )
                else:
                    changes = await conn.execute(
                        text(
                            DELTA_SELECT
                            + "FROM orderbook_deltas WHERE market_id = :m AND ingest_seq > :after "
                            "AND ingest_seq <= :h ORDER BY ingest_seq"
                        ),
                        {"m": mid, "after": ref.ingest_seq, "h": head},
                    )
                self._initial += [
                    event_for("orderbook_deltas", r, ticker, replay=True) for r in changes
                ]
        return missing

    async def flush_initial(self) -> None:
        pending, self._initial = self._initial, []
        for event in pending:
            await self.send(event)

    # ------------------------------------------------------------ the pump
    def keep(self, table: str, row: Any) -> bool:
        """Orderbook rows follow the (sid, seq) rule once a market has a reference book."""
        if table not in ("orderbook_snapshots", "orderbook_deltas"):
            return True
        ref = self.sub.refs.get(row.market_id)
        if table == "orderbook_snapshots":
            self.sub.refs[row.market_id] = BookRef(row.sid, row.seq, row.ingest_seq)
            return True
        if ref is None:
            return True  # a catch-up from `since`: the client holds the book, send everything
        if ref.sid is not None:
            return bool(row.sid == ref.sid and row.seq > (ref.seq or 0))
        return bool(row.ingest_seq > ref.ingest_seq)

    async def advance(self) -> None:
        """Send every event in (pos, watermark] for the subscribed markets, in cursor order."""
        sub = self.sub
        if not sub.channels or not sub.tickers:
            return
        head = await self.watermark()
        page = self.config.stream_page
        while sub.pos < head:
            ids = sub.market_ids
            batch: list[tuple[int, str, Any]] = []
            ceiling = head
            for channel in sub.channels:
                for table in CHANNEL_TABLES[channel]:
                    found = await self.rows(table, ids, sub.pos, head, page)
                    if len(found) == page:  # more beyond this page: do not pass its last row
                        ceiling = min(ceiling, found[-1].ingest_seq)
                    batch += [(r.ingest_seq, table, r) for r in found]
            batch.sort(key=lambda item: item[0])
            for seq, table, row in batch:
                if seq > ceiling:
                    break
                if self.keep(table, row):
                    await self.send(event_for(table, row, sub.tickers[row.market_id]))
            sub.pos = ceiling
            if sub.catching_up and sub.pos >= head:
                sub.catching_up = False
                await self.send({"type": "caught_up", "cursor": str(sub.pos)})

    async def refresh_watchlist(self) -> None:
        sub = self.sub
        if not sub.use_watchlist:
            return
        now = await self.watchlist_markets()
        added = {t: m for t, m in now.items() if t not in sub.watched and t not in sub.explicit}
        removed = [t for t in sub.watched if t not in now and t not in sub.explicit]
        if not added and not removed:
            return
        for ticker in removed:
            mid = sub.watched[ticker]
            sub.tickers.pop(mid, None)
            sub.refs.pop(mid, None)
        sub.watched = now
        for ticker, mid in added.items():
            sub.tickers[mid] = ticker
        if added and "orderbook" in sub.channels:
            await self.initial_books(list(added.values()), sub.pos)
            await self.flush_initial()
        await self.send(
            {"type": "watchlist_changed", "added": sorted(added), "removed": sorted(removed)}
        )

    async def pump(self) -> None:
        last_beat = last_refresh = time.monotonic()
        while True:
            self.wake.clear()
            while not self.commands.empty():
                command = self.commands.get_nowait()
                if command.get("op") == "subscribe":
                    await self.subscribe(command)
                elif command.get("op") == "unsubscribe":
                    self.sub = Subscription(pos=self.sub.pos)
                    await self.send({"type": "unsubscribed"})
                else:
                    await self.error("unknown_op")
            await self.advance()
            now = time.monotonic()
            if now - last_refresh >= WATCHLIST_REFRESH:
                last_refresh = now
                await self.refresh_watchlist()
            if now - last_beat >= self.config.stream_heartbeat_seconds:
                last_beat = now
                await self.send(
                    {"type": "heartbeat", "cursor": str(self.sub.pos or await self.watermark())}
                )
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self.wake.wait(), timeout=1.0)


async def authenticate_socket(ws: WebSocket) -> auth.Principal | None:
    """Header first; otherwise a first ``auth`` frame. Every failure looks the same."""
    app = ws.app
    address = ws.client.host if ws.client else "unknown"
    throttle: auth.FailureThrottle = app.state.throttle
    if throttle.retry_after(address) is not None:
        return None
    token = auth.bearer_token(ws.headers.get("authorization"))
    if token is None:
        try:
            first = json.loads(
                await asyncio.wait_for(ws.receive_text(), app.state.settings.stream_auth_timeout)
            )
            if (
                isinstance(first, dict)
                and first.get("op") == "auth"
                and isinstance(first.get("token"), str)
            ):
                token = first["token"]
        except (TimeoutError, ValueError, WebSocketDisconnect):
            token = None
    who = await auth.authenticate(app.state.engine, token, app.state.clock())
    if who is None:
        throttle.record_failure(address)
    return who


@router.websocket("/v1/stream")
async def stream(ws: WebSocket) -> None:
    await ws.accept()
    who = await authenticate_socket(ws)
    if who is None:
        await ws.send_text(json.dumps({"type": "error", "error": "unauthorized"}))
        await ws.close(code=4401)
        return
    hub: Hub = ws.app.state.hub
    limit = ws.app.state.settings.stream_max_connections_per_user
    if hub.connections.get(who.user_id, 0) >= limit:
        await ws.send_text(
            json.dumps({"type": "error", "error": "too_many_connections", "limit": limit})
        )
        await ws.close(code=4429)
        return
    hub.connections[who.user_id] = hub.connections.get(who.user_id, 0) + 1
    session = Session(ws, who, hub)
    hub.add(session.wake)
    tasks = [asyncio.create_task(session.receive_loop()), asyncio.create_task(session.pump())]
    try:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            exc = task.exception()
            if exc and not isinstance(exc, WebSocketDisconnect):
                log.warning("stream session ended: %s", type(exc).__name__)
    finally:
        # Bookkeeping first, with no `await` before it: if this handler is itself cancelled
        # (server shutdown, a client that vanished) the slot must still be released.
        hub.discard(session.wake)
        hub.connections[who.user_id] -= 1
        for task in tasks:
            task.cancel()
        await asyncio.wait(tasks)
        with contextlib.suppress(Exception):
            await ws.close()
