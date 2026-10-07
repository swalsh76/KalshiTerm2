"""Streaming ingestor: WebSocket messages -> batched, atomic ``COPY`` into the hypertables.

Intake filters and timestamps each message and appends it to a buffer; a flusher task writes
the buffer every ``flush_interval`` seconds or when it reaches ``batch_size``. A batch is one
transaction, so a failed write retries cleanly (with backoff) and leaves no partial data.
The number of buffered plus in-flight messages is capped: when the database stalls, intake
pauses, the WebSocket client's bounded queue fills, and its overflow-reconnect path takes over.

Combo (multivariate, ``KXMVE…``) markets are only *counted* per minute and ticker family
(``combo_stats_1m``), plus individual trades above a dollar threshold are logged
(``combo_large_trades``); there are no per-combo rows (PLAN decision 17).

Watched markets (PLAN decisions 12-13): ``watch()`` / ``unwatch()`` are applied inside the next
flush transaction, in order with the data. On *watch* the market's recent raw trades are copied
from ``trades`` to ``trades_watchlist`` and its coverage period is opened; from that same batch
on, its trades are written to both tables, so there is neither a gap nor a duplicate.
"""

import asyncio
import contextlib
import logging
import time
import uuid
from collections import Counter, OrderedDict, deque
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from kalshi_core.models import Trade
from kalshi_core.orderbook import (
    DELTA,
    GAP,
    MESSAGE,
    RESET,
    RESUBSCRIBE_FAILED,
    RESYNC_FAILED,
    SNAPSHOT,
    BookEvent,
)
from kalshi_core.ws import RECONNECTED
from kalshi_core.ws_models import (
    LifecycleMsg,
    OrderbookDeltaMsg,
    TickerMsg,
    TradeMsg,
    WsMessage,
)
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncEngine

from kalshiterm_server.fixedpoint import PrecisionError, to_e2, to_e6
from kalshiterm_server.ingest.combos import (
    LARGE_TRADE_COLUMNS,
    LARGE_TRADE_E6,
    UPSERT_STATS,
    Aggregates,
    family_of,
    notional_e6,
)
from kalshiterm_server.storage.market_ids import MarketIds

log = logging.getLogger("kalshiterm_server")

MVE_PREFIX = "KXMVE"
CHANNEL_TYPES = frozenset(
    {"ticker", "trade", "market_lifecycle_v2", "multivariate_market_lifecycle"}
)
COMBO_LIFECYCLE_FIELD = {
    "created": "created",
    "determined": "determined",
    "settled": "settled",
    "close_date_updated": "close_updated",
}
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

TICKER_COLUMNS = [
    "ts", "received_at", "market_id", "price_e6", "yes_bid_e6", "yes_ask_e6",
    "yes_bid_size_e2", "yes_ask_size_e2", "volume_e2", "open_interest_e2", "last_trade_size_e2",
]  # fmt: skip
TRADE_COLUMNS = [
    "ts", "received_at", "market_id", "trade_id", "yes_price_e6", "count_e2", "taker_side",
    "is_block_trade",
]  # fmt: skip
OB_SNAPSHOT_COLUMNS = [
    "ts", "received_at", "market_id", "seq", "approximate", "yes_prices_e6", "yes_sizes_e2",
    "no_prices_e6", "no_sizes_e2",
]  # fmt: skip
OB_DELTA_COLUMNS = [
    "ts", "received_at", "market_id", "seq", "is_yes", "price_e6", "delta_e2",
]  # fmt: skip
LIFECYCLE_COLUMNS = [
    "ts", "received_at", "market_id", "event_type", "open_ts", "close_ts", "determination_ts",
    "settled_ts", "result", "settlement_value_e6", "is_deactivated",
]  # fmt: skip


def from_ms(ms: int) -> datetime:
    return EPOCH + timedelta(milliseconds=ms)


def from_s(seconds: int | None) -> datetime | None:
    return None if seconds is None else EPOCH + timedelta(seconds=seconds)


def from_epoch(seconds: float) -> datetime:
    return EPOCH + timedelta(seconds=seconds)


def price_paid(trade: TradeMsg) -> Decimal:
    """What the taker paid per contract: the yes price for a yes taker, else the no price."""
    return trade.yes_price_dollars if trade.taker_side == "yes" else trade.no_price_dollars


@dataclass(slots=True)
class Item:
    # "ticker" | "trade" | "market_lifecycle_v2" | "combo_large" | "ob_snapshot" | "ob_delta"
    kind: str
    payload: TickerMsg | TradeMsg | LifecycleMsg | None
    ts: datetime  # exchange time
    received: datetime  # when this process read it off the socket
    notional: int = 0  # combo_large only: taker dollars in millionths
    symbol: str | None = None  # orderbook items carry their ticker and row parts directly
    extra: tuple[Any, ...] = ()  # orderbook items: values prebuilt at intake (books are mutable)

    @property
    def ticker(self) -> str:
        if self.symbol is not None:
            return self.symbol
        assert self.payload is not None and self.payload.market_ticker is not None
        return self.payload.market_ticker


def ticker_row(item: Item, market_id: int) -> tuple[Any, ...]:
    p = item.payload
    assert isinstance(p, TickerMsg)
    return (
        item.ts, item.received, market_id, to_e6(p.price_dollars), to_e6(p.yes_bid_dollars),
        to_e6(p.yes_ask_dollars), to_e2(p.yes_bid_size_fp), to_e2(p.yes_ask_size_fp),
        to_e2(p.volume_fp), to_e2(p.open_interest_fp), to_e2(p.last_trade_size_fp),
    )  # fmt: skip


def trade_row(item: Item, market_id: int) -> tuple[Any, ...]:
    p = item.payload
    assert isinstance(p, TradeMsg)
    yes_price, count = to_e6(p.yes_price_dollars), to_e2(p.count_fp)
    assert yes_price is not None and count is not None
    no_price = to_e6(p.no_price_dollars)
    if no_price is not None and yes_price + no_price != 1_000_000:
        log.warning(
            "trade %s: yes %s + no %s != 1", p.trade_id, p.yes_price_dollars, p.no_price_dollars
        )
    return (
        item.ts, item.received, market_id, uuid.UUID(p.trade_id), yes_price, count,
        p.taker_side, p.is_block_trade,
    )  # fmt: skip


def lifecycle_row(item: Item, market_id: int) -> tuple[Any, ...]:
    p = item.payload
    assert isinstance(p, LifecycleMsg)
    return (
        item.ts, item.received, market_id, p.event_type, from_s(p.open_ts), from_s(p.close_ts),
        from_s(p.determination_ts), from_s(p.settled_ts), p.result, to_e6(p.settlement_value),
        p.is_deactivated,
    )  # fmt: skip


def combo_large_row(item: Item, _: int) -> tuple[Any, ...]:
    p = item.payload
    assert isinstance(p, TradeMsg)
    yes_price, count = to_e6(p.yes_price_dollars), to_e2(p.count_fp)
    assert yes_price is not None and count is not None
    return (
        item.ts, item.received, item.ticker, uuid.UUID(p.trade_id), yes_price, count,
        p.taker_side, item.notional,
    )  # fmt: skip


def ob_snapshot_row(item: Item, market_id: int) -> tuple[Any, ...]:
    return (item.ts, item.received, market_id, *item.extra)


def ob_delta_row(item: Item, market_id: int) -> tuple[Any, ...]:
    return (item.ts, item.received, market_id, *item.extra)


# kind -> (table, columns, row builder)
TABLES: dict[str, tuple[str, list[str], Callable[[Item, int], tuple[Any, ...]]]] = {
    "ticker": ("tickers", TICKER_COLUMNS, ticker_row),
    "trade": ("trades", TRADE_COLUMNS, trade_row),
    "market_lifecycle_v2": ("market_lifecycle", LIFECYCLE_COLUMNS, lifecycle_row),
    "combo_large": ("combo_large_trades", LARGE_TRADE_COLUMNS, combo_large_row),
    "ob_snapshot": ("orderbook_snapshots", OB_SNAPSHOT_COLUMNS, ob_snapshot_row),
    "ob_delta": ("orderbook_deltas", OB_DELTA_COLUMNS, ob_delta_row),
    # Not an intake kind: a watched market's trades are written here as well as to ``trades``.
    "trade_watched": ("trades_watchlist", TRADE_COLUMNS, trade_row),
}
ORDINARY_KINDS = ("ticker", "trade", "market_lifecycle_v2", "ob_snapshot", "ob_delta")

OPEN_PERIOD = """
INSERT INTO watchlist_periods (market_id, source) VALUES ($1, $2)
ON CONFLICT (market_id) WHERE removed_at IS NULL DO NOTHING
"""
CLOSE_PERIOD = (
    "UPDATE watchlist_periods SET removed_at = now() WHERE market_id = $1 AND removed_at IS NULL"
)
# Everything already in trades that trades_watchlist lacks (>= the newest copy, minus exact
# duplicates, so a re-added market is filled in without repeating or missing a trade).
COPY_HISTORY = """
INSERT INTO trades_watchlist ({columns})
SELECT {selected} FROM trades t
WHERE t.market_id = $1
  AND t.ts >= COALESCE((SELECT max(w.ts) FROM trades_watchlist w WHERE w.market_id = $1),
                       '-infinity'::timestamptz)
  AND NOT EXISTS (SELECT 1 FROM trades_watchlist w
                  WHERE w.market_id = t.market_id AND w.trade_id = t.trade_id AND w.ts = t.ts)
""".format(columns=", ".join(TRADE_COLUMNS), selected=", ".join(f"t.{c}" for c in TRADE_COLUMNS))


@dataclass(frozen=True, slots=True)
class GapInfo:
    """A period the live stream may have missed, reported when the connection comes back."""

    started: datetime  # when we last heard from the stream
    ended: datetime  # when the new connection was established
    reason: str  # "connection_lost" | "overflow"
    dropped: int  # messages the client had to drop (overflow only)


SEEN_IDS = 200_000  # recent trade ids remembered, so a backfill never repeats a live trade


@dataclass(frozen=True, slots=True)
class WatchOp:
    ticker: str
    watch: bool  # False = unwatch
    source: str = "manual"


class Samples:
    """Rolling window of recent measurements with percentiles."""

    def __init__(self, size: int = 20_000) -> None:
        self._values: deque[float] = deque(maxlen=size)

    def add(self, value: float) -> None:
        self._values.append(value)

    def summary(self) -> dict[str, float]:
        if not self._values:
            return {"p50": 0.0, "p95": 0.0, "max": 0.0}
        ordered = sorted(self._values)
        return {
            "p50": ordered[len(ordered) // 2],
            "p95": ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))],
            "max": ordered[-1],
        }


class StreamIngestor:
    def __init__(
        self,
        messages: AsyncIterator[WsMessage | BookEvent],
        engine: AsyncEngine,
        *,
        batch_size: int = 5_000,
        flush_interval: float = 1.0,
        max_buffered: int = 100_000,
        large_trade_e6: int = LARGE_TRADE_E6,
        retry_base: float = 0.5,
        retry_max: float = 30.0,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._messages = messages
        self._engine = engine
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._max_buffered = max_buffered
        self._large_trade_e6 = large_trade_e6
        self._retry_base = retry_base
        self._retry_max = retry_max
        self._sleep = sleep
        self._clock = clock
        self._ids = MarketIds(engine)
        self._buffer: list[Item] = []
        self._agg = Aggregates()
        self._in_flight = 0
        self.on_gap: Callable[[GapInfo], None] | None = None
        self._last_heard: datetime | None = None
        self._seen_ids: OrderedDict[str, None] = OrderedDict()
        self._ops: list[WatchOp] = []  # watch/unwatch requests not yet applied
        self._watched: set[str] = set()  # tickers whose trades also go to trades_watchlist
        self._wake = asyncio.Event()
        self._space = asyncio.Event()
        self._space.set()
        self.seen: Counter[str] = Counter()
        self.written: Counter[str] = Counter()
        self.combo_counted: Counter[str] = Counter()  # combo messages counted, by kind
        self.rejected = 0
        self.book_problems: Counter[str] = Counter()  # gaps / resync and resubscribe failures
        self.reconnects = 0
        self.flushes = 0
        self.retries = 0
        self.last_flush_seconds = 0.0
        self.lag_ms = Samples()  # received_at - exchange ts (network + server + clock skew)
        self.write_delay_ms = Samples()  # written - received_at (our own buffering)

    # ------------------------------------------------------------------ watchlist
    def watch(self, tickers: list[str], source: str = "manual") -> None:
        """Start the permanent trade copy and coverage period for these markets."""
        self._ops.extend(WatchOp(t, True, source) for t in tickers)
        self._wake.set()

    def unwatch(self, tickers: list[str]) -> None:
        """Close the coverage period and stop the copy (what was copied is kept)."""
        self._ops.extend(WatchOp(t, False) for t in tickers)
        self._wake.set()

    def _remember(self, trade_id: str) -> bool:
        """Note a trade id; False if it was already seen recently."""
        if trade_id in self._seen_ids:
            return False
        self._seen_ids[trade_id] = None
        if len(self._seen_ids) > SEEN_IDS:
            self._seen_ids.popitem(last=False)
        return True

    async def backfill_trade(self, trade: Trade, known: set[str]) -> str:
        """Feed one REST trade through the normal write path.

        Returns "added", "combo_large", "duplicate" or "combo_skipped". Unrepresentable values
        are rejected by the write path like any other row. ``known``
        holds ids already in the database for the window; the recent-id memory covers live
        trades still in the buffer. Combo trades below the large-trade threshold are not
        stored or counted (the per-minute counters cannot be de-duplicated).
        """
        if trade.trade_id in known or trade.trade_id in self._seen_ids:
            return "duplicate"
        payload = TradeMsg(
            trade_id=trade.trade_id,
            market_ticker=trade.ticker,
            yes_price_dollars=trade.yes_price_dollars,
            no_price_dollars=trade.no_price_dollars,
            count_fp=trade.count_fp,
            taker_side=trade.taker_side,
            is_block_trade=trade.is_block_trade,
        )
        received = from_epoch(self._clock())
        kind, notional = "trade", 0
        if trade.ticker.startswith(MVE_PREFIX):
            try:
                value = notional_e6(trade.count_fp, price_paid(payload))
            except (PrecisionError, ArithmeticError):
                value = 0
            if value < self._large_trade_e6:
                return "combo_skipped"
            kind, notional = "combo_large", value
        self._remember(trade.trade_id)
        await self._buffer_item(Item(kind, payload, trade.created_time, received, notional))
        return "combo_large" if kind == "combo_large" else "added"

    @property
    def watched(self) -> frozenset[str]:
        return frozenset(self._watched)

    # ------------------------------------------------------------------ intake
    async def run(self) -> None:
        flusher = asyncio.create_task(self._flush_loop())
        try:
            async for item in self._messages:
                if isinstance(item, BookEvent):
                    await self._admit_book(item)
                else:
                    await self._admit(item)
        finally:
            flusher.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await flusher
            await self._drain()

    async def _admit_book(self, event: BookEvent) -> None:
        """Orderbook feed events: snapshots and deltas are stored; the rest are accounted."""
        if event.kind in (MESSAGE, RESET):
            if event.message is not None:
                await self._admit(event.message)  # passthrough messages and the reconnect signal
            return
        if event.kind in (GAP, RESUBSCRIBE_FAILED, RESYNC_FAILED):
            self.book_problems[event.kind] += 1
            log.warning(
                "orderbook %s: %s %s", event.kind, event.tickers or event.ticker, event.detail
            )
            return
        if event.kind not in (SNAPSHOT, DELTA) or event.ticker is None:
            return
        message = event.message
        received = from_epoch((message.received_at if message else None) or self._clock())
        sending = message.sending_ts_ms if message else None
        try:
            if event.kind == SNAPSHOT:
                assert event.book is not None
                yes, no = event.book.bids("yes"), event.book.bids("no")
                kind, ts = "ob_snapshot", from_ms(sending) if sending else received
                extra: tuple[Any, ...] = (
                    message.seq if message else None,
                    event.book.approximate,
                    [to_e6(p) for p, _ in yes], [to_e2(q) for _, q in yes],
                    [to_e6(p) for p, _ in no], [to_e2(q) for _, q in no],
                )  # fmt: skip
            else:
                assert message is not None
                payload = message.payload()
                assert isinstance(payload, OrderbookDeltaMsg)
                kind = "ob_delta"
                exchange_ms = payload.ts_ms or sending
                ts = from_ms(exchange_ms) if exchange_ms else received
                extra = (
                    message.seq, payload.side == "yes", to_e6(payload.price_dollars),
                    to_e2(payload.delta_fp),
                )  # fmt: skip
        except PrecisionError as exc:
            self.rejected += 1
            log.error("rejected orderbook %s: %s", event.kind, exc)
            return
        await self._buffer_item(Item(kind, None, ts, received, symbol=event.ticker, extra=extra))

    async def _buffer_item(self, item: Item) -> None:
        while len(self._buffer) + self._in_flight >= self._max_buffered:
            self._space.clear()  # backpressure: leave the backlog in the bounded WS queue
            await self._space.wait()
        self._buffer.append(item)
        if len(self._buffer) >= self._batch_size:
            self._wake.set()

    async def _admit(self, message: WsMessage) -> None:
        if message.type == RECONNECTED:
            self.reconnects += 1
            log.warning("reconnected: data may have been missed (%s)", message.msg)
            self._wake.set()  # get everything received before the gap onto disk
            if self.on_gap is not None:
                ended = from_epoch(message.received_at or self._clock())
                info = message.msg if isinstance(message.msg, dict) else {}
                self.on_gap(
                    GapInfo(
                        started=self._last_heard or ended,
                        ended=ended,
                        reason=str(info.get("reason") or "connection_lost"),
                        dropped=int(info.get("dropped") or 0),
                    )
                )
            return
        if message.type not in CHANNEL_TYPES:
            return
        self.seen[message.type] += 1
        try:
            payload = message.payload()
        except ValidationError as exc:
            self.rejected += 1
            log.error("rejected unparseable %s message: %s", message.type, exc.errors()[:1])
            return
        assert isinstance(payload, TickerMsg | TradeMsg | LifecycleMsg)
        ticker = payload.market_ticker
        if ticker is None:
            self.rejected += 1
            return
        received = from_epoch(message.received_at or self._clock())
        self._last_heard = received
        exchange_ms = getattr(payload, "ts_ms", None) or message.sending_ts_ms
        ts = from_ms(exchange_ms) if exchange_ms else received
        kind = message.type
        notional = 0
        if message.type == "multivariate_market_lifecycle" or ticker.startswith(MVE_PREFIX):
            keep = self._count_combo(payload, ts)
            if keep is None:
                return  # counted only: combos get no per-message rows
            kind, notional = "combo_large", keep
        elif kind not in ORDINARY_KINDS:
            return
        if isinstance(payload, TradeMsg):
            self._remember(payload.trade_id)
        await self._buffer_item(Item(kind, payload, ts, received, notional))

    def _count_combo(
        self, payload: TickerMsg | TradeMsg | LifecycleMsg, ts: datetime
    ) -> int | None:
        """Count a combo message. Returns the notional if it is a trade to log, else None."""
        family = family_of(payload.market_ticker or "")
        if isinstance(payload, LifecycleMsg):
            field = COMBO_LIFECYCLE_FIELD.get(payload.event_type)
            if field is not None:  # e.g. activated/deactivated: nothing to count
                self._agg.add(ts, family, field)
                self.combo_counted["lifecycle"] += 1
            return None
        if isinstance(payload, TradeMsg):
            try:
                count = to_e2(payload.count_fp) or 0
                value = notional_e6(payload.count_fp, price_paid(payload))
            except (PrecisionError, ArithmeticError):
                count = value = 0
            self._agg.add(ts, family, "trades")
            self._agg.add(ts, family, "contracts_e2", count)
            self._agg.add(ts, family, "notional_e6", value)
            self.combo_counted["trade"] += 1
            return value if value >= self._large_trade_e6 else None
        self._agg.add(ts, family, "ticker_msgs")
        self.combo_counted["ticker"] += 1
        return None

    # ------------------------------------------------------------------ writing
    async def _flush_loop(self) -> None:
        while True:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._wake.wait(), self._flush_interval)
            self._wake.clear()
            await self._flush(retry_forever=True)

    async def _drain(self) -> None:
        """Final flush on shutdown; gives up after a few attempts instead of hanging."""
        await self._flush(retry_forever=False)
        if self._buffer:
            log.error("dropping %d unwritten items at shutdown", len(self._buffer))

    async def _flush(self, *, retry_forever: bool) -> None:
        if not self._buffer and not self._agg and not self._ops:
            return
        batch, self._buffer = self._buffer, []
        agg, self._agg = self._agg, Aggregates()
        self._in_flight = len(batch)
        attempt = 0
        try:
            while True:
                started = time.monotonic()
                try:
                    await self._write(batch, agg)
                except Exception as exc:
                    attempt += 1
                    self.retries += 1
                    if not retry_forever and attempt >= 3:
                        self._restore(batch, agg)  # counted as dropped by the caller
                        return
                    delay = min(self._retry_max, self._retry_base * 2 ** (attempt - 1))
                    log.warning("write failed (%s); retry %d in %.1fs", exc, attempt, delay)
                    await self._sleep(delay)
                    continue
                self.last_flush_seconds = time.monotonic() - started
                self.flushes += 1
                self._record_delays(batch)
                return
        except asyncio.CancelledError:
            self._restore(batch, agg)  # keep the data; the shutdown drain writes it
            raise
        finally:
            self._in_flight = 0
            self._space.set()

    def _restore(self, batch: list[Item], agg: Aggregates) -> None:
        self._buffer[:0] = batch
        agg.merge(self._agg)
        self._agg = agg

    def _record_delays(self, batch: list[Item]) -> None:
        now = from_epoch(self._clock())
        for item in batch:
            self.lag_ms.add((item.received - item.ts).total_seconds() * 1000)
            self.write_delay_ms.add((now - item.received).total_seconds() * 1000)

    async def _write(self, batch: list[Item], agg: Aggregates) -> None:
        ops = list(self._ops)  # ops added while we write stay queued for the next batch
        ordinary = {i.ticker for i in batch if i.kind in ORDINARY_KINDS}
        ordinary |= {op.ticker for op in ops}
        ids = await self._ids.resolve(ordinary) if ordinary else {}
        watched = set(self._watched)
        for op in ops:
            (watched.add if op.watch else watched.discard)(op.ticker)
        rows: dict[str, list[tuple[Any, ...]]] = {kind: [] for kind in TABLES}
        rejected = 0
        for item in batch:
            market_id = ids[item.ticker] if item.kind in ORDINARY_KINDS else 0
            try:
                rows[item.kind].append(TABLES[item.kind][2](item, market_id))
                if item.kind == "trade" and item.ticker in watched:
                    rows["trade_watched"].append(TABLES["trade_watched"][2](item, market_id))
            except (PrecisionError, ValueError) as exc:
                rejected += 1
                log.error("rejected %s row: %s", item.kind, exc)

        async with self._engine.connect() as conn:
            raw = await conn.get_raw_connection()
            driver = raw.driver_connection
            assert driver is not None
            async with driver.transaction():
                # Ops first: the history copy sees every earlier batch, and this batch's
                # trades then go to both tables.
                for op in ops:
                    market_id = ids[op.ticker]
                    if op.watch:
                        await driver.execute(OPEN_PERIOD, market_id, op.source)
                        await driver.execute(COPY_HISTORY, market_id)
                    else:
                        await driver.execute(CLOSE_PERIOD, market_id)
                for kind, (table, columns, _) in TABLES.items():
                    if rows[kind]:
                        await driver.copy_records_to_table(
                            table, records=rows[kind], columns=columns
                        )
                if agg:
                    await driver.executemany(UPSERT_STATS, agg.records())

        # Mutate shared state only after the transaction committed, so a retry starts clean.
        self.rejected += rejected
        del self._ops[: len(ops)]
        self._watched = watched
        for kind, kind_rows in rows.items():
            self.written[TABLES[kind][0]] += len(kind_rows)

    # ------------------------------------------------------------------ monitoring
    def stats(self) -> dict[str, Any]:
        return {
            "seen": dict(self.seen),
            "written": dict(self.written),
            "combo_counted": dict(self.combo_counted),
            "rejected": self.rejected,
            "book_problems": dict(self.book_problems),
            "reconnects": self.reconnects,
            "flushes": self.flushes,
            "retries": self.retries,
            "buffered": len(self._buffer) + self._in_flight,
            "watched": len(self._watched),
            "last_flush_seconds": self.last_flush_seconds,
            "placeholders_created": self._ids.placeholders_created,
            "lag_ms": self.lag_ms.summary(),
            "write_delay_ms": self.write_delay_ms.summary(),
        }
