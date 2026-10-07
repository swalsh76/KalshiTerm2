"""Streaming ingestor: WebSocket messages -> batched, atomic ``COPY`` into the hypertables.

Intake filters and timestamps each message and appends it to a buffer; a flusher task writes
the buffer every ``flush_interval`` seconds or when it reaches ``batch_size``. A batch is one
transaction, so a failed write retries cleanly (with backoff) and leaves no partial data.
The number of buffered plus in-flight messages is capped: when the database stalls, intake
pauses, the WebSocket client's bounded queue fills, and its overflow-reconnect path takes over.

Combo (multivariate, ``KXMVE…``) markets: with a REST client the ingestor keeps the whole
universe's per-minute counters, stores a per-market row only for combos that have *traded*
(looked up in batches by a separate resolver task, so the write path never touches the
network), and stores raw tickers/trades only for those. Without a REST client combo messages
are skipped and counted.
"""

import asyncio
import contextlib
import logging
import time
import uuid
from collections import Counter, deque
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
from kalshi_core.rest import KalshiAPIError, KalshiRestClient
from kalshi_core.ws import RECONNECTED
from kalshi_core.ws_models import LifecycleMsg, TickerMsg, TradeMsg, WsMessage
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncEngine

from kalshiterm_server.fixedpoint import PrecisionError, to_e2, to_e6
from kalshiterm_server.ingest.combos import (
    UPDATE_CLOSE,
    UPDATE_DETERMINED,
    UPDATE_SETTLED,
    UPSERT_STATS,
    Aggregates,
    ComboStore,
    family_of,
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
LOOKUP_BATCH = 100
LOOKUP_ATTEMPTS = 5
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

TICKER_COLUMNS = [
    "ts", "received_at", "market_id", "price_e6", "yes_bid_e6", "yes_ask_e6",
    "yes_bid_size_e2", "yes_ask_size_e2", "volume_e2", "open_interest_e2", "last_trade_size_e2",
]  # fmt: skip
TRADE_COLUMNS = [
    "ts", "received_at", "market_id", "trade_id", "yes_price_e6", "count_e2", "taker_side",
    "is_block_trade",
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


@dataclass(slots=True)
class Item:
    # "ticker" | "trade" | "market_lifecycle_v2" | "combo_ticker" | "combo_trade" | "combo_life"
    kind: str
    payload: TickerMsg | TradeMsg | LifecycleMsg
    ts: datetime  # exchange time
    received: datetime  # when this process read it off the socket

    @property
    def ticker(self) -> str:
        ticker = self.payload.market_ticker
        assert ticker is not None
        return ticker


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


# kind -> (table, columns, row builder); combo kinds reuse the builders with a combo id
TABLES: dict[str, tuple[str, list[str], Callable[[Item, int], tuple[Any, ...]]]] = {
    "ticker": ("tickers", TICKER_COLUMNS, ticker_row),
    "trade": ("trades", TRADE_COLUMNS, trade_row),
    "market_lifecycle_v2": ("market_lifecycle", LIFECYCLE_COLUMNS, lifecycle_row),
    "combo_ticker": ("combo_tickers", TICKER_COLUMNS, ticker_row),
    "combo_trade": ("combo_trades", TRADE_COLUMNS, trade_row),
}
ORDINARY_KINDS = ("ticker", "trade", "market_lifecycle_v2")


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
        messages: AsyncIterator[WsMessage],
        engine: AsyncEngine,
        *,
        rest: KalshiRestClient | None = None,
        batch_size: int = 5_000,
        flush_interval: float = 1.0,
        max_buffered: int = 100_000,
        max_held: int = 50_000,
        unresolved_delays: tuple[float, ...] = (2.0, 10.0),
        retry_base: float = 0.5,
        retry_max: float = 30.0,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._messages = messages
        self._engine = engine
        self._rest = rest
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._max_buffered = max_buffered
        self._max_held = max_held
        self._unresolved_delays = unresolved_delays
        self._retry_base = retry_base
        self._retry_max = retry_max
        self._sleep = sleep
        self._clock = clock
        self._ids = MarketIds(engine)
        self._combos = ComboStore(engine, self._ids)
        self._buffer: list[Item] = []
        self._agg = Aggregates()
        self._in_flight = 0
        self._wake = asyncio.Event()
        self._space = asyncio.Event()
        self._space.set()
        # combo trades waiting for their market's metadata, and the lookups that will fetch it
        self._held: dict[str, list[Item]] = {}
        self._held_count = 0
        self._to_lookup: dict[str, datetime] = {}
        self._not_before: dict[str, float] = {}  # monotonic time before which not to retry
        self._misses: Counter[str] = Counter()  # lookups that found nothing, per combo
        self._lookup_wake = asyncio.Event()
        self.seen: Counter[str] = Counter()
        self.written: Counter[str] = Counter()
        self.combo: Counter[str] = Counter()
        self.skipped_mve = 0
        self.rejected = 0
        self.reconnects = 0
        self.flushes = 0
        self.retries = 0
        self.last_flush_seconds = 0.0
        self.lag_ms = Samples()  # received_at - exchange ts (network + server + clock skew)
        self.write_delay_ms = Samples()  # written - received_at (our own buffering)

    @property
    def combos_enabled(self) -> bool:
        return self._rest is not None

    # ------------------------------------------------------------------ intake
    async def run(self) -> None:
        tasks = [asyncio.create_task(self._flush_loop())]
        if self.combos_enabled:
            tasks.append(asyncio.create_task(self._resolver_loop()))
        try:
            async for message in self._messages:
                await self._admit(message)
        finally:
            for task in tasks:
                task.cancel()
            for task in tasks:
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            await self._drain()
            if self._held_count:
                log.error("dropping %d combo trades still awaiting lookup", self._held_count)

    def _classify(self, message_type: str, ticker: str) -> str | None:
        """The Item kind for a message, or None if it is to be skipped."""
        is_combo = ticker.startswith(MVE_PREFIX)
        if message_type == "multivariate_market_lifecycle":
            return "combo_life" if self.combos_enabled else None
        if is_combo:
            if not self.combos_enabled:
                return None
            return f"combo_{message_type}" if message_type in ("ticker", "trade") else None
        return message_type if message_type in ORDINARY_KINDS else None

    async def _admit(self, message: WsMessage) -> None:
        if message.type == RECONNECTED:
            self.reconnects += 1
            log.warning("reconnected: data may have been missed (%s)", message.msg)
            self._wake.set()  # get everything received before the gap onto disk
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
        kind = self._classify(message.type, ticker)
        if kind is None:
            self.skipped_mve += 1
            return
        received = from_epoch(message.received_at or self._clock())
        exchange_ms = getattr(payload, "ts_ms", None) or message.sending_ts_ms
        ts = from_ms(exchange_ms) if exchange_ms else received
        if kind.startswith("combo_") and not self._count_combo(kind, payload, ts):
            return
        while len(self._buffer) + self._in_flight >= self._max_buffered:
            self._space.clear()  # backpressure: leave the backlog in the bounded WS queue
            await self._space.wait()
        self._buffer.append(Item(kind, payload, ts, received))
        if len(self._buffer) >= self._batch_size:
            self._wake.set()

    def _count_combo(
        self, kind: str, payload: TickerMsg | TradeMsg | LifecycleMsg, ts: datetime
    ) -> bool:
        """Update the universe counters; False if the message needs no further handling."""
        family = family_of(payload.market_ticker or "")
        if isinstance(payload, LifecycleMsg):
            field = COMBO_LIFECYCLE_FIELD.get(payload.event_type)
            if field is None:
                return False  # e.g. activated/deactivated: nothing to count or store
            self._agg.add(ts, family, field)
            return True
        if isinstance(payload, TradeMsg):
            self._agg.add(ts, family, "trades")
            with contextlib.suppress(PrecisionError):
                self._agg.add(ts, family, "contracts_e2", to_e2(payload.count_fp) or 0)
            return True
        self._agg.add(ts, family, "ticker_msgs")
        return True

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
        if not self._buffer and not self._agg:
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
        ordinary = {i.ticker for i in batch if i.kind in ORDINARY_KINDS}
        ids = await self._ids.resolve(ordinary) if ordinary else {}
        combo_tickers = {i.ticker for i in batch if i.kind.startswith("combo_")}
        combo_ids = await self._combos.known_ids(combo_tickers) if combo_tickers else {}

        rows: dict[str, list[tuple[Any, ...]]] = {kind: [] for kind in TABLES}
        determined: list[tuple[str, str, int | None]] = []
        settled: list[tuple[str, datetime]] = []
        closed: list[tuple[str, datetime]] = []
        to_hold: list[Item] = []
        dropped_tickers = rejected = 0
        for item in batch:
            kind = item.kind
            if kind == "combo_life":
                if item.ticker in combo_ids:
                    self._collect_lifecycle(item, determined, settled, closed)
                continue
            if kind.startswith("combo_"):
                market_id = combo_ids.get(item.ticker)
                if market_id is None:
                    if kind == "combo_trade":
                        to_hold.append(item)
                    else:
                        dropped_tickers += 1
                    continue
            else:
                market_id = ids[item.ticker]
            try:
                rows[kind].append(TABLES[kind][2](item, market_id))
            except (PrecisionError, ValueError) as exc:
                rejected += 1
                log.error("rejected %s row: %s", kind, exc)

        async with self._engine.connect() as conn:
            raw = await conn.get_raw_connection()
            driver = raw.driver_connection
            assert driver is not None
            async with driver.transaction():
                for kind, (table, columns, _) in TABLES.items():
                    if rows[kind]:
                        await driver.copy_records_to_table(
                            table, records=rows[kind], columns=columns
                        )
                if determined:
                    t, r, v = zip(*determined, strict=True)
                    await driver.execute(UPDATE_DETERMINED, list(t), list(r), list(v))
                if settled:
                    t2, a2 = zip(*settled, strict=True)
                    await driver.execute(UPDATE_SETTLED, list(t2), list(a2))
                if closed:
                    t3, a3 = zip(*closed, strict=True)
                    await driver.execute(UPDATE_CLOSE, list(t3), list(a3))
                if agg:
                    await driver.executemany(UPSERT_STATS, agg.records())

        # Mutate shared state only after the transaction committed, so a retry starts clean.
        self.rejected += rejected
        self.combo["tickers_dropped"] += dropped_tickers
        for kind, kind_rows in rows.items():
            self.written[TABLES[kind][0]] += len(kind_rows)
        for item in to_hold:
            self._hold(item)

    def _collect_lifecycle(
        self,
        item: Item,
        determined: list[tuple[str, str, int | None]],
        settled: list[tuple[str, datetime]],
        closed: list[tuple[str, datetime]],
    ) -> None:
        p = item.payload
        assert isinstance(p, LifecycleMsg)
        if p.event_type == "determined":
            try:
                value = to_e6(p.settlement_value)
            except PrecisionError:
                value = None
            determined.append((item.ticker, p.result or "", value))
        elif p.event_type == "settled":
            settled.append((item.ticker, from_s(p.settled_ts) or item.ts))
        elif p.event_type == "close_date_updated" and p.close_ts is not None:
            closed.append((item.ticker, from_s(p.close_ts) or item.ts))

    # ------------------------------------------------------------------ combo lookups
    def _hold(self, item: Item) -> None:
        """A trade on a combo we have no row for yet: wait for its metadata."""
        if not self.combos_enabled:
            return
        if self._held_count >= self._max_held:
            self.combo["held_overflow"] += 1
            return
        self._held.setdefault(item.ticker, []).append(item)
        self._held_count += 1
        earliest = self._to_lookup.get(item.ticker)
        self._to_lookup[item.ticker] = item.ts if earliest is None else min(earliest, item.ts)
        self._lookup_wake.set()

    async def _resolver_loop(self) -> None:
        while True:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._lookup_wake.wait(), 0.5)
            self._lookup_wake.clear()
            while True:
                now = time.monotonic()
                due = [t for t in self._to_lookup if self._not_before.get(t, 0.0) <= now]
                if not due:
                    break
                first_trade = {t: self._to_lookup.pop(t) for t in due[:LOOKUP_BATCH]}
                await self._resolve(first_trade)

    async def _resolve(self, first_trade: dict[str, datetime]) -> None:
        """Fetch metadata for newly traded combos, create their rows, release their trades."""
        assert self._rest is not None
        tickers = list(first_trade)
        found: dict[str, Any] = {}
        for attempt in range(1, LOOKUP_ATTEMPTS + 1):
            try:
                page = await self._rest.markets_page(tickers=",".join(tickers), limit=len(tickers))
                found = {m.ticker: m for m in page.markets if m.mve_collection_ticker}
                await self._combos.create(list(found.values()), first_trade)
                break
            except (KalshiAPIError, httpx.HTTPError, OSError) as exc:
                self.combo["lookup_errors"] += 1
                log.warning("combo lookup failed (%s); attempt %d", exc, attempt)
                if attempt == LOOKUP_ATTEMPTS:
                    self._drop_held(tickers, "lookup_failed")
                    return
                await self._sleep(min(self._retry_max, self._retry_base * 2 ** (attempt - 1)))
        for ticker in tickers:
            if ticker in found:
                items = self._held.pop(ticker, [])
                self._held_count -= len(items)
                self._buffer.extend(items)  # now has a row: written on the next flush
                self.combo["rows_created"] += 1
                self._misses.pop(ticker, None)
                self._not_before.pop(ticker, None)
            elif self._misses[ticker] < len(self._unresolved_delays):
                # A combo traded seconds after it was created may not be queryable yet.
                delay = self._unresolved_delays[self._misses[ticker]]
                self._misses[ticker] += 1
                self._to_lookup[ticker] = first_trade[ticker]
                self._not_before[ticker] = time.monotonic() + delay
            else:
                items = self._held.pop(ticker, [])
                self._held_count -= len(items)
                self.combo["unresolved"] += len(items)
                self._misses.pop(ticker, None)
                self._not_before.pop(ticker, None)
        self._wake.set()

    def _drop_held(self, tickers: list[str], reason: str) -> None:
        for ticker in tickers:
            items = self._held.pop(ticker, [])
            self._held_count -= len(items)
            self.combo[reason] += len(items)

    # ------------------------------------------------------------------ monitoring
    def stats(self) -> dict[str, Any]:
        return {
            "seen": dict(self.seen),
            "written": dict(self.written),
            "skipped_mve": self.skipped_mve,
            "combo": {**self.combo, "held": self._held_count},
            "rejected": self.rejected,
            "reconnects": self.reconnects,
            "flushes": self.flushes,
            "retries": self.retries,
            "buffered": len(self._buffer) + self._in_flight,
            "last_flush_seconds": self.last_flush_seconds,
            "placeholders_created": self._ids.placeholders_created,
            "lag_ms": self.lag_ms.summary(),
            "write_delay_ms": self.write_delay_ms.summary(),
        }
