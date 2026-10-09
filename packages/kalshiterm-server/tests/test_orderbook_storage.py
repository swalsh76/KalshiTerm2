import asyncio
import random
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal as D
from typing import Any

import pytest
from kalshi_core.models import OrderBook, PriceLevel
from kalshi_core.orderbook import (
    GAP,
    MESSAGE,
    RESET,
    RESUBSCRIBE_FAILED,
    RESYNC_FAILED,
    BookEvent,
    BookTracker,
)
from kalshi_core.ws import RECONNECTED
from kalshi_core.ws_models import WsMessage
from kalshiterm_server import db
from kalshiterm_server.ingest.stream import StreamIngestor
from sqlalchemy import text
from test_stream import RECEIVE_DELAY, T_MS, rows, scalar, trade_msg

pytestmark = pytest.mark.db

Levels = dict[D, D]
Books = dict[str, dict[str, Levels]]  # ticker -> side -> price -> size


def snap_msg(sid: int, seq: int, ticker: str, yes: Any, no: Any, offset_ms: int = 0) -> WsMessage:
    msg = {"market_ticker": ticker, "yes_dollars_fp": yes, "no_dollars_fp": no}
    return WsMessage(
        type="orderbook_snapshot",
        sid=sid,
        seq=seq,
        msg=msg,
        sending_ts_ms=T_MS + offset_ms,
        received_at=(T_MS + offset_ms) / 1000 + RECEIVE_DELAY,
    )


def delta_msg(
    sid: int, seq: int, ticker: str, side: str, price: str, change: str, offset_ms: int = 0
) -> WsMessage:
    msg = {
        "market_ticker": ticker,
        "side": side,
        "price_dollars": price,
        "delta_fp": change,
        "ts_ms": T_MS + offset_ms,
    }
    return WsMessage(
        type="orderbook_delta",
        sid=sid,
        seq=seq,
        msg=msg,
        sending_ts_ms=T_MS + offset_ms + 5,
        received_at=(T_MS + offset_ms) / 1000 + RECEIVE_DELAY,
    )


def events_for(tracker: BookTracker, messages: list[WsMessage]) -> list[BookEvent]:
    return [event for message in messages for event in tracker.process(message)]


async def source(events: list[BookEvent]) -> AsyncIterator[BookEvent]:
    for event in events:
        yield event


async def ingest(url: str, events: list[BookEvent], **kwargs: Any) -> StreamIngestor:
    engine = db.make_engine(url)
    try:
        ingestor = StreamIngestor(source(events), engine, **kwargs)
        async with asyncio.timeout(30):
            await ingestor.run()
        return ingestor
    finally:
        await engine.dispose()


# ---------------------------------------------------------------- exact stored values
async def test_snapshots_and_deltas_are_stored_with_exact_values(migrated_db_url: str) -> None:
    tracker = BookTracker()
    tracker.begin_subscription(1, ["MKT-A"])
    yes_levels = [["0.40", "10.00"], ["0.41", "5.50"]]
    events = events_for(
        tracker,
        [
            snap_msg(1, 1, "MKT-A", yes_levels, [["0.55", "7.00"]]),
            delta_msg(1, 2, "MKT-A", "yes", "0.41", "-3.00", 100),
            delta_msg(1, 3, "MKT-A", "no", "0.56", "2.50", 200),
        ],
    )
    ingestor = await ingest(migrated_db_url, events)
    assert ingestor.written["orderbook_snapshots"] == 1
    assert ingestor.written["orderbook_deltas"] == 2

    snapshot_sql = (
        "select ts, received_at, seq, approximate, yes_prices_e6, yes_sizes_e2, no_prices_e6, "
        "no_sizes_e2 from orderbook_snapshots"
    )
    [snap] = await rows(migrated_db_url, snapshot_sql)
    assert snap[0] == datetime.fromtimestamp(T_MS / 1000, UTC)  # sending_ts_ms
    assert (snap[1] - snap[0]).total_seconds() == pytest.approx(RECEIVE_DELAY)
    assert snap[2:4] == (1, False)
    assert snap[4:] == ([410_000, 400_000], [550, 1000], [550_000], [700])  # best price first

    delta_sql = "select ts, seq, is_yes, price_e6, delta_e2 from orderbook_deltas order by seq"
    assert await rows(migrated_db_url, delta_sql) == [
        (datetime.fromtimestamp((T_MS + 100) / 1000, UTC), 2, True, 410_000, -300),
        (datetime.fromtimestamp((T_MS + 200) / 1000, UTC), 3, False, 560_000, 250),
    ]
    view_sql = "select ticker, side, price, delta from orderbook_deltas_v order by seq"
    view = await rows(migrated_db_url, view_sql)
    assert [(t, s, str(p), str(d)) for t, s, p, d in view] == [
        ("MKT-A", "yes", "0.410000", "-3.00"),
        ("MKT-A", "no", "0.560000", "2.50"),
    ]
    best_sql = "select best_yes_bid, best_no_bid, yes_levels, no_levels from orderbook_snapshots_v"
    best = await rows(migrated_db_url, best_sql)
    assert [(str(a), str(b), c, d) for a, b, c, d in best] == [("0.410000", "0.550000", 2, 1)]


# ---------------------------------------------------------------- round trip
class Simulator:
    """Random but valid orderbook traffic, with the true books tracked on the side."""

    def __init__(self, tickers: list[str], seed: int) -> None:
        self.rng = random.Random(seed)
        self.tickers = tickers
        self.prices = [f"0.{p}0" for p in range(30, 70, 2)]
        self.seq = 0
        self.messages: list[WsMessage] = []
        self.books: Books = {t: {"yes": {}, "no": {}} for t in tickers}

    def _next(self) -> int:
        self.seq += 1
        return self.seq

    def snapshot(self, ticker: str, offset_ms: int) -> None:
        book = self.books[ticker]
        yes = [[str(p), f"{q:.2f}"] for p, q in book["yes"].items()]
        no = [[str(p), f"{q:.2f}"] for p, q in book["no"].items()]
        self.messages.append(snap_msg(1, self._next(), ticker, yes, no, offset_ms))

    def seed_books(self) -> None:
        for ticker in self.tickers:
            for side in ("yes", "no"):
                self.books[ticker][side] = {D(p): D("20.00") for p in self.prices[:6]}
            self.snapshot(ticker, self.seq + 1)

    def random_delta(self, offset_ms: int) -> None:
        ticker = self.rng.choice(self.tickers)
        side = self.rng.choice(["yes", "no"])
        price = D(self.rng.choice(self.prices))
        levels = self.books[ticker][side]
        current = levels.get(price, D(0))
        size = D(self.rng.randint(1, 500)) / 100
        change = size if current == 0 or self.rng.random() < 0.6 else -min(current, size)
        levels[price] = current + change
        if levels[price] == 0:
            del levels[price]
        self.messages.append(
            delta_msg(1, self._next(), ticker, side, str(price), f"{change:.2f}", offset_ms)
        )


def replay(snapshot_row: tuple[Any, ...], later: list[tuple[Any, ...]]) -> tuple[Levels, Levels]:
    """Rebuild a book from a stored snapshot (arrays) plus the stored deltas after it."""
    yes = dict(zip(snapshot_row[2], snapshot_row[3], strict=True))
    no = dict(zip(snapshot_row[4], snapshot_row[5], strict=True))
    for is_yes, price, change in later:
        levels = yes if is_yes else no
        levels[price] = levels.get(price, 0) + change
        if levels[price] == 0:
            del levels[price]
    scaled = (
        {D(p) / 1_000_000: D(q) / 100 for p, q in yes.items()},
        {D(p) / 1_000_000: D(q) / 100 for p, q in no.items()},
    )
    return scaled


async def test_replaying_stored_snapshot_and_deltas_reproduces_the_live_book(
    migrated_db_url: str,
) -> None:
    """The point of storing both: a stored checkpoint plus later deltas rebuilds any book."""
    tickers = ["MKT-A", "MKT-B", "MKT-C"]
    sim = Simulator(tickers, seed=11)
    sim.seed_books()
    for step in range(300):
        sim.random_delta(1000 + step)
        if step == 150:  # a periodic checkpoint part-way through
            for ticker in tickers:
                sim.snapshot(ticker, 1000 + step)

    tracker = BookTracker()
    tracker.begin_subscription(1, tickers)
    events = events_for(tracker, sim.messages)
    assert not [e for e in events if e.kind == GAP]
    await ingest(migrated_db_url, events)

    for ticker in tickers:
        market_id = await scalar(
            migrated_db_url, f"select id from markets where ticker = '{ticker}'"
        )
        latest_sql = (
            "select ts, seq, yes_prices_e6, yes_sizes_e2, no_prices_e6, no_sizes_e2 "
            f"from orderbook_snapshots where market_id = {market_id} order by seq desc limit 1"
        )
        [latest] = await rows(migrated_db_url, latest_sql)
        later_sql = (
            "select is_yes, price_e6, delta_e2 from orderbook_deltas "
            f"where market_id = {market_id} and seq > {latest[1]} order by seq"
        )
        later = await rows(migrated_db_url, later_sql)
        assert later, "the replay must apply deltas on top of the checkpoint"
        yes, no = replay(latest, later)
        assert yes == sim.books[ticker]["yes"]
        assert no == sim.books[ticker]["no"]
        assert yes == tracker.books[ticker].yes and no == tracker.books[ticker].no


# ---------------------------------------------------------------- other event kinds
async def test_a_rest_built_snapshot_is_flagged_approximate_with_no_sequence(
    migrated_db_url: str,
) -> None:
    tracker = BookTracker()
    tracker.begin_subscription(1, ["MKT-A"])
    rest_book = OrderBook(
        yes=[PriceLevel(price=D("0.40"), quantity=D("3.00"))],
        no=[PriceLevel(price=D("0.55"), quantity=D("2.00"))],
    )
    event = tracker.apply_rest_snapshot("MKT-A", rest_book)
    assert event.message is None and event.detail == "rest"
    await ingest(migrated_db_url, [event], clock=lambda: T_MS / 1000 + 60)
    sql = "select ts, seq, approximate, yes_prices_e6, no_sizes_e2 from orderbook_snapshots"
    [row] = await rows(migrated_db_url, sql)
    assert row == (datetime.fromtimestamp(T_MS / 1000 + 60, UTC), None, True, [400_000], [200])


async def test_other_messages_and_reconnects_pass_through_the_feed_events(
    migrated_db_url: str,
) -> None:
    trade = trade_msg("KXA-E1-X", offset_ms=10)
    events = [
        BookEvent(MESSAGE, message=trade),
        BookEvent(RESET, message=WsMessage(type=RECONNECTED, msg={"reason": "connection_lost"})),
        BookEvent(GAP, tickers=("MKT-A", "MKT-B"), detail="seq 9, expected 4"),
        BookEvent(RESYNC_FAILED, ticker="MKT-A", detail="404"),
        BookEvent(RESUBSCRIBE_FAILED, tickers=("MKT-A",), detail="not restored"),
    ]
    ingestor = await ingest(migrated_db_url, events)
    assert ingestor.written["trades"] == 1
    assert ingestor.reconnects == 1
    assert dict(ingestor.book_problems) == {GAP: 1, RESYNC_FAILED: 1, RESUBSCRIBE_FAILED: 1}
    assert ingestor.stats()["book_problems"][GAP] == 1
    assert await scalar(migrated_db_url, "select count(*) from orderbook_deltas") == 0


async def test_orderbook_markets_reuse_ids_and_unknown_ones_get_placeholders(
    migrated_db_url: str,
) -> None:
    engine = db.make_engine(migrated_db_url)
    insert = (
        "insert into markets (ticker, event_ticker, market_type, status) "
        "values ('MKT-A', 'MKT', 'binary', 'active')"
    )
    async with engine.begin() as conn:
        await conn.execute(text(insert))
    await engine.dispose()
    known_id = await scalar(migrated_db_url, "select id from markets where ticker = 'MKT-A'")
    tracker = BookTracker()
    tracker.begin_subscription(1, ["MKT-A", "MKT-NEW"])
    events = events_for(
        tracker,
        [
            snap_msg(1, 1, "MKT-A", [["0.4", "1.00"]], []),
            snap_msg(1, 2, "MKT-NEW", [], [["0.5", "1.00"]]),
        ],
    )
    ingestor = await ingest(migrated_db_url, events)
    assert ingestor.stats()["placeholders_created"] == 1
    counts_sql = "select market_id, count(*) from orderbook_snapshots group by 1"
    stored = dict(await rows(migrated_db_url, counts_sql))
    assert known_id in stored and len(stored) == 2


async def test_a_value_finer_than_the_scale_is_rejected_without_stopping_the_rest(
    migrated_db_url: str, caplog: pytest.LogCaptureFixture
) -> None:
    tracker = BookTracker()
    tracker.begin_subscription(1, ["MKT-A"])
    events = events_for(
        tracker,
        [
            snap_msg(1, 1, "MKT-A", [["0.40", "10.00"]], []),
            delta_msg(1, 2, "MKT-A", "yes", "0.1234567", "1.00"),  # price finer than 1e-6
            delta_msg(1, 3, "MKT-A", "yes", "0.40", "1.00"),
        ],
    )
    ingestor = await ingest(migrated_db_url, events)
    assert ingestor.rejected == 1 and ingestor.written["orderbook_deltas"] == 1
    assert "rejected orderbook delta" in caplog.text


async def test_the_subscription_id_is_stored_with_every_snapshot_and_delta(
    migrated_db_url: str,
) -> None:
    tracker = BookTracker()
    tracker.begin_subscription(5, ["MKT-A"])
    events = events_for(
        tracker,
        [
            snap_msg(5, 1, "MKT-A", [("0.40", "10.00")], []),
            delta_msg(5, 2, "MKT-A", "yes", "0.40", "1.00", offset_ms=10),
        ],
    )
    events.append(
        tracker.apply_rest_snapshot(
            "MKT-A", OrderBook(yes=[PriceLevel(price=D("0.45"), quantity=D("2.00"))], no=[])
        )
    )
    await ingest(migrated_db_url, events)
    snaps = await rows(
        migrated_db_url,
        "select seq, sid, approximate from orderbook_snapshots order by ts, seq nulls last",
    )
    assert snaps == [(1, 5, False), (None, None, True)]  # a REST rebuild has neither
    assert await rows(migrated_db_url, "select seq, sid from orderbook_deltas") == [(2, 5)]
