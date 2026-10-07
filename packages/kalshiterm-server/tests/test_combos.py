import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
import respx
from kalshi_core.config import KalshiSettings
from kalshi_core.rest import KalshiRestClient
from kalshi_core.ws_models import WsMessage
from kalshiterm_server import db
from kalshiterm_server.ingest.stream import StreamIngestor
from test_stream import (
    RECEIVE_DELAY,
    T_MS,
    Gate,
    rows,
    scalar,
    ticker_msg,
    trade_msg,
    until,
)

pytestmark = pytest.mark.db

BASE = "https://external-api.demo.kalshi.co/trade-api/v2"
COMBO = "KXMVECROSSCATEGORY-S2026ABC-111"
COLLECTION = "KXMVECROSSCATEGORY-R"
LEGS = [
    ("KXATP-26OCT07AB-A", "yes"),
    ("KXNBA-26OCT07CD-C", "no"),
    ("KXMLB-26OCT07EF-E", "yes"),
]


def combo_market(ticker: str = COMBO, **overrides: Any) -> dict[str, Any]:
    market = {
        "ticker": ticker,
        "event_ticker": ticker.rsplit("-", 1)[0],
        "market_type": "binary",
        "status": "active",
        "mve_collection_ticker": COLLECTION,
        "mve_selected_legs": [
            {"event_ticker": t.rsplit("-", 1)[0], "market_ticker": t, "side": side}
            for t, side in LEGS
        ],
        "created_time": "2026-10-07T10:00:00Z",
        "open_time": "2026-10-07T10:00:00Z",
        "close_time": "2026-10-14T10:00:00Z",
    }
    return {**market, **overrides}


def lifecycle(ticker: str, event_type: str, offset_ms: int = 0, **msg: Any) -> WsMessage:
    return WsMessage(
        type="multivariate_market_lifecycle",
        sid=5,
        msg={"market_ticker": ticker, "event_type": event_type, **msg},
        sending_ts_ms=T_MS + offset_ms,
        received_at=(T_MS + offset_ms) / 1000 + RECEIVE_DELAY,
    )


class FakeLookup:
    """Answers GET /markets?tickers=... from a dict and records the requests."""

    def __init__(self, known: list[dict[str, Any]] | None = None) -> None:
        self.known = {m["ticker"]: m for m in (known if known is not None else [combo_market()])}
        self.requests: list[str] = []
        self.failures = 0  # respond 500 this many times first
        self.hide_first = 0  # answer "not found" for this many lookups, then find it
        self.gate: asyncio.Event | None = None  # if set, block until released

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        wanted = request.url.params["tickers"].split(",")
        self.requests.append(request.url.params["tickers"])
        if self.gate is not None:
            await self.gate.wait()
        if self.failures > 0:
            self.failures -= 1
            return httpx.Response(500, text="boom")
        if self.hide_first > 0:
            self.hide_first -= 1
            return httpx.Response(200, json={"markets": [], "cursor": ""})
        found = [self.known[t] for t in wanted if t in self.known]
        return httpx.Response(200, json={"markets": found, "cursor": ""})


@pytest.fixture
def lookup() -> FakeLookup:
    return FakeLookup()


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    monkeypatch.chdir(tmp_path)
    for name in ("KALSHI_ENV", "KALSHI_KEY_ID", "KALSHI_PRIVATE_KEY_PATH"):
        monkeypatch.delenv(name, raising=False)


def flushed(harness: "Harness", count: int) -> Callable[[], bool]:
    return lambda: harness.ingestor.flushes >= count


async def fast_sleep(_: float) -> None:
    await asyncio.sleep(0.01)


class Harness:
    """A running ingestor fed through a Gate, with a mocked Kalshi lookup behind it."""

    def __init__(self, url: str, lookup: FakeLookup, **kwargs: Any) -> None:
        respx.get(f"{BASE}/markets").mock(side_effect=lookup)
        self.gate = Gate()
        self.engine = db.make_engine(url)
        self.rest = KalshiRestClient(KalshiSettings(), max_retries=0)
        kwargs.setdefault("flush_interval", 0.05)
        kwargs.setdefault("unresolved_delays", (0.05, 0.05))
        kwargs.setdefault("sleep", fast_sleep)
        self.ingestor = StreamIngestor(self.gate.__aiter__(), self.engine, rest=self.rest, **kwargs)
        self.task: asyncio.Task[None] | None = None

    async def __aenter__(self) -> "Harness":
        self.task = asyncio.create_task(self.ingestor.run())
        return self

    async def __aexit__(self, *exc: object) -> None:
        self.gate.close()
        assert self.task is not None
        await asyncio.wait_for(self.task, 10)
        await self.rest.aclose()
        await self.engine.dispose()


@respx.mock
async def test_a_combo_that_trades_gets_a_row_with_its_legs_and_its_data_is_stored(
    migrated_db_url: str, lookup: FakeLookup
) -> None:
    async with Harness(migrated_db_url, lookup) as h:
        h.gate.put(ticker_msg(COMBO, offset_ms=0))  # quote before any trade: not stored
        h.gate.put(trade_msg(COMBO, offset_ms=1000))
        await until(lambda: h.ingestor.written["combo_trades"] == 1)
        h.gate.put(ticker_msg(COMBO, offset_ms=2000))  # now the combo has a row
        await until(lambda: h.ingestor.written["combo_tickers"] == 1)
    assert len(lookup.requests) == 1 and lookup.requests[0] == COMBO
    [row] = await rows(
        migrated_db_url,
        "select ticker, collection_ticker, event_ticker, status, first_trade_at, close_time, "
        "leg_market_ids, leg_yes from combo_markets",
    )
    assert row[:4] == (COMBO, COLLECTION, "KXMVECROSSCATEGORY-S2026ABC", "active")
    assert row[4] == datetime.fromtimestamp((T_MS + 1000) / 1000, UTC)  # time of the first trade
    assert row[5] == datetime(2026, 10, 14, 10, tzinfo=UTC)
    assert row[7] == [True, False, True]  # yes, no, yes
    leg_tickers = await rows(
        migrated_db_url, f"select ticker, status from markets where id = any(array{row[6]})"
    )
    assert sorted(t for t, _ in leg_tickers) == sorted(t for t, _ in LEGS)
    combo_id = await scalar(
        migrated_db_url, f"select id from combo_markets where ticker = '{COMBO}'"
    )
    assert await rows(migrated_db_url, "select market_id from combo_trades") == [(combo_id,)]
    assert await rows(migrated_db_url, "select market_id from combo_tickers") == [(combo_id,)]
    for ordinary_table in ("tickers", "trades"):
        assert await scalar(migrated_db_url, f"select count(*) from {ordinary_table}") == 0
    view = await rows(migrated_db_url, "select ticker, yes_price, count from combo_trades_v")
    assert [(t, str(p), str(c)) for t, p, c in view] == [(COMBO, "0.560000", "3.00")]


@respx.mock
async def test_known_legs_are_reused_and_unknown_legs_become_placeholders(
    migrated_db_url: str, lookup: FakeLookup
) -> None:
    engine = db.make_engine(migrated_db_url)
    async with engine.begin() as conn:
        from sqlalchemy import text

        await conn.execute(
            text(
                "insert into markets (ticker, event_ticker, market_type, status) "
                "values ('KXATP-26OCT07AB-A', 'KXATP-26OCT07AB', 'binary', 'active')"
            )
        )
    await engine.dispose()
    known_id = await scalar(
        migrated_db_url, "select id from markets where ticker = 'KXATP-26OCT07AB-A'"
    )
    async with Harness(migrated_db_url, lookup) as h:
        h.gate.put(trade_msg(COMBO))
        await until(lambda: h.ingestor.written["combo_trades"] == 1)
    [(leg_ids,)] = await rows(migrated_db_url, "select leg_market_ids from combo_markets")
    assert leg_ids[0] == known_id  # the existing ordinary market was reused, not duplicated
    statuses = dict(await rows(migrated_db_url, "select ticker, status from markets"))
    assert statuses["KXATP-26OCT07AB-A"] == "active"
    assert statuses["KXNBA-26OCT07CD-C"] == "unknown" and statuses["KXMLB-26OCT07EF-E"] == "unknown"


@respx.mock
async def test_universe_counters_include_combos_that_never_get_a_row(
    migrated_db_url: str, lookup: FakeLookup
) -> None:
    other = "KXMVECROSSCATEGORY0-S2026XYZ-222"  # a different family, never traded
    async with Harness(migrated_db_url, lookup) as h:
        for i in range(5):
            h.gate.put(ticker_msg(other, offset_ms=i))
        h.gate.put(
            lifecycle(other, "created", 10),
            lifecycle(other, "created", 11),
            lifecycle(other, "determined", 12, result="no", settlement_value="0.0000"),
            lifecycle(other, "settled", 13, settled_ts=1_791_000_100),
            lifecycle(other, "close_date_updated", 14, close_ts=1_791_000_200),
            lifecycle(other, "activated", 15),  # not counted
            trade_msg(COMBO, offset_ms=20, count_fp="2.50"),
            trade_msg(COMBO, offset_ms=21, count_fp="1.00"),
        )
        await until(lambda: h.ingestor.written["combo_trades"] == 2)
    stats = {
        family: row
        for family, *row in await rows(
            migrated_db_url,
            "select family, created, determined, settled, close_updated, ticker_msgs, trades, "
            "contracts_e2 from combo_stats_1m order by family",
        )
    }
    assert stats["KXMVECROSSCATEGORY0"] == [2, 1, 1, 1, 5, 0, 0]
    assert stats["KXMVECROSSCATEGORY"] == [0, 0, 0, 0, 0, 2, 350]  # 2.50 + 1.00 contracts
    assert await scalar(migrated_db_url, "select count(*) from combo_markets") == 1  # only COMBO


@respx.mock
async def test_counters_accumulate_across_flushes_within_a_minute(
    migrated_db_url: str, lookup: FakeLookup
) -> None:
    other = "KXMVECROSSCATEGORY-S2026XYZ-333"
    async with Harness(migrated_db_url, lookup, flush_interval=0.02) as h:
        for i in range(3):
            h.gate.put(ticker_msg(other, offset_ms=i))
            await until(flushed(h, i + 1))
            await asyncio.sleep(0.05)
    total = await scalar(migrated_db_url, "select sum(ticker_msgs) from combo_stats_1m")
    assert total == 3
    assert await scalar(migrated_db_url, "select count(*) from combo_stats_1m") == 1  # one minute


@respx.mock
async def test_lifecycle_events_update_stored_combos_and_ignore_unstored_ones(
    migrated_db_url: str, lookup: FakeLookup
) -> None:
    stranger = "KXMVECROSSCATEGORY-S2026ZZZ-999"
    settled_at = 1_791_000_100
    async with Harness(migrated_db_url, lookup) as h:
        h.gate.put(trade_msg(COMBO))
        await until(lambda: h.ingestor.written["combo_trades"] == 1)
        h.gate.put(
            lifecycle(COMBO, "close_date_updated", 100, close_ts=1_791_000_050),
            lifecycle(COMBO, "determined", 101, result="yes", settlement_value="1.0000"),
            lifecycle(COMBO, "settled", 102, settled_ts=settled_at),
            lifecycle(stranger, "determined", 103, result="no", settlement_value="0.0000"),
        )
        await until(lambda: h.ingestor.flushes >= 2 and h.ingestor.stats()["buffered"] == 0)
        await asyncio.sleep(0.2)
    [row] = await rows(
        migrated_db_url,
        "select status, result, settlement_value_e6, settled_at, close_time from combo_markets",
    )
    assert row == (
        "finalized",
        "yes",
        1_000_000,
        datetime.fromtimestamp(settled_at, UTC),
        datetime.fromtimestamp(1_791_000_050, UTC),
    )
    assert await scalar(migrated_db_url, "select count(*) from combo_markets") == 1


@respx.mock
async def test_a_combo_kalshi_does_not_know_is_dropped_and_counted(
    migrated_db_url: str,
) -> None:
    lookup = FakeLookup(known=[])
    async with Harness(migrated_db_url, lookup) as h:
        h.gate.put(trade_msg(COMBO), trade_msg(COMBO, offset_ms=5))
        await until(lambda: h.ingestor.combo["unresolved"] == 2)
    assert await scalar(migrated_db_url, "select count(*) from combo_trades") == 0
    assert h.ingestor.stats()["combo"]["held"] == 0


@respx.mock
async def test_lookup_errors_are_retried_and_the_trade_is_kept(
    migrated_db_url: str, lookup: FakeLookup
) -> None:
    lookup.failures = 2
    async with Harness(migrated_db_url, lookup) as h:
        h.gate.put(trade_msg(COMBO))
        await until(lambda: h.ingestor.written["combo_trades"] == 1)
    assert h.ingestor.combo["lookup_errors"] == 2
    assert len(lookup.requests) == 3


@respx.mock
async def test_repeated_lookup_failures_give_up_instead_of_holding_forever(
    migrated_db_url: str, lookup: FakeLookup
) -> None:
    lookup.failures = 100
    async with Harness(migrated_db_url, lookup) as h:
        h.gate.put(trade_msg(COMBO))
        await until(lambda: h.ingestor.combo["lookup_failed"] == 1)
    assert h.ingestor.combo["lookup_errors"] == 5
    assert await scalar(migrated_db_url, "select count(*) from combo_trades") == 0


@respx.mock
async def test_held_trades_are_capped(migrated_db_url: str) -> None:
    tickers = [f"KXMVECROSSCATEGORY-S2026ABC-{i}" for i in range(10)]
    lookup = FakeLookup(known=[combo_market(t) for t in tickers])
    lookup.gate = asyncio.Event()  # the lookup hangs, so trades pile up
    async with Harness(migrated_db_url, lookup, max_held=3) as h:
        h.gate.put(*[trade_msg(t, offset_ms=i) for i, t in enumerate(tickers)])
        await until(lambda: h.ingestor.combo["held_overflow"] == 7)
        assert h.ingestor.stats()["combo"]["held"] == 3
        lookup.gate.set()
        await until(lambda: h.ingestor.written["combo_trades"] == 3)
    assert await scalar(migrated_db_url, "select count(*) from combo_markets") == 3


@respx.mock
async def test_a_second_trade_on_a_known_combo_needs_no_lookup(
    migrated_db_url: str, lookup: FakeLookup
) -> None:
    async with Harness(migrated_db_url, lookup) as h:
        h.gate.put(trade_msg(COMBO))
        await until(lambda: h.ingestor.written["combo_trades"] == 1)
        h.gate.put(trade_msg(COMBO, offset_ms=500), trade_msg(COMBO, offset_ms=900))
        await until(lambda: h.ingestor.written["combo_trades"] == 3)
    assert len(lookup.requests) == 1
    assert await scalar(migrated_db_url, "select count(*) from combo_markets") == 1


@respx.mock
async def test_ordinary_markets_are_unaffected_by_combo_handling(
    migrated_db_url: str, lookup: FakeLookup
) -> None:
    async with Harness(migrated_db_url, lookup) as h:
        h.gate.put(ticker_msg("KXA-E1-X"), trade_msg("KXA-E1-X"), trade_msg(COMBO, offset_ms=5))
        await until(lambda: h.ingestor.written["combo_trades"] == 1)
        await until(
            lambda: h.ingestor.written["tickers"] == 1 and h.ingestor.written["trades"] == 1
        )
    assert await scalar(migrated_db_url, "select count(*) from combo_trades") == 1
    assert h.ingestor.skipped_mve == 0


@respx.mock
async def test_a_combo_that_is_not_queryable_yet_is_retried_before_giving_up(
    migrated_db_url: str, lookup: FakeLookup
) -> None:
    lookup.hide_first = 2  # a brand-new combo: the first two lookups find nothing
    async with Harness(migrated_db_url, lookup) as h:
        h.gate.put(trade_msg(COMBO))
        await until(lambda: h.ingestor.written["combo_trades"] == 1)
    assert len(lookup.requests) == 3
    assert h.ingestor.combo["unresolved"] == 0


@respx.mock
async def test_a_combo_that_never_appears_is_retried_a_bounded_number_of_times(
    migrated_db_url: str,
) -> None:
    lookup = FakeLookup(known=[])
    async with Harness(migrated_db_url, lookup) as h:
        h.gate.put(trade_msg(COMBO))
        await until(lambda: h.ingestor.combo["unresolved"] == 1)
    assert len(lookup.requests) == 3  # the first try plus two delayed retries
