"""The client library against the real server: the real app, a real database, real TLS with the
certificate pinned. If the server grows or renames a field these fail, which is the point
(PLAN §10.6: the client does not import the server, so this is what keeps them in step)."""

import asyncio
import threading
import time
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, get_args

import pytest
import uvicorn
from kalshiterm_client import models
from kalshiterm_client.client import ServerClient
from kalshiterm_client.errors import Forbidden, NotFound
from kalshiterm_client.pinning import fingerprint_of_pem
from kalshiterm_client.profiles import ProfileStore
from kalshiterm_server import auth, db, tls
from kalshiterm_server.api.app import create_app
from kalshiterm_server.config import ServerSettings
from pydantic import BaseModel
from sqlalchemy import text
from test_candles import add_ticks, add_trades, refresh
from timescale_jobs import quiet_background_jobs

pytestmark = pytest.mark.db

HOUR = (datetime.now(UTC) - timedelta(hours=3)).replace(minute=0, second=0, microsecond=0)
A, B, C, SLIM, GONE = "KX-E1-A", "KX-E1-B", "KX-E1-C", "KX-OLD-SLIM", "KX-OLD-TOMB"


async def seed(url: str) -> dict[str, str]:
    engine = db.make_engine(url)
    try:
        await quiet_background_jobs(engine)
        async with engine.begin() as conn:
            for event, title in (("KX-E1", "The first event"), ("KX-OLD", "An old event")):
                await conn.execute(
                    text(
                        "INSERT INTO events (event_ticker, series_ticker, title, sub_title, "
                        "mutually_exclusive, strike_period) "
                        "VALUES (:e, 'KX', :t, 'sub', true, 'day')"
                    ),
                    {"e": event, "t": title},
                )
            for ticker in (A, B, C):
                await conn.execute(
                    text(
                        "INSERT INTO markets (ticker, event_ticker, market_type, status, "
                        "yes_sub_title, no_sub_title, created_time, open_time, rules_primary, "
                        "rules_secondary, strike_type, floor_strike_e6) VALUES (:t, 'KX-E1', "
                        "'binary', 'active', 'Yes', 'No', :c, :c, 'Rules A', 'Rules B', 'greater', "
                        "118749900000)"
                    ),
                    {"t": ticker, "c": HOUR - timedelta(days=2)},
                )
            await conn.execute(
                text(
                    "INSERT INTO markets (ticker, event_ticker, market_type, status, created_time, "
                    "result, settlement_value_e6, settlement_ts, strike_type, floor_strike_e6) "
                    "VALUES (:t, 'KX-OLD', 'binary', 'finalized', :c, 'no', 0, :c, 'greater', 5)"
                ),
                {"t": SLIM, "c": HOUR - timedelta(days=60)},
            )
            await conn.execute(
                text(
                    "INSERT INTO markets (ticker, event_ticker, market_type, status, result, "
                    "settlement_value_e6, settlement_ts) VALUES (:t, 'KX-OLD', 'binary', "
                    "'finalized', 'yes', 1000000, :c)"
                ),
                {"t": GONE, "c": HOUR - timedelta(days=100)},
            )
        await add_trades(
            engine, A, HOUR, [(s, 400_000 + 1000 * s, 100 + s) for s in range(0, 50, 2)]
        )
        await add_trades(
            engine, A, HOUR + timedelta(minutes=1), [(s, 500_000 + s, 200) for s in range(0, 10)]
        )
        await add_ticks(
            engine, A, HOUR,
            [(s, 500_000 + s, 490_000, 510_000, 10_000 + s, 5_000) for s in range(1, 31)],
        )  # fmt: skip
        for view in ("candles_1m", "candles_1h", "ticker_1m", "ticker_1h"):
            await refresh(engine, view, HOUR - timedelta(hours=2), HOUR + timedelta(hours=4))
        async with engine.begin() as conn:
            market = (
                await conn.execute(text("select id from markets where ticker = :t"), {"t": A})
            ).scalar_one()
            await conn.execute(
                text(
                    "INSERT INTO orderbook_snapshots (ts, received_at, market_id, seq, sid, "
                    "approximate, yes_prices_e6, yes_sizes_e2, no_prices_e6, no_sizes_e2) VALUES "
                    "(:t, :t, :m, 10, 1, false, '{400000,390000}', '{1000,500}', "
                    "'{580000}', '{700}')"
                ),
                {"t": HOUR, "m": market},
            )
            await conn.execute(
                text(
                    "INSERT INTO orderbook_deltas (ts, received_at, market_id, seq, sid, is_yes, "
                    "price_e6, delta_e2) VALUES (:t, :t, :m, 11, 1, true, 400000, 200)"
                ),
                {"t": HOUR + timedelta(seconds=1), "m": market},
            )
            await conn.execute(
                text(
                    "INSERT INTO ingest_gaps (started_at, ended_at, reason, status, trades_added, "
                    "dropped, note) VALUES (:s, :e, 'connection_lost', 'done', 3, 4, 'n')"
                ),
                {
                    "s": datetime.now(UTC) - timedelta(minutes=5),
                    "e": datetime.now(UTC) - timedelta(minutes=4),
                },
            )
        tokens = {}
        for name, role in (("alice", "read"), ("root", "admin")):
            await auth.add_user(engine, name)
            tokens[name] = (await auth.create_token(engine, name, role))[1]
        return tokens
    finally:
        await engine.dispose()


def free_port() -> int:
    import socket

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


class World:
    def __init__(self, url: str, tokens: dict[str, str], profile_pin_dir: Path) -> None:
        self.url, self.tokens, self.dir = url, tokens, profile_pin_dir

    def client(self, who: str = "alice") -> ServerClient:
        profile = ProfileStore().get("srv")
        return ServerClient.for_profile(profile, token=self.tokens[who])


@pytest.fixture
def world(migrated_db_url: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[World]:
    """The real API over TLS on a loopback port, and a profile that pins its certificate."""
    tokens = asyncio.run(seed(migrated_db_url))
    certs = tmp_path / "certs"
    certs.mkdir()
    tls.write_certificate(certs, ["localhost"], ["127.0.0.1"])
    port = free_port()
    app = create_app(ServerSettings(db_url=migrated_db_url))
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            host="127.0.0.1",
            port=port,
            log_level="warning",
            ssl_certfile=str(certs / tls.CERT_FILE),
            ssl_keyfile=str(certs / tls.KEY_FILE),
        )  # fmt: skip
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 20
    while not server.started and time.monotonic() < deadline:
        time.sleep(0.05)
    assert server.started, "the API did not start"
    monkeypatch.setenv("KTERM_CONFIG_DIR", str(tmp_path / "config"))
    url = f"https://127.0.0.1:{port}"
    store = ProfileStore()
    store.add("srv", url)
    pem = (certs / tls.CERT_FILE).read_bytes()
    store.save_pin("srv", pem, fingerprint_of_pem(pem))
    try:
        yield World(url, tokens, certs)
    finally:
        server.should_exit = True
        thread.join(15)


@pytest.fixture
async def api(world: World) -> AsyncIterator[ServerClient]:
    async with world.client() as client:
        yield client


# ------------------------------------------------------------------ the coverage check


def nested_model(annotation: Any) -> type[BaseModel] | None:
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return annotation
    for arg in get_args(annotation):
        found = nested_model(arg)
        if found:
            return found
    return None


def uncovered(model: type[BaseModel], payload: Any, path: str = "") -> list[str]:
    """Every key the server sent that ``model`` has no field for (recursively)."""
    if isinstance(payload, list):
        return [p for item in payload for p in uncovered(model, item, path)]
    if not isinstance(payload, dict):
        return []
    fields: dict[str, Any] = {}
    for name, info in model.model_fields.items():
        fields[name] = info
        if info.alias:
            fields[info.alias] = info
    missing = []
    for key, value in payload.items():
        if key not in fields:
            missing.append(f"{path}{key}")
        elif value is not None and (inner := nested_model(fields[key].annotation)):
            missing += uncovered(inner, value, f"{path}{key}.")
    return missing


async def test_the_models_cover_every_field_the_server_sends(api: ServerClient) -> None:
    start = HOUR - timedelta(minutes=5)
    cases: list[tuple[str, dict[str, Any] | None, type[BaseModel]]] = [
        ("/v1/me", None, models.Me),
        ("/v1/markets", {"limit": 10}, models.MarketPage),
        (f"/v1/markets/{A}", None, models.MarketDetail),
        (f"/v1/markets/{SLIM}", None, models.MarketDetail),
        (f"/v1/markets/{GONE}", None, models.MarketDetail),
        ("/v1/events/KX-E1", None, models.Event),
        (f"/v1/markets/{A}/candles", {"source": "trades", "start": start}, models.CandlePage),
        (f"/v1/markets/{A}/candles", {"source": "ticker", "start": start}, models.CandlePage),
        (f"/v1/markets/{A}/trades", {"limit": 5}, models.TradePage),
        (f"/v1/markets/{A}/ticks", {"limit": 5}, models.TickPage),
        (f"/v1/markets/{A}/orderbook", {"at": HOUR + timedelta(seconds=30)}, models.OrderBook),
    ]
    for path, params, model in cases:
        payload = await api._get(path, params)
        assert uncovered(model, payload) == [], path
        model.model_validate(payload)  # and the values really parse
    gaps = await api._get("/v1/gaps")
    assert uncovered(models.Gap, gaps["gaps"]) == [] and gaps["gaps"]


def test_the_coverage_check_itself_catches_a_new_field() -> None:
    payload = {"user": "a", "role": "read", "token_id": 1, "surprise": 1}
    assert uncovered(models.Me, payload) == ["surprise"]
    book = {
        "yes": [{"price": "1", "size": "1", "extra": 2}],
        "gaps": [{"from": "x", "to": "y", "z": 1}],
    }
    assert sorted(uncovered(models.OrderBook, book)) == ["gaps.z", "yes.extra"]


# ------------------------------------------------------------------ behaviour over TLS


async def test_who_am_i_over_the_pinned_connection(api: ServerClient) -> None:
    me = await api.me()
    assert (me.user, me.role) == ("alice", "read")


async def test_markets_are_paged_filtered_and_typed(api: ServerClient) -> None:
    everything = [m async for m in api.iter_markets(limit=2)]
    assert [m.ticker for m in everything] == sorted([A, B, C, SLIM, GONE])
    active = [m.ticker async for m in api.iter_markets(status="active")]
    assert active == [A, B, C]
    assert [m.ticker async for m in api.iter_markets(q="old")] == [SLIM, GONE]
    assert [m.ticker async for m in api.iter_markets(event="KX-E1", limit=1)] == [A, B, C]
    stages = {m.ticker: m.stage for m in everything}
    assert stages == {A: "full", B: "full", C: "full", SLIM: "slim", GONE: "tombstone"}
    detail = await api.market(A)
    assert detail.rules_primary == "Rules A" and str(detail.floor_strike) == "118749.900000"
    with pytest.raises(NotFound) as caught:
        await api.market("KX-NOPE")
    assert caught.value.code == "not_found" and caught.value.status == 404


async def test_an_event_with_its_markets(api: ServerClient) -> None:
    event = await api.event("KX-E1")
    assert event.title == "The first event" and event.mutually_exclusive is True
    assert [m.ticker for m in event.markets] == [A, B, C]
    with pytest.raises(NotFound):
        await api.event("KX-NOPE")


async def test_candles_come_back_as_exact_bars_from_both_sources(api: ServerClient) -> None:
    trade_bars = [b async for b in api.iter_candles(A, start=HOUR - timedelta(minutes=1), limit=1)]
    assert [b.time for b in trade_bars] == [
        HOUR,
        HOUR + timedelta(minutes=1),
    ]  # paged one at a time
    first = trade_bars[0]
    assert first.trades == 25
    assert str(first.open) == "0.400000" and first.volume is not None
    ticker_bars = [
        b async for b in api.iter_candles(A, source="ticker", start=HOUR - timedelta(minutes=1))
    ]
    assert ticker_bars and ticker_bars[0].ticks == 30 and str(ticker_bars[0].yes_bid) == "0.490000"
    latest = await api.candles_page(A, end=HOUR + timedelta(hours=4), limit=1)
    assert len(latest.bars) == 1 and latest.next_cursor is None  # the "latest N" form is one page


async def test_trades_and_ticks_page_without_repeats_or_gaps(api: ServerClient) -> None:
    trades = [t async for t in api.iter_trades(A, start=HOUR - timedelta(minutes=1), limit=7)]
    assert len(trades) == 35 and len({t.trade_id for t in trades}) == 35
    assert [t.time for t in trades] == sorted(t.time for t in trades)
    assert all(t.yes_price + t.no_price == 1 for t in trades)
    newest = await api.trades_page(A, limit=5)
    assert [t.trade_id for t in newest.trades] == [t.trade_id for t in trades[-5:]]
    ticks = [t async for t in api.iter_ticks(A, start=HOUR - timedelta(minutes=1), limit=8)]
    assert len(ticks) == 30 and [str(t.volume) for t in ticks][:2] == ["100.01", "100.02"]


async def test_the_orderbook_and_its_failures(api: ServerClient) -> None:
    book = await api.orderbook(A, at=HOUR + timedelta(seconds=30), depth=10)
    assert [str(level.price) for level in book.yes] == ["0.400000", "0.390000"]
    assert [str(level.size) for level in book.yes] == ["12.00", "5.00"]  # the +2.00 delta applied
    assert book.complete is True and str(book.best_yes_ask) == "0.420000"
    with pytest.raises(NotFound) as before:
        await api.orderbook(A, at=HOUR - timedelta(hours=1))
    assert before.value.code == "no_orderbook"
    with pytest.raises(NotFound) as other:
        await api.orderbook(B)
    assert other.value.code == "no_orderbook"


async def test_the_gap_log(api: ServerClient) -> None:
    [gap] = await api.gaps()
    assert (
        gap.reason == "connection_lost" and gap.trades_recovered == 3 and gap.messages_dropped == 4
    )
    assert gap.end > gap.start


async def test_status_is_for_admins(world: World) -> None:
    async with world.client("alice") as reader:
        with pytest.raises(Forbidden, match="needs an admin token"):
            await reader.status()
    async with world.client("root") as admin:
        report = await admin.status()
    assert report.data["database"]["revision"] == report.data["database"]["head"]
    assert isinstance(report.problems, list)
