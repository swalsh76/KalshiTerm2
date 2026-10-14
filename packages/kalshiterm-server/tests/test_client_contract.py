"""The client library against the real server: the real app, a real database, real TLS with the
certificate pinned. If the server grows or renames a field these fail, which is the point
(PLAN §10.6: the client does not import the server, so this is what keeps them in step)."""

import asyncio
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, get_args

import pytest
from kalshiterm_client import models
from kalshiterm_client.client import ServerClient
from kalshiterm_client.errors import Forbidden, InvalidRequest, NotFound
from kalshiterm_client.pinning import fingerprint_of_pem
from kalshiterm_client.profiles import ProfileStore
from kalshiterm_server import auth, db
from kalshiterm_server.api.app import create_app
from kalshiterm_server.config import ServerSettings
from live_server import LiveServer
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
        now = datetime.now(UTC)
        async with engine.begin() as conn:
            market = (
                await conn.execute(text("select id from markets where ticker = :t"), {"t": A})
            ).scalar_one()
            results: list[tuple[datetime, str, str, int, int | None, str | None]] = [
                (now - timedelta(seconds=30), "market_quotes", "spread", 20_000, market, None),
                (now - timedelta(seconds=90), "market_quotes", "spread", 30_000, market, None),
                (
                    now - timedelta(seconds=30),
                    "market_quotes",
                    "bid_size",
                    12_340_000,
                    market,
                    None,
                ),
            ]
            results += [
                (
                    now - timedelta(minutes=10 + n),
                    "market_volatility",
                    "volatility_1h",
                    10_000 * (n + 1),
                    market,
                    None,
                )
                for n in range(5)
            ]
            results += [
                (now - timedelta(seconds=20), "arbitrage_bid", metric, value, None, "KX-E1")
                for metric, value in (
                    ("bid_sum", 1_050_000),
                    ("edge", 50_000),
                    ("size", 10_000_000),
                    ("fees", 480_000),
                    ("profit", 20_000),
                    ("legs", 3_000_000),
                    ("legs_total", 4_000_000),
                    ("oldest_quote_age", 45_000_000),
                )
            ]
            for ts, analyzer, metric, value, market_id, event_key in results:
                await conn.execute(
                    text(
                        "INSERT INTO analysis_results (ts, analyzer, metric, market_id, "
                        "event_ticker, value_e6) VALUES (:ts, :a, :m, :mk, :ev, :v)"
                    ),
                    {
                        "ts": ts,
                        "a": analyzer,
                        "m": metric,
                        "mk": market_id,
                        "ev": event_key,
                        "v": value,
                    },
                )
            users = {n: i for n, i in await conn.execute(text("select name, id from users"))}
            for owner, n in (("alice", 5), ("root", 2)):
                for i in range(n):
                    await conn.execute(
                        text(
                            "INSERT INTO alerts (user_id, fired_at, rule_name, analyzer, metric, "
                            "event_ticker, op, threshold_e6, value_e6) VALUES (:u, :t, :n, "
                            "'arbitrage_bid', 'profit', 'KX-E1', 'gt', 0, 20000)"
                        ),
                        {
                            "u": users[owner],
                            "t": now - timedelta(minutes=n - i),
                            "n": f"{owner}-{i}",
                        },
                    )
        return tokens
    finally:
        await engine.dispose()


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
    app = create_app(ServerSettings(db_url=migrated_db_url))
    with LiveServer(app, tmp_path / "certs") as server:
        monkeypatch.setenv("KTERM_CONFIG_DIR", str(tmp_path / "config"))
        store = ProfileStore()
        store.add("srv", server.url)
        store.save_pin("srv", server.pem, fingerprint_of_pem(server.pem))
        yield World(server.url, tokens, tmp_path / "certs")


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


# ------------------------------------------------------------------ watchlist, analytics, alerts


async def test_the_models_cover_the_account_and_analytics_endpoints(api: ServerClient) -> None:
    await api.watch(A)
    rule = await api.create_alert_rule("market_quotes.spread", "gt", "0.05", ticker=A, name="wide")
    event_rule = await api.create_alert_rule(
        "arbitrage_bid.profit", "gt", "0", event_ticker="KX-E1", cooldown_seconds=600
    )
    cases: list[tuple[str, dict[str, Any] | None, type[BaseModel]]] = [
        ("/v1/watchlist", None, models.Watchlist),
        (f"/v1/analytics/markets/{A}", None, models.MarketResults),
        (
            f"/v1/analytics/markets/{A}/history",
            {"metric": "market_volatility.volatility_1h"},
            models.MetricHistory,
        ),
        ("/v1/analytics/events/KX-E1", None, models.EventResults),
        ("/v1/analytics/arbitrage", None, models.Opportunities),
        ("/v1/alert-metrics", None, models.AlertMetrics),
        ("/v1/alert-rules", None, models.AlertRules),
        (f"/v1/alert-rules/{rule.id}", None, models.AlertRule),
        (f"/v1/alert-rules/{event_rule.id}", None, models.AlertRule),
        ("/v1/alerts", None, models.AlertPage),
    ]
    for path, params, model in cases:
        payload = await api._get(path, params)
        assert uncovered(model, payload) == [], path
        model.model_validate(payload)


async def test_the_models_cover_the_layout_endpoints(api: ServerClient) -> None:
    await api.save_layout("Main", {"version": 1, "panels": []})
    cases: list[tuple[str, dict[str, Any] | None, type[BaseModel]]] = [
        ("/v1/layouts", None, models.LayoutList),
        ("/v1/layouts/Main", None, models.Layout),
    ]
    for path, params, model in cases:
        payload = await api._get(path, params)
        assert uncovered(model, payload) == [], path
        model.model_validate(payload)
    saved = await api._request("PUT", "/v1/layouts/Main", json={"data": {}})
    assert uncovered(models.LayoutSaved, saved) == []
    models.LayoutSaved.model_validate(saved)


async def test_layouts_through_their_whole_life(world: World) -> None:
    async with world.client("alice") as alice, world.client("root") as root:
        assert (await alice.layouts()).layouts == []
        data = {"version": 2, "panels": [{"id": "book", "ticker": A}], "price": "0.560000"}
        first = await alice.save_layout("My Layout", data)
        assert first.created is True and first.name == "My Layout"
        again = await alice.save_layout("My Layout", {**data, "version": 3})
        assert again.created is False
        got = await alice.layout("My Layout")
        assert got.data["version"] == 3 and got.data["price"] == "0.560000"
        listing = await alice.layouts()
        assert [(x.name, x.size_bytes) for x in listing.layouts] == [("My Layout", got.size_bytes)]
        assert listing.limit == 50 and listing.max_bytes == 262_144
        with pytest.raises(NotFound):  # another user's layout looks like no layout at all
            await root.layout("My Layout")
        assert (await root.layouts()).layouts == []
        with pytest.raises(InvalidRequest) as big:
            await alice.save_layout("big", {"p": "x" * 300_000})
        assert big.value.code == "layout_too_large" and big.value.extra["limit_bytes"] == 262_144
        with pytest.raises(InvalidRequest) as bad:
            await alice.save_layout("nul", {"p": "a\u0000b"})
        assert bad.value.code == "invalid_layout"
        await alice.delete_layout("My Layout")
        with pytest.raises(NotFound):
            await alice.delete_layout("My Layout")
        assert (await alice.layouts()).layouts == []


async def test_the_watchlist_round_trip(api: ServerClient) -> None:
    assert (await api.watchlist()).markets == []
    assert await api.watch(A) is True and await api.watch(A) is False  # idempotent
    watchlist = await api.watchlist()
    assert [m.ticker for m in watchlist.markets] == [A] and watchlist.limit == 50
    assert (
        watchlist.markets[0].capturing is False
        and watchlist.markets[0].event_title == "The first event"
    )
    assert await api.unwatch(A) is True and await api.unwatch(A) is False
    with pytest.raises(NotFound):
        await api.watch("KX-NOPE")


async def test_analytics_values_are_typed_and_exact(api: ServerClient) -> None:
    results = await api.market_results(A)
    by_metric = {r.metric: r for r in results.results}
    assert by_metric["market_quotes.spread"].value == Decimal("0.020000")  # the newer of two
    assert by_metric["market_quotes.bid_size"].value == Decimal("12.340000")
    assert by_metric["market_quotes.bid_size"].unit == "contracts"
    assert 20 <= by_metric["market_quotes.spread"].age_seconds <= 90
    history = [
        p async for p in api.iter_market_history(A, "market_volatility.volatility_1h", limit=2)
    ]
    assert [str(p.value) for p in history] == [
        "0.010000",
        "0.020000",
        "0.030000",
        "0.040000",
        "0.050000",
    ]
    assert [p.time for p in history] == sorted((p.time for p in history), reverse=True)
    event = await api.event_results("KX-E1")
    assert {r.metric: r.value for r in event.results}["arbitrage_bid.profit"] == Decimal("0.020000")
    with pytest.raises(NotFound):
        await api.market_results("KX-NOPE")
    with pytest.raises(InvalidRequest) as bad:
        await api.market_results(A, max_age_seconds=0)
    assert bad.value.code == "invalid_parameter" and bad.value.extra["fields"] == [
        "max_age_seconds"
    ]


async def test_arbitrage_opportunities_and_the_profit_filter(api: ServerClient) -> None:
    found = await api.arbitrage()
    [opportunity] = found.opportunities
    assert opportunity.event_ticker == "KX-E1" and opportunity.profit == Decimal("0.020000")
    assert opportunity.legs == 3 and opportunity.legs_total == 4 and found.total == 1
    assert (await api.arbitrage(min_profit=Decimal("0.02"))).total == 1
    assert (await api.arbitrage(min_profit="0.03")).opportunities == []


async def test_alert_rules_through_their_whole_life(api: ServerClient) -> None:
    catalog = await api.alert_metrics()
    spread = next(m for m in catalog.metrics if m.metric == "market_quotes.spread")
    assert spread.subject == "market" and spread.unit == "dollars" and spread.ops == ["gt", "lt"]

    with pytest.raises(InvalidRequest) as unwatched:  # a market rule needs the market watched
        await api.create_alert_rule("market_quotes.spread", "gt", "0.05", ticker=A)
    assert unwatched.value.code == "not_watched" and unwatched.value.extra == {"ticker": A}
    await api.watch(A)
    with pytest.raises(InvalidRequest) as short:
        await api.create_alert_rule(
            "market_quotes.spread", "gt", "0.05", ticker=A, cooldown_seconds=60
        )
    assert short.value.code == "cooldown_too_short" and short.value.extra == {"minimum": 300}
    with pytest.raises(InvalidRequest) as wrong_op:
        await api.create_alert_rule("market_quotes.spread", "abs_gt", "0.05", ticker=A)
    assert wrong_op.value.code == "invalid_comparison"

    rule = await api.create_alert_rule(
        "market_quotes.spread", "gt", Decimal("0.05"), ticker=A, name="wide"
    )
    assert rule.threshold == Decimal("0.050000") and rule.cooldown_seconds == 900
    assert (rule.enabled, rule.active, rule.ticker, rule.event_ticker) == (True, False, A, None)
    assert [r.id for r in (await api.alert_rules()).rules] == [rule.id]

    changed = await api.update_alert_rule(rule.id, threshold=Decimal("0.2"), enabled=False)
    assert changed.threshold == Decimal("0.200000") and changed.enabled is False
    assert changed.name == "wide" and changed.cooldown_seconds == 900  # untouched
    assert (await api.alert_rule(rule.id)).enabled is False

    await api.delete_alert_rule(rule.id)
    with pytest.raises(NotFound):
        await api.alert_rule(rule.id)
    with pytest.raises(NotFound):
        await api.delete_alert_rule(rule.id)


async def test_alerts_page_acknowledge_and_belong_to_their_owner(world: World) -> None:
    async with world.client("alice") as alice, world.client("root") as root:
        mine = [a async for a in alice.iter_alerts(limit=2)]
        assert [a.rule_name for a in mine] == [
            "alice-4",
            "alice-3",
            "alice-2",
            "alice-1",
            "alice-0",
        ]
        assert mine[0].value == Decimal("0.020000") and mine[0].event_ticker == "KX-E1"
        assert mine[0].rule_id is None and mine[0].acknowledged_at is None
        assert len([a async for a in root.iter_alerts()]) == 2  # not alice's
        await alice.acknowledge_alert(mine[0].id)
        await alice.acknowledge_alert(mine[0].id)  # idempotent
        open_ones = [a.id async for a in alice.iter_alerts(unacknowledged=True)]
        assert open_ones == [a.id for a in mine[1:]]
        with pytest.raises(NotFound):  # another user's alert looks like no alert at all
            await root.acknowledge_alert(mine[1].id)
        acked = (await alice.alerts_page(limit=1)).alerts[0]
        assert acked.acknowledged_at is not None
