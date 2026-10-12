import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from fastapi.testclient import TestClient
from kalshiterm_server import auth, db
from kalshiterm_server.api.app import create_app
from kalshiterm_server.config import ServerSettings
from sqlalchemy import text

pytestmark = pytest.mark.db

NOW = datetime(2026, 10, 12, 12, 0, tzinfo=UTC)


def ago(**kw: float) -> datetime:
    return NOW - timedelta(**kw)


async def seed(url: str) -> dict[str, str]:
    engine = db.make_engine(url)
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO events (event_ticker, series_ticker, title, mutually_exclusive) "
                    "VALUES ('KX-E1', 'KX', 'An event', true), ('KX-E2', 'KX', 'Another', true)"
                )
            )
            for ticker in ("KX-E1-A", "KX-E1-B"):
                await conn.execute(
                    text(
                        "INSERT INTO markets (ticker, event_ticker, market_type, status) "
                        "VALUES (:t, 'KX-E1', 'binary', 'active')"
                    ),
                    {"t": ticker},
                )
            market = (
                await conn.execute(text("select id from markets where ticker='KX-E1-A'"))
            ).scalar_one()
            rows: list[tuple[Any, ...]] = [
                # (ts, analyzer, metric, value_e6, market_id, event_ticker)
                (ago(minutes=5), "market_quotes", "spread", 30_000, market, None),
                (ago(minutes=1), "market_quotes", "spread", 20_000, market, None),  # newer wins
                (ago(minutes=1), "market_quotes", "bid_size", 12_340_000, market, None),
                (ago(hours=3), "market_moves", "last", 640_000, market, None),  # outside 1 h
                (ago(minutes=2), "my_plugin", "score", -7, market, None),  # unknown to the catalog
            ]
            for n in range(5):  # a history: one spread per minute, 0.01 .. 0.05
                rows.append((ago(minutes=10 + n), "market_volatility", "volatility_1h",
                             10_000 * (n + 1), market, None))  # fmt: skip
            for event, profit, edge in (("KX-E1", 20_000, 50_000), ("KX-E2", -260_000, 10_000)):
                metrics = {
                    "profit": profit,
                    "edge": edge,
                    "bid_sum": 1_000_000 + edge,
                    "size": 10_000_000,
                    "fees": 480_000,
                    "legs": 3_000_000,
                    "legs_total": 4_000_000,
                    "oldest_quote_age": 45_000_000,
                }
                for metric, value in metrics.items():
                    rows.append((ago(seconds=30), "arbitrage_bid", metric, value, None, event))
            rows.append((ago(minutes=20), "arbitrage_bid", "profit", 999_000, None, "KX-E2"))  # old
            for ts, analyzer, metric, value, market_id, event in rows:
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
                        "ev": event,
                        "v": value,
                    },
                )
        tokens = {}
        for name, role in (("alice", "read"), ("root", "admin")):
            await auth.add_user(engine, name)
            tokens[name] = (await auth.create_token(engine, name, role))[1]
        return tokens
    finally:
        await engine.dispose()


@pytest.fixture
def world(migrated_db_url: str) -> Any:
    tokens = asyncio.run(seed(migrated_db_url))
    settings = ServerSettings(db_url=migrated_db_url)
    with TestClient(create_app(settings, clock=lambda: NOW)) as client:
        yield client, tokens, migrated_db_url


def get(world: Any, path: str, who: str = "alice", **params: Any) -> Any:
    client, tokens, _ = world
    return client.get(path, headers={"Authorization": f"Bearer {tokens[who]}"}, params=params)


def test_everything_needs_a_token(world: Any) -> None:
    client = world[0]
    paths = [
        "/v1/analytics/markets/KX-E1-A",
        "/v1/analytics/markets/KX-E1-A/history?metric=a.b",
        "/v1/analytics/events/KX-E1",
        "/v1/analytics/arbitrage",
    ]
    for path in paths:
        assert client.get(path).status_code == 401, path


def test_a_market_shows_the_newest_value_of_each_metric_within_the_window(world: Any) -> None:
    body = get(world, "/v1/analytics/markets/KX-E1-A").json()
    assert body["ticker"] == "KX-E1-A" and body["as_of"] == "2026-10-12T12:00:00.000000+00:00"
    by_metric = {r["metric"]: r for r in body["results"]}
    assert by_metric["market_quotes.spread"]["value"] == "0.020000"  # not the older 0.03
    assert by_metric["market_quotes.spread"]["age_seconds"] == 60
    assert by_metric["market_quotes.spread"]["unit"] == "dollars"
    assert by_metric["market_quotes.bid_size"]["value"] == "12.340000"
    assert by_metric["market_quotes.bid_size"]["unit"] == "contracts"
    assert (
        by_metric["my_plugin.score"]["value"] == "-0.000007"
        and by_metric["my_plugin.score"]["unit"] is None
    )  # noqa: E501
    assert "market_moves.last" not in by_metric  # 3 hours old, outside the default hour
    wide = get(world, "/v1/analytics/markets/KX-E1-A", max_age_seconds=86_400).json()
    assert any(r["metric"] == "market_moves.last" for r in wide["results"])


def test_an_unwatched_market_is_empty_and_an_unknown_one_is_not_found(world: Any) -> None:
    assert get(world, "/v1/analytics/markets/KX-E1-B").json()["results"] == []
    assert get(world, "/v1/analytics/markets/KX-NOPE").status_code == 404
    for age in (0, 86_401, "x"):
        assert get(world, "/v1/analytics/markets/KX-E1-A", max_age_seconds=age).status_code == 422


def test_history_is_newest_first_exact_and_pages_without_gaps_or_repeats(world: Any) -> None:
    url = "/v1/analytics/markets/KX-E1-A/history"
    everything = get(world, url, metric="market_volatility.volatility_1h").json()
    assert [p["value"] for p in everything["points"]] == [
        "0.010000", "0.020000", "0.030000", "0.040000", "0.050000",
    ]  # fmt: skip
    assert everything["unit"] == "dollars" and everything["next_cursor"] is None
    seen: list[str] = []
    cursor = None
    for _ in range(5):
        page = get(world, url, metric="market_volatility.volatility_1h", limit=2,
                   **({"cursor": cursor} if cursor else {})).json()  # fmt: skip
        seen += [p["time"] for p in page["points"]]
        cursor = page["next_cursor"]
        if cursor is None:
            break
    assert seen == [p["time"] for p in everything["points"]] and len(set(seen)) == 5


def test_history_can_be_bounded_in_time_and_rejects_bad_input(world: Any) -> None:
    url = "/v1/analytics/markets/KX-E1-A/history"
    metric = "market_volatility.volatility_1h"
    window = get(world, url, metric=metric, since=ago(minutes=12).isoformat(),
                 until=ago(minutes=10).isoformat()).json()  # fmt: skip
    assert [p["value"] for p in window["points"]] == [
        "0.020000",
        "0.030000",
    ]  # [since, until): 11 and 12 min ago
    bad: list[dict[str, Any]] = [
        {"metric": "nonsense"},
        {"metric": metric, "since": "2026-10-12T10:00:00"},  # no timezone
        {
            "metric": metric,
            "since": ago(minutes=1).isoformat(),
            "until": ago(minutes=2).isoformat(),
        },
        {"metric": metric, "limit": 0},
        {"metric": metric, "limit": 1001},
        {"metric": metric, "cursor": "garbage"},
    ]
    for params in bad:
        assert get(world, url, **params).status_code == 422, params
    assert get(world, "/v1/analytics/markets/KX-NOPE/history", metric=metric).status_code == 404
    empty = get(world, url, metric="market_quotes.nothing").json()
    assert empty["points"] == [] and empty["unit"] is None


def test_an_event_shows_its_arbitrage_metrics(world: Any) -> None:
    body = get(world, "/v1/analytics/events/KX-E1").json()
    by_metric = {r["metric"]: r["value"] for r in body["results"]}
    assert (
        by_metric["arbitrage_bid.profit"] == "0.020000"
        and by_metric["arbitrage_bid.edge"] == "0.050000"
    )  # noqa: E501
    assert get(world, "/v1/analytics/events/KX-NOPE").status_code == 404


def test_arbitrage_lists_current_opportunities_best_first(world: Any) -> None:
    body = get(world, "/v1/analytics/arbitrage").json()
    assert [o["event_ticker"] for o in body["opportunities"]] == ["KX-E1", "KX-E2"]
    best, worst = body["opportunities"]
    assert (
        best["profit"] == "0.020000" and best["edge"] == "0.050000" and best["size"] == "10.000000"
    )
    assert best["legs"] == 3 and best["legs_total"] == 4 and best["oldest_quote_age_seconds"] == 45
    assert worst["profit"] == "-0.260000"  # the 20-minute-old +0.999 is outside the window
    assert body["total"] == 2
    only_profitable = get(world, "/v1/analytics/arbitrage", min_profit="0").json()
    assert [o["event_ticker"] for o in only_profitable["opportunities"]] == ["KX-E1"]
    assert only_profitable["total"] == 1
    one = get(world, "/v1/analytics/arbitrage", limit=1).json()
    assert len(one["opportunities"]) == 1 and one["total"] == 2
    assert get(world, "/v1/analytics/arbitrage", max_age_seconds=10).json()["opportunities"] == []
    for bad in ("abc", "0.1234567", "1e99"):
        assert get(world, "/v1/analytics/arbitrage", min_profit=bad).status_code == 422, bad


def test_the_status_report_shows_the_analyzers_and_what_is_wrong_with_them(world: Any) -> None:
    async def runs() -> None:
        engine = db.make_engine(world[2])
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text(
                        "insert into analyzer_runs (analyzer, started_at, finished_at, status, "
                        "results, interval_seconds, error) values "
                        "('market_quotes', now(), now(), 'ok', 7, 60, ''), "
                        "('market_moves', now(), now(), 'failed', 0, 60, 'ValueError: boom')"
                    )
                )
        finally:
            await engine.dispose()

    asyncio.run(runs())
    assert get(world, "/v1/status", who="alice").status_code == 403  # admin only
    report = get(world, "/v1/status", who="root").json()
    by_name = {a["analyzer"]: a for a in report["analyzers"]}
    assert by_name["market_quotes"]["status"] == "ok" and by_name["market_quotes"]["results"] == 7
    assert by_name["market_moves"]["status"] == "failed"
    assert any("analyzer market_moves failed: ValueError: boom" in p for p in report["problems"])
