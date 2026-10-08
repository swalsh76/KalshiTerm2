import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from fastapi.testclient import TestClient
from kalshiterm_server import auth, db
from kalshiterm_server.api.app import create_app
from kalshiterm_server.config import ServerSettings
from sqlalchemy import text
from test_candles import add_ticks, add_trades, refresh
from timescale_jobs import quiet_background_jobs

pytestmark = pytest.mark.db

HOUR = (datetime.now(UTC) - timedelta(hours=3)).replace(minute=0, second=0, microsecond=0)
BTC = "KXBTC-26OCT08-T80000"
FED = "KXFED-26OCT-HOLD"
OLD = "KXOLD-25-SLIM"
GONE = "KXOLD-25-TOMB"


async def seed(url: str) -> str:
    """Events, ~30 markets (some reduced by retention), trades, ticks, refreshed aggregates."""
    engine = db.make_engine(url)
    try:
        await quiet_background_jobs(engine)
        now = datetime.now(UTC)
        for view in ("candles_1m", "candles_1h", "ticker_1m", "ticker_1h"):
            await refresh(engine, view, now - timedelta(hours=9), now - timedelta(hours=6))
        async with engine.begin() as conn:
            for event, series, title in (
                ("KXBTC-26OCT08", "KXBTC", "Bitcoin price on Oct 8"),
                ("KXFED-26OCT", "KXFED", "Fed rate decision (100% sure)"),
                ("KXPAD-26", "KXPAD", "Padding event"),
                ("KXEMPTY-26", "KXEMPTY", "An event with no markets yet"),
            ):
                await conn.execute(
                    text(
                        "INSERT INTO events (event_ticker, series_ticker, title, "
                        "mutually_exclusive) VALUES (:e, :s, :t, true)"
                    ),
                    {"e": event, "s": series, "t": title},
                )
            full = (
                "INSERT INTO markets (ticker, event_ticker, market_type, status, yes_sub_title, "
                "no_sub_title, created_time, open_time, close_time, latest_expiration_time, "
                "result, settlement_value_e6, settlement_ts, rules_primary, rules_secondary, "
                "strike_type, floor_strike_e6, cap_strike_e6) VALUES (:t, :e, 'binary', :s, "
                "'Above 80000', 'Below 80000', :c, :c, :c, :c, :r, :v, :st, 'Rules A', 'Rules B', "
                "'greater', 80000000000, NULL)"
            )
            created = now - timedelta(days=5)
            await conn.execute(
                text(full),
                {
                    "t": BTC,
                    "e": "KXBTC-26OCT08",
                    "s": "active",
                    "c": created,
                    "r": "",
                    "v": None,
                    "st": None,
                },
            )
            await conn.execute(
                text(full),
                {
                    "t": "KXBTC-26OCT08-T81000",
                    "e": "KXBTC-26OCT08",
                    "s": "active",
                    "c": created,
                    "r": "",
                    "v": None,
                    "st": None,
                },
            )
            await conn.execute(
                text(full),
                {
                    "t": FED,
                    "e": "KXFED-26OCT",
                    "s": "finalized",
                    "c": created,
                    "r": "yes",
                    "v": 1_000_000,
                    "st": now - timedelta(days=2),
                },
            )
            for i in range(24):  # enough to need several pages
                await conn.execute(
                    text(full),
                    {
                        "t": f"KXPAD-26-N{i:02d}",
                        "e": "KXPAD-26",
                        "s": "closed",
                        "c": created,
                        "r": "",
                        "v": None,
                        "st": None,
                    },
                )
            await conn.execute(  # a slimmed market: text gone, the rest kept
                text(
                    "INSERT INTO markets (ticker, event_ticker, market_type, status, created_time,"
                    " result, settlement_value_e6, settlement_ts, strike_type, floor_strike_e6) "
                    "VALUES (:t, 'KXPAD-26', 'binary', 'finalized', :c, 'no', 0, :c, 'greater', 5)"
                ),
                {"t": OLD, "c": created},
            )
            await conn.execute(  # a tombstone: only identity and outcome
                text(
                    "INSERT INTO markets (ticker, event_ticker, market_type, status, result, "
                    "settlement_value_e6, settlement_ts) VALUES (:t, 'KXPAD-26', 'binary', "
                    "'finalized', 'yes', 1000000, :c)"
                ),
                {"t": GONE, "c": created},
            )
        await add_trades(
            engine,
            BTC,
            HOUR,
            [(5, 400000, 1000), (20, 550000, 500), (40, 300000, 250), (50, 450000, 100)],
        )
        await add_trades(engine, BTC, HOUR + timedelta(minutes=1), [(10, 480000, 700)])
        await add_trades(engine, BTC, HOUR + timedelta(minutes=70), [(0, 600000, 200)])
        await add_ticks(
            engine, BTC, HOUR,
            [(1, 500000, 490000, 510000, 10000, 5000), (30, 520000, 500000, 530000, 12000, 6000)],
        )  # fmt: skip
        for view in ("candles_1m", "candles_1h", "ticker_1m", "ticker_1h"):
            await refresh(engine, view, HOUR - timedelta(hours=2), HOUR + timedelta(hours=4))
        await auth.add_user(engine, "alice")
        _, token = await auth.create_token(engine, "alice", "read")
        return token
    finally:
        await engine.dispose()


@pytest.fixture
def api(migrated_db_url: str) -> Any:
    token = asyncio.run(seed(migrated_db_url))
    app = create_app(ServerSettings(db_url=migrated_db_url))
    with TestClient(app) as client:
        client.headers["Authorization"] = f"Bearer {token}"
        yield client


def no_floats(value: Any) -> bool:
    if isinstance(value, float):
        return False
    if isinstance(value, dict):
        return all(no_floats(v) for v in value.values())
    if isinstance(value, list):
        return all(no_floats(v) for v in value)
    return True


def tickers(response: Any) -> list[str]:
    return [m["ticker"] for m in response.json()["markets"]]


# ---------------------------------------------------------------- the market list


def test_every_data_route_needs_a_token(api: TestClient) -> None:
    anonymous = TestClient(api.app)
    for path in (
        "/v1/markets",
        f"/v1/markets/{BTC}",
        "/v1/events/KXBTC-26OCT08",
        f"/v1/markets/{BTC}/candles",
    ):
        assert anonymous.get(path).status_code == 401, path


def test_the_list_is_in_ticker_order_and_one_market_has_exactly_this_shape(api: TestClient) -> None:
    response = api.get("/v1/markets", params={"series": "KXFED"})
    assert response.status_code == 200 and response.json()["next_cursor"] is None
    assert response.json()["markets"] == [
        {
            "ticker": FED,
            "event_ticker": "KXFED-26OCT",
            "series_ticker": "KXFED",
            "event_title": "Fed rate decision (100% sure)",
            "type": "binary",
            "status": "finalized",
            "yes_label": "Above 80000",
            "no_label": "Below 80000",
            "open_time": response.json()["markets"][0]["open_time"],
            "close_time": response.json()["markets"][0]["close_time"],
            "expiration_time": response.json()["markets"][0]["expiration_time"],
            "result": "yes",
            "settlement_value": "1.000000",
            "settlement_time": response.json()["markets"][0]["settlement_time"],
            "strike_type": "greater",
            "floor_strike": "80000.000000",
            "cap_strike": None,
            "stage": "full",
        }
    ]
    assert response.json()["markets"][0]["settlement_time"].endswith("+00:00")
    assert no_floats(response.json())


def test_paging_walks_every_market_once_in_order_with_no_gaps_or_repeats(api: TestClient) -> None:
    seen: list[str] = []
    cursor = None
    pages = 0
    while True:
        params: dict[str, Any] = {"limit": 7}
        if cursor:
            params["cursor"] = cursor
        body = api.get("/v1/markets", params=params).json()
        seen += [m["ticker"] for m in body["markets"]]
        pages += 1
        cursor = body["next_cursor"]
        if cursor is None:
            break
    everything = tickers(api.get("/v1/markets", params={"limit": 500}))
    assert seen == everything == sorted(everything) and len(set(seen)) == len(seen) == 29
    assert pages == 5  # 29 markets, 7 per page


def test_filters_combine_and_search_matches_tickers_and_event_titles(api: TestClient) -> None:
    assert tickers(api.get("/v1/markets", params={"status": "active"})) == [
        BTC,
        "KXBTC-26OCT08-T81000",
    ]
    assert (
        tickers(api.get("/v1/markets", params={"event": "KXBTC-26OCT08", "status": "finalized"}))
        == []
    )
    assert tickers(api.get("/v1/markets", params={"q": "t81000"})) == [
        "KXBTC-26OCT08-T81000"
    ]  # any case
    assert tickers(api.get("/v1/markets", params={"q": "bitcoin"})) == [
        BTC,
        "KXBTC-26OCT08-T81000",
    ]  # event title
    assert tickers(api.get("/v1/markets", params={"q": "100%"})) == [
        FED
    ]  # '%' is literal, not a wildcard
    assert tickers(api.get("/v1/markets", params={"q": "N0_"})) == []  # '_' is literal too
    assert tickers(api.get("/v1/markets", params={"status": "nonsense"})) == []


def test_bad_parameters_are_named_without_echoing_them(api: TestClient) -> None:
    for params in (
        {"limit": 501},
        {"limit": 0},
        {"q": "a"},
        {"status": "BAD STATUS!"},
        {"cursor": "!!!"},
    ):
        response = api.get("/v1/markets", params=params)
        assert response.status_code == 422, params
        body = response.json()
        assert body["error"] == "invalid_parameter" and body["fields"]
        assert "BAD STATUS" not in response.text and "!!!" not in response.text


# ---------------------------------------------------------------- one market, one event


def test_a_full_market_shows_its_rules_and_a_reduced_one_says_so(api: TestClient) -> None:
    full = api.get(f"/v1/markets/{BTC}").json()
    assert full["stage"] == "full" and full["rules_primary"] == "Rules A"
    assert full["created_time"] and full["floor_strike"] == "80000.000000"
    slim = api.get(f"/v1/markets/{OLD}").json()
    assert (
        slim["stage"] == "slim"
        and slim["rules_primary"] == ""
        and slim["floor_strike"] == "0.000005"
    )
    tomb = api.get(f"/v1/markets/{GONE}").json()
    assert tomb["stage"] == "tombstone" and tomb["result"] == "yes"
    assert tomb["settlement_value"] == "1.000000" and tomb["strike_type"] is None
    assert tomb["created_time"] is None and tomb["floor_strike"] is None


def test_unknown_things_are_plain_404s(api: TestClient) -> None:
    for path in ("/v1/markets/NOPE", "/v1/events/NOPE", "/v1/markets/NOPE/candles", "/v1/nothing"):
        response = api.get(path)
        assert response.status_code == 404 and response.json() == {"error": "not_found"}, path


def test_an_event_lists_its_markets_and_an_empty_event_is_fine(api: TestClient) -> None:
    event = api.get("/v1/events/KXBTC-26OCT08").json()
    assert event["title"] == "Bitcoin price on Oct 8" and event["mutually_exclusive"] is True
    assert [m["ticker"] for m in event["markets"]] == [BTC, "KXBTC-26OCT08-T81000"]
    empty = api.get("/v1/events/KXEMPTY-26").json()
    assert empty["markets"] == [] and empty["series_ticker"] == "KXEMPTY"


# ---------------------------------------------------------------- candles


def bars(api: TestClient, **params: Any) -> dict[str, Any]:
    response = api.get(f"/v1/markets/{BTC}/candles", params=params)
    assert response.status_code == 200, response.text
    assert no_floats(response.json())
    return response.json()  # type: ignore[no-any-return]


def test_trade_candles_carry_the_exact_stored_values_as_strings(api: TestClient) -> None:
    body = bars(
        api,
        interval="1m",
        start=(HOUR - timedelta(minutes=5)).isoformat(),
        end=(HOUR + timedelta(hours=2)).isoformat(),
    )
    assert body["ticker"] == BTC and body["interval"] == "1m" and body["source"] == "trades"
    assert body["next_cursor"] is None
    first, second = body["bars"][0], body["bars"][1]
    assert first == {
        "time": HOUR.isoformat(timespec="microseconds"),
        "open": "0.400000", "high": "0.550000", "low": "0.300000", "close": "0.450000",
        "volume": "18.50", "trades": 4,
    }  # fmt: skip
    assert second["time"] == (HOUR + timedelta(minutes=1)).isoformat(timespec="microseconds")
    assert (second["open"], second["volume"], second["trades"]) == ("0.480000", "7.00", 1)
    assert len(body["bars"]) == 3  # a bar at +70 minutes; the silent minutes in between have none


def test_hourly_candles_combine_the_minutes(api: TestClient) -> None:
    body = bars(api, interval="1h", end=(HOUR + timedelta(hours=3)).isoformat())
    first = body["bars"][0]
    assert (first["open"], first["high"], first["low"], first["close"]) == (
        "0.400000",
        "0.550000",
        "0.300000",
        "0.480000",
    )
    assert (first["volume"], first["trades"]) == ("25.50", 5)
    assert body["bars"][1]["trades"] == 1


def test_ticker_bars_have_the_last_quote_and_cumulative_figures(api: TestClient) -> None:
    body = bars(api, interval="1m", source="ticker", end=(HOUR + timedelta(hours=1)).isoformat())
    assert body["bars"] == [
        {
            "time": HOUR.isoformat(timespec="microseconds"),
            "open": "0.500000", "high": "0.520000", "low": "0.500000", "close": "0.520000",
            "volume": "120.00", "yes_bid": "0.500000", "yes_ask": "0.530000",
            "open_interest": "60.00", "ticks": 2,
        }
    ]  # fmt: skip


def test_without_a_start_the_most_recent_bars_come_back_oldest_first(api: TestClient) -> None:
    body = bars(api, interval="1m", limit=2, end=(HOUR + timedelta(hours=4)).isoformat())
    times = [b["time"] for b in body["bars"]]
    assert len(times) == 2 and times == sorted(times)
    assert body["bars"][-1]["time"] == (HOUR + timedelta(minutes=70)).isoformat(
        timespec="microseconds"
    )
    assert body["bars"][0]["time"] == (HOUR + timedelta(minutes=1)).isoformat(
        timespec="microseconds"
    )


def test_forward_paging_covers_every_bar_exactly_once(api: TestClient) -> None:
    start = (HOUR - timedelta(minutes=5)).isoformat()
    end = (HOUR + timedelta(hours=4)).isoformat()
    collected: list[str] = []
    cursor = None
    for _ in range(5):
        params: dict[str, Any] = {"interval": "1m", "limit": 1, "start": start, "end": end}
        if cursor:
            params["cursor"] = cursor
        body = bars(api, **params)
        collected += [b["time"] for b in body["bars"]]
        cursor = body["next_cursor"]
        if cursor is None:
            break
    assert len(collected) == len(set(collected)) == 3 and collected == sorted(collected)


def test_a_quiet_market_has_no_bars_not_an_error(api: TestClient) -> None:
    response = api.get(f"/v1/markets/{FED}/candles")
    assert response.status_code == 200 and response.json()["bars"] == []


def test_bad_candle_parameters_are_rejected(api: TestClient) -> None:
    path = f"/v1/markets/{BTC}/candles"
    now = datetime.now(UTC)
    cases = [
        {"interval": "5m"},
        {"source": "orders"},
        {"limit": 5001},
        {"start": "2026-10-08T12:00:00"},  # no timezone: ambiguous
        {"end": "2026-10-08T12:00:00"},
        {"start": now.isoformat(), "end": (now - timedelta(hours=1)).isoformat()},
        {"cursor": "!!!"},
        {"start": "yesterday"},
    ]
    for params in cases:
        response = api.get(path, params=params)
        assert response.status_code == 422, params
        assert response.json()["error"] == "invalid_parameter", params
    assert (
        api.get(path, params={"start": "2026-10-08T12:00:00"}).json()["reason"]
        == "timezone_required"
    )


def test_everything_returned_is_free_of_floats(api: TestClient) -> None:
    for path in (
        "/v1/markets?limit=500",
        f"/v1/markets/{BTC}",
        f"/v1/markets/{GONE}",
        "/v1/events/KXPAD-26",
    ):
        assert no_floats(api.get(path).json()), path
