"""The browser's view of the KalshiTerm server: every data route, the real ``ServerClient`` behind
it, a scripted server in front of it, and the security layer in between."""

from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import pytest
import server_payloads as p
from fake_server import FakeServer
from kalshiterm_client.client import ServerClient
from kalshiterm_client.ui.app import create_app
from kalshiterm_client.ui.data_routes import DataAccess
from kalshiterm_client.ui.security import new_token
from starlette.testclient import TestClient

PORT = 51235
TOKEN = new_token()
AUTH = {"Authorization": f"Bearer {TOKEN}"}
WRITE = {**AUTH, "Origin": f"http://127.0.0.1:{PORT}"}
SERVER_TOKEN = "kt_7_" + "Zy9" * 14


@pytest.fixture
def server() -> Iterator[FakeServer]:
    fake = FakeServer().start()
    yield fake
    fake.stop()


@pytest.fixture
def browser(server: FakeServer, tmp_path: Path) -> Iterator[TestClient]:
    """A test client for the backend, whose ServerClient talks to the fake server."""
    import asyncio

    client = ServerClient(server.url, SERVER_TOKEN)
    app = create_app(
        port=PORT,
        token=TOKEN,
        environment="demo",
        assets_dir=tmp_path / "none",
        data=DataAccess(client, profile="lan", url=server.url),
    )
    with TestClient(app, base_url=f"http://127.0.0.1:{PORT}") as tc:
        yield tc
    asyncio.run(client.aclose())


def seen(server: FakeServer) -> list[str]:
    return [path for path, _ in server.requests]


# --- reference data -----------------------------------------------------------------------------


def test_markets_pass_their_filters_to_the_server_and_prices_stay_exact_strings(
    browser: TestClient, server: FakeServer
) -> None:
    server.routes["/v1/markets"] = lambda q: {
        "markets": [p.market(floor_strike="118749.900000")],
        "next_cursor": "c2",
        "echo": q,
    }
    response = browser.get(
        "/api/markets", params={"q": "bitcoin", "status": "active", "limit": 5}, headers=AUTH
    )
    assert response.status_code == 200
    body = response.json()
    assert body["markets"][0]["floor_strike"] == "118749.900000"  # a string, not 118749.9
    assert body["next_cursor"] == "c2"
    asked = [r for r in seen(server) if r.startswith("/v1/markets")][0]
    assert "q=bitcoin" in asked and "status=active" in asked and "limit=5" in asked


def test_a_market_and_an_event(browser: TestClient, server: FakeServer) -> None:
    server.routes["/v1/markets/KXA-E1-X"] = p.detail()
    server.routes["/v1/events/KXA-E1"] = {
        "event_ticker": p.EVENT, "series_ticker": "KXA", "title": "T", "sub_title": "",
        "mutually_exclusive": True, "markets": [p.market()],
    }  # fmt: skip
    assert browser.get("/api/markets/KXA-E1-X", headers=AUTH).json()["rules_primary"] == "The rules"
    assert browser.get("/api/events/KXA-E1", headers=AUTH).json()["title"] == "T"


# --- market data --------------------------------------------------------------------------------


def test_candles_trades_ticks_orderbook_and_gaps(browser: TestClient, server: FakeServer) -> None:
    t = p.TICKER
    server.routes[f"/v1/markets/{t}/candles"] = {
        "ticker": t, "interval": "1h", "source": "trades", "bars": [p.BAR], "next_cursor": None,
    }  # fmt: skip
    server.routes[f"/v1/markets/{t}/trades"] = {
        "ticker": t,
        "trades": [p.TRADE],
        "next_cursor": None,
    }
    server.routes[f"/v1/markets/{t}/ticks"] = {"ticker": t, "ticks": [p.TICK], "next_cursor": None}
    server.routes[f"/v1/markets/{t}/orderbook"] = p.BOOK
    server.routes["/v1/gaps"] = {"gaps": [p.GAP]}
    bars = browser.get(f"/api/markets/{t}/candles", params={"interval": "1h"}, headers=AUTH).json()
    assert bars["bars"][0]["close"] == "0.450000" and bars["interval"] == "1h"
    assert (
        browser.get(f"/api/markets/{t}/trades", headers=AUTH).json()["trades"][0]["count"] == "2.50"
    )
    assert (
        browser.get(f"/api/markets/{t}/ticks", headers=AUTH).json()["ticks"][0]["price"]
        == "0.560000"
    )
    assert browser.get(f"/api/markets/{t}/orderbook", params={"depth": 5}, headers=AUTH).json()[
        "yes"
    ] == [{"price": "0.550000", "size": "10.00"}]
    assert browser.get("/api/gaps", headers=AUTH).json()[0]["trades_recovered"] == 12


def test_a_time_without_a_zone_is_refused_before_the_server_is_asked(
    browser: TestClient, server: FakeServer
) -> None:
    response = browser.get(
        f"/api/markets/{p.TICKER}/candles", params={"start": "2026-10-12T18:00:00"}, headers=AUTH
    )
    assert response.status_code == 400 and "time zone" in response.json()["detail"]
    assert not [r for r in seen(server) if "candles" in r]
    ok = browser.get(
        f"/api/markets/{p.TICKER}/candles", params={"start": "2026-10-12T18:00:00Z"}, headers=AUTH
    )
    assert ok.status_code in (200, 502)  # asked the server this time


@pytest.mark.parametrize(
    ("path", "params"),
    [
        ("/api/markets", {"limit": 0}),
        ("/api/markets", {"limit": 100000}),
        (f"/api/markets/{p.TICKER}/candles", {"interval": "weekly"}),
        (f"/api/markets/{p.TICKER}/candles", {"source": "other"}),
        (f"/api/markets/{p.TICKER}/orderbook", {"depth": 0}),
        ("/api/analytics/arbitrage", {"min_profit": "abc"}),
        ("/api/analytics/arbitrage", {"min_profit": "1e3"}),
    ],
)
def test_bad_parameters_are_422_and_never_reach_the_server(
    browser: TestClient, server: FakeServer, path: str, params: dict[str, Any]
) -> None:
    assert browser.get(path, params=params, headers=AUTH).status_code == 422
    assert not [r for r in seen(server) if r.startswith("/v1/")]


# --- watchlist, analytics, alerts --------------------------------------------------------------


def test_the_watchlist_can_be_read_and_changed(browser: TestClient, server: FakeServer) -> None:
    server.routes["/v1/watchlist"] = p.WATCHLIST
    server.routes[f"PUT /v1/watchlist/{p.TICKER}"] = {"added": True}
    server.routes[f"DELETE /v1/watchlist/{p.TICKER}"] = {"removed": False}
    assert browser.get("/api/watchlist", headers=AUTH).json()["markets"][0]["ticker"] == p.TICKER
    assert browser.put(f"/api/watchlist/{p.TICKER}", headers=WRITE).json() == {"changed": True}
    assert browser.delete(f"/api/watchlist/{p.TICKER}", headers=WRITE).json() == {"changed": False}
    # the server really was asked, with the right verb and path, and its answer is what comes back
    assert [(m, path) for m, path, _ in server.calls if m in ("PUT", "DELETE")] == [
        ("PUT", f"/v1/watchlist/{p.TICKER}"),
        ("DELETE", f"/v1/watchlist/{p.TICKER}"),
    ]
    server.routes[f"PUT /v1/watchlist/{p.TICKER}"] = {"added": False}  # it was already there
    assert browser.put(f"/api/watchlist/{p.TICKER}", headers=WRITE).json() == {"changed": False}


def test_analytics_and_arbitrage(browser: TestClient, server: FakeServer) -> None:
    server.routes[f"/v1/analytics/markets/{p.TICKER}"] = p.RESULTS
    server.routes[f"/v1/analytics/markets/{p.TICKER}/history"] = p.HISTORY
    server.routes[f"/v1/analytics/events/{p.EVENT}"] = p.EVENT_RESULTS
    server.routes["/v1/analytics/arbitrage"] = p.OPPORTUNITIES
    assert (
        browser.get(f"/api/analytics/markets/{p.TICKER}", headers=AUTH).json()["results"][0][
            "value"
        ]
        == "0.020000"
    )
    history = browser.get(
        f"/api/analytics/markets/{p.TICKER}/history", params={"metric": "spread"}, headers=AUTH
    ).json()
    assert history["points"][0]["value"] == "0.020000"
    assert (
        browser.get(f"/api/analytics/events/{p.EVENT}", headers=AUTH).json()["event_ticker"]
        == p.EVENT
    )
    opportunities = browser.get(
        "/api/analytics/arbitrage", params={"min_profit": "0"}, headers=AUTH
    ).json()
    assert opportunities["total"] == 1 and opportunities["opportunities"][0]["profit"] == "-0.01"
    assert "min_profit=0" in [r for r in seen(server) if "arbitrage" in r][0]


def test_alert_rules_lifecycle_sends_exactly_what_was_asked(
    browser: TestClient, server: FakeServer
) -> None:
    server.routes["/v1/alert-metrics"] = p.METRICS
    server.routes["/v1/alert-rules"] = p.RULES
    server.routes["POST /v1/alert-rules"] = (201, p.RULE)
    server.routes["/v1/alert-rules/5"] = p.RULE
    server.routes["PATCH /v1/alert-rules/5"] = {**p.RULE, "enabled": False}
    server.routes["DELETE /v1/alert-rules/5"] = {"id": 5, "deleted": True}
    server.routes["/v1/alerts"] = p.ALERTS
    server.routes["POST /v1/alerts/9/acknowledge"] = {"id": 9, "acknowledged": True}
    assert (
        browser.get("/api/alerts/metrics", headers=AUTH).json()["metrics"][0]["metric"] == "spread"
    )
    assert browser.get("/api/alerts/rules", headers=AUTH).json()["limit"] == 50
    created = browser.post(
        "/api/alerts/rules",
        headers=WRITE,
        json={"metric": "spread", "op": ">", "threshold": "0.10", "ticker": p.TICKER, "name": "w"},
    )
    assert created.status_code == 201 and created.json()["id"] == 5
    sent = [b for m, path, b in server.calls if m == "POST" and path == "/v1/alert-rules"][0]
    assert sent["threshold"] == "0.10" and sent["ticker"] == p.TICKER and sent["metric"] == "spread"
    assert browser.get("/api/alerts/rules/5", headers=AUTH).json()["name"] == "wide spread"
    patched = browser.patch("/api/alerts/rules/5", headers=WRITE, json={"enabled": False})
    assert patched.json()["enabled"] is False
    assert [b for m, path, b in server.calls if m == "PATCH"][0] == {"enabled": False}
    assert browser.delete("/api/alerts/rules/5", headers=WRITE).status_code == 204
    assert (
        browser.get("/api/alerts", params={"unacknowledged": "true"}, headers=AUTH).json()[
            "alerts"
        ][0]["id"]
        == 9
    )
    assert browser.post("/api/alerts/9/acknowledge", headers=WRITE).status_code == 204
    assert any("unacknowledged=true" in r for r in seen(server) if r.startswith("/v1/alerts"))


@pytest.mark.parametrize("threshold", [0.1, "abc", "1e3", "", "0.1; drop"])
def test_a_bad_rule_change_is_422_and_nothing_is_sent_to_the_server(
    browser: TestClient, server: FakeServer, threshold: object
) -> None:
    response = browser.patch("/api/alerts/rules/5", headers=WRITE, json={"threshold": threshold})
    assert response.status_code == 422 and not server.calls


@pytest.mark.parametrize(
    "body",
    [
        {"metric": "spread", "op": ">", "threshold": 0.1},  # a JSON number: precision lost
        {"metric": "spread", "op": ">", "threshold": "abc"},
        {"metric": "spread", "op": ">", "threshold": "1e3"},
        {"metric": "spread", "op": ">", "threshold": "0.1", "surprise": 1},
        {"op": ">", "threshold": "0.1"},
    ],
)
def test_a_bad_rule_is_422_and_nothing_is_sent_to_the_server(
    browser: TestClient, server: FakeServer, body: dict[str, Any]
) -> None:
    assert browser.post("/api/alerts/rules", headers=WRITE, json=body).status_code == 422
    assert not server.calls


# --- writes are protected like every other write ------------------------------------------------


def test_changing_server_data_needs_the_right_origin(
    browser: TestClient, server: FakeServer
) -> None:
    for method, path in (
        ("put", f"/api/watchlist/{p.TICKER}"), ("delete", f"/api/watchlist/{p.TICKER}"),
        ("post", "/api/alerts/9/acknowledge"), ("delete", "/api/alerts/rules/5"),
    ):  # fmt: skip
        no_origin = getattr(browser, method)(path, headers=AUTH)
        hostile = getattr(browser, method)(path, headers={**AUTH, "Origin": "https://evil.example"})
        assert no_origin.status_code == hostile.status_code == 403, (method, path)
    assert not server.calls


# --- the server is not always there -------------------------------------------------------------


def test_with_no_server_configured_data_routes_say_so_and_the_rest_still_works(
    tmp_path: Path,
) -> None:
    app = create_app(port=PORT, token=TOKEN, environment="demo", assets_dir=tmp_path / "x")
    with TestClient(app, base_url=f"http://127.0.0.1:{PORT}") as tc:
        response = tc.get("/api/markets", headers=AUTH)
        assert response.status_code == 503 and response.json()["error"] == "server_not_configured"
        info = tc.get("/api/server", headers=AUTH).json()
        assert info["configured"] is False and info["reachable"] is False and info["error"]
        assert tc.get("/api/session", headers=AUTH).json()["mode"] == "read-only"


def test_server_info_reports_reachability_without_failing(
    browser: TestClient, server: FakeServer
) -> None:
    server.routes["/v1/me"] = {"user": "stebe", "role": "admin", "token_id": 3}
    up = browser.get("/api/server", headers=AUTH).json()
    assert up == {"configured": True, "reachable": True, "profile": "lan", "url": server.url,
                  "user": "stebe", "role": "admin", "error": None}  # fmt: skip
    server.status = 500
    down = browser.get("/api/server", headers=AUTH)
    assert down.status_code == 200 and down.json()["reachable"] is False and down.json()["error"]


@pytest.mark.parametrize(
    ("status", "body", "expect_status", "expect_error"),
    [
        (404, {"error": "not_found"}, 404, "not_found"),
        (401, {"error": "unauthorized"}, 502, "server_rejected_token"),
        (403, {"error": "forbidden"}, 403, "forbidden_by_server"),
        (
            400,
            {"error": "cooldown_too_short", "minimum": 60},
            422,
            "rejected_by_server",
        ),
        (500, {"error": "boom"}, 502, "server_error"),
    ],
)
def test_what_the_server_says_becomes_a_clear_answer(
    browser: TestClient,
    server: FakeServer,
    status: int,
    body: dict[str, Any],
    expect_status: int,
    expect_error: str,
) -> None:
    server.routes["/v1/watchlist"] = (status, body)
    response = browser.get("/api/watchlist", headers=AUTH)
    assert response.status_code == expect_status and response.json()["error"] == expect_error
    assert SERVER_TOKEN not in response.text and TOKEN not in response.text


def test_a_dead_server_is_a_502_with_a_reason_and_no_token_in_it(tmp_path: Path) -> None:
    import asyncio

    dead = ServerClient(
        "http://127.0.0.1:1", SERVER_TOKEN, timeout=1.0, sleep=lambda _: asyncio.sleep(0)
    )
    app = create_app(
        port=PORT, token=TOKEN, environment="demo", assets_dir=tmp_path / "x",
        data=DataAccess(dead, profile="lan", url="http://127.0.0.1:1"),
    )  # fmt: skip
    with TestClient(app, base_url=f"http://127.0.0.1:{PORT}") as tc:
        response = tc.get("/api/watchlist", headers=AUTH)
        assert response.status_code == 502 and response.json()["error"] == "server_unreachable"
        assert SERVER_TOKEN not in response.text
    asyncio.run(dead.aclose())


def test_the_server_token_is_never_in_any_answer(browser: TestClient, server: FakeServer) -> None:
    server.routes["/v1/watchlist"] = p.WATCHLIST
    for response in (
        browser.get("/api/watchlist", headers=AUTH),
        browser.get("/api/server", headers=AUTH),
    ):
        assert SERVER_TOKEN not in response.text and SERVER_TOKEN not in str(response.headers)
    # ...and the server was in fact sent its own token, not the browser's
    assert all(auth == f"Bearer {SERVER_TOKEN}" for _, auth in server.requests)
    _ = AsyncIterator  # (imported for typing completeness)
