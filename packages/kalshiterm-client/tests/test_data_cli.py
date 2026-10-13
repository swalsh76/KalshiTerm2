"""``kterm markets | market | candles | trades | book | analytics | arbitrage`` against a scripted
HTTPS server: what each command asks for, what it prints, and how it fails."""

import json
from collections.abc import Iterator
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest
from fake_server import FakeServer, make_certificate
from kalshiterm_client import secrets
from kalshiterm_client.cli import app
from kalshiterm_client.pinning import fingerprint_of_pem
from kalshiterm_client.profiles import ProfileStore
from typer.testing import CliRunner

TOKEN = "kt_7_" + "Zy9" * 14
WHEN = "2026-10-12T18:00:00.000000+00:00"
runner = CliRunner()


def market(ticker: str = "KXA-E1-X", **changes: Any) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "event_ticker": "KXA-E1",
        "series_ticker": "KXA",
        "event_title": "An event",
        "type": "binary",
        "status": "active",
        "yes_label": "Yes",
        "no_label": "No",
        "open_time": WHEN,
        "close_time": "2026-10-13T18:00:00.000000+00:00",
        "expiration_time": None,
        "result": None,
        "settlement_value": None,
        "settlement_time": None,
        "strike_type": "greater",
        "floor_strike": "118749.900000",
        "cap_strike": None,
        "stage": "full",
        **changes,
    }


def detail(**changes: Any) -> dict[str, Any]:
    return {
        **market(),
        "rules_primary": "The rules",
        "rules_secondary": "",
        "created_time": WHEN,
        "updated_time": None,
        **changes,
    }


@pytest.fixture
def server() -> Iterator[FakeServer]:
    cert = make_certificate()
    fake = FakeServer(cert=cert).start()
    store = ProfileStore()
    store.add("lab", fake.url)
    store.save_pin("lab", cert[0], fingerprint_of_pem(cert[0]))
    secrets.set_token("lab", TOKEN)
    yield fake
    fake.stop()


def run(*args: str) -> tuple[int, str]:
    result = runner.invoke(app, list(args))
    return result.exit_code, result.output


def asked(server: FakeServer, n: int = -1) -> dict[str, str]:
    """The query parameters of the n-th request the server saw."""
    return {k: v[0] for k, v in parse_qs(urlsplit(server.requests[n][0]).query).items()}


# ------------------------------------------------------------------ markets


def test_markets_asks_for_what_was_given_and_prints_a_row_each(server: FakeServer) -> None:
    server.routes["/v1/markets"] = {"markets": [market(), market("KXA-E1-Y")], "next_cursor": None}
    code, out = run("markets", "btc", "--status", "active", "--series", "KXA", "--limit", "10")
    assert code == 0 and "KXA-E1-X" in out and "KXA-E1-Y" in out and "2026-10-13 18:00" in out
    assert asked(server) == {"q": "btc", "status": "active", "series": "KXA", "limit": "10"}
    assert "showing the first" not in out  # fewer than --limit came back


def test_markets_stops_at_the_limit_across_pages_and_says_so(server: FakeServer) -> None:
    def page(params: dict[str, str]) -> dict[str, Any]:
        offset = int(params.get("cursor", "0"))
        rows = [market(f"KXA-E1-{n:03d}") for n in range(offset, offset + 2)]
        return {"markets": rows, "next_cursor": str(offset + 2)}  # never ends

    server.routes["/v1/markets"] = page
    code, out = run("markets", "--limit", "5")
    assert code == 0 and out.count("KXA-E1-0") == 5
    assert "KXA-E1-004" in out and "KXA-E1-005" not in out
    assert "(showing the first 5; narrow the search or raise --limit)" in out
    assert len(server.requests) == 3  # 2 + 2 + 2 rows, stopped once 5 were in hand


def test_markets_json_is_the_models_with_exact_decimal_strings(server: FakeServer) -> None:
    server.routes["/v1/markets"] = {"markets": [market()], "next_cursor": None}
    code, out = run("markets", "--json")
    data = json.loads(out)
    assert code == 0 and data["markets"][0]["floor_strike"] == "118749.900000"
    assert data["markets"][0]["ticker"] == "KXA-E1-X" and data["markets"][0]["stage"] == "full"


@pytest.mark.parametrize(
    ("args", "code"),
    [(["markets", "x"], 1), (["markets", "--limit", "0"], 2), (["markets", "--limit", "5001"], 2)],
)
def test_markets_refuses_unreasonable_requests_before_asking(
    server: FakeServer, args: list[str], code: int
) -> None:
    assert run(*args)[0] == code and server.requests == []


# ------------------------------------------------------------------ market


def test_market_shows_detail_last_quote_and_analytics(server: FakeServer) -> None:
    server.routes["/v1/markets/KXA-E1-X"] = detail()
    server.routes["/v1/markets/KXA-E1-X/ticks"] = {
        "ticker": "KXA-E1-X",
        "next_cursor": None,
        "ticks": [
            {
                "time": WHEN,
                "price": "0.560000",
                "yes_bid": "0.550000",
                "yes_ask": "0.570000",
                "yes_bid_size": "12.50",
                "yes_ask_size": "3.00",
                "volume": "1000.00",
                "open_interest": "40.00",
                "last_trade_size": None,
            }
        ],
    }
    server.routes["/v1/analytics/markets/KXA-E1-X"] = {
        "ticker": "KXA-E1-X",
        "as_of": WHEN,
        "results": [
            {
                "metric": "market_quotes.spread",
                "value": "0.020000",
                "unit": "dollars",
                "time": WHEN,
                "age_seconds": 12,
            }
        ],
    }
    code, out = run("market", "KXA-E1-X")
    assert code == 0 and "KXA-E1-X" in out and "An event" in out and "greater  118749.9" in out
    assert "price 0.56" in out and "bid 0.55 x 12.5" in out and "market_quotes.spread" in out
    assert "The rules" in out
    assert asked(server, 1) == {"limit": "1"}


def test_market_still_works_when_the_optional_pieces_are_unavailable(server: FakeServer) -> None:
    server.routes["/v1/markets/KXA-E1-X"] = detail()
    server.routes["/v1/markets/KXA-E1-X/ticks"] = (404, {"error": "not_found"})
    server.routes["/v1/analytics/markets/KXA-E1-X"] = (500, {"error": "boom"})
    code, out = run("market", "KXA-E1-X")
    assert code == 0 and "KXA-E1-X" in out and "last quote" not in out and "analytics" not in out


def test_market_json_holds_the_detail_the_tick_and_the_analytics(server: FakeServer) -> None:
    server.routes["/v1/markets/KXA-E1-X"] = detail()
    server.routes["/v1/markets/KXA-E1-X/ticks"] = {
        "ticker": "KXA-E1-X",
        "ticks": [],
        "next_cursor": None,
    }
    server.routes["/v1/analytics/markets/KXA-E1-X"] = {
        "ticker": "KXA-E1-X",
        "as_of": WHEN,
        "results": [],
    }
    code, out = run("market", "KXA-E1-X", "--json")
    data = json.loads(out)
    assert code == 0 and set(data) == {"market", "latest_tick", "analytics"}
    assert data["market"]["rules_primary"] == "The rules" and data["latest_tick"] is None


def test_an_unknown_market_is_a_clear_error_and_never_shows_the_token(server: FakeServer) -> None:
    server.routes["/v1/markets/KX-NOPE"] = (404, {"error": "not_found"})
    code, out = run("market", "KX-NOPE")
    assert code == 1 and "not found (not_found)" in out and TOKEN not in out


# ------------------------------------------------------------------ candles and trades


BAR = {
    "time": WHEN,
    "open": "0.400000",
    "high": "0.550000",
    "low": "0.300000",
    "close": "0.450000",
    "volume": "17.50",
    "trades": 4,
}


def test_candles_sends_its_options_and_reads_a_naive_time_as_utc(server: FakeServer) -> None:
    server.routes["/v1/markets/T/candles"] = {
        "ticker": "T",
        "interval": "1h",
        "source": "ticker",
        "bars": [{**BAR, "yes_bid": "0.44", "yes_ask": "0.46", "ticks": 31}],
        "next_cursor": "abc",
    }
    code, out = run(
        "candles",
        "T",
        "--interval",
        "1h",
        "--source",
        "ticker",
        "--limit",
        "5",
        "--start",
        "2026-10-12T08:00",
        "--end",
        "2026-10-12T20:00:00+02:00",
    )
    assert code == 0 and "1h bars from ticker" in out and "TICKS" in out and "0.55" in out
    assert asked(server) == {
        "interval": "1h",
        "source": "ticker",
        "limit": "5",
        "start": "2026-10-12T08:00:00+00:00",
        "end": "2026-10-12T20:00:00+02:00",
    }
    assert "more bars exist after 2026-10-12 18:00; use --start" in out


def test_candles_json_and_the_defaults(server: FakeServer) -> None:
    server.routes["/v1/markets/T/candles"] = {
        "ticker": "T",
        "interval": "1m",
        "source": "trades",
        "bars": [BAR],
        "next_cursor": None,
    }
    code, out = run("candles", "T", "--json")
    assert code == 0 and json.loads(out)["bars"][0]["volume"] == "17.50"
    assert asked(server) == {"interval": "1m", "source": "trades", "limit": "30"}


@pytest.mark.parametrize(
    "args",
    [
        ["--interval", "5m"],
        ["--source", "orders"],
        ["--limit", "0"],
        ["--start", "yesterday"],
        ["--end", "2026-13-45"],
    ],
)
def test_candles_rejects_bad_options_without_asking_the_server(
    server: FakeServer, args: list[str]
) -> None:
    code, out = run("candles", "T", *args)
    assert code == 2 and server.requests == []


def test_trades_table_and_parameters(server: FakeServer) -> None:
    trade = {
        "time": WHEN,
        "trade_id": "00000000-0000-0000-0000-000000000001",
        "yes_price": "0.620000",
        "no_price": "0.380000",
        "count": "250.00",
        "taker_side": "no",
        "is_block_trade": True,
    }
    server.routes["/v1/markets/T/trades"] = {"ticker": "T", "trades": [trade], "next_cursor": None}
    code, out = run("trades", "T", "--limit", "7", "--start", "2026-10-12T17:00Z")
    assert code == 0 and "0.62" in out and "0.38" in out and "250" in out and "block" in out
    assert asked(server) == {"limit": "7", "start": "2026-10-12T17:00:00+00:00"}


# ------------------------------------------------------------------ book


BOOK = {
    "ticker": "T",
    "as_of": WHEN,
    "snapshot_time": WHEN,
    "changes_applied": 3,
    "approximate_snapshot": False,
    "complete": True,
    "gaps": [],
    "yes": [{"price": "0.400000", "size": "10.00"}],
    "no": [{"price": "0.580000", "size": "7.00"}],
    "best_yes_bid": "0.400000",
    "best_no_bid": "0.580000",
    "best_yes_ask": "0.420000",
    "best_no_ask": "0.600000",
}


def test_book_prints_both_sides_and_passes_the_time_and_depth(server: FakeServer) -> None:
    server.routes["/v1/markets/T/orderbook"] = BOOK
    code, out = run("book", "T", "--at", "2026-10-12T17:59:00", "--depth", "3")
    assert code == 0 and "YES BID" in out and "0.58" in out and "best YES ask 0.42" in out
    assert asked(server) == {"at": "2026-10-12T17:59:00+00:00", "depth": "3"}


def test_book_warns_when_the_servers_book_may_be_incomplete(server: FakeServer) -> None:
    server.routes["/v1/markets/T/orderbook"] = {
        **BOOK,
        "complete": False,
        "approximate_snapshot": True,
    }
    code, out = run("book", "T")
    assert code == 0 and "WARNING: this book may be incomplete" in out and "approximate" in out


def test_a_market_without_a_book_says_so(server: FakeServer) -> None:
    server.routes["/v1/markets/T/orderbook"] = (404, {"error": "no_orderbook"})
    code, out = run("book", "T")
    assert code == 1 and "no_orderbook" in out


# ------------------------------------------------------------------ analytics and arbitrage


def test_analytics_for_a_market_an_event_and_one_metric(server: FakeServer) -> None:
    value = {
        "metric": "arbitrage_bid.profit",
        "value": "0.020000",
        "unit": "dollars",
        "time": WHEN,
        "age_seconds": 3,
    }
    server.routes["/v1/analytics/markets/T"] = {"ticker": "T", "as_of": WHEN, "results": [value]}
    server.routes["/v1/analytics/events/KX-E1"] = {
        "event_ticker": "KX-E1",
        "as_of": WHEN,
        "results": [value],
    }
    server.routes["/v1/analytics/markets/T/history"] = {
        "ticker": "T",
        "metric": "a.b",
        "unit": "dollars",
        "next_cursor": None,
        "points": [{"time": WHEN, "value": "0.010000"}, {"time": WHEN, "value": "0.020000"}],
    }
    code, out = run("analytics", "T", "--max-age", "600")
    assert code == 0 and "arbitrage_bid.profit" in out and "0.02" in out
    assert asked(server) == {"max_age_seconds": "600"}
    code, out = run("analytics", "--event", "KX-E1")
    assert code == 0 and "KX-E1" in out and "arbitrage_bid.profit" in out
    code, out = run("analytics", "T", "--metric", "a.b", "--limit", "2")
    assert code == 0 and "0.01" in out and "0.02" in out
    assert asked(server) == {"metric": "a.b", "limit": "2"}


def test_analytics_needs_exactly_one_subject_and_says_when_there_is_nothing(
    server: FakeServer,
) -> None:
    assert run("analytics")[0] == 1 and run("analytics", "T", "--event", "E")[0] == 1
    assert run("analytics", "--event", "E", "--metric", "a.b")[0] == 1
    assert server.requests == []
    server.routes["/v1/analytics/markets/T"] = {"ticker": "T", "as_of": WHEN, "results": []}
    code, out = run("analytics", "T")
    assert code == 0 and "no analytics for T" in out and "watched markets" in out


OPPORTUNITY = {
    "event_ticker": "KX-E1",
    "time": WHEN,
    "bid_sum": "1.050000",
    "edge": "0.050000",
    "size": "10.000000",
    "fees": "0.480000",
    "profit": "0.020000",
    "legs": 3,
    "legs_total": 4,
    "oldest_quote_age_seconds": 45,
}


def test_arbitrage_with_nothing_found_says_so(server: FakeServer) -> None:
    server.routes["/v1/analytics/arbitrage"] = {"as_of": WHEN, "total": 0, "opportunities": []}
    code, out = run("arbitrage")
    assert code == 0 and "no opportunities right now" in out


def test_arbitrage_lists_opportunities_with_the_profit_filter(server: FakeServer) -> None:
    server.routes["/v1/analytics/arbitrage"] = {
        "as_of": WHEN,
        "total": 3,
        "opportunities": [OPPORTUNITY],
    }
    code, out = run("arbitrage", "--min-profit", "0", "--limit", "5", "--max-age", "120")
    assert code == 0 and "KX-E1" in out and "1.05" in out and "0.48" in out and "0.02" in out
    assert (
        "3/4" in out and "45s" in out and "1 of 3 shown" in out and "re-read the live book" in out
    )
    assert asked(server) == {"min_profit": "0", "limit": "5", "max_age_seconds": "120"}
    code, out = run("arbitrage", "--json")
    assert json.loads(out)["opportunities"][0]["profit"] == "0.020000"


@pytest.mark.parametrize("value", ["abc", "1e", "0,5"])
def test_arbitrage_rejects_a_min_profit_that_is_not_a_number(
    server: FakeServer, value: str
) -> None:
    assert run("arbitrage", "--min-profit", value)[0] == 2 and server.requests == []


# ------------------------------------------------------------------ across commands


def test_a_long_ticker_is_never_cut_off_when_piped(server: FakeServer) -> None:
    long_ticker = "KXVERYLONGSERIES-26OCT121445-T" + "9" * 30
    server.routes["/v1/markets"] = {"markets": [market(long_ticker)], "next_cursor": None}
    assert long_ticker in run("markets")[1]


def test_every_command_uses_the_chosen_profile_and_the_stored_token(server: FakeServer) -> None:
    server.routes["/v1/markets"] = {"markets": [], "next_cursor": None}
    ProfileStore().add("other", "https://127.0.0.1:1", replace=True)
    ProfileStore().use("other")  # the default points nowhere
    assert run("--profile", "lab", "markets")[0] == 0
    assert server.requests[0][1] == f"Bearer {TOKEN}"


def test_an_unreachable_server_is_one_line_not_a_traceback(server: FakeServer) -> None:
    server.stop()
    code, out = run("markets")
    assert code == 1 and "cannot reach" in out and "Traceback" not in out and TOKEN not in out


def test_without_a_server_configured_the_error_says_what_to_do() -> None:
    code, out = run("markets")
    assert code == 1 and "no server configured" in out
