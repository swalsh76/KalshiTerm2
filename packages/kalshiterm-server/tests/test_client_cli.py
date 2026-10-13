"""Every data command of ``kterm`` against the real server (real TLS, real database)."""

import json
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

import httpx
import pytest
from kalshiterm_client.cli import app
from kalshiterm_client.pinning import trusting
from kalshiterm_client.profiles import ProfileStore
from test_client_contract import GONE, HOUR, SLIM, A, B, C, World, world  # noqa: F401
from typer.testing import CliRunner

pytestmark = pytest.mark.db
runner = CliRunner()


@pytest.fixture
def kterm(world: World, monkeypatch: pytest.MonkeyPatch) -> Any:  # noqa: F811
    monkeypatch.setenv("KTERM_TOKEN", world.tokens["alice"])

    def run(*args: str) -> tuple[int, str]:
        result = runner.invoke(app, ["--profile", "srv", *args])
        return result.exit_code, result.output

    return run


def test_markets_lists_searches_and_reports_what_the_server_keeps(kterm: Any) -> None:
    code, out = kterm("markets", "--status", "active")
    assert code == 0 and all(t in out for t in (A, B, C)) and SLIM not in out
    code, out = kterm("markets", "old")
    assert code == 0 and SLIM in out and GONE in out and A not in out
    code, out = kterm("markets", "--json", "--limit", "10")
    stages = {m["ticker"]: m["stage"] for m in json.loads(out)["markets"]}
    assert stages == {A: "full", B: "full", C: "full", SLIM: "slim", GONE: "tombstone"}


def test_market_shows_the_detail_the_last_tick_and_the_analytics(kterm: Any) -> None:
    code, out = kterm("market", A)
    assert code == 0, out
    for expected in (
        A,
        "The first event",
        "greater  118749.9",
        "Rules A",
        "price 0.50003",
        "bid 0.49 x -",
        "market_quotes.spread",
        "0.02",
        "market_quotes.bid_size",
        "12.34",
    ):
        assert expected in out, expected
    code, out = kterm("market", SLIM)
    assert code == 0 and "(kept as: slim)" in out and "result" in out


def test_candles_trades_and_the_book_show_the_seeded_market_data(kterm: Any) -> None:
    start = (HOUR - timedelta(minutes=1)).isoformat()
    code, out = kterm("candles", A, "--start", start, "--limit", "5")
    assert code == 0 and "1m bars from trades" in out
    rows = [
        line
        for line in out.splitlines()
        if line.strip().startswith(HOUR.strftime("%Y-%m-%d %H:%M"))
    ]
    assert rows and rows[0].split()[-1] == "25"  # 25 trades in the first minute
    code, out = kterm("candles", A, "--source", "ticker", "--interval", "1h", "--start", start)
    assert code == 0 and "TICKS" in out and "30" in out
    code, out = kterm("trades", A, "--limit", "3")
    assert code == 0 and out.count("0.50") >= 3 and "CONTRACTS" in out
    code, out = kterm("book", A, "--at", (HOUR + timedelta(seconds=30)).isoformat())
    assert (
        code == 0 and "0.40" in out and "12" in out and "0.39" in out and "best YES ask 0.42" in out
    )
    assert "WARNING" not in out


def test_a_market_without_a_book_or_that_does_not_exist_is_a_one_line_error(kterm: Any) -> None:
    code, out = kterm("book", B)
    assert code == 1 and "no_orderbook" in out and "Traceback" not in out
    code, out = kterm("market", "KX-NOPE")
    assert code == 1 and "not found" in out


def test_analytics_and_arbitrage_show_what_the_analyzers_stored(kterm: Any) -> None:
    code, out = kterm("analytics", A)
    assert code == 0 and "market_quotes.spread" in out and "contracts" in out
    code, out = kterm("analytics", A, "--metric", "market_volatility.volatility_1h", "--limit", "3")
    assert code == 0 and out.count("0.0") >= 3 and "0.01" in out
    code, out = kterm("analytics", "--event", "KX-E1")
    assert code == 0 and "arbitrage_bid.profit" in out
    code, out = kterm("arbitrage")
    assert code == 0 and "KX-E1" in out and "0.02" in out and "3/4" in out
    code, out = kterm("arbitrage", "--min-profit", "0.03")
    assert code == 0 and "KX-E1" not in out and "no opportunities right now" in out


def test_the_json_output_carries_exactly_the_values_the_server_sent(
    kterm: Any,
    world: World,  # noqa: F811
) -> None:
    """The CLI's --json against the server's own REST answer, field by field (decimals compared as
    decimals and times as times: the two spell timestamps differently)."""
    start = HOUR - timedelta(minutes=1)
    code, out = kterm("candles", A, "--start", start.isoformat(), "--json", "--limit", "10")
    ours = json.loads(out)["bars"]
    profile = ProfileStore().get("srv")
    with httpx.Client(
        base_url=profile.url,
        verify=trusting(profile),
        headers={"Authorization": f"Bearer {world.tokens['alice']}"},
    ) as http:
        theirs = http.get(
            f"/v1/markets/{A}/candles", params={"start": start.isoformat(), "limit": 10}
        ).json()["bars"]
    assert len(ours) == len(theirs) and ours
    for mine, raw in zip(ours, theirs, strict=True):
        assert datetime.fromisoformat(mine["time"]) == datetime.fromisoformat(raw["time"])
        for field in ("open", "high", "low", "close", "volume"):
            assert Decimal(mine[field]) == Decimal(raw[field]) and mine[field] == raw[field]
        assert mine["trades"] == raw["trades"]
