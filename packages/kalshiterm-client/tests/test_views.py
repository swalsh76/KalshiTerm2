from datetime import UTC, datetime
from decimal import Decimal
from io import StringIO

import pytest
from kalshiterm_client import views
from kalshiterm_client.models import (
    AnalyticsValue,
    CandlePage,
    HistoryPoint,
    Market,
    MarketDetail,
    Opportunity,
    OrderBook,
    Tick,
    Trade,
)
from rich.console import Console, RenderableType

D = Decimal
T0 = datetime(2026, 10, 12, 18, 0, tzinfo=UTC)
WHEN = "2026-10-12T18:00:00.000000+00:00"


def render(renderable: RenderableType, width: int = 120) -> str:
    out = StringIO()
    Console(file=out, width=width, color_system=None, force_terminal=False).print(renderable)
    return out.getvalue()


def market(**changes: object) -> MarketDetail:
    base = {
        "ticker": "KXA-E1-X",
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
        "strike_type": "between",
        "floor_strike": "73200.000000",
        "cap_strike": "73300.000000",
        "stage": "full",
        "rules_primary": "Rules text",
        "rules_secondary": "",
        "created_time": None,
        "updated_time": None,
    }
    return MarketDetail.model_validate({**base, **changes})


# ------------------------------------------------------------------ numbers


@pytest.mark.parametrize(
    ("value", "num", "money"),
    [
        ("0.560000", "0.56", "0.56"),
        ("0.123456", "0.123456", "0.123456"),
        ("1", "1", "1.00"),
        ("118749.900000", "118749.9", "118749.90"),
        ("100.00", "100", "100.00"),
        ("12.50", "12.5", "12.50"),
        ("-0.050000", "-0.05", "-0.05"),
        ("0.000000", "0", "0.00"),
        ("1E+2", "100", "100.00"),
        ("1234567.890000", "1234567.89", "1234567.89"),
        ("0.000001", "0.000001", "0.000001"),
    ],
)
def test_numbers_are_exact_with_trailing_zeros_dropped(value: str, num: str, money: str) -> None:
    assert views.num(D(value)) == num and views.money(D(value)) == money


def test_a_missing_value_is_a_dash_and_nothing_is_ever_a_float() -> None:
    assert views.num(None) == views.money(None) == views.stamp(None) == "-"
    assert views.money(D("0.1") + D("0.2")) == "0.30"  # decimal arithmetic, not 0.30000000000000004
    assert "e" not in views.money(D("0.000001")) and "e" not in views.num(D("1E-7"))


def test_times_are_shown_in_utc_to_the_minute_or_the_second() -> None:
    assert views.stamp(T0) == "2026-10-12 18:00"
    assert views.stamp(T0, seconds=True) == "2026-10-12 18:00:00"


# ------------------------------------------------------------------ server text is never markup


def test_text_from_the_server_cannot_change_the_display() -> None:
    nasty = "Will [bold red]X[/bold red] win? [link=http://evil.example]click[/link] [/]"
    listed = views.markets_table([Market.model_validate(market(event_title=nasty).model_dump())])
    shown = render(listed)
    assert "[bold red]X[/bold red]" in shown  # (the list clips titles at 56 characters)
    detail = render(views.market_view(market(event_title=nasty, rules_primary=nasty), None, []))
    assert detail.count("[bold red]X[/bold red]") == 2
    assert detail.count("[link=http://evil.example]click[/link]") == 2
    trade = Trade.model_validate(
        {
            "time": WHEN,
            "trade_id": "00000000-0000-0000-0000-000000000001",
            "yes_price": "0.5",
            "no_price": "0.5",
            "count": "1",
            "taker_side": "[red]yes[/red]",
            "is_block_trade": False,
        }
    )
    assert "[red]yes[/red]" in render(views.trades_table("T", [trade]))


def test_long_text_is_shortened_with_an_ellipsis_and_empty_text_is_a_dash() -> None:
    assert views.clip("x" * 100, 10).plain == "x" * 9 + "…"
    assert views.clip("short", 10).plain == "short"
    assert views.clip(None, 10).plain == views.clip("", 10).plain == "-"


# ------------------------------------------------------------------ the tables


def test_the_market_list_has_one_row_per_market_with_its_close_time() -> None:
    shown = render(views.markets_table([market(), market(ticker="KXA-E1-Y", close_time=None)]))
    lines = [line for line in shown.splitlines() if line.strip()]
    assert "TICKER" in lines[0] and "CLOSES (UTC)" in lines[0]
    assert "KXA-E1-X" in shown and "2026-10-13 18:00" in shown
    assert lines[-1].split()[0] == "KXA-E1-Y" and lines[-1].rstrip().endswith("-")


def test_a_market_view_shows_what_it_is_its_last_quote_and_its_analytics() -> None:
    tick = Tick.model_validate(
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
    )
    values = [
        AnalyticsValue(
            metric="market_quotes.spread",
            value=D("0.020000"),
            unit="dollars",
            time=T0,
            age_seconds=12,
        )
    ]
    shown = render(views.market_view(market(), tick, values))
    for expected in (
        "KXA-E1-X",
        "active",
        "An event",
        "between  73200",
        ".. 73300",
        "Yes  /  No",
        "price 0.56",
        "bid 0.55 x 12.5",
        "ask 0.57 x 3",
        "volume 1000",
        "open interest 40",
        "market_quotes.spread",
        "0.02",
        "12s",
        "Rules text",
    ):
        assert expected in shown, expected


def test_a_settled_and_reduced_market_says_so() -> None:
    reduced = market(
        stage="slim",
        result="yes",
        settlement_value="1.000000",
        settlement_time=WHEN,
        rules_primary="",
    )
    shown = render(views.market_view(reduced, None, []))
    assert (
        "(kept as: slim)" in shown and "result" in shown and "yes" in shown and "at 1.00" in shown
    )
    assert "rules" not in shown.lower().replace("rules", "", 0) or "Rules text" not in shown


def test_rules_text_is_clipped_unless_all_of_it_is_asked_for() -> None:
    long_rules = "word " * 200
    clipped = render(views.market_view(market(rules_primary=long_rules), None, []))
    assert "(--rules for all)" in clipped
    full = render(views.market_view(market(rules_primary=long_rules), None, [], all_rules=True))
    assert "(--rules for all)" not in full and full.count("word") == 200


def test_candles_from_trades_and_from_tickers_have_their_own_columns() -> None:
    trade_bar = {
        "time": WHEN,
        "open": "0.400000",
        "high": "0.550000",
        "low": "0.300000",
        "close": "0.450000",
        "volume": "17.50",
        "trades": 4,
    }
    tick_bar = {**trade_bar, "yes_bid": "0.440000", "yes_ask": "0.460000", "ticks": 31}
    tick_bar.pop("trades")
    a = render(
        views.candles_table(
            CandlePage.model_validate(
                {
                    "ticker": "T",
                    "interval": "1m",
                    "source": "trades",
                    "bars": [trade_bar],
                    "next_cursor": None,
                }
            )
        )
    )
    assert (
        "TRADES" in a
        and "TICKS" not in a
        and "17.5" in a
        and "0.55" in a
        and "1m bars from trades" in a
    )
    b = render(
        views.candles_table(
            CandlePage.model_validate(
                {
                    "ticker": "T",
                    "interval": "1h",
                    "source": "ticker",
                    "bars": [tick_bar],
                    "next_cursor": None,
                }
            )
        )
    )
    assert "TICKS" in b and "BID" in b and "0.44" in b and "31" in b and "1h bars from ticker" in b


def test_trades_show_both_prices_the_size_and_block_trades() -> None:
    rows = [
        Trade.model_validate(
            {
                "time": WHEN,
                "trade_id": f"00000000-0000-0000-0000-00000000000{n}",
                "yes_price": "0.620000",
                "no_price": "0.380000",
                "count": "250.00",
                "taker_side": "no",
                "is_block_trade": n == 2,
            }
        )
        for n in (1, 2)
    ]
    shown = render(views.trades_table("KXA-E1-X", rows))
    assert shown.count("0.62") == 2 and "0.38" in shown and "250" in shown
    assert shown.count("block") == 1 and "18:00:00" in shown


def book(**changes: object) -> OrderBook:
    base = {
        "ticker": "T",
        "as_of": WHEN,
        "snapshot_time": WHEN,
        "changes_applied": 3,
        "approximate_snapshot": False,
        "complete": True,
        "gaps": [],
        "yes": [{"price": "0.400000", "size": "10.00"}, {"price": "0.390000", "size": "5.50"}],
        "no": [{"price": "0.580000", "size": "7.00"}],
        "best_yes_bid": "0.400000",
        "best_no_bid": "0.580000",
        "best_yes_ask": "0.420000",
        "best_no_ask": "0.600000",
    }
    return OrderBook.model_validate({**base, **changes})


def test_the_book_puts_yes_and_no_bids_side_by_side_with_the_best_prices() -> None:
    shown = render(views.book_view(book(), depth=10))
    assert "YES BID" in shown and "NO BID" in shown
    assert "0.40" in shown and "0.39" in shown and "5.5" in shown and "0.58" in shown
    assert "best YES bid 0.40   best YES ask 0.42" in shown and "3 changes applied" in shown
    assert "WARNING" not in shown
    assert "0.39" not in render(views.book_view(book(), depth=1))  # depth limits each side


def test_an_incomplete_book_is_flagged_with_the_reasons() -> None:
    gap = {"reason": "delta_shed", "from": WHEN, "to": "2026-10-12T18:05:00.000000+00:00"}
    shown = render(views.book_view(book(complete=False, gaps=[gap], approximate_snapshot=True), 10))
    assert "WARNING: this book may be incomplete" in shown
    assert "the snapshot is approximate" in shown and "delta_shed" in shown
    assert "inconsistent" in render(views.book_view(book(complete=False), 10))


def test_analytics_show_dollars_with_cents_and_other_units_plainly() -> None:
    values = [
        AnalyticsValue(
            metric="market_quotes.spread",
            value=D("0.020000"),
            unit="dollars",
            time=T0,
            age_seconds=5,
        ),
        AnalyticsValue(
            metric="market_quotes.bid_size",
            value=D("12.340000"),
            unit="contracts",
            time=T0,
            age_seconds=7,
        ),
        AnalyticsValue(
            metric="my_plugin.score", value=D("-0.000007"), unit=None, time=T0, age_seconds=9
        ),
    ]
    shown = render(views.analytics_table(values, title="KXA-E1-X"))
    assert "0.02" in shown and "12.34" in shown and "-0.000007" in shown
    assert "contracts" in shown and "5s" in shown and "KXA-E1-X" in shown
    points = [HistoryPoint(time=T0, value=D("0.010000"))]
    assert "0.01" in render(views.history_table("T", "a.b", "dollars", points))
    assert "VALUE (unit unknown)" in render(views.history_table("T", "a.b", None, points))


def test_arbitrage_lists_profit_after_fees_with_the_caveat_and_the_count() -> None:
    one = Opportunity(
        event_ticker="KX-E1",
        time=T0,
        bid_sum=D("1.050000"),
        edge=D("0.050000"),
        size=D("10.000000"),
        fees=D("0.480000"),
        profit=D("0.020000"),
        legs=3,
        legs_total=4,
        oldest_quote_age_seconds=45,
    )
    neg = one.model_copy(update={"event_ticker": "KX-E2", "profit": D("-0.260000"), "legs": None})
    shown = render(views.arbitrage_table([one, neg], total=5))
    for expected in ("KX-E1", "1.05", "0.48", "0.02", "-0.26"):
        assert expected in shown, expected
    assert "3/4" in shown and "45s" in shown and "2 of 5 shown" in shown
    assert "re-read the live book before trading" in shown
    assert "found" in render(views.arbitrage_table([one], total=1))


def test_a_strike_without_a_floor_shows_just_its_kind() -> None:
    bare = market(strike_type="structured", floor_strike=None, cap_strike=None)
    shown = render(views.market_view(bare, None, []))
    strike_line = next(line for line in shown.splitlines() if line.startswith("strike"))
    assert strike_line.split() == ["strike", "structured"]
    assert "between  73200 .. 73300" in render(
        views.market_view(market(strike_type="between"), None, [])
    )


def test_no_arbitrage_is_said_in_words_and_still_carries_the_caveat() -> None:
    shown = render(views.arbitrage_table([], total=0))
    assert "no opportunities right now" in shown and "re-read the live book" in shown
    assert "EVENT" not in shown  # no empty table
