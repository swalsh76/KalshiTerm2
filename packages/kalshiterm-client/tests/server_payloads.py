"""Smallest valid JSON bodies for the KalshiTerm server's endpoints (required fields only).

The contract tests (``test_client_contract``) already prove these shapes against the real server;
here they only have to be valid for the client's models.
"""

from typing import Any

WHEN = "2026-10-12T18:00:00.000000+00:00"
TICKER = "KXA-E1-X"
EVENT = "KXA-E1"


def market(ticker: str = TICKER, **changes: Any) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "event_ticker": EVENT,
        "type": "binary",
        "status": "active",
        "yes_label": "Yes",
        "no_label": "No",
        "stage": "full",
        **changes,
    }


def detail(ticker: str = TICKER) -> dict[str, Any]:
    return {**market(ticker), "rules_primary": "The rules", "rules_secondary": ""}


BAR = {"time": WHEN, "open": "0.400000", "high": "0.550000", "low": "0.300000",
       "close": "0.450000", "volume": "17.50", "trades": 4}  # fmt: skip

TRADE = {
    "time": WHEN,
    "trade_id": "0b3f1a72-2c1f-4f5e-9b6d-5a0e8c1d7e11",
    "yes_price": "0.560000",
    "no_price": "0.440000",
    "count": "2.50",
    "is_block_trade": False,
}
TICK = {"time": WHEN, "price": "0.560000", "yes_bid": "0.550000", "yes_ask": "0.570000"}
BOOK = {
    "ticker": TICKER,
    "as_of": WHEN,
    "snapshot_time": WHEN,
    "changes_applied": 3,
    "approximate_snapshot": False,
    "complete": True,
    "gaps": [],
    "yes": [{"price": "0.550000", "size": "10.00"}],
    "no": [{"price": "0.430000", "size": "7.00"}],
}
GAP = {"id": 1, "reason": "connection_lost", "status": "done", "start": WHEN, "end": WHEN,
       "messages_dropped": 0, "trades_recovered": 12, "note": ""}  # fmt: skip
WATCHLIST = {
    "markets": [
        {
            "ticker": TICKER,
            "event_title": "An event",
            "status": "active",
            "added_at": WHEN,
            "capturing": True,
        }  # fmt: skip
    ],
    "limit": 100,
}
VALUE = {"metric": "spread", "value": "0.020000", "unit": "dollars", "time": WHEN, "age_seconds": 3}
RESULTS = {"ticker": TICKER, "as_of": WHEN, "results": [VALUE]}
EVENT_RESULTS = {"event_ticker": EVENT, "as_of": WHEN, "results": [VALUE]}
HISTORY = {"ticker": TICKER, "metric": "spread", "unit": "dollars",
           "points": [{"time": WHEN, "value": "0.020000"}], "next_cursor": None}  # fmt: skip
OPPORTUNITIES = {
    "as_of": WHEN,
    "total": 1,
    "opportunities": [{"event_ticker": EVENT, "time": WHEN, "edge": "0.030000", "profit": "-0.01"}],
}
METRICS = {
    "metrics": [
        {
            "metric": "spread",
            "subject": "market",
            "unit": "dollars",
            "ops": [">", "<"],
            "meaning": "bid-ask spread",
        }  # fmt: skip
    ]
}
RULE = {
    "id": 5,
    "name": "wide spread",
    "metric": "spread",
    "op": ">",
    "threshold": "0.100000",
    "cooldown_seconds": 300,
    "enabled": True,
    "active": True,
    "created_at": WHEN,
    "ticker": TICKER,
}
RULES = {"rules": [RULE], "limit": 50}
ALERT = {
    "id": 9,
    "rule_name": "wide spread",
    "fired_at": WHEN,
    "metric": "spread",
    "op": ">",
    "threshold": "0.100000",
    "value": "0.120000",
    "ticker": TICKER,
}
ALERTS = {"alerts": [ALERT], "next_cursor": None}
