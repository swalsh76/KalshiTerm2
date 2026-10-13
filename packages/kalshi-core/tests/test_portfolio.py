"""Account reads. The fixtures carry exactly the field sets measured on a live account on
2026-10-13 (values are invented), and every test fails if a payload has a field the model lacks."""

from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
import respx
from kalshi_core.config import KalshiSettings
from kalshi_core.portfolio import (
    Balance,
    Fill,
    MarketPosition,
    Order,
    QueuePosition,
    Settlement,
)
from kalshi_core.rest import KalshiRestClient
from pydantic import BaseModel

BASE = "https://external-api.demo.kalshi.co/trade-api/v2"

BALANCE = {
    "balance": 1034,
    "balance_dollars": "10.3490",
    "portfolio_value": 250,
    "updated_ts": 1791908624,
    "balance_breakdown": [
        {"balance": "10.3490", "exchange_index": 0},
        {"balance": "0.0000", "exchange_index": 1},
        {"balance": "2.5000", "exchange_index": 3},
    ],
}
POSITION = {
    "ticker": "KXA-1",
    "exchange_index": 0,
    "total_traded_dollars": "1.250000",
    "position_fp": "-3.00",
    "market_exposure_dollars": "1.500000",
    "realized_pnl_dollars": "-0.100000",
    "fees_paid_dollars": "0.020000",
    "last_updated_ts": "2026-10-13T12:00:00Z",
}
EVENT_POSITION = {
    "event_ticker": "KXA",
    "total_cost_dollars": "1.25",
    "total_cost_shares_fp": "3.00",
    "event_exposure_dollars": "1.5",
    "realized_pnl_dollars": "0",
    "fees_paid_dollars": "0.02",
}
ORDER = {
    "action": "sell",
    "book_side": "ask",
    "client_order_id": "e2300918-c088-4100-992a-6eee2b09dd29",
    "created_time": "2026-10-02T05:07:05.899187Z",
    "exchange_index": 0,
    "fill_count_fp": "1.00",
    "initial_count_fp": "2.00",
    "last_update_time": "2026-10-02T05:07:05.899187Z",
    "maker_fees_dollars": "0.000000",
    "maker_fill_cost_dollars": "0.000000",
    "no_price_dollars": "0.9900",
    "order_id": "01a0fb02-44a8-776b-9801-c464cfe31239",
    "outcome_side": "no",
    "remaining_count_fp": "1.00",
    "self_trade_prevention_type": "taker_at_cross",
    "side": "yes",
    "status": "resting",
    "subaccount_number": 0,
    "taker_fees_dollars": "0.000700",
    "taker_fill_cost_dollars": "0.990000",
    "ticker": "KXA-1",
    "type": "limit",
    "user_id": "u-1",
    "yes_price_dollars": "0.0100",
}
FILL = {
    "action": "sell",
    "book_side": "ask",
    "count_fp": "1.00",
    "created_time": "2026-10-02T05:07:05.899187Z",
    "exchange_index": 0,
    "fee_cost": "0.000700",
    "fill_id": "07231bca-c049-a753-f68e-059a8e3283ad",
    "is_taker": True,
    "market_ticker": "KXA-1",
    "no_price_dollars": "0.9900",
    "order_id": "01a0fb02-44a8-776b-9801-c464cfe31239",
    "outcome_side": "no",
    "side": "no",
    "subaccount_number": 0,
    "ticker": "KXA-1",
    "trade_id": "07231bca-c049-a753-f68e-059a8e3283ad",
    "ts": 1790917625,
    "yes_price_dollars": "0.0100",
}
SETTLEMENT = {
    "event_ticker": "KXA-26OCT02",
    "exchange_index": 0,
    "fee_cost": "0.008400",
    "market_result": "no",
    "no_count_fp": "4.00",
    "no_total_cost_dollars": "3.960000",
    "revenue": 0,
    "settled_time": "2026-10-03T13:09:17.122067Z",
    "ticker": "KXA-1",
    "value": 0,
    "yes_count_fp": "4.00",
    "yes_total_cost_dollars": "0.080000",
}
QUEUE = {"order_id": "o-1", "market_ticker": "KXA-1", "queue_position_fp": "12.00"}


def missing_fields(model: type[BaseModel], payload: dict[str, Any]) -> list[str]:
    """Payload keys the model does not declare (Kalshi added a field, or the model is lacking)."""
    return sorted(set(payload) - set(model.model_fields))


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    for name in ("KALSHI_ENV", "KALSHI_KEY_ID", "KALSHI_PRIVATE_KEY_PATH"):
        monkeypatch.delenv(name, raising=False)


@pytest.mark.parametrize(
    ("model", "payload"),
    [
        (Balance, BALANCE),
        (MarketPosition, POSITION),
        (Order, ORDER),
        (Fill, FILL),
        (Settlement, SETTLEMENT),
        (QueuePosition, QUEUE),
    ],
)
def test_the_models_declare_every_field_kalshi_sends(
    model: type[BaseModel], payload: dict[str, Any]
) -> None:
    assert missing_fields(model, payload) == []


def test_the_check_has_teeth() -> None:
    assert missing_fields(Order, {**ORDER, "new_kalshi_field": 1}) == ["new_kalshi_field"]


@respx.mock
async def test_balance_is_exact_and_per_shard() -> None:
    route = respx.get(f"{BASE}/portfolio/balance").respond(json=BALANCE)
    async with KalshiRestClient(KalshiSettings()) as c:
        balance = await c.balance(exchange_index=None)
    assert route.called
    assert balance.balance == 1034 and balance.balance_dollars == Decimal("10.3490")
    assert balance.shard(3) == Decimal("2.5") and balance.shard(0) == Decimal("10.349")
    assert balance.shard(2) == 0  # not listed
    assert isinstance(balance.balance_dollars, Decimal)


@respx.mock
async def test_positions_orders_fills_settlements_parse_and_follow_cursors() -> None:
    respx.get(f"{BASE}/portfolio/positions").respond(
        json={
            "market_positions": [POSITION],
            "event_positions": [EVENT_POSITION],
            "cursor": "",
        }
    )
    page_one = {"orders": [ORDER], "cursor": "c1"}
    page_two = {"orders": [{**ORDER, "order_id": "o-2", "status": "executed"}], "cursor": ""}
    orders = respx.get(f"{BASE}/portfolio/orders")
    orders.side_effect = [
        respx.MockResponse(200, json=page_one),
        respx.MockResponse(200, json=page_two),
    ]
    respx.get(f"{BASE}/portfolio/fills").respond(json={"fills": [FILL], "cursor": ""})
    respx.get(f"{BASE}/portfolio/settlements").respond(
        json={"settlements": [SETTLEMENT], "cursor": ""}
    )
    async with KalshiRestClient(KalshiSettings()) as c:
        positions = [p async for p in c.iter_positions()]
        page = await c.positions_page()
        all_orders = [o async for o in c.iter_orders(limit=1)]
        fills = [f async for f in c.iter_fills()]
        settlements = [s async for s in c.iter_settlements()]
    assert positions[0].position_fp == Decimal("-3.00")  # long NO is negative
    assert page.event_positions[0].event_exposure_dollars == Decimal("1.5")
    assert [o.order_id for o in all_orders] == [ORDER["order_id"], "o-2"]
    assert orders.calls[1].request.url.params["cursor"] == "c1"
    assert fills[0].fee_cost == Decimal("0.0007") and fills[0].is_taker
    assert settlements[0].yes_count_fp == 4 and settlements[0].revenue == 0


def test_an_order_keeps_both_prices_and_is_resting_only_while_resting() -> None:
    order = Order.model_validate(ORDER)
    assert order.yes_price_dollars == Decimal("0.0100") and order.no_price_dollars == Decimal(
        "0.9900"
    )
    assert order.is_resting
    done = Order.model_validate({**ORDER, "status": "executed"})
    assert not done.is_resting


@respx.mock
async def test_a_single_order_and_queue_positions() -> None:
    respx.get(f"{BASE}/portfolio/orders/o-9").respond(json={"order": {**ORDER, "order_id": "o-9"}})
    queue = respx.get(f"{BASE}/portfolio/orders/queue_positions").respond(
        json={"queue_positions": [QUEUE]}
    )
    async with KalshiRestClient(KalshiSettings()) as c:
        order = await c.order("o-9")
        positions = await c.queue_positions(market_tickers="KXA-1,KXB-1")
    assert order.order_id == "o-9"
    assert positions[0].queue_position_fp == Decimal("12.00")
    assert queue.calls.last.request.url.params["market_tickers"] == "KXA-1,KXB-1"
