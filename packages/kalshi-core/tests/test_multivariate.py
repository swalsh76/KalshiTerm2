from decimal import Decimal
from pathlib import Path

import httpx
import pytest
import respx
from kalshi_core.config import KalshiSettings
from kalshi_core.models import Market
from kalshi_core.rest import KalshiRestClient
from kalshi_core.ws_models import EventLifecycleMsg, LifecycleMsg, WsMessage

BASE = "https://external-api.demo.kalshi.co/trade-api/v2"


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    for name in ("KALSHI_ENV", "KALSHI_KEY_ID", "KALSHI_PRIVATE_KEY_PATH"):
        monkeypatch.delenv(name, raising=False)


# Shapes copied from live production responses (2026-10-06).
MVE_MARKET = {
    "ticker": "KXMVECROSSCATEGORY-S20264B622355CC-ABC",
    "event_ticker": "KXMVECROSSCATEGORY-S20264B622355CC",
    "market_type": "binary",
    "status": "active",
    "mve_collection_ticker": "KXMVECROSSCATEGORY-R",
    "mve_selected_legs": [
        {
            "event_ticker": "KXATPMATCH-26OCT07COPTSI",
            "market_ticker": "KXATPMATCH-26OCT07COPTSI-TSI",
            "side": "yes",
        },
        {
            "event_ticker": "KXCONCACAFNLGAME-26OCT06ANTARU",
            "market_ticker": "KXCONCACAFNLGAME-26OCT06ANTARU-ANT",
            "side": "no",
            "yes_settlement_value_dollars": "1.0000",
        },
    ],
    "is_provisional": False,
    "exchange_index": 1,
}
PLAIN_MARKET = {
    "ticker": "KXNHL2PTOTAL-26OCT06NYINYR-6",
    "event_ticker": "KXNHL2PTOTAL-26OCT06NYINYR",
    "market_type": "binary",
    "status": "active",
}


def test_multivariate_market_legs_are_parsed() -> None:
    m = Market.model_validate(MVE_MARKET)
    assert m.is_multivariate
    assert m.mve_collection_ticker == "KXMVECROSSCATEGORY-R"
    assert m.mve_selected_legs is not None and len(m.mve_selected_legs) == 2
    assert m.mve_selected_legs[0].market_ticker == "KXATPMATCH-26OCT07COPTSI-TSI"
    assert m.mve_selected_legs[0].yes_settlement_value_dollars is None
    assert m.mve_selected_legs[1].yes_settlement_value_dollars == Decimal("1.0000")


def test_ordinary_market_is_not_multivariate() -> None:
    m = Market.model_validate(PLAIN_MARKET)
    assert not m.is_multivariate and m.mve_selected_legs is None


@respx.mock
async def test_mve_filter_is_passed_through() -> None:
    route = respx.get(f"{BASE}/markets").respond(json={"markets": [MVE_MARKET], "cursor": ""})
    async with KalshiRestClient(KalshiSettings()) as c:
        page = await c.markets_page(mve_filter="only", status="open")
    assert route.calls.last.request.url.params["mve_filter"] == "only"
    assert page.markets[0].is_multivariate


@respx.mock
async def test_multivariate_events_use_their_own_endpoint_and_paginate() -> None:
    def event(ticker: str, *, nested: bool = False) -> dict[str, object]:
        e: dict[str, object] = {
            "event_ticker": ticker,
            "series_ticker": "KXMVECROSSCATEGORY-SHARD1",
            "title": "Arsenal vs Coventry",
            "sub_title": "",
            "collateral_return_type": "",
            "mutually_exclusive": False,
        }
        if nested:
            e["markets"] = [MVE_MARKET]
        return e

    route = respx.get(f"{BASE}/events/multivariate").mock(
        side_effect=[
            httpx.Response(200, json={"events": [event("E1", nested=True)], "cursor": "c1"}),
            httpx.Response(200, json={"events": [event("E2")], "cursor": ""}),
        ]
    )
    plain = respx.get(f"{BASE}/events").respond(json={"events": [], "cursor": ""})
    async with KalshiRestClient(KalshiSettings()) as c:
        events = [
            e
            async for e in c.iter_multivariate_events(
                collection_ticker="KXMVECROSSCATEGORY-R", with_nested_markets=True, limit=1
            )
        ]
    assert [e.event_ticker for e in events] == ["E1", "E2"]
    first, second = (call.request.url.params for call in route.calls)
    assert first["collection_ticker"] == "KXMVECROSSCATEGORY-R"
    assert first["with_nested_markets"] == "true" and "cursor" not in first
    assert second["cursor"] == "c1"
    assert events[0].markets is not None and events[0].markets[0].is_multivariate
    assert plain.call_count == 0  # never touches the ordinary events endpoint


def ws(type_: str, msg: dict[str, object], sid: int = 1, seq: int = 1) -> WsMessage:
    return WsMessage(type=type_, sid=sid, seq=seq, msg=msg)


def test_multivariate_lifecycle_messages_parse_from_live_samples() -> None:
    created = ws(
        "multivariate_market_lifecycle",
        {
            "market_ticker": "KXMVECROSSCATEGORY0-S2026318A0FCD16B-D55E742D86B",
            "exchange_index": 1,
            "open_ts": 1791333932,
            "close_ts": 1791923100,
            "additional_metadata": {"name": "", "title": "yes Ted Hurst III: 15+"},
            "event_type": "created",
        },
    ).payload()
    assert isinstance(created, LifecycleMsg)
    assert created.event_type == "created" and created.exchange_index == 1
    assert created.close_ts == 1791923100
    assert created.additional_metadata == {"name": "", "title": "yes Ted Hurst III: 15+"}

    determined = ws(
        "multivariate_market_lifecycle",
        {
            "market_ticker": "M",
            "determination_ts": 1791333932,
            "result": "no",
            "settlement_value": "0.0000",
            "event_type": "determined",
        },
    ).payload()
    assert isinstance(determined, LifecycleMsg)
    assert determined.result == "no" and determined.settlement_value == "0.0000"

    settled = ws(
        "multivariate_market_lifecycle",
        {"market_ticker": "M", "settled_ts": 1791333932, "event_type": "settled"},
    ).payload()
    assert isinstance(settled, LifecycleMsg) and settled.settled_ts == 1791333932


@pytest.mark.parametrize("type_", ["event_lifecycle"])
def test_event_lifecycle_parses_on_either_channel(type_: str) -> None:
    msg = ws(
        type_,
        {
            "event_ticker": "KXMVECROSSCATEGORY-S20267CEA02510A6",
            "exchange_index": 1,
            "title": "Antigua and Barbuda vs Aruba",
            "subtitle": "",
            "collateral_return_type": "",
            "series_ticker": "KXMVECROSSCATEGORY",
        },
    ).payload()
    assert isinstance(msg, EventLifecycleMsg)
    assert msg.event_ticker == "KXMVECROSSCATEGORY-S20267CEA02510A6"
    assert msg.series_ticker == "KXMVECROSSCATEGORY" and msg.title.startswith("Antigua")
