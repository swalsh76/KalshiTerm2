"""ServerClient behaviour against a scripted transport: retries, errors, paging, parameters."""

import re
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx
import pytest
from kalshiterm_client import client as client_module
from kalshiterm_client import secrets
from kalshiterm_client.client import ServerClient
from kalshiterm_client.errors import (
    ConnectionProblem,
    Forbidden,
    InvalidRequest,
    NotFound,
    RateLimited,
    ServerError,
    Unauthorized,
)
from kalshiterm_client.models import Market
from kalshiterm_client.profiles import Profile, ProfileError

TOKEN = "kt_7_" + "Zy9" * 14
MARKET = {
    "ticker": "KXA-E1-X",
    "event_ticker": "KXA-E1",
    "series_ticker": "KXA",
    "event_title": "An event",
    "type": "binary",
    "status": "active",
    "yes_label": "Yes",
    "no_label": "No",
    "open_time": "2026-10-01T00:00:00.000000+00:00",
    "close_time": None,
    "expiration_time": None,
    "result": None,
    "settlement_value": None,
    "settlement_time": None,
    "strike_type": "greater",
    "floor_strike": "118749.900000",
    "cap_strike": None,
    "stage": "full",
}


class Server:
    """A scripted server: pops one reply per request and records what was asked."""

    def __init__(self, *replies: Any) -> None:
        self.replies = list(replies)
        self.requests: list[httpx.Request] = []
        self.sleeps: list[float] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        reply = self.replies.pop(0) if len(self.replies) > 1 else self.replies[0]
        if isinstance(reply, Exception):
            raise reply
        if isinstance(reply, httpx.Response):
            return reply
        return httpx.Response(200, json=reply)

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)

    def client(self, **kw: Any) -> ServerClient:
        return ServerClient(
            "https://example.test",
            TOKEN,
            transport=httpx.MockTransport(self.handler),
            sleep=self.sleep,
            **kw,
        )


def error(
    status: int, code: str, headers: dict[str, str] | None = None, **extra: Any
) -> httpx.Response:
    return httpx.Response(status, json={"error": code, **extra}, headers=headers)


@pytest.fixture
def real_delays(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(client_module, "RETRY_DELAYS", (0.5, 1.0, 2.0))


# ------------------------------------------------------------------ requests


async def test_every_request_carries_the_token_and_only_the_given_parameters() -> None:
    server = Server({"markets": [MARKET], "next_cursor": None})
    async with server.client() as api:
        await api.markets_page(status="active", q=None, limit=5)
    [request] = server.requests
    assert (
        request.headers["authorization"] == f"Bearer {TOKEN}"
        and request.headers["user-agent"] == "kterm"
    )
    assert dict(request.url.params) == {"status": "active", "limit": "5"}  # None is "not given"
    assert request.url.path == "/v1/markets"


async def test_times_are_sent_as_iso_and_naive_ones_are_refused_before_any_request() -> None:
    server = Server({"ticker": "T", "trades": [], "next_cursor": None})
    start = datetime(2026, 10, 12, 8, 30, tzinfo=UTC)
    async with server.client() as api:
        await api.trades_page("T", start=start, limit=10)
        with pytest.raises(ValueError, match="timezone-aware"):
            await api.trades_page("T", start=datetime(2026, 10, 12, 8, 30))
    assert len(server.requests) == 1
    assert server.requests[0].url.params["start"] == "2026-10-12T08:30:00+00:00"


async def test_prices_arrive_as_exact_decimals_never_floats() -> None:
    server = Server({"markets": [MARKET], "next_cursor": None})
    async with server.client() as api:
        [market] = (await api.markets_page()).markets
    assert isinstance(market, Market) and market.floor_strike == Decimal("118749.900000")
    assert str(market.floor_strike) == "118749.900000" and market.stage == "full"
    assert market.open_time == datetime(2026, 10, 1, tzinfo=UTC)


# ------------------------------------------------------------------ retries


async def test_a_get_is_retried_after_server_errors_with_growing_waits(real_delays: None) -> None:
    server = Server(
        error(500, "x"), error(503, "x"), {"user": "alice", "role": "read", "token_id": 1}
    )
    async with server.client() as api:
        assert (await api.me()).user == "alice"
    assert len(server.requests) == 3 and server.sleeps == [0.5, 1.0]


async def test_a_server_that_keeps_failing_is_reported_after_a_bounded_number_of_tries(
    real_delays: None,
) -> None:
    server = Server(error(500, "x"))
    async with server.client() as api:
        with pytest.raises(ServerError, match="the server answered 500"):
            await api.me()
    assert len(server.requests) == 4 and server.sleeps == [0.5, 1.0, 2.0]


async def test_a_short_rate_limit_wait_is_honoured_and_a_long_one_is_reported() -> None:
    server = Server(
        error(429, "too_many_failed_attempts", {"Retry-After": "2"}),
        {"user": "a", "role": "read", "token_id": 1},
    )
    async with server.client() as api:
        await api.me()
    assert server.sleeps == [2.0] and len(server.requests) == 2

    long = Server(error(429, "too_many_failed_attempts", {"Retry-After": "42"}))
    async with long.client() as api:
        with pytest.raises(RateLimited, match="retry in 42 s") as caught:
            await api.me()
    assert caught.value.retry_after == 42.0 and len(long.requests) == 1 and long.sleeps == []


async def test_network_errors_are_retried_and_then_reported_without_the_token() -> None:
    flaky = Server(httpx.ConnectError("refused"), {"user": "a", "role": "read", "token_id": 1})
    async with flaky.client() as api:
        assert (await api.me()).role == "read"
    dead = Server(httpx.ConnectError("refused"))
    async with dead.client() as api:
        with pytest.raises(ConnectionProblem, match="cannot reach https://example.test") as caught:
            await api.me()
    assert len(dead.requests) == 4 and TOKEN not in str(caught.value)


@pytest.mark.parametrize(
    ("response", "kind", "text"),
    [
        (error(401, "unauthorized"), Unauthorized, "rejected the token"),
        (error(403, "forbidden"), Forbidden, "needs an admin token"),
        (error(404, "no_orderbook"), NotFound, "no_orderbook"),
        (
            error(422, "invalid_parameter", fields=["cursor"]),
            InvalidRequest,
            "invalid_parameter (cursor)",
        ),
        (error(409, "rule_limit", limit=50), InvalidRequest, "rule_limit"),
    ],
)
async def test_client_errors_are_mapped_and_never_retried(
    response: httpx.Response, kind: type[Exception], text: str
) -> None:
    server = Server(response)
    async with server.client() as api:
        with pytest.raises(kind, match=re.escape(text)) as caught:
            await api.me()
    assert len(server.requests) == 1 and TOKEN not in str(caught.value)
    assert getattr(caught.value, "code", None) == response.json()["error"]


async def test_the_servers_extra_error_detail_is_kept_for_the_caller() -> None:
    server = Server(
        error(422, "invalid_parameter", fields=["start", "end"], reason="start_after_end")
    )
    async with server.client() as api:
        with pytest.raises(InvalidRequest) as caught:
            await api.me()
    assert caught.value.extra == {"fields": ["start", "end"], "reason": "start_after_end"}
    assert caught.value.status == 422


async def test_writes_are_never_retried() -> None:
    server = Server(error(500, "x"))
    async with server.client() as api:
        with pytest.raises(ServerError):
            await api._request("POST", "/v1/alert-rules", json={"a": 1})
    assert len(server.requests) == 1  # a retried POST could create the rule twice


async def test_an_answer_that_is_not_what_the_client_expects_says_so_without_echoing_it() -> None:
    not_json = Server(httpx.Response(200, text="<html>nope</html>"))
    async with not_json.client() as api:
        with pytest.raises(ConnectionProblem, match="not JSON"):
            await api.me()
    secret = "do-not-print-me"
    wrong = Server({"user": secret, "role": "read"})  # token_id missing
    async with wrong.client() as api:
        with pytest.raises(
            ConnectionProblem, match=r"Me is not what this client expects \(token_id\)"
        ) as caught:
            await api.me()
    assert secret not in str(caught.value) and "older than the server" in str(caught.value)


# ------------------------------------------------------------------ other endpoints


async def test_status_keeps_the_whole_report_and_types_the_problems() -> None:
    report = {
        "time": "2026-10-12T12:00:00+00:00",
        "problems": ["disk 91% full"],
        "storage": {"x": 1},
    }
    async with Server(report).client() as api:
        status = await api.status()
    assert status.problems == ["disk 91% full"] and status.data["storage"] == {"x": 1}
    assert status.time == datetime(2026, 10, 12, 12, tzinfo=UTC)


async def test_gaps_and_the_orderbook_map_the_servers_from_and_to_fields() -> None:
    gap = {
        "id": 3,
        "reason": "connection_lost",
        "status": "done",
        "from": "2026-10-12T10:00:00+00:00",
        "to": "2026-10-12T10:01:00+00:00",
        "messages_dropped": 4,
        "trades_recovered": 9,
        "note": "",
    }
    async with Server({"gaps": [gap]}).client() as api:
        [parsed] = await api.gaps()
    assert parsed.start == datetime(2026, 10, 12, 10, tzinfo=UTC) and parsed.trades_recovered == 9
    book = {
        "ticker": "T",
        "as_of": "2026-10-12T10:00:00+00:00",
        "snapshot_time": "2026-10-12T09:59:00+00:00",
        "changes_applied": 2,
        "approximate_snapshot": False,
        "complete": False,
        "gaps": [{"reason": "delta_shed", "from": gap["from"], "to": gap["to"]}],
        "yes": [{"price": "0.400000", "size": "10.00"}],
        "no": [],
        "best_yes_bid": "0.400000",
        "best_no_bid": None,
        "best_yes_ask": None,
        "best_no_ask": "0.600000",
    }
    async with Server(book).client() as api:
        parsed_book = await api.orderbook("T", depth=5)
    assert (
        parsed_book.yes[0].size == Decimal("10.00") and parsed_book.gaps[0].reason == "delta_shed"
    )
    assert parsed_book.complete is False and parsed_book.best_yes_ask is None


# ------------------------------------------------------------------ paging


def pages(key: str, items: list[Any], size: int) -> Callable[[httpx.Request], httpx.Response]:
    """A server that serves ``items`` in pages of ``size``, cursor = the offset."""

    def handler(request: httpx.Request) -> httpx.Response:
        offset = int(request.url.params.get("cursor", "0"))
        chunk = items[offset : offset + size]
        more = offset + size < len(items)
        body = {
            "ticker": "T",
            "interval": "1m",
            "source": "trades",
            key: chunk,
            "next_cursor": str(offset + size) if more else None,
        }
        return httpx.Response(200, json=body)

    return handler


async def test_iterators_follow_the_cursor_to_the_end_and_stop() -> None:
    markets = [{**MARKET, "ticker": f"KXA-E1-{n:02d}"} for n in range(7)]

    def handler(request: httpx.Request) -> httpx.Response:
        offset = int(request.url.params.get("cursor", "0"))
        chunk = markets[offset : offset + 3]
        return httpx.Response(
            200, json={"markets": chunk, "next_cursor": str(offset + 3) if offset + 3 < 7 else None}
        )  # noqa: E501

    api = ServerClient("https://example.test", TOKEN, transport=httpx.MockTransport(handler))
    async with api:
        seen = [m.ticker async for m in api.iter_markets(status="active")]
    assert seen == [f"KXA-E1-{n:02d}" for n in range(7)]


async def test_trade_candle_and_tick_iterators_page_the_same_way() -> None:
    trade = {
        "time": "2026-10-12T10:00:00+00:00",
        "trade_id": "00000000-0000-0000-0000-000000000001",
        "yes_price": "0.500000",
        "no_price": "0.500000",
        "count": "1.00",
        "taker_side": "yes",
        "is_block_trade": False,
    }
    bar = {
        "time": "2026-10-12T10:00:00+00:00",
        "open": "0.5",
        "high": "0.6",
        "low": "0.4",
        "close": "0.5",
        "volume": "3.00",
        "trades": 2,
    }
    tick = {
        "time": "2026-10-12T10:00:00+00:00",
        "price": "0.5",
        "yes_bid": "0.49",
        "yes_ask": "0.51",
        "yes_bid_size": None,
        "yes_ask_size": None,
        "volume": "1.00",
        "open_interest": "2.00",
        "last_trade_size": None,
    }
    for key, item, method in (
        ("trades", trade, "iter_trades"),
        ("bars", bar, "iter_candles"),
        ("ticks", tick, "iter_ticks"),
    ):
        handler = pages(key, [item] * 5, 2)
        api = ServerClient("https://example.test", TOKEN, transport=httpx.MockTransport(handler))
        async with api:
            got = [
                x async for x in getattr(api, method)("T", start=datetime(2026, 10, 12, tzinfo=UTC))
            ]
        assert len(got) == 5, method


# ------------------------------------------------------------------ for_profile


def profile(url: str = "https://studio:8700", fingerprint: str | None = None) -> Profile:
    return Profile("lab", url, fingerprint)


def test_a_client_for_a_profile_needs_a_token_and_a_pinned_certificate() -> None:
    with pytest.raises(ConnectionProblem, match="no token for lab"):
        ServerClient.for_profile(profile())
    secrets.set_token("lab", TOKEN)
    with pytest.raises(ProfileError, match="no trusted certificate yet"):
        ServerClient.for_profile(profile())  # https without a pin never connects


async def test_loopback_http_profiles_need_no_pin_and_use_the_stored_token() -> None:
    secrets.set_token("lab", TOKEN)
    api = ServerClient.for_profile(profile("http://127.0.0.1:8700"))
    async with api:
        assert api.base_url == "http://127.0.0.1:8700"
    explicit = ServerClient.for_profile(profile("http://127.0.0.1:8700"), token="kt_1_" + "A" * 43)
    await explicit.aclose()
