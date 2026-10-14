"""``ServerClient``'s layout calls against a scripted server: what goes on the wire, what comes
back, and that a write is never sent twice."""

from collections.abc import AsyncIterator, Iterator
from typing import Any

import pytest
from fake_server import FakeServer
from kalshiterm_client.client import ServerClient
from kalshiterm_client.errors import ConnectionProblem, InvalidRequest, NotFound, ServerError

WHEN = "2026-10-14T12:00:00.000000+00:00"
LIST = {
    "layouts": [{"name": "Main", "size_bytes": 120, "created_at": WHEN, "updated_at": WHEN}],
    "limit": 50,
    "max_bytes": 262144,
}
SAVED = {"name": "My Layout", "created": True, "size_bytes": 9, "updated_at": WHEN}


@pytest.fixture
def server() -> Iterator[FakeServer]:
    fake = FakeServer().start()
    yield fake
    fake.stop()


@pytest.fixture
async def api(server: FakeServer) -> AsyncIterator[ServerClient]:
    async with ServerClient(server.url, "kt_7_" + "Zy9" * 14, sleep=_no_sleep) as client:
        yield client


async def _no_sleep(_: float) -> None:
    return None


async def test_the_list_carries_the_limits(api: ServerClient, server: FakeServer) -> None:
    server.routes["/v1/layouts"] = LIST
    listing = await api.layouts()
    assert [(x.name, x.size_bytes) for x in listing.layouts] == [("Main", 120)]
    assert listing.limit == 50 and listing.max_bytes == 262144
    assert listing.layouts[0].updated_at.tzinfo is not None


async def test_a_layout_is_fetched_by_its_name_percent_encoded(
    api: ServerClient, server: FakeServer
) -> None:
    body = {"version": 3, "price": "0.560000", "panels": [{"id": 1}]}
    server.routes["/v1/layouts/My%20Layout"] = {
        "name": "My Layout", "data": body, "size_bytes": 60, "updated_at": WHEN,
    }  # fmt: skip
    layout = await api.layout("My Layout")
    assert layout.name == "My Layout" and layout.data == body  # prices stay strings
    assert server.requests[0][0] == "/v1/layouts/My%20Layout"


async def test_saving_sends_the_data_in_an_envelope_and_says_whether_it_was_new(
    api: ServerClient, server: FakeServer
) -> None:
    server.routes["PUT /v1/layouts/My%20Layout"] = (201, SAVED)
    saved = await api.save_layout("My Layout", {"a": [1, 2, {"b": "0.5"}]})
    assert saved.created is True and saved.size_bytes == 9
    method, path, body = server.calls[0]
    assert (method, path, body) == (
        "PUT",
        "/v1/layouts/My%20Layout",
        {"data": {"a": [1, 2, {"b": "0.5"}]}},
    )


async def test_deleting(api: ServerClient, server: FakeServer) -> None:
    server.routes["DELETE /v1/layouts/Main"] = {"name": "Main", "deleted": True}
    await api.delete_layout("Main")
    assert server.calls[0][:2] == ("DELETE", "/v1/layouts/Main")


@pytest.mark.parametrize(
    "name",
    [
        "",
        " a",
        "a ",
        "../x",
        "a/b",
        "x" * 65,
        "a\x00b",
        "é",
        "-a",
        ".a",
        "a;b",
        "a?b",
        "a#b",
        "a%20b",
    ],
)
async def test_a_bad_name_is_refused_before_any_request(
    api: ServerClient, server: FakeServer, name: str
) -> None:
    for call in (api.layout(name), api.save_layout(name, {}), api.delete_layout(name)):
        with pytest.raises(ValueError, match="layout name"):
            await call
    assert not server.requests


@pytest.mark.parametrize("name", ["a", "1", "My Layout", "a.b-c_d", "x" * 64])
async def test_good_names_are_accepted(api: ServerClient, server: FakeServer, name: str) -> None:
    server.routes["PUT /v1/layouts/" + name.replace(" ", "%20")] = (200, {**SAVED, "name": name})
    assert (await api.save_layout(name, {})).name == name


@pytest.mark.parametrize("failure", [(500, {"error": "boom"}), (503, {"error": "busy"})])
async def test_a_save_or_delete_is_sent_exactly_once_whatever_goes_wrong(
    api: ServerClient, server: FakeServer, failure: tuple[int, Any]
) -> None:
    server.routes["PUT /v1/layouts/a"] = failure
    server.routes["DELETE /v1/layouts/a"] = failure
    with pytest.raises(ServerError):
        await api.save_layout("a", {})
    with pytest.raises(ServerError):
        await api.delete_layout("a")
    assert [m for m, _, _ in server.calls] == ["PUT", "DELETE"]


async def test_a_dropped_connection_on_a_write_is_not_retried() -> None:
    dead = ServerClient("http://127.0.0.1:1", "kt_7_" + "Zy9" * 14, timeout=1.0, sleep=_no_sleep)
    try:
        with pytest.raises(ConnectionProblem):
            await dead.save_layout("a", {})
    finally:
        await dead.aclose()


async def test_the_servers_reasons_are_kept(api: ServerClient, server: FakeServer) -> None:
    server.routes["PUT /v1/layouts/full"] = (422, {"error": "layout_limit", "limit": 50})
    with pytest.raises(InvalidRequest) as limit:
        await api.save_layout("full", {})
    assert limit.value.code == "layout_limit" and limit.value.extra["limit"] == 50
    server.routes["PUT /v1/layouts/big"] = (
        413, {"error": "layout_too_large", "limit_bytes": 262144, "size_bytes": 300000},
    )  # fmt: skip
    with pytest.raises(InvalidRequest) as big:
        await api.save_layout("big", {})
    assert big.value.code == "layout_too_large" and big.value.extra["limit_bytes"] == 262144
    server.routes["/v1/layouts/gone"] = (404, {"error": "not_found"})
    with pytest.raises(NotFound):
        await api.layout("gone")


def test_a_layout_name_is_percent_encoded_for_the_url() -> None:
    from kalshiterm_client.client import _layout_name

    assert _layout_name("My Layout") == "My%20Layout"
    assert _layout_name("a.b-c_d") == "a.b-c_d"
