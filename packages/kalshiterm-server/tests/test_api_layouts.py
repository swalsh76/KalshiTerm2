import asyncio
import json
from collections.abc import Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient
from kalshiterm_server import auth, db, layouts
from kalshiterm_server.api.app import create_app
from kalshiterm_server.config import ServerSettings
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

pytestmark = pytest.mark.db

MAX_BYTES = 2_000
MAX_LAYOUTS = 3


async def seed(url: str) -> dict[str, str]:
    engine = db.make_engine(url)
    try:
        tokens = {}
        for name, role in (("alice", "read"), ("bob", "read"), ("root", "admin")):
            await auth.add_user(engine, name)
            tokens[name] = (await auth.create_token(engine, name, role))[1]
        return tokens
    finally:
        await engine.dispose()


@pytest.fixture
def world(migrated_db_url: str) -> Iterator[tuple[TestClient, dict[str, str], str]]:
    tokens = asyncio.run(seed(migrated_db_url))
    settings = ServerSettings(
        db_url=migrated_db_url, layouts_max_per_user=MAX_LAYOUTS, layout_max_bytes=MAX_BYTES
    )
    with TestClient(create_app(settings)) as client:
        yield client, tokens, migrated_db_url


def call(
    world: Any, who: str | None, method: str, path: str, body: Any = None, raw: bytes | None = None
) -> Any:
    client, tokens, _ = world
    headers = {} if who is None else {"Authorization": f"Bearer {tokens[who]}"}
    if raw is not None:
        return client.request(method, path, headers=headers, content=raw)
    return client.request(method, path, headers=headers, json=body)


def save(world: Any, who: str, name: str, data: Any) -> Any:
    return call(world, who, "PUT", f"/v1/layouts/{name}", {"data": data})


# --- the basics -----------------------------------------------------------------------------


def test_every_route_needs_a_token(world: Any) -> None:
    for method, path in (
        ("GET", "/v1/layouts"),
        ("GET", "/v1/layouts/a"),
        ("PUT", "/v1/layouts/a"),
        ("DELETE", "/v1/layouts/a"),
    ):
        assert call(world, None, method, path, {"data": {}}).status_code == 401, (method, path)


def test_a_layout_is_saved_listed_fetched_replaced_and_deleted(world: Any) -> None:
    empty = call(world, "alice", "GET", "/v1/layouts").json()
    assert empty == {"layouts": [], "limit": MAX_LAYOUTS, "max_bytes": MAX_BYTES}

    layout = {
        "version": 3,
        "panels": [{"id": "book", "ticker": "KXA-1", "size": [0.5, 12]}],
        "note": "très bien \U0001f4c8",
        "big": 12345678901234567890,
        "price": "0.560000",
    }
    created = save(world, "alice", "My Layout 1", layout)
    assert created.status_code == 201
    body = created.json()
    assert body["name"] == "My Layout 1" and body["created"] is True
    assert body["size_bytes"] == len(
        json.dumps(layout, separators=(",", ":"), ensure_ascii=False).encode()
    )
    fetched = call(world, "alice", "GET", "/v1/layouts/My%20Layout%201").json()
    assert fetched["data"] == layout and fetched["name"] == "My Layout 1"  # exact, big ints too
    assert fetched["updated_at"].endswith("+00:00")

    replaced = save(world, "alice", "My Layout 1", {"version": 4})
    assert replaced.status_code == 200 and replaced.json()["created"] is False
    listing = call(world, "alice", "GET", "/v1/layouts").json()["layouts"]
    assert [(x["name"], x["size_bytes"]) for x in listing] == [("My Layout 1", 13)]
    assert listing[0]["updated_at"] >= listing[0]["created_at"]
    assert call(world, "alice", "GET", "/v1/layouts/My%20Layout%201").json()["data"] == {
        "version": 4
    }

    gone = call(world, "alice", "DELETE", "/v1/layouts/My%20Layout%201")
    assert gone.json() == {"name": "My Layout 1", "deleted": True}
    assert call(world, "alice", "GET", "/v1/layouts/My%20Layout%201").status_code == 404
    assert call(world, "alice", "DELETE", "/v1/layouts/My%20Layout%201").status_code == 404
    assert call(world, "alice", "GET", "/v1/layouts").json()["layouts"] == []


def test_layouts_come_back_sorted_by_name(world: Any) -> None:
    for name in ("zeta", "Alpha", "mid"):
        save(world, "alice", name, {})
    names = [x["name"] for x in call(world, "alice", "GET", "/v1/layouts").json()["layouts"]]
    assert names == sorted(names)


# --- layouts belong to one user -------------------------------------------------------------


def test_two_users_can_use_the_same_name_and_never_see_each_others(world: Any) -> None:
    save(world, "alice", "main", {"owner": "alice"})
    save(world, "bob", "main", {"owner": "bob"})
    save(world, "alice", "only-alice", {"secret": 1})
    assert call(world, "alice", "GET", "/v1/layouts/main").json()["data"] == {"owner": "alice"}
    assert call(world, "bob", "GET", "/v1/layouts/main").json()["data"] == {"owner": "bob"}
    assert [x["name"] for x in call(world, "bob", "GET", "/v1/layouts").json()["layouts"]] == [
        "main"
    ]
    assert call(world, "bob", "GET", "/v1/layouts/only-alice").status_code == 404
    assert call(world, "bob", "DELETE", "/v1/layouts/only-alice").status_code == 404
    assert call(world, "alice", "GET", "/v1/layouts/only-alice").json()["data"] == {"secret": 1}
    # an admin has layouts of their own, not a window into anyone else's
    assert call(world, "root", "GET", "/v1/layouts").json()["layouts"] == []
    assert call(world, "root", "GET", "/v1/layouts/main").status_code == 404


def test_a_replace_by_one_user_leaves_the_other_users_layout_alone(world: Any) -> None:
    save(world, "alice", "main", {"v": 1})
    save(world, "bob", "main", {"v": 2})
    save(world, "bob", "main", {"v": 3})
    assert call(world, "alice", "GET", "/v1/layouts/main").json()["data"] == {"v": 1}


def test_removing_a_user_removes_their_layouts(world: Any) -> None:
    _, _, url = world
    save(world, "bob", "main", {"v": 1})

    async def check() -> int:
        engine = db.make_engine(url)
        try:
            await auth.remove_user(engine, "bob")
            async with engine.connect() as conn:
                return int(
                    (await conn.execute(text("SELECT count(*) FROM ui_layouts"))).scalar_one()
                )
        finally:
            await engine.dispose()

    assert asyncio.run(check()) == 0


# --- names -----------------------------------------------------------------------------------


@pytest.mark.parametrize("name", ["a", "1", "My Layout", "a.b-c_d", "x" * 64, "A b C", "9-9"])
def test_good_names(world: Any, name: str) -> None:
    assert save(world, "alice", name.replace(" ", "%20"), {}).status_code == 201


@pytest.mark.parametrize(
    "name",
    [
        "%20a",
        "a%20",
        "x" * 65,
        "a%00b",
        "%C3%A9",
        "a%3Bb",
        "a%2Fb",
        "-a",
        ".a",
        "a%0Ab",
        "%20",
        "a%5Cb",
    ],
)
def test_bad_names_are_refused_before_anything_is_stored(world: Any, name: str) -> None:
    response = save(world, "alice", name, {})
    assert response.status_code in (404, 422), (name, response.status_code)
    assert call(world, "alice", "GET", "/v1/layouts").json()["layouts"] == []


# --- what a layout may contain ---------------------------------------------------------------


@pytest.mark.parametrize(
    "body",
    [
        {"data": []},
        {"data": "text"},
        {"data": None},
        {"data": 5},
        {},
        {"data": {}, "extra": 1},
        {"layout": {}},
        [],
        "just a string",
    ],
)
def test_the_body_must_be_exactly_an_object_called_data(world: Any, body: Any) -> None:
    response = call(world, "alice", "PUT", "/v1/layouts/a", body)
    assert response.status_code == 422 and response.json()["error"] == "invalid_layout"
    assert call(world, "alice", "GET", "/v1/layouts/a").status_code == 404


def test_a_json_null_body_is_a_422_and_no_body_at_all_a_400(world: Any) -> None:
    null = call(world, "alice", "PUT", "/v1/layouts/a", raw=b"null")
    assert null.status_code == 422 and null.json()["error"] == "invalid_layout"
    assert call(world, "alice", "PUT", "/v1/layouts/a").status_code == 400


def test_invalid_json_and_non_json_numbers_are_a_400(world: Any) -> None:
    for raw in (b"{", b"", b"not json", b'{"data": {"a": NaN}}', b'{"data": {"a": Infinity}}',
                b'{"data": {"a": -Infinity}}'):  # fmt: skip
        response = call(world, "alice", "PUT", "/v1/layouts/a", raw=raw)
        assert response.status_code == 400 and response.json()["error"] == "invalid_json", raw


def nested(depth: int) -> dict[str, Any]:
    node: dict[str, Any] = {}
    for _ in range(depth - 1):
        node = {"k": node}
    return node


def test_nesting_is_limited(world: Any) -> None:
    assert save(world, "alice", "deep64", nested(64)).status_code == 201
    too_deep = save(world, "alice", "deep65", nested(65))
    assert too_deep.status_code == 422 and "deep" in too_deep.json()["reason"]
    in_list = save(world, "alice", "lists", {"a": [[[[nested(62)]]]]})
    assert in_list.status_code == 422  # depth counts arrays too


@pytest.mark.parametrize(
    "raw",
    [
        b'{"data": {"a": "x\\u0000y"}}',
        b'{"data": {"a\\u0000": 1}}',
        b'{"data": {"a": ["ok", "bad\\u0000"]}}',
    ],
)
def test_a_nul_character_anywhere_is_refused_not_a_500(world: Any, raw: bytes) -> None:
    response = call(world, "alice", "PUT", "/v1/layouts/a", raw=raw)
    assert response.status_code == 422 and "NUL" in response.json()["reason"]


# --- size and count ---------------------------------------------------------------------------


def sized(total: int) -> dict[str, str]:
    """A layout whose compact JSON is exactly ``total`` bytes."""
    overhead = len(json.dumps({"p": ""}, separators=(",", ":")))
    return {"p": "x" * (total - overhead)}


def test_the_size_limit_is_exact(world: Any) -> None:
    assert save(world, "alice", "exact", sized(MAX_BYTES)).status_code == 201
    over = save(world, "alice", "over", sized(MAX_BYTES + 1))
    assert over.status_code == 413
    assert over.json() == {
        "error": "layout_too_large",
        "limit_bytes": MAX_BYTES,
        "size_bytes": MAX_BYTES + 1,
    }
    assert call(world, "alice", "GET", "/v1/layouts/over").status_code == 404


def test_size_is_measured_in_bytes_not_characters(world: Any) -> None:
    just_fits = {"p": "é" * ((MAX_BYTES - 8) // 2)}  # two bytes each
    assert save(world, "alice", "fits", just_fits).status_code == 201
    too_big = {"p": "\U0001f4c8" * (MAX_BYTES // 3)}  # four bytes each
    assert save(world, "alice", "big", too_big).status_code == 413


def test_a_huge_upload_is_cut_off_without_being_read_to_the_end() -> None:
    """The test client buffers a whole request before the app sees it, so the reader is exercised
    directly: a stream that would never end is abandoned after a few chunks."""
    from kalshiterm_server.api.errors import ApiError
    from kalshiterm_server.api.layouts import read_limited

    pulled = 0

    class Endless:
        headers: dict[str, str] = {}  # no Content-Length: a chunked upload

        async def stream(self) -> Any:
            nonlocal pulled
            while True:
                pulled += 1
                yield b"x" * 1024

    with pytest.raises(ApiError) as info:
        asyncio.run(read_limited(Endless(), 10_000))  # type: ignore[arg-type]
    assert info.value.status == 413 and info.value.extra == {"limit_bytes": 10_000}
    assert pulled == 10  # 10 x 1 KiB crossed 10,000 bytes; nothing further was read


def test_an_honest_stream_within_the_limit_is_read_whole() -> None:
    from kalshiterm_server.api.layouts import read_limited

    class Chunked:
        headers: dict[str, str] = {}

        async def stream(self) -> Any:
            for part in (b'{"da', b'ta": ', b"{}}"):
                yield part

    assert asyncio.run(read_limited(Chunked(), 100)) == b'{"data": {}}'  # type: ignore[arg-type]


def test_an_oversized_upload_reports_the_real_layout_cap_not_the_read_cutoff(world: Any) -> None:
    client, tokens, _ = world
    headers = {"Authorization": f"Bearer {tokens['alice']}"}
    body = json.dumps({"data": {"p": "x" * (MAX_BYTES * 10)}}).encode()
    declared = client.put("/v1/layouts/a", headers=headers, content=body)
    streamed = client.put(
        "/v1/layouts/a", headers=headers, content=iter([body[:5000], body[5000:]])
    )
    for response in (declared, streamed):
        assert response.status_code == 413
        assert response.json() == {"error": "layout_too_large", "limit_bytes": MAX_BYTES}


def test_a_declared_length_over_the_limit_is_refused_at_once(world: Any) -> None:
    client, tokens, _ = world
    response = client.put(
        "/v1/layouts/a",
        headers={"Authorization": f"Bearer {tokens['alice']}", "Content-Length": "99999999"},
        content=b"{}",
    )
    assert response.status_code in (400, 413)


def test_the_count_limit_applies_to_new_names_only(world: Any) -> None:
    for name in ("a", "b", "c"):
        assert save(world, "alice", name, {"n": name}).status_code == 201
    fourth = save(world, "alice", "d", {})
    assert fourth.status_code == 422
    assert fourth.json() == {"error": "layout_limit", "limit": MAX_LAYOUTS}
    assert save(world, "alice", "b", {"n": "changed"}).status_code == 200  # replacing is fine
    assert save(world, "bob", "a", {}).status_code == 201  # the cap is per user
    call(world, "alice", "DELETE", "/v1/layouts/a")
    assert save(world, "alice", "d", {}).status_code == 201


def test_simultaneous_saves_cannot_slip_past_the_count_limit(world: Any) -> None:
    _, tokens, url = world

    async def race() -> list[str]:
        engine = db.make_engine(url)
        try:
            async with engine.connect() as conn:
                user_id = int(
                    (
                        await conn.execute(text("SELECT id FROM users WHERE name = 'alice'"))
                    ).scalar_one()
                )

            async def attempt(index: int) -> str:
                try:
                    await layouts.save_layout(
                        engine, user_id, f"race-{index}", {"i": index},
                        max_bytes=MAX_BYTES, max_layouts=MAX_LAYOUTS,
                    )  # fmt: skip
                    return "saved"
                except layouts.LayoutError as exc:
                    return exc.code

            return list(await asyncio.gather(*(attempt(i) for i in range(12))))
        finally:
            await engine.dispose()

    results = asyncio.run(race())
    assert results.count("saved") == MAX_LAYOUTS and results.count("layout_limit") == 9
    assert len(call(world, "alice", "GET", "/v1/layouts").json()["layouts"]) == MAX_LAYOUTS
    _ = tokens


# --- the database is a second line of defence ------------------------------------------------


def test_the_table_itself_refuses_nonsense(world: Any) -> None:
    _, _, url = world

    async def insert(name: str, data: str, size: int) -> None:
        engine = db.make_engine(url)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text(
                        "INSERT INTO ui_layouts (user_id, name, data, size_bytes) "
                        "SELECT id, :n, CAST(:d AS jsonb), :s FROM users WHERE name = 'alice'"
                    ),
                    {"n": name, "d": data, "s": size},
                )
        finally:
            await engine.dispose()

    for name, data, size in (("", "{}", 2), ("x" * 65, "{}", 2), ("a", "[]", 2), ("a", "{}", 0),
                             ("a", "{}", 2_000_000)):  # fmt: skip
        with pytest.raises(DBAPIError):
            asyncio.run(insert(name, data, size))
