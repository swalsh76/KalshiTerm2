import asyncio
from typing import Any

import pytest
from fastapi.testclient import TestClient
from kalshiterm_server import auth, db
from kalshiterm_server.api.app import create_app
from kalshiterm_server.config import ServerSettings
from sqlalchemy import text

pytestmark = pytest.mark.db


async def seed(url: str) -> dict[str, str]:
    engine = db.make_engine(url)
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO events (event_ticker, series_ticker, title, mutually_exclusive) "
                    "VALUES ('KX-E1', 'KX', 'An event', true)"
                )
            )
            for ticker in [f"KX-E1-M{i:02d}" for i in range(12)] + ["KXMVECROSS-S1-A"]:
                await conn.execute(
                    text(
                        "INSERT INTO markets (ticker, event_ticker, market_type, status) "
                        "VALUES (:t, 'KX-E1', 'binary', 'active')"
                    ),
                    {"t": ticker},
                )
            await conn.execute(
                text(
                    "INSERT INTO markets (ticker, event_ticker, market_type, status, result, "
                    "settlement_ts) VALUES ('KX-E1-DONE', 'KX-E1', 'binary', 'finalized', 'yes', "
                    "now())"
                )
            )
        tokens = {}
        for name, role in (
            ("alice", "read"),
            ("bob", "read"),
            ("carol", "read"),
            ("root", "admin"),
        ):
            await auth.add_user(engine, name)
            tokens[name] = (await auth.create_token(engine, name, role))[1]
        return tokens
    finally:
        await engine.dispose()


@pytest.fixture
def world(migrated_db_url: str) -> Any:
    tokens = asyncio.run(seed(migrated_db_url))
    settings = ServerSettings(
        db_url=migrated_db_url, watchlist_max_per_user=4, watchlist_max_total=6
    )
    with TestClient(create_app(settings)) as client:
        yield client, tokens


def call(client: TestClient, tokens: dict[str, str], who: str, method: str, path: str) -> Any:
    return client.request(method, path, headers={"Authorization": f"Bearer {tokens[who]}"})


def tickers(client: TestClient, tokens: dict[str, str], who: str) -> list[str]:
    response = call(client, tokens, who, "GET", "/v1/watchlist")
    assert response.status_code == 200
    return [m["ticker"] for m in response.json()["markets"]]


def test_a_user_adds_lists_and_removes_markets_on_their_own_list(world: Any) -> None:
    client, tokens = world
    assert tickers(client, tokens, "alice") == []
    added = call(client, tokens, "alice", "PUT", "/v1/watchlist/KX-E1-M01")
    assert added.status_code == 200 and added.json() == {"ticker": "KX-E1-M01", "added": True}
    call(client, tokens, "alice", "PUT", "/v1/watchlist/KX-E1-M00")
    listing = call(client, tokens, "alice", "GET", "/v1/watchlist").json()
    assert [m["ticker"] for m in listing["markets"]] == ["KX-E1-M00", "KX-E1-M01"]  # by ticker
    assert listing["limit"] == 4
    first = listing["markets"][0]
    assert first["event_title"] == "An event" and first["status"] == "active"
    assert first["capturing"] is False and first["added_at"].endswith("+00:00")

    removed = call(client, tokens, "alice", "DELETE", "/v1/watchlist/KX-E1-M01")
    assert removed.json() == {"ticker": "KX-E1-M01", "removed": True}
    assert tickers(client, tokens, "alice") == ["KX-E1-M00"]


def test_adding_and_removing_are_idempotent(world: Any) -> None:
    client, tokens = world
    call(client, tokens, "alice", "PUT", "/v1/watchlist/KX-E1-M01")
    again = call(client, tokens, "alice", "PUT", "/v1/watchlist/KX-E1-M01")
    assert again.status_code == 200 and again.json()["added"] is False
    assert tickers(client, tokens, "alice") == ["KX-E1-M01"]
    call(client, tokens, "alice", "DELETE", "/v1/watchlist/KX-E1-M01")
    twice = call(client, tokens, "alice", "DELETE", "/v1/watchlist/KX-E1-M01")
    assert twice.status_code == 200 and twice.json()["removed"] is False


def test_lists_are_private_to_each_user_and_even_admins_see_only_their_own(world: Any) -> None:
    client, tokens = world
    call(client, tokens, "alice", "PUT", "/v1/watchlist/KX-E1-M01")
    call(client, tokens, "bob", "PUT", "/v1/watchlist/KX-E1-M02")
    assert tickers(client, tokens, "alice") == ["KX-E1-M01"]
    assert tickers(client, tokens, "bob") == ["KX-E1-M02"]
    assert tickers(client, tokens, "root") == []
    # bob removing alice's market does nothing to alice's list
    assert (
        call(client, tokens, "bob", "DELETE", "/v1/watchlist/KX-E1-M01").json()["removed"] is False
    )
    assert tickers(client, tokens, "alice") == ["KX-E1-M01"]


def test_the_per_user_limit_is_enforced_and_clearly_reported(world: Any) -> None:
    client, tokens = world
    for i in range(4):
        assert call(client, tokens, "alice", "PUT", f"/v1/watchlist/KX-E1-M0{i}").status_code == 200
    over = call(client, tokens, "alice", "PUT", "/v1/watchlist/KX-E1-M04")
    assert over.status_code == 409 and over.json() == {"error": "watchlist_limit", "limit": 4}
    assert (
        call(client, tokens, "alice", "PUT", "/v1/watchlist/KX-E1-M03").status_code == 200
    )  # known
    call(client, tokens, "alice", "DELETE", "/v1/watchlist/KX-E1-M00")  # room again
    assert call(client, tokens, "alice", "PUT", "/v1/watchlist/KX-E1-M04").status_code == 200


def test_the_server_wide_limit_counts_distinct_markets_so_shared_ones_are_free(world: Any) -> None:
    client, tokens = world
    for i in range(4):  # alice: M00..M03
        call(client, tokens, "alice", "PUT", f"/v1/watchlist/KX-E1-M0{i}")
    call(client, tokens, "bob", "PUT", "/v1/watchlist/KX-E1-M04")
    call(client, tokens, "bob", "PUT", "/v1/watchlist/KX-E1-M05")  # six distinct markets now
    full = call(client, tokens, "bob", "PUT", "/v1/watchlist/KX-E1-M06")
    assert full.status_code == 409 and full.json() == {"error": "server_watchlist_full", "limit": 6}
    shared = call(client, tokens, "bob", "PUT", "/v1/watchlist/KX-E1-M00")  # alice already has it
    assert shared.status_code == 200 and shared.json()["added"] is True


def test_combos_settled_and_unknown_markets_are_refused(world: Any) -> None:
    client, tokens = world
    combo = call(client, tokens, "alice", "PUT", "/v1/watchlist/KXMVECROSS-S1-A")
    assert combo.status_code == 422
    assert combo.json() == {"error": "not_watchable", "reason": "combo_market"}
    settled = call(client, tokens, "alice", "PUT", "/v1/watchlist/KX-E1-DONE")
    assert settled.status_code == 422 and settled.json()["reason"] == "settled"
    for method in ("PUT", "DELETE"):
        missing = call(client, tokens, "alice", method, "/v1/watchlist/NOPE")
        assert missing.status_code == 404 and missing.json() == {"error": "not_found"}
    assert tickers(client, tokens, "alice") == []


def test_odd_tickers_in_the_path_are_rejected_before_any_work(world: Any) -> None:
    client, tokens = world
    for bad in ("a b", "x" * 200, "semi;colon", "q%27uote"):
        response = call(client, tokens, "alice", "PUT", f"/v1/watchlist/{bad}")
        assert response.status_code in (404, 422), bad  # 404: the path itself never matched


def test_every_watchlist_route_needs_a_token(world: Any) -> None:
    client, _ = world
    anonymous = TestClient(client.app)
    for method, path in (
        ("GET", "/v1/watchlist"),
        ("PUT", "/v1/watchlist/KX-E1-M01"),
        ("DELETE", "/v1/watchlist/KX-E1-M01"),
    ):
        assert anonymous.request(method, path).status_code == 401, (method, path)


def test_removing_a_user_removes_their_list_and_only_theirs(
    world: Any, migrated_db_url: str
) -> None:
    client, tokens = world
    call(client, tokens, "alice", "PUT", "/v1/watchlist/KX-E1-M01")
    call(client, tokens, "bob", "PUT", "/v1/watchlist/KX-E1-M01")

    async def remove() -> None:
        engine = db.make_engine(migrated_db_url)
        await auth.remove_user(engine, "alice")
        await engine.dispose()

    asyncio.run(remove())
    assert tickers(client, tokens, "bob") == ["KX-E1-M01"]
    count = asyncio.run(_count(migrated_db_url))
    assert count == 1


async def _count(url: str) -> int:
    engine = db.make_engine(url)
    async with engine.connect() as conn:
        n = (await conn.execute(text("SELECT count(*) FROM user_watchlists"))).scalar_one()
    await engine.dispose()
    return int(n)


def test_a_list_shows_whether_the_server_is_capturing_the_market_yet(
    world: Any, migrated_db_url: str
) -> None:
    client, tokens = world
    call(client, tokens, "alice", "PUT", "/v1/watchlist/KX-E1-M01")
    call(client, tokens, "alice", "PUT", "/v1/watchlist/KX-E1-M02")

    async def open_period() -> None:
        engine = db.make_engine(migrated_db_url)
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO watchlist_periods (market_id, source) "
                    "SELECT id, 'user' FROM markets WHERE ticker = 'KX-E1-M01'"
                )
            )
        await engine.dispose()

    asyncio.run(open_period())  # also proves the 'user' source is allowed by the schema
    markets = call(client, tokens, "alice", "GET", "/v1/watchlist").json()["markets"]
    assert {m["ticker"]: m["capturing"] for m in markets} == {"KX-E1-M01": True, "KX-E1-M02": False}


def test_simultaneous_additions_cannot_slip_past_the_server_wide_limit(
    migrated_db_url: str,
) -> None:
    from kalshiterm_server import watchlists

    async def go() -> list[str]:
        tokens = await seed(migrated_db_url)
        assert tokens
        engine = db.make_engine(migrated_db_url)
        users = {}
        async with engine.connect() as conn:
            for name in ("alice", "bob", "carol"):
                users[name] = (
                    await conn.execute(text("SELECT id FROM users WHERE name = :n"), {"n": name})
                ).scalar_one()
        # 3 users x 4 distinct markets each, at once, against a server-wide cap of 6
        jobs = [
            watchlists.add(engine, users[name], f"KX-E1-M{i + 4 * n:02d}", 4, 6)
            for n, name in enumerate(users)
            for i in range(4)
        ]
        results = await asyncio.gather(*jobs, return_exceptions=True)
        async with engine.connect() as conn:
            distinct = (
                await conn.execute(text("SELECT count(DISTINCT market_id) FROM user_watchlists"))
            ).scalar_one()
        await engine.dispose()
        assert distinct == 6  # exactly the cap, never more
        return [type(r).__name__ for r in results]

    outcomes = asyncio.run(go())
    assert outcomes.count("WatchlistError") == 12 - 6
