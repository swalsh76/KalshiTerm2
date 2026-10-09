import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from fastapi.testclient import TestClient
from kalshiterm_server import auth, db
from kalshiterm_server.api.app import create_app
from kalshiterm_server.config import ServerSettings
from sqlalchemy import text
from timescale_jobs import quiet_background_jobs

pytestmark = pytest.mark.db

T0 = (datetime.now(UTC) - timedelta(hours=2)).replace(microsecond=0)
A, B, C, D, E = "KXA-E1-X", "KXB-E1-Y", "KXC-E1-Z", "KXD-E1-W", "KXE-E1-V"
U = [uuid.UUID(int=i) for i in range(1, 12)]  # ordered ids, so tie order is known


def sec(n: float) -> datetime:
    return T0 + timedelta(seconds=n)


def iso(when: datetime) -> str:
    return when.isoformat(timespec="microseconds")


async def seed(url: str) -> str:
    engine = db.make_engine(url)
    try:
        await quiet_background_jobs(engine)
        async with engine.begin() as conn:
            ids = {}
            for ticker in (A, B, C, D, E):
                ids[ticker] = (
                    await conn.execute(
                        text(
                            "INSERT INTO markets (ticker, event_ticker, market_type, status) "
                            "VALUES (:t, 'KX-E1', 'binary', 'active') RETURNING id"
                        ),
                        {"t": ticker},
                    )
                ).scalar_one()
            a = ids[A]

            async def trade(
                table: str,
                ts: datetime,
                tid: uuid.UUID,
                price: int,
                n: int,
                side: str | None = "yes",
            ) -> None:
                await conn.execute(
                    text(
                        f"INSERT INTO {table} (ts, received_at, market_id, trade_id, yes_price_e6, "
                        "count_e2, taker_side, is_block_trade) VALUES (:ts, :ts, :m, :id, :p, :n, "
                        ":s, false)"
                    ),
                    {"ts": ts, "m": a, "id": tid, "p": price, "n": n, "s": side},
                )

            # three trades share one timestamp; t3 and t4 also sit in the permanent copy
            await trade("trades", sec(1), U[0], 400000, 1000)
            await trade("trades", sec(2), U[1], 410000, 200)
            for tid in U[2:5]:
                await trade("trades", sec(3), tid, 420000, 300, "no")
            await trade("trades", sec(4), U[5], 430000, 50)
            for tid in (U[3], U[4]):
                await trade("trades_watchlist", sec(3), tid, 420000, 300, "no")
            await trade(
                "trades_watchlist", sec(-60 * 86_400), U[6], 100000, 7
            )  # long past retention

            for i, (ts, bid) in enumerate(
                [(1, 10), (2, 11), (3, 12), (3, 13), (3, 14), (3, 15), (4, 16)]
            ):
                await conn.execute(
                    text(
                        "INSERT INTO tickers (ts, received_at, market_id, price_e6, yes_bid_e6, "
                        "yes_ask_e6, yes_bid_size_e2, volume_e2, open_interest_e2) VALUES "
                        "(:ts, :rt, :m, 500000, :b, 600000, 100, :v, 50)"
                    ),
                    {
                        "ts": sec(ts),
                        "rt": sec(ts + i / 10),
                        "m": a,
                        "b": bid * 10_000,
                        "v": 1000 + i,
                    },
                )

            async def snapshot(
                m: int,
                ts: datetime,
                seq: int | None,
                approx: bool,
                yes: Any,
                no: Any,
                sid: int | None = None,
            ) -> None:
                await conn.execute(
                    text(
                        "INSERT INTO orderbook_snapshots (ts, received_at, market_id, seq, "
                        "approximate, yes_prices_e6, yes_sizes_e2, no_prices_e6, no_sizes_e2, sid) "
                        "VALUES (:ts, :ts, :m, :seq, :a, :yp, :ys, :np, :ns, :sid)"
                    ),
                    {
                        "ts": ts, "m": m, "seq": seq, "a": approx, "sid": sid,
                        "yp": [p for p, _ in yes], "ys": [s for _, s in yes],
                        "np": [p for p, _ in no], "ns": [s for _, s in no],
                    },
                )  # fmt: skip

            async def delta(
                m: int,
                ts: datetime,
                seq: int,
                is_yes: bool,
                price: int,
                change: int,
                sid: int | None = None,
            ) -> None:
                await conn.execute(
                    text(
                        "INSERT INTO orderbook_deltas (ts, received_at, market_id, seq, is_yes, "
                        "price_e6, delta_e2, sid) VALUES (:ts, :ts, :m, :seq, :y, :p, :d, :sid)"
                    ),
                    {"ts": ts, "m": m, "seq": seq, "y": is_yes, "p": price, "d": change,
                     "sid": sid},
                )  # fmt: skip

            await snapshot(
                a, sec(0), 10, False, [(400000, 1000), (390000, 500)], [(580000, 700)], 1
            )
            await delta(a, sec(-1), 9, True, 400000, 10_000, 1)  # before the snapshot: ignored
            await delta(a, sec(1), 11, True, 400000, 200, 1)
            await delta(a, sec(2), 12, True, 390000, -500, 1)  # removes a level
            await delta(a, sec(3), 13, False, 570000, 300, 1)  # a new level
            await delta(a, sec(10), 14, True, 410000, 100, 1)
            await snapshot(ids[C], sec(0), None, True, [(500000, 100)], [])  # rebuilt from REST
            await delta(ids[C], sec(0), 99, True, 500000, 7000)  # same instant: unorderable
            await delta(ids[C], sec(1), 1, True, 500000, 50)
            await snapshot(ids[D], sec(0), 5, False, [(500000, 100)], [])
            await delta(ids[D], sec(1), 6, True, 500000, -300)  # more removed than existed

            e = ids[E]  # sequence, not timestamps, decides what follows a snapshot
            await snapshot(e, sec(10), 100, False, [(500000, 1000)], [], 7)
            await delta(e, sec(10) - timedelta(milliseconds=500), 101, True, 500000, 100, 7)
            await delta(e, sec(10) + timedelta(milliseconds=500), 99, True, 500000, 5000, 7)
            await delta(e, sec(11), 500, True, 500000, 900, 3)  # another subscription
            await delta(e, sec(12), 102, True, 500000, 200, 7)

            for reason, status, start, end in (
                ("connection_lost", "done", 4, 6),
                ("delta_shed", "done", 100, 200),
                ("startup", "failed", -3600 * 24 * 30, -3600 * 24 * 30 + 60),
            ):
                await conn.execute(
                    text(
                        "INSERT INTO ingest_gaps (started_at, ended_at, reason, status, "
                        "trades_added, note) VALUES (:s, :e, :r, :st, 3, 'n')"
                    ),
                    {"s": sec(start), "e": sec(end), "r": reason, "st": status},
                )
        await auth.add_user(engine, "alice")
        return (await auth.create_token(engine, "alice", "read"))[1]
    finally:
        await engine.dispose()


@pytest.fixture
def api(migrated_db_url: str) -> Any:
    token = asyncio.run(seed(migrated_db_url))
    with TestClient(create_app(ServerSettings(db_url=migrated_db_url))) as client:
        client.headers["Authorization"] = f"Bearer {token}"
        yield client


def get(api: TestClient, path: str, **params: Any) -> Any:
    response = api.get(path, params={k: v for k, v in params.items() if v is not None})
    assert response.status_code == 200, (path, params, response.text)
    return response.json()


# ---------------------------------------------------------------- trades


def test_trades_come_back_once_each_in_time_order_with_exact_values(api: TestClient) -> None:
    body = get(api, f"/v1/markets/{A}/trades", start=iso(sec(0)), end=iso(sec(10)))
    got = body["trades"]
    assert [t["trade_id"] for t in got] == [str(u) for u in U[:6]]  # t3, t4 not repeated
    assert got[0] == {
        "time": iso(sec(1)), "trade_id": str(U[0]), "yes_price": "0.400000",
        "no_price": "0.600000", "count": "10.00", "taker_side": "yes", "is_block_trade": False,
    }  # fmt: skip
    assert got[2]["taker_side"] == "no" and got[2]["yes_price"] == "0.420000"
    assert body["next_cursor"] is None


def test_a_trade_older_than_the_retention_window_is_found_through_the_permanent_copy(
    api: TestClient,
) -> None:
    body = get(api, f"/v1/markets/{A}/trades", start=iso(sec(-61 * 86_400)), end=iso(sec(0)))
    assert [t["trade_id"] for t in body["trades"]] == [str(U[6])]
    assert body["trades"][0]["count"] == "0.07"


def test_paging_through_trades_that_share_a_timestamp_loses_and_repeats_nothing(
    api: TestClient,
) -> None:
    seen: list[str] = []
    cursor = None
    for _ in range(10):
        body = get(api, f"/v1/markets/{A}/trades", start=iso(sec(0)), limit=2, cursor=cursor)
        seen += [t["trade_id"] for t in body["trades"]]
        cursor = body["next_cursor"]
        if cursor is None:
            break
    assert seen == [
        str(u) for u in U[:6]
    ]  # the three trades at one instant were split across pages


def test_without_a_start_the_most_recent_trades_are_returned_oldest_first(api: TestClient) -> None:
    body = get(api, f"/v1/markets/{A}/trades", limit=3, end=iso(sec(10)))
    assert [t["trade_id"] for t in body["trades"]] == [str(U[3]), str(U[4]), str(U[5])]
    assert body["next_cursor"] is None


def test_a_market_without_trades_is_an_empty_list_and_an_unknown_one_is_404(
    api: TestClient,
) -> None:
    assert get(api, f"/v1/markets/{B}/trades")["trades"] == []
    assert api.get("/v1/markets/NOPE/trades").status_code == 404


# ---------------------------------------------------------------- ticker history


def test_ticks_carry_every_stored_figure(api: TestClient) -> None:
    body = get(api, f"/v1/markets/{A}/ticks", start=iso(sec(0)), end=iso(sec(2.5)))
    assert body["ticks"][0] == {
        "time": iso(sec(1)), "price": "0.500000", "yes_bid": "0.100000", "yes_ask": "0.600000",
        "yes_bid_size": "1.00", "yes_ask_size": None, "volume": "10.00",
        "open_interest": "0.50", "last_trade_size": None,
    }  # fmt: skip
    assert len(body["ticks"]) == 2


def test_paging_through_ticks_with_identical_timestamps_is_exact(api: TestClient) -> None:
    everything = get(api, f"/v1/markets/{A}/ticks", start=iso(sec(0)), limit=1000)["ticks"]
    assert len(everything) == 7
    for page_size in (1, 2, 3, 5):
        collected: list[dict[str, Any]] = []
        cursor = None
        for _ in range(20):
            body = get(
                api, f"/v1/markets/{A}/ticks", start=iso(sec(0)), limit=page_size, cursor=cursor
            )
            collected += body["ticks"]
            cursor = body["next_cursor"]
            if cursor is None:
                break
        assert collected == everything, page_size  # four updates share one instant: none lost


def test_the_latest_ticks_without_a_start(api: TestClient) -> None:
    body = get(api, f"/v1/markets/{A}/ticks", limit=2)
    assert [t["yes_bid"] for t in body["ticks"]] == ["0.150000", "0.160000"]


# ---------------------------------------------------------------- the orderbook


def test_the_book_is_the_snapshot_plus_the_net_of_the_deltas_up_to_the_moment(
    api: TestClient,
) -> None:
    book = get(api, f"/v1/markets/{A}/orderbook", at=iso(sec(3.5)))
    assert book["yes"] == [{"price": "0.400000", "size": "12.00"}]  # +2.00; the 0.39 level is gone
    assert book["no"] == [
        {"price": "0.580000", "size": "7.00"},
        {"price": "0.570000", "size": "3.00"},
    ]
    assert (book["best_yes_bid"], book["best_no_bid"]) == ("0.400000", "0.580000")
    assert (book["best_yes_ask"], book["best_no_ask"]) == ("0.420000", "0.600000")  # 1 - other side
    assert book["snapshot_time"] == iso(sec(0)) and book["changes_applied"] == 3
    assert book["complete"] is True and book["gaps"] == [] and book["approximate_snapshot"] is False


def test_a_later_moment_includes_later_changes_and_the_one_before_the_snapshot_never_counts(
    api: TestClient,
) -> None:
    later = get(api, f"/v1/markets/{A}/orderbook", at=iso(sec(20)))
    assert later["yes"][0] == {"price": "0.410000", "size": "1.00"}
    assert later["best_yes_bid"] == "0.410000" and later["changes_applied"] == 4
    at_snapshot = get(api, f"/v1/markets/{A}/orderbook", at=iso(sec(0)))
    assert at_snapshot["changes_applied"] == 0
    assert at_snapshot["yes"] == [
        {"price": "0.400000", "size": "10.00"},
        {"price": "0.390000", "size": "5.00"},
    ]


def test_depth_limits_the_levels_returned(api: TestClient) -> None:
    book = get(api, f"/v1/markets/{A}/orderbook", at=iso(sec(0)), depth=1)
    assert len(book["yes"]) == 1 and book["yes"][0]["price"] == "0.400000"


def test_a_recorded_outage_after_the_snapshot_marks_the_book_incomplete(api: TestClient) -> None:
    expected = [{"reason": "connection_lost", "from": iso(sec(4)), "to": iso(sec(6))}]
    for moment in (5, 7):  # during the outage (+4 s to +6 s), and after it
        flagged = get(api, f"/v1/markets/{A}/orderbook", at=iso(sec(moment)))
        assert flagged["complete"] is False and flagged["gaps"] == expected, moment
    before_outage = get(api, f"/v1/markets/{A}/orderbook", at=iso(sec(3)))
    assert before_outage["complete"] is True and before_outage["gaps"] == []


def test_a_snapshot_rebuilt_from_rest_is_flagged_and_only_later_deltas_count(
    api: TestClient,
) -> None:
    book = get(api, f"/v1/markets/{C}/orderbook", at=iso(sec(5)))
    assert book["approximate_snapshot"] is True and book["complete"] is False
    # +0.50 arrived after the snapshot and counts; the +70.00 at the snapshot's own instant
    # cannot be ordered against a snapshot with no sequence number, so it is left out
    assert book["yes"] == [{"price": "0.500000", "size": "1.50"}]


def test_a_change_that_removes_more_than_existed_is_flagged_not_hidden(api: TestClient) -> None:
    book = get(api, f"/v1/markets/{D}/orderbook", at=iso(sec(5)))
    assert book["complete"] is False and book["yes"] == []


def test_markets_without_snapshots_or_before_their_first_one_have_no_book(api: TestClient) -> None:
    for path, at in (
        (f"/v1/markets/{B}/orderbook", None),
        (f"/v1/markets/{A}/orderbook", iso(sec(-5))),
    ):
        response = api.get(path, params={"at": at} if at else {})
        assert response.status_code == 404 and response.json() == {"error": "no_orderbook"}


def test_the_default_moment_is_now(api: TestClient) -> None:
    book = get(api, f"/v1/markets/{A}/orderbook")
    assert book["yes"][0]["price"] == "0.410000"  # everything stored so far has been applied


# ---------------------------------------------------------------- the gap log


def test_the_gap_log_lists_recent_gaps_newest_first_with_the_default_window(
    api: TestClient,
) -> None:
    gaps = get(api, "/v1/gaps")["gaps"]
    assert [g["reason"] for g in gaps] == [
        "delta_shed",
        "connection_lost",
    ]  # the 30-day-old one is out
    assert gaps[1] == {
        "id": gaps[1]["id"], "reason": "connection_lost", "status": "done",
        "from": iso(sec(4)), "to": iso(sec(6)), "messages_dropped": 0,
        "trades_recovered": 3, "note": "n",
    }  # fmt: skip


def test_the_gap_log_can_be_asked_about_any_period(api: TestClient) -> None:
    old = get(api, "/v1/gaps", start=iso(sec(-40 * 86_400)), end=iso(sec(-20 * 86_400)))["gaps"]
    assert [g["reason"] for g in old] == ["startup"] and old[0]["status"] == "failed"
    assert get(api, "/v1/gaps", limit=1)["gaps"][0]["reason"] == "delta_shed"


# ---------------------------------------------------------------- shared rules


def test_every_raw_data_route_needs_a_token_and_rejects_bad_parameters(api: TestClient) -> None:
    anonymous = TestClient(api.app)
    paths = [
        f"/v1/markets/{A}/trades",
        f"/v1/markets/{A}/ticks",
        f"/v1/markets/{A}/orderbook",
        "/v1/gaps",
    ]
    for path in paths:
        assert anonymous.get(path).status_code == 401, path
        assert (
            api.get(path, params={"start": "2026-10-08T12:00:00"}).status_code == 422
            or "orderbook" in path
        )
        assert api.get(path, params={"limit": 5000}).status_code == 422 or "orderbook" in path
    assert (
        api.get(f"/v1/markets/{A}/orderbook", params={"at": "2026-10-08T12:00:00"}).status_code
        == 422
    )
    assert api.get(f"/v1/markets/{A}/orderbook", params={"depth": 0}).status_code == 422
    for path in paths[:2]:
        assert api.get(path, params={"cursor": "!!!"}).status_code == 422, path
        assert api.get(path, params={"start": iso(sec(5)), "end": iso(sec(1))}).status_code == 422


def test_sequence_not_timestamps_decides_which_changes_follow_a_snapshot(api: TestClient) -> None:
    """Found live: a change sequenced after the snapshot can be stamped slightly before it."""
    book = get(api, f"/v1/markets/{E}/orderbook", at=iso(sec(20)))
    assert book["yes"] == [{"price": "0.500000", "size": "13.00"}]  # 10.00 + 1.00 + 2.00
    assert book["changes_applied"] == 2  # not the one sequenced before the snapshot (seq 99),
    assert book["complete"] is True  # nor the other subscription's (sid 3)


def test_rows_stored_before_subscription_ids_existed_fall_back_to_timestamps(
    api: TestClient,
) -> None:
    book = get(api, f"/v1/markets/{D}/orderbook", at=iso(sec(5)))  # no sid on these rows
    assert book["changes_applied"] == 1  # the later-timestamped delta still counts
