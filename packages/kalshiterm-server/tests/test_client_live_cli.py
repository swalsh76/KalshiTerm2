"""``kterm watch`` and ``kterm alerts watch`` against the real server, with data being committed
while they run and (in some tests) the connection cut underneath them."""

import asyncio
import json
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

import pytest
from kalshiterm_client.cli import app
from kalshiterm_client.profiles import ProfileStore
from test_api_stream import A, B
from test_client_stream import Rig, rig_factory  # noqa: F401
from typer.testing import CliRunner

pytestmark = pytest.mark.db
runner = CliRunner()


@contextmanager
def producing(work: Callable[[int], Any], every: float = 0.12) -> Iterator[list[Any]]:
    """Run ``work(i)`` on a thread every ``every`` seconds until the block ends; collect results."""
    stop = threading.Event()
    results: list[Any] = []

    def loop() -> None:
        i = 0
        while not stop.is_set():
            results.append(work(i))
            i += 1
            stop.wait(every)

    thread = threading.Thread(target=loop, daemon=True)
    thread.start()
    try:
        yield results
    finally:
        stop.set()
        thread.join(15)


def kterm(rig: Rig, monkeypatch: pytest.MonkeyPatch, *args: str) -> Any:
    monkeypatch.setenv("KTERM_TOKEN", rig.tokens["alice"])
    result = runner.invoke(app, ["--profile", "rig", *args])
    assert result.exception is None or isinstance(result.exception, SystemExit), result.exception
    return result


def lines(text: str) -> list[str]:
    return [line for line in text.splitlines() if line.strip()]


def test_watch_prints_one_line_per_trade_and_tick_for_the_chosen_markets_only(
    rig_factory: Any,
    monkeypatch: pytest.MonkeyPatch,  # noqa: F811
) -> None:
    with rig_factory() as rig:
        with producing(lambda i: rig.writer.batch(trades=(A, B), tickers=(A, B))):
            result = kterm(rig, monkeypatch, "watch", A, "--count", "6")
        assert result.exit_code == 0, result.output
        out = lines(result.stdout)
        assert (
            len(out) == 6 and all(A in line for line in out) and not any(B in line for line in out)
        )
        assert any("TRADE" in line for line in out) and any("TICK" in line for line in out)
        assert "connected" in result.stderr and "following" in result.stderr


def test_watch_json_is_one_object_per_line_in_cursor_order(
    rig_factory: Any,
    monkeypatch: pytest.MonkeyPatch,  # noqa: F811
) -> None:
    with rig_factory() as rig:
        with producing(lambda i: rig.writer.batch(trades=(A,), tickers=(A,))):
            result = kterm(rig, monkeypatch, "watch", A, "--count", "8", "--json")
        events = [json.loads(line) for line in lines(result.stdout)]
        assert len(events) == 8 and {e["type"] for e in events} == {"trade", "ticker"}
        cursors = [e["cursor"] for e in events]
        assert cursors == sorted(cursors) and len(set(cursors)) == 8
        trade = next(e for e in events if e["type"] == "trade")
        assert trade["yes_price"].count(".") == 1 and trade["ticker"] == A  # exact decimal strings


def test_watch_survives_a_cut_connection_with_nothing_missed_or_repeated(
    rig_factory: Any,
    monkeypatch: pytest.MonkeyPatch,  # noqa: F811
) -> None:
    with rig_factory() as rig:
        committed: list[int] = []

        def work(i: int) -> None:
            committed.extend(rig.writer.batch(trades=(A,))["trades"])
            if i in (8, 16):
                rig.proxy.sever()  # cut the link under the running command

        with producing(work, every=0.1):
            result = kterm(rig, monkeypatch, "watch", A, "--no-ticker", "--count", "30", "--json")
        got = [json.loads(line)["cursor"] for line in lines(result.stdout)]
        assert len(got) == 30 and got == sorted(got) and len(set(got)) == 30
        assert got == [c for c in committed if got[0] <= c <= got[-1]]  # every row in range, once
        assert "disconnected" in result.stderr and "reconnected (resumed" in result.stderr


def test_watch_book_prints_the_book_at_the_end_when_piped(
    rig_factory: Any,
    monkeypatch: pytest.MonkeyPatch,  # noqa: F811
) -> None:
    with rig_factory() as rig:
        rig.writer.batch(snapshots=((A, 1, 1, [(400_000, 1000), (390_000, 500)]),))
        seq = [1]

        def work(i: int) -> None:
            seq[0] += 1
            rig.writer.batch(deltas=((A, 1, seq[0], 410_000, 100),))

        with producing(work):
            result = kterm(
                rig, monkeypatch, "watch", A, "--book", "--no-trades", "--no-ticker", "--count", "4"
            )
        assert result.exit_code == 0, result.output
        assert "YES BID" in result.stdout and "0.41" in result.stdout and "0.40" in result.stdout
        assert "best YES bid 0.41" in result.stdout


def test_watch_needs_something_to_follow_and_says_what_it_could_not_find(
    rig_factory: Any,
    monkeypatch: pytest.MonkeyPatch,  # noqa: F811
) -> None:
    with rig_factory() as rig:
        for args in (
            ["watch"],
            ["watch", A, "--watchlist"],
            ["watch", A, "--no-trades", "--no-ticker"],
        ):
            assert kterm(rig, monkeypatch, *args).exit_code == 1
        with producing(lambda i: rig.writer.batch(tickers=(A,))):
            result = kterm(rig, monkeypatch, "watch", A, "KX-NOPE", "--count", "2")
        assert "not found: KX-NOPE" in result.stderr


def test_alerts_watch_lists_the_unacknowledged_backlog_then_new_alerts_once_each(
    rig_factory: Any,
    monkeypatch: pytest.MonkeyPatch,  # noqa: F811
) -> None:
    with rig_factory() as rig:
        first = [rig.alerts.add("alice")[0] for _ in range(3)]
        rig.alerts.add("bob")  # someone else's: never shown
        direct = rig.direct()

        async def acknowledge_the_middle_one() -> None:
            async with direct:
                await direct.acknowledge_alert(first[1])

        asyncio.run(acknowledge_the_middle_one())
        with producing(lambda i: rig.alerts.add("alice", "bob")[0], every=0.15) as fresh:
            result = kterm(rig, monkeypatch, "alerts", "watch", "--count", "4", "--json")
        ids = [json.loads(line)["id"] for line in lines(result.stdout)]
        assert ids[:2] == [
            first[0],
            first[2],
        ]  # the backlog, oldest first; the acknowledged one is absent
        assert first[1] not in ids and len(ids) == 4 and len(set(ids)) == 4
        assert ids[2:] == fresh[: len(ids) - 2]  # then the new ones, in order
        assert "following your alerts" in result.stderr


def test_alerts_watch_without_the_backlog_and_in_plain_text(
    rig_factory: Any,
    monkeypatch: pytest.MonkeyPatch,  # noqa: F811
) -> None:
    with rig_factory() as rig:
        rig.alerts.add("alice", "alice")
        with producing(lambda i: rig.alerts.add("alice")[0], every=0.15) as fresh:
            result = kterm(rig, monkeypatch, "alerts", "watch", "--no-backlog", "--count", "2")
        out = lines(result.stdout)
        assert len(out) == 2 and all(
            "ALERT #" in line and "arbitrage_bid.profit > 0" in line for line in out
        )
        shown = [int(line.split("ALERT #")[1].split()[0]) for line in out]
        assert shown == sorted(shown) and set(shown) <= set(fresh)  # only the producer's, in order
        assert not {1, 2} & set(shown)  # the two that existed before were not listed


def test_alerts_watch_keeps_every_alert_across_a_cut_connection(
    rig_factory: Any,
    monkeypatch: pytest.MonkeyPatch,  # noqa: F811
) -> None:
    with rig_factory() as rig:
        sent: list[int] = []

        def work(i: int) -> None:
            sent.append(rig.alerts.add("alice")[0])
            if i in (5, 11):
                rig.proxy.sever()

        with producing(work, every=0.1):
            result = kterm(
                rig, monkeypatch, "alerts", "watch", "--no-backlog", "--count", "18", "--json"
            )
        got = [json.loads(line)["id"] for line in lines(result.stdout)]
        assert len(got) == 18 and got == sorted(got) and len(set(got)) == 18
        assert got == [i for i in sent if got[0] <= i <= got[-1]]  # none missed, none repeated
        assert "disconnected" in result.stderr


def test_a_wrong_token_is_one_clear_line(rig_factory: Any, monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: F811
    with rig_factory():
        monkeypatch.setenv("KTERM_TOKEN", "kt_1_" + "x" * 43)
        result = runner.invoke(app, ["--profile", "rig", "watch", A, "--count", "1"])
        assert result.exit_code == 1 and "rejected the token" in result.stderr
        assert "Traceback" not in result.output and ProfileStore().get("rig").url
