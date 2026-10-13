import os
from pathlib import Path

import pytest
from kalshiterm_client.trading_state import TradingState, TradingStateError, state_path, utc_day


def test_a_missing_file_is_a_fresh_state(tmp_path: Path) -> None:
    state = TradingState.load(tmp_path / "s.json")
    assert state.day == "" and not state.halted and state.transferred_e6 == 0


def test_everything_survives_a_restart(tmp_path: Path) -> None:
    path = tmp_path / "s.json"
    state = TradingState(path)
    state.roll("2026-10-13")
    state.set_baseline(-1_500_000)
    state.begin_transfer("t-1", 0, 3, 2_000_000)
    state.halt("daily loss")
    state.note_unknown_order("coid-1")
    again = TradingState.load(path)
    assert again.day == "2026-10-13" and again.realized_baseline_e6 == -1_500_000
    assert again.transferred_e6 == 2_000_000 and again.halted and again.halt_reason == "daily loss"
    assert again.unknown_orders == ["coid-1"]
    assert again.pending_transfers == [
        {"id": "t-1", "source": 0, "destination": 3, "amount_e6": 2_000_000}
    ]


@pytest.mark.skipif(os.name != "posix", reason="file modes are POSIX")
def test_the_file_is_private_and_written_atomically(tmp_path: Path) -> None:
    path = tmp_path / "sub" / "s.json"
    TradingState(path).save()
    assert oct(path.stat().st_mode & 0o777) == "0o600"
    assert [p.name for p in path.parent.iterdir()] == ["s.json"]  # no temp files left


@pytest.mark.parametrize(
    "content",
    [
        "",
        "not json",
        "[]",
        "{}",
        '{"day": "x"}',
        '{"day":"d","realized_baseline_e6":"x","transferred_e6":0,"halted":false}',
    ],
)
def test_a_damaged_file_is_an_error_never_a_reset(tmp_path: Path, content: str) -> None:
    path = tmp_path / "s.json"
    path.write_text(content)
    with pytest.raises(TradingStateError, match="refused"):
        TradingState.load(path)
    assert path.read_text() == content  # left for the user to look at


def test_a_new_day_resets_the_counters_but_not_the_halt(tmp_path: Path) -> None:
    state = TradingState(tmp_path / "s.json")
    state.roll("2026-10-13")
    state.set_baseline(5)
    state.begin_transfer("t", 0, 1, 100)
    state.halt("x")
    state.note_unknown_order("c")
    assert not state.roll("2026-10-13")  # same day: nothing changes
    assert state.transferred_e6 == 100
    assert state.roll("2026-10-14")
    assert state.realized_baseline_e6 is None and state.transferred_e6 == 0
    assert state.halted and state.unknown_orders == ["c"] and len(state.pending_transfers) == 1


def test_the_baseline_is_recorded_once_per_day(tmp_path: Path) -> None:
    state = TradingState(tmp_path / "s.json")
    assert state.realized_today_e6(7_000_000) == 0  # no baseline yet
    state.set_baseline(1_000_000)
    state.set_baseline(9_999_999)  # ignored
    assert state.realized_today_e6(400_000) == -600_000


def test_transfers_are_counted_before_they_are_confirmed_and_taken_back_if_refused(
    tmp_path: Path,
) -> None:
    state = TradingState(tmp_path / "s.json")
    state.begin_transfer("a", 0, 1, 300)
    state.begin_transfer("b", 0, 1, 500)
    assert TradingState.load(state.path).transferred_e6 == 800  # saved before any request
    state.finish_transfer("a")  # confirmed: stays counted
    state.abort_transfer("b")  # refused: taken back
    assert state.transferred_e6 == 300 and state.pending_transfers == []
    state.abort_transfer("unknown")  # harmless
    assert state.transferred_e6 == 300


def test_unknown_orders_are_remembered_until_resolved(tmp_path: Path) -> None:
    state = TradingState(tmp_path / "s.json")
    state.note_unknown_order("c1")
    state.note_unknown_order("c1")
    assert state.unknown_orders == ["c1"]
    state.resolve_unknown_order("c1")
    state.resolve_unknown_order("c1")
    assert TradingState.load(state.path).unknown_orders == []


def test_halt_can_be_cleared(tmp_path: Path) -> None:
    state = TradingState(tmp_path / "s.json")
    state.halt("kill switch")
    state.clear_halt()
    assert not TradingState.load(state.path).halted


def test_the_default_path_follows_the_config_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("KTERM_CONFIG_DIR", str(tmp_path))
    assert state_path() == tmp_path / "trading_state.json"


def test_days_are_utc() -> None:
    from datetime import UTC, datetime, timedelta, timezone

    late_in_new_york = datetime(2026, 10, 13, 22, 0, tzinfo=timezone(timedelta(hours=-5)))
    assert utc_day(late_in_new_york) == "2026-10-14"
    assert utc_day(datetime(2026, 10, 13, 23, 59, tzinfo=UTC)) == "2026-10-13"
