from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from kalshiterm_server.ingest.watchlist import Entry, load_config, plan

NOW = datetime(2026, 10, 7, 12, 0, tzinfo=UTC)
DWELL = timedelta(hours=12)


def held(**sources: str) -> dict[str, Entry]:
    """Markets currently watched: ticker -> source, all added 13 hours ago."""
    return {t: Entry(s, NOW - timedelta(hours=13)) for t, s in sources.items()}


def test_manual_markets_and_the_top_list_are_added_and_manual_wins_a_tie() -> None:
    todo = plan(frozenset({"M1", "M2"}), ["M2", "A1", "A2"], {}, NOW)
    assert todo.add == {"M1": "manual", "M2": "manual", "A1": "auto", "A2": "auto"}
    assert todo.remove == []


def test_nothing_changes_when_the_list_is_already_right() -> None:
    todo = plan(frozenset({"M1"}), ["A1"], held(M1="manual", A1="auto"), NOW)
    assert todo.add == {} and todo.remove == []


def test_an_auto_market_that_fell_out_of_the_top_stays_until_the_dwell_is_over() -> None:
    fresh = {"A1": Entry("auto", NOW - timedelta(hours=11, minutes=59))}
    assert plan(frozenset(), [], fresh, NOW, DWELL).remove == []
    old = {"A1": Entry("auto", NOW - DWELL)}
    assert plan(frozenset(), [], old, NOW, DWELL).remove == ["A1"]


def test_an_auto_market_still_in_the_top_is_never_removed() -> None:
    assert plan(frozenset(), ["A1"], held(A1="auto"), NOW).remove == []


def test_a_manual_market_is_never_auto_removed_but_leaves_at_once_when_the_file_drops_it() -> None:
    assert plan(frozenset({"M1"}), [], held(M1="manual"), NOW).remove == []
    fresh = {"M1": Entry("manual", NOW)}  # no dwell for manual removals
    assert plan(frozenset(), [], fresh, NOW).remove == ["M1"]


def test_a_market_promoted_from_auto_to_manual_is_kept_whatever_the_top_says() -> None:
    assert plan(frozenset({"A1"}), [], held(A1="auto"), NOW).remove == []


def write(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "watchlist.toml"
    path.write_text(body)
    return path


def test_the_config_file_is_parsed_with_defaults(tmp_path: Path) -> None:
    config = load_config(write(tmp_path, '[watchlist]\nmarkets = ["KXA-E1-X", " KXB-E2-Y "]\n'))
    assert config.markets == {"KXA-E1-X", "KXB-E2-Y"}
    assert config.auto_top_n == 0
    assert config.auto_window == timedelta(hours=1)
    full = load_config(write(tmp_path, "[watchlist]\nauto_top_n = 25\nauto_window_minutes = 30\n"))
    assert (full.auto_top_n, full.auto_window) == (25, timedelta(minutes=30))
    assert load_config(write(tmp_path, "")).markets == frozenset()


@pytest.mark.parametrize(
    "body",
    [
        "[watchlist\n",
        '[watchlist]\nmarkets = "KXA"\n',
        "[watchlist]\nmarkets = [1]\n",
        "[watchlist]\nauto_top_n = -1\n",
        "[watchlist]\nauto_top_n = true\n",
        '[watchlist]\nauto_top_n = "50"\n',
        "[watchlist]\nauto_window_minutes = 0\n",
    ],
)
def test_a_bad_config_file_is_rejected_with_the_file_name(tmp_path: Path, body: str) -> None:
    path = write(tmp_path, body)
    with pytest.raises(ValueError, match="watchlist.toml"):
        load_config(path)


# ---------------------------------------------------------------- users' lists


def test_markets_only_users_asked_for_are_added_as_user_markets() -> None:
    todo = plan(frozenset({"M1"}), ["A1"], {}, NOW, users=frozenset({"U1", "M1"}))
    assert todo.add == {"M1": "manual", "U1": "user", "A1": "auto"}  # the file's claim wins


def test_a_market_a_user_wants_is_never_removed_whatever_else_changes() -> None:
    current = held(U1="user", M1="manual", A1="auto")
    # the file dropped M1, the top-N dropped A1, but users still want all three
    users = frozenset({"U1", "M1", "A1"})
    assert plan(frozenset(), [], current, NOW, users=users).remove == []


def test_when_the_last_user_leaves_the_market_goes_only_after_the_dwell() -> None:
    fresh = {"U1": Entry("user", NOW - timedelta(hours=1))}
    assert plan(frozenset(), [], fresh, NOW, DWELL).remove == []  # added an hour ago: stays
    old = {"U1": Entry("user", NOW - DWELL)}
    assert plan(frozenset(), [], old, NOW, DWELL).remove == ["U1"]


def test_a_user_adding_and_removing_repeatedly_cannot_churn_the_feed() -> None:
    entry = {"U1": Entry("user", NOW - timedelta(minutes=5))}
    for wanted in (frozenset({"U1"}), frozenset(), frozenset({"U1"}), frozenset()):
        todo = plan(frozenset(), [], entry, NOW, DWELL, users=wanted)
        assert todo.add == {} and todo.remove == []  # nothing is added or dropped meanwhile


def test_a_market_dropped_from_the_file_but_still_in_the_top_stays() -> None:
    current = {"X": Entry("manual", NOW)}
    assert plan(frozenset(), ["X"], current, NOW, DWELL).remove == []
    assert plan(frozenset(), [], current, NOW, DWELL).remove == ["X"]  # nobody wants it: at once
