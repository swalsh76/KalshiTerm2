import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from kalshiterm_server.backup import (
    MARKER,
    BackupError,
    Conn,
    check_database_name,
    check_target,
    init_target,
    next_run,
    plan_retention,
    stamp_of,
)

posix_only = pytest.mark.skipif(sys.platform == "win32", reason="POSIX permissions")


def name(day: datetime) -> str:
    return f"kterm-{day:%Y%m%d-%H%M%S}.dump"


def daily(start: datetime, days: int, hour: int = 3) -> list[str]:
    return [name(start.replace(hour=hour) + timedelta(days=i)) for i in range(days)]


NOW = datetime(2026, 9, 29, 12, 0, tzinfo=UTC)


def test_a_name_carries_its_time_and_other_files_have_none() -> None:
    assert stamp_of("kterm-20260929-030000.dump") == datetime(2026, 9, 29, 3, 0, tzinfo=UTC)
    for other in (
        "kterm-20260929.dump",
        "notes.txt",
        "kterm-20260929-030000.dump.json",
        ".partial-x",
    ):
        assert stamp_of(other) is None


def test_seven_daily_and_four_weekly_backups_are_kept_out_of_two_months_of_dailies() -> None:
    names = daily(datetime(2026, 8, 1, tzinfo=UTC), 60)  # 1 Aug .. 29 Sep
    keep, delete = plan_retention(names, NOW)
    kept_days = sorted(stamp_of(n).date().isoformat() for n in keep)  # type: ignore[union-attr]
    assert kept_days == [  # the last seven days, then the newest of each of the four weeks before
        "2026-08-30", "2026-09-06", "2026-09-13", "2026-09-20",
        "2026-09-23", "2026-09-24", "2026-09-25", "2026-09-26",
        "2026-09-27", "2026-09-28", "2026-09-29",
    ]  # fmt: skip
    assert len(keep) + len(delete) == 60 and set(keep).isdisjoint(delete)


def test_only_the_newest_backup_of_each_day_survives() -> None:
    twice = [name(datetime(2026, 9, 29, h, tzinfo=UTC)) for h in (3, 9, 15)]
    keep, delete = plan_retention(twice, NOW)
    assert keep == [twice[2]] and delete == twice[:2]


def test_the_newest_backup_is_never_deleted_however_old() -> None:
    only = [name(datetime(2026, 1, 5, 3, tzinfo=UTC))]
    assert plan_retention(only, NOW) == (only, [])
    stale = daily(datetime(2026, 1, 1, tzinfo=UTC), 3)
    keep, _ = plan_retention(stale, NOW)
    assert stale[-1] in keep


def test_a_young_history_is_kept_whole_and_foreign_files_are_never_listed() -> None:
    names = daily(datetime(2026, 9, 25, tzinfo=UTC), 5) + ["README.txt", "kterm-bad.dump"]
    keep, delete = plan_retention(names, NOW)
    assert len(keep) == 5 and delete == []
    assert "README.txt" not in keep + delete
    assert plan_retention([], NOW) == ([], [])


def test_retention_counts_are_settings() -> None:
    names = daily(datetime(2026, 8, 1, tzinfo=UTC), 60)
    keep, _ = plan_retention(names, NOW, daily=2, weekly=1)
    assert len(keep) == 3  # two recent days and one week


@pytest.mark.parametrize(
    ("now", "expected"),
    [
        (datetime(2026, 9, 29, 6, 0, tzinfo=UTC), datetime(2026, 9, 29, 7, 0, tzinfo=UTC)),
        (datetime(2026, 9, 29, 7, 0, tzinfo=UTC), datetime(2026, 9, 30, 7, 0, tzinfo=UTC)),
        (datetime(2026, 9, 29, 23, 59, tzinfo=UTC), datetime(2026, 9, 30, 7, 0, tzinfo=UTC)),
        (datetime(2026, 12, 31, 8, 0, tzinfo=UTC), datetime(2027, 1, 1, 7, 0, tzinfo=UTC)),
    ],
)
def test_the_next_run_is_the_next_occurrence_of_the_time_of_day(
    now: datetime, expected: datetime
) -> None:
    assert next_run(now, "07:00") == expected


def test_a_target_without_the_marker_is_refused_because_it_is_probably_not_the_nas(
    tmp_path: Path,
) -> None:
    with pytest.raises(BackupError, match="NOT the NAS"):
        check_target(tmp_path)
    with pytest.raises(BackupError, match="does not exist"):
        check_target(tmp_path / "missing")
    init_target(tmp_path)
    init_target(tmp_path)  # idempotent
    assert (tmp_path / MARKER).exists()
    check_target(tmp_path)  # now fine
    assert not [p for p in tmp_path.iterdir() if p.name.startswith(".probe")]


def test_init_target_needs_a_directory(tmp_path: Path) -> None:
    with pytest.raises(BackupError, match="mounted"):
        init_target(tmp_path / "nowhere")


def test_a_target_without_enough_free_space_is_refused_before_anything_is_written(
    tmp_path: Path,
) -> None:
    init_target(tmp_path)
    with pytest.raises(BackupError, match="not enough space"):
        check_target(tmp_path, needed_bytes=10**18)


@posix_only
@pytest.mark.skipif(hasattr(os, "geteuid") and os.geteuid() == 0, reason="root can always write")
def test_a_read_only_target_is_reported(tmp_path: Path) -> None:
    init_target(tmp_path)
    tmp_path.chmod(0o500)
    try:
        with pytest.raises(BackupError, match="not writable"):
            check_target(tmp_path)
    finally:
        tmp_path.chmod(0o700)


def test_the_password_is_passed_in_the_environment_never_on_a_command_line() -> None:
    conn = Conn.from_url("postgresql+asyncpg://kterm:s3cret@db:5432/kterm")
    assert conn == Conn("db", 5432, "kterm", "s3cret", "kterm")
    assert "s3cret" not in " ".join(conn.args()) and conn.env() == {"PGPASSWORD": "s3cret"}
    assert Conn.from_url("postgresql+asyncpg://u:p@h/x", database="other").database == "other"


@pytest.mark.parametrize(
    "bad", ["", "Prod", "1db", "a-b", "a b", 'a"; drop database x; --', "x" * 64]
)
def test_database_names_cannot_smuggle_sql(bad: str) -> None:
    with pytest.raises(BackupError):
        check_database_name(bad)


def test_ordinary_database_names_are_accepted() -> None:
    assert check_database_name("kterm_restored_2") == "kterm_restored_2"
