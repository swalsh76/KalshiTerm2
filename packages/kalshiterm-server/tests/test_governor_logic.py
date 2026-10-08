from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from kalshiterm_server.governor import (
    NORMAL,
    SHEDDING,
    TIGHTENED,
    _days,
    next_mode,
    probe_drive,
    project,
)

T0 = datetime(2026, 10, 8, 12, 0, tzinfo=UTC)
GB = 1024**3


@pytest.mark.parametrize(
    ("mode", "fraction", "expected"),
    [
        (NORMAL, 0.50, NORMAL),
        (NORMAL, 0.79, NORMAL),
        (NORMAL, 0.80, TIGHTENED),
        (NORMAL, 0.95, SHEDDING),  # jumps straight over the middle mode
        (TIGHTENED, 0.75, TIGHTENED),  # between 70% and 80%: hold
        (TIGHTENED, 0.70, TIGHTENED),
        (TIGHTENED, 0.69, NORMAL),
        (TIGHTENED, 0.90, SHEDDING),
        (SHEDDING, 0.89, SHEDDING),  # shedding stops only below 85%
        (SHEDDING, 0.85, SHEDDING),
        (SHEDDING, 0.84, TIGHTENED),
        (SHEDDING, 0.60, NORMAL),  # a large drop (e.g. after retention ran) goes straight down
    ],
)
def test_modes_move_with_hysteresis(mode: str, fraction: float, expected: str) -> None:
    assert next_mode(mode, fraction) == expected


def test_a_value_hovering_around_a_threshold_does_not_flap() -> None:
    mode = NORMAL
    seen = []
    for fraction in (0.79, 0.81, 0.79, 0.81, 0.78, 0.81, 0.72, 0.79):
        mode = next_mode(mode, fraction)
        seen.append(mode)
    assert seen == [NORMAL, TIGHTENED, TIGHTENED, TIGHTENED, TIGHTENED, TIGHTENED] + [TIGHTENED] * 2


def samples(*gb: float, hours: float = 1.0) -> list[tuple[datetime, int]]:
    return [(T0 + timedelta(hours=hours * i), int(g * GB)) for i, g in enumerate(gb)]


def test_projection_gives_the_growth_rate_and_the_days_left() -> None:
    # 1 GB every 6 hours = 4 GB a day; 400 GB used of 500 -> 25 days left
    result = project(samples(397, 398, 399, 400, hours=6)[:4], 500 * GB)
    assert result.bytes_per_day == pytest.approx(4 * GB, rel=1e-6)
    assert result.days_to_full == pytest.approx(25, rel=1e-6)  # 100 GB left at 4 GB/day


def test_projection_ignores_a_noisy_sawtooth_around_a_trend() -> None:
    # compression makes sizes dip; the trend is still +2 GB/day over three days
    data = samples(100, 103, 101, 104, 102, 105, 103, 106, hours=9)
    result = project(data, 500 * GB)
    assert result.bytes_per_day is not None and result.bytes_per_day > 0
    assert result.days_to_full is not None and result.days_to_full > 50


def test_projection_says_so_when_there_is_not_enough_data_or_no_growth() -> None:
    assert project(samples(100, 101), 500 * GB).note.startswith("need 3 samples")
    short = [(T0 + timedelta(minutes=10 * i), (100 + i) * GB) for i in range(4)]
    assert "span only" in project(short, 500 * GB).note
    flat = project(samples(100, 100, 100, 100, hours=6), 500 * GB)
    assert flat.days_to_full is None and flat.note == "not growing"
    shrinking = project(samples(120, 110, 100, 90, hours=6), 500 * GB)
    assert shrinking.days_to_full is None and shrinking.note == "not growing"


def test_already_over_budget_means_zero_days_left() -> None:
    result = project(samples(510, 520, 530, 540, hours=6), 500 * GB)
    assert result.days_to_full == 0.0


def test_the_drive_probe_reports_space_and_proves_it_can_write(tmp_path: Path) -> None:
    total, free, error = probe_drive(str(tmp_path))
    assert error is None and total and free is not None and 0 <= free <= total
    assert list(tmp_path.iterdir()) == []  # the probe file is cleaned up


def test_the_drive_probe_reports_a_missing_or_read_only_location(tmp_path: Path) -> None:
    total, free, error = probe_drive(str(tmp_path / "not-mounted"))
    assert (total, free) == (None, None) and error and "FileNotFoundError" in error
    locked = tmp_path / "ro"
    locked.mkdir()
    locked.chmod(0o500)
    try:
        _, _, error = probe_drive(str(locked))
    finally:
        locked.chmod(0o700)
    assert error is None or "PermissionError" in error  # running as root can still write


def test_retention_windows_are_parsed_only_in_whole_days() -> None:
    assert _days("14 days") == 14 and _days("1 day") == 1
    assert _days("2 weeks") is None and _days("14 days 02:00:00") is None
