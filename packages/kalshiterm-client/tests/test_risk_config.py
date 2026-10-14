from pathlib import Path

import pytest
from kalshiterm_client.risk import RiskPolicy
from kalshiterm_client.risk_config import RiskConfigError, load_policy, risk_path


def write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "risk.toml"
    path.write_text(text)
    return path


def test_no_file_means_the_tiny_defaults(tmp_path: Path) -> None:
    policy = load_policy(tmp_path / "missing.toml")
    assert policy == RiskPolicy()
    assert policy.production_enabled is False
    assert policy.max_order_notional_e6 == 1_000_000 and policy.max_open_orders == 3


def test_values_are_read_exactly(tmp_path: Path) -> None:
    policy = load_policy(
        write(
            tmp_path,
            '[limits]\nmax_order_count = "2.5"\nmax_position = 10\n'
            'max_order_notional = "0.123456"\n'
            'max_daily_loss = "7"\nmax_open_orders = 9\nsnapshot_max_age_seconds = 3\n'
            "[trading]\nproduction_enabled = true\n",
        )
    )
    assert policy.max_order_count_e2 == 250 and policy.max_position_e2 == 1000
    assert policy.max_order_notional_e6 == 123_456 and policy.max_daily_loss_e6 == 7_000_000
    assert policy.max_open_orders == 9 and policy.snapshot_max_age_seconds == 3
    assert policy.production_enabled is True
    assert policy.max_open_exposure_e6 == RiskPolicy().max_open_exposure_e6  # untouched


@pytest.mark.parametrize(
    ("text", "fragment"),
    [
        ("[limits]\nmax_daily_loss = 2.5\n", "max_daily_loss"),  # a TOML float
        ('[limits]\nmax_daily_loss = "-1"\n', "zero or more"),
        ('[limits]\nmax_daily_loss = "abc"\n', "not a number"),
        ('[limits]\nmax_daily_loss = "NaN"\n', "zero or more"),
        ('[limits]\nmax_daily_loss = "1.0000001"\n', "6 decimal places"),
        ('[limits]\nmax_position = "1.001"\n', "2 decimal places"),
        ("[limits]\nmax_open_orders = -1\n", "max_open_orders"),
        ("[limits]\nmax_open_orders = true\n", "max_open_orders"),
        ("[limits]\nmax_open_orderz = 1\n", "unknown limit"),
        ('[trading]\nproduction_enabled = "yes"\n', "true or false"),
        ("[trading]\nlive = true\n", "unknown setting"),
        ("[other]\nx = 1\n", "unknown section"),
        ("[limits\n", "not valid TOML"),
    ],
)
def test_mistakes_are_refused_with_the_key_named(tmp_path: Path, text: str, fragment: str) -> None:
    with pytest.raises(RiskConfigError, match=fragment):
        load_policy(write(tmp_path, text))


def test_the_file_lives_in_the_config_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("KTERM_CONFIG_DIR", str(tmp_path))
    assert risk_path() == tmp_path / "risk.toml"
    (tmp_path / "risk.toml").write_text('[limits]\nmax_daily_loss = "1"\n')
    assert load_policy().max_daily_loss_e6 == 1_000_000


def test_the_ui_timers_default_and_can_be_set(tmp_path: Path) -> None:
    assert load_policy(tmp_path / "none.toml").ui_idle_minutes == 30
    assert load_policy(tmp_path / "none.toml").ui_disconnect_seconds == 60
    policy = load_policy(
        write(tmp_path, "[trading]\nui_idle_minutes = 5\nui_disconnect_seconds = 120\n")
    )
    assert (policy.ui_idle_minutes, policy.ui_disconnect_seconds) == (5, 120)


@pytest.mark.parametrize(
    "line",
    [
        "ui_idle_minutes = 0",
        "ui_idle_minutes = 1441",
        "ui_idle_minutes = -1",
        'ui_idle_minutes = "30"',
        "ui_idle_minutes = 2.5",
        "ui_idle_minutes = true",
        "ui_disconnect_seconds = 4",
        "ui_disconnect_seconds = 3601",
        "ui_idle_seconds = 30",
    ],
)
def test_bad_ui_timers_are_refused_with_the_key_named(tmp_path: Path, line: str) -> None:
    with pytest.raises(RiskConfigError, match=line.split(" ")[0]):
        load_policy(write(tmp_path, f"[trading]\n{line}\n"))
