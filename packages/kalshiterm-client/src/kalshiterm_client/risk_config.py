"""Loading the risk limits from ``risk.toml`` (in the same config directory as the profiles).

The file is the only way to change a limit; there is no command-line override, so a limit cannot
be loosened by a typo in a command. A missing file means the tiny built-in defaults. Mistakes are
errors, never silently ignored: an unknown key, a negative value or a float is refused, and the
message names the key. Money is written as a string of dollars (``"1.00"``), counts as a whole or
decimal number of contracts (``"2"``, ``"0.5"``); TOML floats are refused so a number never goes
through binary rounding.

    [limits]
    max_order_count = "5"          # contracts per order
    max_position = "10"            # contracts per market, resting orders included
    max_order_notional = "1.00"    # worst-case cash per order, fee included
    max_open_exposure = "3.00"
    max_daily_loss = "2.00"
    max_open_orders = 3
    max_transfer_per_order = "2.00"
    max_transfer_per_day = "5.00"
    snapshot_max_age_seconds = 10

    [trading]
    production_enabled = false
    ui_idle_minutes = 30           # the browser UI switches trading off after this much inactivity
    ui_disconnect_seconds = 60     # ... or this long after the last browser connection has gone
"""

import tomllib
from dataclasses import replace
from decimal import Decimal, InvalidOperation
from pathlib import Path

from kalshiterm_client.profiles import ProfileError, config_dir
from kalshiterm_client.risk import RiskPolicy

FILE_NAME = "risk.toml"
MONEY = {
    "max_order_notional": "max_order_notional_e6",
    "max_open_exposure": "max_open_exposure_e6",
    "max_daily_loss": "max_daily_loss_e6",
    "max_transfer_per_order": "max_transfer_per_order_e6",
    "max_transfer_per_day": "max_transfer_per_day_e6",
}
COUNTS = {"max_order_count": "max_order_count_e2", "max_position": "max_position_e2"}
UI_TIMERS = {"ui_idle_minutes": (1, 1440), "ui_disconnect_seconds": (5, 3600)}
INTEGERS = {
    "max_open_orders": "max_open_orders",
    "snapshot_max_age_seconds": "snapshot_max_age_seconds",
}


class RiskConfigError(ProfileError):
    """The risk file is wrong; the message names the key and the file."""


def risk_path() -> Path:
    return config_dir() / FILE_NAME


def _decimal(key: str, value: object, places: int) -> int:
    """A non-negative decimal (string or int) as an integer scaled by ``10**places``."""
    if isinstance(value, bool | float) or not isinstance(value, str | int):
        raise RiskConfigError(f'{key}: write it as a string such as "1.00", not {value!r}')
    try:
        number = Decimal(value)
    except InvalidOperation:
        raise RiskConfigError(f"{key}: {value!r} is not a number") from None
    if not number.is_finite() or number < 0:
        raise RiskConfigError(f"{key}: must be zero or more, not {value!r}")
    scaled = number * 10**places
    if scaled != scaled.to_integral_value():
        raise RiskConfigError(f"{key}: at most {places} decimal places, not {value!r}")
    return int(scaled)


def parse_policy(data: dict[str, object], source: str = FILE_NAME) -> RiskPolicy:
    allowed = {"limits", "trading"}
    unknown = set(data) - allowed
    if unknown:
        raise RiskConfigError(f"{source}: unknown section {sorted(unknown)[0]!r}")
    limits = data.get("limits", {})
    trading = data.get("trading", {})
    if not isinstance(limits, dict) or not isinstance(trading, dict):
        raise RiskConfigError(f"{source}: [limits] and [trading] must be tables")

    policy = RiskPolicy()
    changes: dict[str, object] = {}
    for key, value in limits.items():
        if key in MONEY:
            changes[MONEY[key]] = _decimal(key, value, 6)
        elif key in COUNTS:
            changes[COUNTS[key]] = _decimal(key, value, 2)
        elif key in INTEGERS:
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise RiskConfigError(f"{key}: must be a whole number, zero or more")
            changes[INTEGERS[key]] = value
        else:
            raise RiskConfigError(f"{source}: unknown limit {key!r}")
    for key, value in trading.items():
        if key == "production_enabled":
            if not isinstance(value, bool):
                raise RiskConfigError("production_enabled: must be true or false")
            changes["production_enabled"] = value
        elif key in UI_TIMERS:
            low, high = UI_TIMERS[key]
            if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
                raise RiskConfigError(f"{key}: must be a whole number from {low} to {high}")
            changes[key] = value
        else:
            raise RiskConfigError(f"{source}: unknown setting {key!r}")
    return replace(policy, **changes)  # type: ignore[arg-type]


def load_policy(path: Path | None = None) -> RiskPolicy:
    target = path or risk_path()
    try:
        raw = target.read_bytes()
    except FileNotFoundError:
        return RiskPolicy()
    except OSError as exc:
        raise RiskConfigError(f"cannot read {target}: {exc.strerror}") from None
    try:
        data = tomllib.loads(raw.decode("utf-8"))
    except (tomllib.TOMLDecodeError, UnicodeDecodeError) as exc:
        raise RiskConfigError(f"{target} is not valid TOML: {exc}") from None
    return parse_policy(data, str(target))
