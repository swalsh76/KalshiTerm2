"""The single numeric representation: ``bigint`` fixed-point, converted only at the edges.

Two scales, carried in column and field names: dollar amounts and strike levels in millionths
(``*_e6``), contract counts in hundredths (``*_e2``). Conversion is exact: a value with more
precision than the scale raises :class:`PrecisionError` instead of being rounded.

This is the one place that knows these scales. The server stores them, the client does its risk
arithmetic in them and the fee model uses them, so they must agree.
"""

from decimal import Decimal, InvalidOperation
from typing import overload

E6 = 10**6  # dollars (and strike levels) in millionths; also "one dollar" as an amount
E2 = 10**2  # contracts in hundredths
E2_TO_E6 = E6 // E2  # a count in hundredths, rescaled to millionths (for values stored as e6)
_BIGINT_MAX = 2**63 - 1


class PrecisionError(ValueError):
    """The value cannot be represented exactly at the column's scale."""


def _to_decimal(value: Decimal | float | int | str) -> Decimal:
    if isinstance(value, float):
        value = repr(value)  # shortest round-trip form: 0.1 -> '0.1', not 0.1000000000000000055
    try:
        number = Decimal(value)
    except InvalidOperation as exc:
        raise PrecisionError(f"not a number: {value!r}") from exc
    if not number.is_finite():
        raise PrecisionError(f"not a finite number: {value!r}")
    return number


def _scale(value: Decimal | float | int | str, factor: int) -> int:
    scaled = _to_decimal(value) * factor
    if scaled != scaled.to_integral_value():
        raise PrecisionError(f"{value!r} has more precision than 1/{factor}")
    result = int(scaled)
    if abs(result) > _BIGINT_MAX:
        raise PrecisionError(f"{value!r} does not fit in a bigint at scale 1/{factor}")
    return result


@overload
def to_e6(value: None) -> None: ...
@overload
def to_e6(value: Decimal | float | int | str) -> int: ...
def to_e6(value: Decimal | float | int | str | None) -> int | None:
    """Dollars (or a strike level) to millionths. ``None`` stays ``None``."""
    return None if value is None else _scale(value, E6)


@overload
def to_e2(value: None) -> None: ...
@overload
def to_e2(value: Decimal | float | int | str) -> int: ...
def to_e2(value: Decimal | float | int | str | None) -> int | None:
    """A contract count to hundredths. ``None`` stays ``None``."""
    return None if value is None else _scale(value, E2)


def from_e6(value: int) -> Decimal:
    """Millionths back to a dollar amount."""
    return Decimal(value) / E6


def from_e2(value: int) -> Decimal:
    """Hundredths back to a contract count."""
    return Decimal(value) / E2
