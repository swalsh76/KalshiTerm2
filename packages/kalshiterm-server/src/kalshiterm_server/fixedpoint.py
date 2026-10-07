"""The single numeric representation: ``bigint`` fixed-point, converted only at the edges.

Two scales, carried in column names: dollar amounts and strike levels in millionths
(``*_e6``), contract counts in hundredths (``*_e2``). Conversion is exact: a value with more
precision than the scale raises :class:`PrecisionError` instead of being rounded.
"""

from decimal import Decimal, InvalidOperation

E6 = 10**6
E2 = 10**2
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


def to_e6(value: Decimal | float | int | str | None) -> int | None:
    """Dollars (or a strike level) to millionths. ``None`` stays ``None``."""
    return None if value is None else _scale(value, E6)


def to_e2(value: Decimal | float | int | str | None) -> int | None:
    """A contract count to hundredths. ``None`` stays ``None``."""
    return None if value is None else _scale(value, E2)


def from_e6(value: int) -> Decimal:
    return Decimal(value) / E6


def from_e2(value: int) -> Decimal:
    return Decimal(value) / E2
