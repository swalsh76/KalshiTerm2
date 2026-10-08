"""JSON representation of stored values (PLAN §10.3).

Prices and counts leave the server as decimal **strings**, built from the stored integers with
no floating point anywhere, so a client reading them gets exactly what was stored. Timestamps
are ISO-8601 UTC with microseconds.
"""

import base64
import binascii
from datetime import UTC, datetime
from decimal import Decimal

DOLLAR_SCALE = 1_000_000  # *_e6 columns
COUNT_SCALE = 100  # *_e2 columns


def _fixed(value: int | Decimal | None, scale: int, places: int) -> str | None:
    if value is None:
        return None
    if not isinstance(value, int):
        # Postgres' sum() of a bigint is numeric. An integral Decimal is exact; anything else
        # would mean a stored value was not a whole number of units, which must never be rounded.
        if value != value.to_integral_value():
            raise ValueError(f"{value} is not a whole number of units")
        value = int(value)
    sign = "-" if value < 0 else ""
    whole, frac = divmod(abs(value), scale)
    return f"{sign}{whole}.{frac:0{places}d}"


def dollars(e6: int | Decimal | None) -> str | None:
    """``560000`` -> ``"0.560000"``."""
    return _fixed(e6, DOLLAR_SCALE, 6)


def count(e2: int | Decimal | None) -> str | None:
    """``1850`` -> ``"18.50"``."""
    return _fixed(e2, COUNT_SCALE, 2)


def moment(value: datetime | None) -> str | None:
    return None if value is None else value.astimezone(UTC).isoformat(timespec="microseconds")


def encode_cursor(text: str) -> str:
    return base64.urlsafe_b64encode(text.encode()).decode().rstrip("=")


def decode_cursor(cursor: str) -> str | None:
    """The text inside a cursor, or None if it is not one of ours."""
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        # validate=True: the default decoder silently drops illegal characters
        decoded = base64.b64decode(padded.encode(), altchars=b"-_", validate=True).decode()
    except (binascii.Error, UnicodeDecodeError, ValueError):
        return None
    return decoded or None
