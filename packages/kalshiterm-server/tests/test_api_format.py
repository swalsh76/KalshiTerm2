from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from kalshiterm_server.api.format import (
    count,
    decode_cursor,
    dollars,
    encode_cursor,
    moment,
)


@pytest.mark.parametrize(
    ("e6", "expected"),
    [
        (560_000, "0.560000"),
        (0, "0.000000"),
        (1, "0.000001"),
        (999_999, "0.999999"),
        (1_000_000, "1.000000"),
        (12_345_678, "12.345678"),
        (-1, "-0.000001"),
        (-1_500_000, "-1.500000"),
        (None, None),
    ],
)
def test_dollars_are_exact_decimal_strings(e6: int | None, expected: str | None) -> None:
    assert dollars(e6) == expected


@pytest.mark.parametrize(
    ("e2", "expected"),
    [(1_850, "18.50"), (5, "0.05"), (0, "0.00"), (100, "1.00"), (-250, "-2.50"), (None, None)],
)
def test_counts_are_exact_decimal_strings(e2: int | None, expected: str | None) -> None:
    assert count(e2) == expected


def test_conversion_never_goes_through_floating_point() -> None:
    biggest = 2**63 - 1  # the largest bigint we can store
    assert dollars(biggest) == "9223372036854.775807"  # a float would have rounded this
    assert count(biggest) == "92233720368547758.07"


def test_timestamps_are_utc_with_microseconds_whatever_the_source_zone() -> None:
    utc = datetime(2026, 10, 8, 12, 0, 0, 123, tzinfo=UTC)
    assert moment(utc) == "2026-10-08T12:00:00.000123+00:00"
    eastern = utc.astimezone(timezone(timedelta(hours=-4)))
    assert moment(eastern) == moment(utc)
    assert moment(None) is None
    assert moment(datetime(2026, 1, 1, tzinfo=UTC)) == "2026-01-01T00:00:00.000000+00:00"


def test_cursors_round_trip_and_reject_foreign_text() -> None:
    for text in ("KXBTC-26OCT08-T80000", "a", "x" * 100, "ünï"):
        assert decode_cursor(encode_cursor(text)) == text
    assert "=" not in encode_cursor("ab")  # URL-safe without padding
    assert decode_cursor("!!!not base64!!!") is None
    assert decode_cursor("__79") is None  # valid base64, but the bytes are not UTF-8
    assert decode_cursor("") is None and decode_cursor("====") is None
    assert decode_cursor("a b") is None and decode_cursor("ab\n") is None  # junk is not skipped


def test_postgres_numeric_sums_are_accepted_when_whole_and_refused_when_not() -> None:
    assert count(Decimal("1850")) == "18.50" and dollars(Decimal("560000")) == "0.560000"
    assert count(Decimal("1850.0")) == "18.50"
    with pytest.raises(ValueError, match="not a whole number"):
        count(Decimal("18.5"))  # never silently rounded
