from decimal import Decimal

import pytest
from kalshiterm_server.fixedpoint import PrecisionError, from_e2, from_e6, to_e2, to_e6


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (Decimal("0.4100"), 410_000),
        ("0.0001", 100),  # the finest tick seen in live data
        ("0.000001", 1),  # the finest Kalshi documents
        (1, 1_000_000),
        (0.41, 410_000),  # floats go through their shortest decimal form, not binary noise
        (118.7499, 118_749_900),  # a strike level
        (-3.5, -3_500_000),
        ("0", 0),
    ],
)
def test_to_e6_is_exact(value: object, expected: int) -> None:
    assert to_e6(value) == expected  # type: ignore[arg-type]


@pytest.mark.parametrize(("value", "expected"), [("104.24", 10_424), ("0.03", 3), (7, 700)])
def test_to_e2_is_exact(value: object, expected: int) -> None:
    assert to_e2(value) == expected  # type: ignore[arg-type]


def test_none_stays_none() -> None:
    assert to_e6(None) is None and to_e2(None) is None


@pytest.mark.parametrize("value", ["0.1234567", "0.0000001", 0.1 + 0.2, "1e-7"])
def test_finer_than_the_scale_is_refused_not_rounded(value: object) -> None:
    with pytest.raises(PrecisionError):
        to_e6(value)  # type: ignore[arg-type]


def test_counts_finer_than_hundredths_are_refused() -> None:
    with pytest.raises(PrecisionError):
        to_e2("0.001")


@pytest.mark.parametrize("value", ["NaN", "Infinity", "abc", float("inf")])
def test_non_numbers_are_refused(value: object) -> None:
    with pytest.raises(PrecisionError):
        to_e6(value)  # type: ignore[arg-type]


def test_values_beyond_bigint_are_refused() -> None:
    with pytest.raises(PrecisionError):
        to_e6("10000000000000")  # 1e13 dollars * 1e6 > 2**63
    assert to_e6("9000000000000") == 9_000_000_000_000_000_000


@pytest.mark.parametrize("text", ["0.4100", "0.000001", "118.749900", "-3.500000", "0"])
def test_e6_round_trip(text: str) -> None:
    assert from_e6(to_e6(text)) == Decimal(text)  # type: ignore[arg-type]


def test_e2_round_trip() -> None:
    assert from_e2(to_e2("104.24")) == Decimal("104.24")  # type: ignore[arg-type]
