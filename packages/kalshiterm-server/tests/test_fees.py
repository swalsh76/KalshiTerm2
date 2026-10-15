from decimal import ROUND_CEILING, Decimal

import pytest
from kalshi_core.fees import round_up, taker_fee_e6, taker_multiplier


def test_kalshis_own_worked_example() -> None:
    """Fee Rounding docs: a buy at $0.055 has a model fee of $0.00363825 and a trade fee of
    $0.003639 (one contract, multiplier 1)."""
    assert taker_fee_e6(1_000_000, 100, 55_000) == 3_639


@pytest.mark.parametrize(
    ("multiplier_e6", "count_e2", "price_e6"),
    [
        (1_000_000, 100, 500_000),  # the peak: 1.75 cents per contract
        (1_000_000, 10_000, 500_000),  # 100 contracts
        (500_000, 2_550, 123_456),  # half multiplier, 25.5 contracts, odd price
        (1_000_000, 1, 999_999),  # one hundredth of a contract at the edge of the range
        (1_234_567, 777, 10_000),
    ],
)
def test_matches_an_independent_decimal_computation(
    multiplier_e6: int, count_e2: int, price_e6: int
) -> None:
    m, c, p = Decimal(multiplier_e6) / 10**6, Decimal(count_e2) / 100, Decimal(price_e6) / 10**6
    exact = m * Decimal("0.07") * c * p * (1 - p)
    expected = int((exact * 10**6).to_integral_value(rounding=ROUND_CEILING))
    assert taker_fee_e6(multiplier_e6, count_e2, price_e6) == expected


def test_the_peak_fee_and_the_extremes() -> None:
    assert taker_fee_e6(1_000_000, 100, 500_000) == 17_500  # $0.0175 per contract at 50 cents
    assert taker_fee_e6(1_000_000, 100, 0) == 0 and taker_fee_e6(1_000_000, 100, 1_000_000) == 0
    assert taker_fee_e6(0, 100, 500_000) == 0  # a series with multiplier 0 charges nothing
    assert taker_fee_e6(1_000_000, 100, 400_000) == taker_fee_e6(1_000_000, 100, 600_000)


def test_the_fee_never_rounds_down_even_by_a_millionth() -> None:
    # 1 contract at 1 cent: 0.07 * 0.01 * 0.99 = $0.000693 exactly
    assert taker_fee_e6(1_000_000, 100, 10_000) == 693
    # one hundredth of a contract: 0.00000693 dollars = 6.93 millionths -> rounds up to 7
    assert taker_fee_e6(1_000_000, 1, 10_000) == 7


def test_inputs_outside_the_domain_are_refused() -> None:
    for bad in ((1_000_000, 100, -1), (1_000_000, 100, 1_000_001), (1_000_000, -1, 5), (-1, 1, 5)):
        with pytest.raises(ValueError):
            taker_fee_e6(*bad)


def test_rounding_up_to_a_unit() -> None:
    assert round_up(17_500, 10_000) == 20_000  # $0.0175 -> $0.02 for a non-direct member
    assert round_up(20_000, 10_000) == 20_000 and round_up(0, 10_000) == 0
    assert round_up(3_639, 100) == 3_700 and round_up(3_639, 1) == 3_639
    with pytest.raises(ValueError):
        round_up(5, 0)


def test_only_the_quadratic_family_is_modelled() -> None:
    assert taker_multiplier("quadratic", 1_000_000) == 1_000_000
    assert taker_multiplier("quadratic_with_maker_fees", 500_000) == 500_000
    assert taker_multiplier("quadratic_with_combo_maker_fees", 0) == 0
    unmodelled = [
        ("flat", 1_000_000),
        ("margin_market_maker_program_fees", 1),
        (None, 1),
        ("quadratic", None),
        ("quadratic", -1),
        ("mystery", 1_000_000),
    ]
    for fee_type, mult in unmodelled:
        assert taker_multiplier(fee_type, mult) is None
