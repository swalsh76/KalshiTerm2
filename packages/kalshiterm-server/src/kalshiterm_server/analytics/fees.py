"""Kalshi trading fees as exact integer arithmetic.

Source: Kalshi's API documentation ("Fee Rounding", series ``fee_type`` / ``fee_multiplier``),
read 2026-10-12. The model fee of a taker trade is::

    M x 0.07 x C x P x (1 - P)        M = the series multiplier, C = contracts, P = price in dollars

and the trade fee is that rounded UP to $0.000001. Kalshi then aligns the balance to $0.0001
(direct members) or $0.01 (non-direct members) and rebates the overpayment across the fills of
one order; ``round_up`` models that conservatively as a ceiling per order. Maker fees are a
further 0.25 (0.5 for combos) of the same shape and do not apply to orders that take liquidity.
Selling YES at a price ``b`` is buying NO at ``1 - b``; ``P(1 - P)`` is the same either way.

Only the ``quadratic`` family is modelled. ``flat`` (a separate table) and the margin-program
type are not, and ``taker_multiplier`` returns None for them so callers skip rather than guess.
"""

QUADRATIC_FAMILY = frozenset(
    {"quadratic", "quadratic_with_maker_fees", "quadratic_with_combo_maker_fees"}
)
E6 = 1_000_000


def taker_multiplier(fee_type: str | None, fee_multiplier_e6: int | None) -> int | None:
    """The taker multiplier M in millionths, or None when the fee cannot be modelled."""
    if fee_type in QUADRATIC_FAMILY and fee_multiplier_e6 is not None and fee_multiplier_e6 >= 0:
        return fee_multiplier_e6
    return None


def taker_fee_e6(multiplier_e6: int, count_e2: int, price_e6: int) -> int:
    """The trade fee in millionths of a dollar for ``count_e2`` hundredths of a contract at
    ``price_e6`` (dollars in millionths), rounded up to a millionth.

    fee_e6 = M x 7/100 x (count_e2/100) x (p/1e6) x (1 - p/1e6) x 1e6, kept exact as one
    fraction and rounded up with integer arithmetic.
    """
    if not 0 <= price_e6 <= E6 or count_e2 < 0 or multiplier_e6 < 0:
        raise ValueError("price must be within [0, 1] dollars; count and multiplier non-negative")
    numerator = multiplier_e6 * 7 * count_e2 * price_e6 * (E6 - price_e6)
    denominator = E6 * 100 * 100 * E6
    return -(-numerator // denominator)


def round_up(value_e6: int, unit_e6: int) -> int:
    """Round up to a multiple of ``unit_e6`` (a unit of 1 leaves the value unchanged)."""
    if unit_e6 < 1:
        raise ValueError("rounding unit must be at least 1")
    return -(-value_e6 // unit_e6) * unit_e6
