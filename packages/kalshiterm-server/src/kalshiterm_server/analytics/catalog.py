"""The built-in metrics a user can set alerts on, with what each one means.

Plugin metrics are not alertable yet (parking lot): a rule names ``analyzer.metric`` and must
be found here, so a typo cannot create a rule that never fires. ``fresh`` is how old a result
may be and still count (three of the analyzer's intervals); an older or missing result means
the condition is not met, which re-arms the rule.
"""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class AlertMetric:
    analyzer: str
    metric: str
    subject: str  # "market" or "event"
    unit: str  # "dollars" | "contracts" | "seconds": what value_e6 millionths of
    ops: tuple[str, ...]
    fresh_seconds: int
    meaning: str

    @property
    def key(self) -> str:
        return f"{self.analyzer}.{self.metric}"


SIGNED = ("gt", "lt", "abs_gt")
LEVEL = ("gt", "lt")
ONLY_GT = ("gt",)

CATALOG: dict[str, AlertMetric] = {
    m.key: m
    for m in (
        AlertMetric("market_moves", "last", "market", "dollars", LEVEL, 180, "last trade price"),
        AlertMetric("market_moves", "change_15m", "market", "dollars", SIGNED, 180,
                    "price change over 15 minutes"),
        AlertMetric("market_moves", "change_1h", "market", "dollars", SIGNED, 180,
                    "price change over 1 hour"),
        AlertMetric("market_moves", "change_24h", "market", "dollars", SIGNED, 180,
                    "price change over 24 hours"),
        AlertMetric("market_moves", "volume_1h", "market", "contracts", ONLY_GT, 180,
                    "contracts traded in the last hour"),
        AlertMetric("market_moves", "volume_24h", "market", "contracts", ONLY_GT, 180,
                    "contracts traded in the last 24 hours"),
        AlertMetric("market_quotes", "spread", "market", "dollars", LEVEL, 180,
                    "best ask minus best bid"),
        AlertMetric("market_quotes", "bid_size", "market", "contracts", LEVEL, 180,
                    "contracts at the best bid"),
        AlertMetric("market_quotes", "ask_size", "market", "contracts", LEVEL, 180,
                    "contracts at the best ask"),
        AlertMetric("market_volatility", "volatility_1h", "market", "dollars", ONLY_GT, 900,
                    "per-minute price volatility over the last hour"),
        AlertMetric("arbitrage_bid", "profit", "event", "dollars", ONLY_GT, 180,
                    "net profit after fees of selling YES on every leg at the best bids"),
        AlertMetric("arbitrage_bid", "edge", "event", "dollars", ONLY_GT, 180,
                    "gross edge per contract set (bid sum minus $1), before fees"),
    )
}  # fmt: skip
