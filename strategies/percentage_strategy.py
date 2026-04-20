from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP

from strategies.base_strategy import BettingStrategy, StrategyContext


class PercentageStrategy(BettingStrategy):
    code = "PERCENTAGE"

    def __init__(self, percent: Decimal) -> None:
        self._percent = percent

    def compute_bet_amount(
        self,
        *,
        current_stake: Decimal,
        context: StrategyContext,
    ) -> Decimal:
        _ = context
        target = (current_stake * self._percent).quantize(
            Decimal("0.01"),
            rounding=ROUND_HALF_UP,
        )
        if target <= Decimal("0.00"):
            return Decimal("0.01") if current_stake >= Decimal("0.01") else current_stake
        return target if target <= current_stake else current_stake
