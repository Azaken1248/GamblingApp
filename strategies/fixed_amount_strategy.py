from __future__ import annotations

from decimal import Decimal

from strategies.base_strategy import BettingStrategy, StrategyContext


class FixedAmountStrategy(BettingStrategy):
    code = "FIXED_AMOUNT"

    def __init__(self, amount: Decimal) -> None:
        self._amount = amount

    def compute_bet_amount(
        self,
        *,
        current_stake: Decimal,
        context: StrategyContext,
    ) -> Decimal:
        _ = context
        return self._amount if self._amount <= current_stake else current_stake
