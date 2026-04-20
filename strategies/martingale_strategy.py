from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP

from strategies.base_strategy import BettingStrategy, StrategyContext


class MartingaleStrategy(BettingStrategy):
    code = "MARTINGALE"

    def __init__(self, base_amount: Decimal) -> None:
        self._base_amount = base_amount

    def compute_bet_amount(
        self,
        *,
        current_stake: Decimal,
        context: StrategyContext,
    ) -> Decimal:
        if context.last_outcome == "LOSS" and context.last_bet_amount is not None:
            target = (context.last_bet_amount * Decimal("2")).quantize(
                Decimal("0.01"),
                rounding=ROUND_HALF_UP,
            )
        else:
            target = self._base_amount

        if target <= Decimal("0.00"):
            target = Decimal("0.01")

        return target if target <= current_stake else current_stake
