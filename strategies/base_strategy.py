from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional


@dataclass(slots=True, frozen=True)
class StrategyContext:
    step_index: int
    last_bet_amount: Optional[Decimal]
    last_outcome: Optional[str]


class BettingStrategy(ABC):
    code: str

    @abstractmethod
    def compute_bet_amount(
        self,
        *,
        current_stake: Decimal,
        context: StrategyContext,
    ) -> Decimal:
        raise NotImplementedError
