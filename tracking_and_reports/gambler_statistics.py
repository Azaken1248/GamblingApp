from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(slots=True, frozen=True)
class GamblerStatistics:
    gambler_id: int
    username: str
    current_stake: Decimal
    initial_stake: Decimal
    total_bets: int
    total_wins: int
    total_losses: int
    total_winnings: Decimal
    total_losses_amount: Decimal
    win_rate: Decimal
    net_profit: Decimal
    reached_win_threshold: bool
    reached_loss_threshold: bool


@dataclass(slots=True, frozen=True)
class EligibilityStatus:
    gambler_id: int
    is_eligible: bool
    current_stake: Decimal
    min_required_stake: Decimal
    reasons: tuple[str, ...]