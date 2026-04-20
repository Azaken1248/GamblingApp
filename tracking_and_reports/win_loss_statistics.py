from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional, Tuple


@dataclass(slots=True, frozen=True)
class OddsConfiguration:
    odds_config_id: int
    odds_type: str
    fixed_multiplier: Optional[Decimal]
    american_odds: Optional[int]
    decimal_odds: Optional[Decimal]
    probability_payout_factor: Optional[Decimal]
    house_edge: Decimal
    is_default: bool


@dataclass(slots=True, frozen=True)
class RunningTotalsByGame:
    snapshot_id: int
    session_id: int
    game_id: Optional[int]
    game_index: Optional[int]
    total_games: int
    total_wins: int
    total_losses: int
    total_pushes: int
    total_winnings: Decimal
    total_losses_amount: Decimal
    net_profit: Decimal
    win_rate: Decimal
    profit_factor: Decimal
    roi: Decimal
    longest_win_streak: int
    longest_loss_streak: int


@dataclass(slots=True, frozen=True)
class WinLossStatistics:
    session_id: int
    gambler_id: int
    total_games: int
    total_wins: int
    total_losses: int
    win_rate: Decimal
    loss_rate: Decimal
    win_loss_ratio: Optional[Decimal]
    total_winnings: Decimal
    total_losses_amount: Decimal
    net_profit: Decimal
    roi: Decimal
    profit_factor: Decimal
    largest_win: Decimal
    largest_loss: Decimal
    current_win_streak: int
    current_loss_streak: int
    longest_win_streak: int
    longest_loss_streak: int
    running_totals: Tuple[RunningTotalsByGame, ...]
