from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional, Tuple


@dataclass(slots=True, frozen=True)
class BetConfirmation:
    bet_id: int
    session_id: int
    gambler_id: int
    strategy_code: str
    game_index: int
    bet_amount: Decimal
    win_probability: Decimal
    odds_value: Decimal
    potential_win: Decimal
    stake_before: Decimal
    placed_at: datetime


@dataclass(slots=True, frozen=True)
class BetSettlementResult:
    bet_id: int
    game_id: int
    session_id: int
    gambler_id: int
    strategy_code: str
    outcome: str
    payout_amount: Decimal
    loss_amount: Decimal
    net_change: Decimal
    stake_before: Decimal
    stake_after: Decimal
    session_status: str
    end_reason: Optional[str]


@dataclass(slots=True, frozen=True)
class ConsecutiveBetSummary:
    session_id: int
    gambler_id: int
    total_bets: int
    total_wins: int
    total_losses: int
    final_stake: Decimal
    results: Tuple[BetSettlementResult, ...]
