from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional


class TransactionType(str, Enum):
    INITIAL_STAKE = "INITIAL_STAKE"
    BET_PLACED = "BET_PLACED"
    BET_WIN = "BET_WIN"
    BET_LOSS = "BET_LOSS"
    DEPOSIT = "DEPOSIT"
    WITHDRAWAL = "WITHDRAWAL"
    ADJUSTMENT = "ADJUSTMENT"
    RESET = "RESET"


class SessionStatus(str, Enum):
    INITIALIZED = "INITIALIZED"
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"
    ENDED_WIN = "ENDED_WIN"
    ENDED_LOSS = "ENDED_LOSS"
    ENDED_MANUAL = "ENDED_MANUAL"
    ENDED_TIMEOUT = "ENDED_TIMEOUT"


class SessionEndReason(str, Enum):
    UPPER_LIMIT_REACHED = "UPPER_LIMIT_REACHED"
    LOWER_LIMIT_REACHED = "LOWER_LIMIT_REACHED"
    MANUAL_STOP = "MANUAL_STOP"
    TIMEOUT = "TIMEOUT"
    NOT_ENDED = "NOT_ENDED"


@dataclass(slots=True, frozen=True)
class StakeBoundary:
    lower_limit: Decimal
    upper_limit: Decimal

    @property
    def warning_lower(self) -> Decimal:
        return self.lower_limit + ((self.upper_limit - self.lower_limit) * Decimal("0.20"))

    @property
    def warning_upper(self) -> Decimal:
        return self.upper_limit * Decimal("0.80")


@dataclass(slots=True, frozen=True)
class StakeTransaction:
    transaction_id: int
    session_id: Optional[int]
    gambler_id: int
    transaction_type: TransactionType
    amount: Decimal
    balance_before: Decimal
    balance_after: Decimal
    transaction_ref: str
    created_at: datetime


@dataclass(slots=True, frozen=True)
class RunningTotalsSnapshot:
    snapshot_id: int
    session_id: int
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
    created_at: datetime
