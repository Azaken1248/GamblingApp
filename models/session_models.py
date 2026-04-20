from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional, Tuple

from models.betting import BetSettlementResult
from models.stake_management import SessionEndReason, SessionStatus


@dataclass(slots=True, frozen=True)
class SessionParameters:
    parameter_id: int
    session_id: int
    lower_limit: Decimal
    upper_limit: Decimal
    min_bet: Decimal
    max_bet: Decimal
    default_win_probability: Decimal
    max_session_minutes: int
    strict_mode: bool
    created_at: datetime


@dataclass(slots=True, frozen=True)
class PauseRecord:
    pause_id: int
    session_id: int
    pause_reason: str
    paused_at: datetime
    resumed_at: Optional[datetime]
    pause_seconds: Optional[int]


@dataclass(slots=True, frozen=True)
class SessionLifecycleState:
    session_id: int
    gambler_id: int
    status: SessionStatus
    end_reason: Optional[SessionEndReason]
    games_played: int
    started_at: datetime
    ended_at: Optional[datetime]


@dataclass(slots=True, frozen=True)
class SessionDurationMetrics:
    total_duration_seconds: int
    active_duration_seconds: int
    pause_duration_seconds: int


@dataclass(slots=True, frozen=True)
class SessionSummary:
    lifecycle: SessionLifecycleState
    parameters: SessionParameters
    duration_metrics: SessionDurationMetrics
    current_stake: Decimal
    starting_stake: Decimal
    ending_stake: Decimal
    peak_stake: Decimal
    lowest_stake: Decimal
    total_wins: int
    total_losses: int


@dataclass(slots=True, frozen=True)
class SessionContinuationResult:
    session_id: int
    gambler_id: int
    requested_games: int
    executed_games: int
    results: Tuple[BetSettlementResult, ...]
    summary: SessionSummary
