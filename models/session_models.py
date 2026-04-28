from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Mapping, Optional, Tuple

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
    max_games: int
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
class SessionListItem:
    session_id: int
    gambler_id: int
    status: SessionStatus
    games_played: int
    started_at: datetime
    ended_at: Optional[datetime]
    current_stake: Decimal


@dataclass(slots=True, frozen=True)
class SessionContinuationResult:
    session_id: int
    gambler_id: int
    requested_games: int
    executed_games: int
    results: Tuple[BetSettlementResult, ...]
    summary: SessionSummary


@dataclass(slots=True, frozen=True)
class SessionSimulationHandle:
    task_id: str
    session_id: int
    gambler_id: int
    requested_games: int


@dataclass(slots=True, frozen=True)
class SessionSimulationProgress:
    task_id: str
    session_id: int
    gambler_id: int
    requested_games: int
    completed_games: int
    percentage: float
    state: str
    message: str


def session_summary_to_payload(summary: SessionSummary) -> dict[str, Any]:
    lifecycle = summary.lifecycle
    parameters = summary.parameters
    duration_metrics = summary.duration_metrics

    return {
        "lifecycle": {
            "session_id": lifecycle.session_id,
            "gambler_id": lifecycle.gambler_id,
            "status": lifecycle.status.value,
            "end_reason": None if lifecycle.end_reason is None else lifecycle.end_reason.value,
            "games_played": lifecycle.games_played,
            "max_games": lifecycle.max_games,
            "started_at": lifecycle.started_at.isoformat(),
            "ended_at": None if lifecycle.ended_at is None else lifecycle.ended_at.isoformat(),
        },
        "parameters": {
            "parameter_id": parameters.parameter_id,
            "session_id": parameters.session_id,
            "lower_limit": str(parameters.lower_limit),
            "upper_limit": str(parameters.upper_limit),
            "min_bet": str(parameters.min_bet),
            "max_bet": str(parameters.max_bet),
            "default_win_probability": str(parameters.default_win_probability),
            "max_session_minutes": parameters.max_session_minutes,
            "strict_mode": parameters.strict_mode,
            "created_at": parameters.created_at.isoformat(),
        },
        "duration_metrics": {
            "total_duration_seconds": duration_metrics.total_duration_seconds,
            "active_duration_seconds": duration_metrics.active_duration_seconds,
            "pause_duration_seconds": duration_metrics.pause_duration_seconds,
        },
        "current_stake": str(summary.current_stake),
        "starting_stake": str(summary.starting_stake),
        "ending_stake": str(summary.ending_stake),
        "peak_stake": str(summary.peak_stake),
        "lowest_stake": str(summary.lowest_stake),
        "total_wins": summary.total_wins,
        "total_losses": summary.total_losses,
    }


def session_summary_from_payload(payload: Mapping[str, Any]) -> SessionSummary:
    lifecycle_payload = payload["lifecycle"]
    parameters_payload = payload["parameters"]
    duration_payload = payload["duration_metrics"]

    return SessionSummary(
        lifecycle=SessionLifecycleState(
            session_id=int(lifecycle_payload["session_id"]),
            gambler_id=int(lifecycle_payload["gambler_id"]),
            status=SessionStatus(str(lifecycle_payload["status"])),
            end_reason=(
                None
                if lifecycle_payload.get("end_reason") is None
                else SessionEndReason(str(lifecycle_payload["end_reason"]))
            ),
            games_played=int(lifecycle_payload["games_played"]),
            max_games=int(lifecycle_payload["max_games"]),
            started_at=datetime.fromisoformat(str(lifecycle_payload["started_at"])),
            ended_at=(
                None
                if lifecycle_payload.get("ended_at") is None
                else datetime.fromisoformat(str(lifecycle_payload["ended_at"]))
            ),
        ),
        parameters=SessionParameters(
            parameter_id=int(parameters_payload["parameter_id"]),
            session_id=int(parameters_payload["session_id"]),
            lower_limit=Decimal(str(parameters_payload["lower_limit"])),
            upper_limit=Decimal(str(parameters_payload["upper_limit"])),
            min_bet=Decimal(str(parameters_payload["min_bet"])),
            max_bet=Decimal(str(parameters_payload["max_bet"])),
            default_win_probability=Decimal(str(parameters_payload["default_win_probability"])),
            max_session_minutes=int(parameters_payload["max_session_minutes"]),
            strict_mode=bool(parameters_payload["strict_mode"]),
            created_at=datetime.fromisoformat(str(parameters_payload["created_at"])),
        ),
        duration_metrics=SessionDurationMetrics(
            total_duration_seconds=int(duration_payload["total_duration_seconds"]),
            active_duration_seconds=int(duration_payload["active_duration_seconds"]),
            pause_duration_seconds=int(duration_payload["pause_duration_seconds"]),
        ),
        current_stake=Decimal(str(payload["current_stake"])),
        starting_stake=Decimal(str(payload["starting_stake"])),
        ending_stake=Decimal(str(payload["ending_stake"])),
        peak_stake=Decimal(str(payload["peak_stake"])),
        lowest_stake=Decimal(str(payload["lowest_stake"])),
        total_wins=int(payload["total_wins"]),
        total_losses=int(payload["total_losses"]),
    )
