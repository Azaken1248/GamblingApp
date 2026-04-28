from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Mapping

from models.session_models import SessionSummary, session_summary_from_payload, session_summary_to_payload
from tracking_and_reports.gambler_statistics import GamblerStatistics
from tracking_and_reports.stake_history_report import (
    StakeBoundaryValidation,
    StakeHistoryItem,
    StakeHistoryReport,
    StakeMonitorSummary,
)
from tracking_and_reports.win_loss_statistics import RunningTotalsByGame, WinLossStatistics


@dataclass(slots=True, frozen=True)
class SessionReportBundle:
    session_id: int
    gambler_id: int
    generated_at: datetime
    session_summary: SessionSummary
    win_loss_statistics: WinLossStatistics
    stake_history_report: StakeHistoryReport
    gambler_statistics: GamblerStatistics


def session_report_to_payload(bundle: SessionReportBundle) -> dict[str, Any]:
    return {
        "session_id": bundle.session_id,
        "gambler_id": bundle.gambler_id,
        "generated_at": bundle.generated_at.isoformat(),
        "session_summary": session_summary_to_payload(bundle.session_summary),
        "win_loss_statistics": win_loss_statistics_to_payload(bundle.win_loss_statistics),
        "stake_history_report": stake_history_report_to_payload(bundle.stake_history_report),
        "gambler_statistics": gambler_statistics_to_payload(bundle.gambler_statistics),
    }


def session_report_from_payload(payload: Mapping[str, Any]) -> SessionReportBundle:
    return SessionReportBundle(
        session_id=int(payload["session_id"]),
        gambler_id=int(payload["gambler_id"]),
        generated_at=datetime.fromisoformat(str(payload["generated_at"])),
        session_summary=session_summary_from_payload(payload["session_summary"]),
        win_loss_statistics=win_loss_statistics_from_payload(payload["win_loss_statistics"]),
        stake_history_report=stake_history_report_from_payload(payload["stake_history_report"]),
        gambler_statistics=gambler_statistics_from_payload(payload["gambler_statistics"]),
    )


def gambler_statistics_to_payload(statistics: GamblerStatistics) -> dict[str, Any]:
    return {
        "gambler_id": statistics.gambler_id,
        "username": statistics.username,
        "current_stake": str(statistics.current_stake),
        "initial_stake": str(statistics.initial_stake),
        "total_bets": statistics.total_bets,
        "total_wins": statistics.total_wins,
        "total_losses": statistics.total_losses,
        "total_winnings": str(statistics.total_winnings),
        "total_losses_amount": str(statistics.total_losses_amount),
        "win_rate": str(statistics.win_rate),
        "net_profit": str(statistics.net_profit),
        "reached_win_threshold": statistics.reached_win_threshold,
        "reached_loss_threshold": statistics.reached_loss_threshold,
    }


def gambler_statistics_from_payload(payload: Mapping[str, Any]) -> GamblerStatistics:
    return GamblerStatistics(
        gambler_id=int(payload["gambler_id"]),
        username=str(payload["username"]),
        current_stake=Decimal(str(payload["current_stake"])),
        initial_stake=Decimal(str(payload["initial_stake"])),
        total_bets=int(payload["total_bets"]),
        total_wins=int(payload["total_wins"]),
        total_losses=int(payload["total_losses"]),
        total_winnings=Decimal(str(payload["total_winnings"])),
        total_losses_amount=Decimal(str(payload["total_losses_amount"])),
        win_rate=Decimal(str(payload["win_rate"])),
        net_profit=Decimal(str(payload["net_profit"])),
        reached_win_threshold=bool(payload["reached_win_threshold"]),
        reached_loss_threshold=bool(payload["reached_loss_threshold"]),
    )


def running_totals_by_game_to_payload(snapshot: RunningTotalsByGame) -> dict[str, Any]:
    return {
        "snapshot_id": snapshot.snapshot_id,
        "session_id": snapshot.session_id,
        "game_id": snapshot.game_id,
        "game_index": snapshot.game_index,
        "total_games": snapshot.total_games,
        "total_wins": snapshot.total_wins,
        "total_losses": snapshot.total_losses,
        "total_pushes": snapshot.total_pushes,
        "total_winnings": str(snapshot.total_winnings),
        "total_losses_amount": str(snapshot.total_losses_amount),
        "net_profit": str(snapshot.net_profit),
        "win_rate": str(snapshot.win_rate),
        "profit_factor": str(snapshot.profit_factor),
        "roi": str(snapshot.roi),
        "longest_win_streak": snapshot.longest_win_streak,
        "longest_loss_streak": snapshot.longest_loss_streak,
    }


def running_totals_by_game_from_payload(payload: Mapping[str, Any]) -> RunningTotalsByGame:
    return RunningTotalsByGame(
        snapshot_id=int(payload["snapshot_id"]),
        session_id=int(payload["session_id"]),
        game_id=None if payload.get("game_id") is None else int(payload["game_id"]),
        game_index=None if payload.get("game_index") is None else int(payload["game_index"]),
        total_games=int(payload["total_games"]),
        total_wins=int(payload["total_wins"]),
        total_losses=int(payload["total_losses"]),
        total_pushes=int(payload["total_pushes"]),
        total_winnings=Decimal(str(payload["total_winnings"])),
        total_losses_amount=Decimal(str(payload["total_losses_amount"])),
        net_profit=Decimal(str(payload["net_profit"])),
        win_rate=Decimal(str(payload["win_rate"])),
        profit_factor=Decimal(str(payload["profit_factor"])),
        roi=Decimal(str(payload["roi"])),
        longest_win_streak=int(payload["longest_win_streak"]),
        longest_loss_streak=int(payload["longest_loss_streak"]),
    )


def win_loss_statistics_to_payload(statistics: WinLossStatistics) -> dict[str, Any]:
    return {
        "session_id": statistics.session_id,
        "gambler_id": statistics.gambler_id,
        "total_games": statistics.total_games,
        "total_wins": statistics.total_wins,
        "total_losses": statistics.total_losses,
        "win_rate": str(statistics.win_rate),
        "loss_rate": str(statistics.loss_rate),
        "win_loss_ratio": None if statistics.win_loss_ratio is None else str(statistics.win_loss_ratio),
        "total_winnings": str(statistics.total_winnings),
        "total_losses_amount": str(statistics.total_losses_amount),
        "net_profit": str(statistics.net_profit),
        "roi": str(statistics.roi),
        "profit_factor": str(statistics.profit_factor),
        "largest_win": str(statistics.largest_win),
        "largest_loss": str(statistics.largest_loss),
        "current_win_streak": statistics.current_win_streak,
        "current_loss_streak": statistics.current_loss_streak,
        "longest_win_streak": statistics.longest_win_streak,
        "longest_loss_streak": statistics.longest_loss_streak,
        "running_totals": [running_totals_by_game_to_payload(row) for row in statistics.running_totals],
    }


def win_loss_statistics_from_payload(payload: Mapping[str, Any]) -> WinLossStatistics:
    return WinLossStatistics(
        session_id=int(payload["session_id"]),
        gambler_id=int(payload["gambler_id"]),
        total_games=int(payload["total_games"]),
        total_wins=int(payload["total_wins"]),
        total_losses=int(payload["total_losses"]),
        win_rate=Decimal(str(payload["win_rate"])),
        loss_rate=Decimal(str(payload["loss_rate"])),
        win_loss_ratio=(None if payload.get("win_loss_ratio") is None else Decimal(str(payload["win_loss_ratio"]))),
        total_winnings=Decimal(str(payload["total_winnings"])),
        total_losses_amount=Decimal(str(payload["total_losses_amount"])),
        net_profit=Decimal(str(payload["net_profit"])),
        roi=Decimal(str(payload["roi"])),
        profit_factor=Decimal(str(payload["profit_factor"])),
        largest_win=Decimal(str(payload["largest_win"])),
        largest_loss=Decimal(str(payload["largest_loss"])),
        current_win_streak=int(payload["current_win_streak"]),
        current_loss_streak=int(payload["current_loss_streak"]),
        longest_win_streak=int(payload["longest_win_streak"]),
        longest_loss_streak=int(payload["longest_loss_streak"]),
        running_totals=tuple(
            running_totals_by_game_from_payload(row)
            for row in payload.get("running_totals", [])
        ),
    )


def stake_boundary_validation_to_payload(validation: StakeBoundaryValidation) -> dict[str, Any]:
    return {
        "lower_limit": str(validation.lower_limit),
        "upper_limit": str(validation.upper_limit),
        "warning_lower": str(validation.warning_lower),
        "warning_upper": str(validation.warning_upper),
        "current_balance": str(validation.current_balance),
        "is_within_bounds": validation.is_within_bounds,
        "approaching_lower_warning": validation.approaching_lower_warning,
        "approaching_upper_warning": validation.approaching_upper_warning,
        "reached_lower_limit": validation.reached_lower_limit,
        "reached_upper_limit": validation.reached_upper_limit,
    }


def stake_boundary_validation_from_payload(payload: Mapping[str, Any]) -> StakeBoundaryValidation:
    return StakeBoundaryValidation(
        lower_limit=Decimal(str(payload["lower_limit"])),
        upper_limit=Decimal(str(payload["upper_limit"])),
        warning_lower=Decimal(str(payload["warning_lower"])),
        warning_upper=Decimal(str(payload["warning_upper"])),
        current_balance=Decimal(str(payload["current_balance"])),
        is_within_bounds=bool(payload["is_within_bounds"]),
        approaching_lower_warning=bool(payload["approaching_lower_warning"]),
        approaching_upper_warning=bool(payload["approaching_upper_warning"]),
        reached_lower_limit=bool(payload["reached_lower_limit"]),
        reached_upper_limit=bool(payload["reached_upper_limit"]),
    )


def stake_monitor_summary_to_payload(summary: StakeMonitorSummary) -> dict[str, Any]:
    return {
        "session_id": summary.session_id,
        "gambler_id": summary.gambler_id,
        "session_status": summary.session_status,
        "end_reason": summary.end_reason,
        "current_stake": str(summary.current_stake),
        "starting_stake": str(summary.starting_stake),
        "peak_stake": str(summary.peak_stake),
        "lowest_stake": str(summary.lowest_stake),
        "volatility": str(summary.volatility),
        "total_changes": summary.total_changes,
        "boundary_validation": stake_boundary_validation_to_payload(summary.boundary_validation),
    }


def stake_monitor_summary_from_payload(payload: Mapping[str, Any]) -> StakeMonitorSummary:
    return StakeMonitorSummary(
        session_id=int(payload["session_id"]),
        gambler_id=int(payload["gambler_id"]),
        session_status=str(payload["session_status"]),
        end_reason=None if payload.get("end_reason") is None else str(payload["end_reason"]),
        current_stake=Decimal(str(payload["current_stake"])),
        starting_stake=Decimal(str(payload["starting_stake"])),
        peak_stake=Decimal(str(payload["peak_stake"])),
        lowest_stake=Decimal(str(payload["lowest_stake"])),
        volatility=Decimal(str(payload["volatility"])),
        total_changes=int(payload["total_changes"]),
        boundary_validation=stake_boundary_validation_from_payload(payload["boundary_validation"]),
    )


def stake_history_item_to_payload(item: StakeHistoryItem) -> dict[str, Any]:
    return {
        "transaction_id": item.transaction_id,
        "transaction_type": item.transaction_type,
        "amount": str(item.amount),
        "balance_before": str(item.balance_before),
        "balance_after": str(item.balance_after),
        "transaction_ref": item.transaction_ref,
        "created_at": item.created_at.isoformat(),
    }


def stake_history_item_from_payload(payload: Mapping[str, Any]) -> StakeHistoryItem:
    return StakeHistoryItem(
        transaction_id=int(payload["transaction_id"]),
        transaction_type=str(payload["transaction_type"]),
        amount=Decimal(str(payload["amount"])),
        balance_before=Decimal(str(payload["balance_before"])),
        balance_after=Decimal(str(payload["balance_after"])),
        transaction_ref=str(payload["transaction_ref"]),
        created_at=datetime.fromisoformat(str(payload["created_at"])),
    )


def stake_history_report_to_payload(report: StakeHistoryReport) -> dict[str, Any]:
    return {
        "session_id": report.session_id,
        "gambler_id": report.gambler_id,
        "transaction_count": report.transaction_count,
        "starting_balance": str(report.starting_balance),
        "ending_balance": str(report.ending_balance),
        "net_change": str(report.net_change),
        "transaction_breakdown": dict(report.transaction_breakdown),
        "monitor_summary": stake_monitor_summary_to_payload(report.monitor_summary),
        "transactions": [stake_history_item_to_payload(item) for item in report.transactions],
    }


def stake_history_report_from_payload(payload: Mapping[str, Any]) -> StakeHistoryReport:
    return StakeHistoryReport(
        session_id=int(payload["session_id"]),
        gambler_id=int(payload["gambler_id"]),
        transaction_count=int(payload["transaction_count"]),
        starting_balance=Decimal(str(payload["starting_balance"])),
        ending_balance=Decimal(str(payload["ending_balance"])),
        net_change=Decimal(str(payload["net_change"])),
        transaction_breakdown={str(key): int(value) for key, value in dict(payload["transaction_breakdown"]).items()},
        monitor_summary=stake_monitor_summary_from_payload(payload["monitor_summary"]),
        transactions=tuple(
            stake_history_item_from_payload(item)
            for item in payload.get("transactions", [])
        ),
    )