from __future__ import annotations

from decimal import Decimal

from models.betting import BetSettlementResult
from models.gambler_profile import GamblerProfile
from models.session_models import SessionSummary
from models.stake_management import SessionStatus
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from utils.exceptions import ValidationResult, ValidationSeverity


class GameStatusDisplay:
    def __init__(self, console: Console) -> None:
        self._console = console

    def show_banner(self) -> None:
        self._console.print(
            Panel(
                "Interactive Session Console\n"
                "Guided setup, betting flow, real-time outcomes, and final summary.",
                title="Gambling App",
                border_style="cyan",
            )
        )

    def show_profile(self, profile: GamblerProfile) -> None:
        table = Table(title="Selected Gambler", show_header=True, header_style="bold cyan")
        table.add_column("Field", style="white")
        table.add_column("Value", style="bright_white")
        table.add_row("ID", str(profile.gambler_id))
        table.add_row("Username", profile.username)
        table.add_row("Name", profile.full_name)
        table.add_row("Email", profile.email)
        table.add_row("Current Stake", self._money(profile.current_stake))
        table.add_row("Win Threshold", self._money(profile.win_threshold))
        table.add_row("Loss Threshold", self._money(profile.loss_threshold))
        table.add_row("Active", "Yes" if profile.is_active else "No")
        self._console.print(table)

    def show_session_status(self, summary: SessionSummary) -> None:
        lifecycle = summary.lifecycle

        title_style = "green" if lifecycle.status == SessionStatus.ACTIVE else "yellow"
        if lifecycle.status in {
            SessionStatus.ENDED_WIN,
            SessionStatus.ENDED_LOSS,
            SessionStatus.ENDED_MANUAL,
            SessionStatus.ENDED_TIMEOUT,
        }:
            title_style = "magenta"

        table = Table(
            title=f"Session Status: {lifecycle.status.value}",
            show_header=True,
            header_style="bold cyan",
        )
        table.add_column("Metric", style="white")
        table.add_column("Value", style="bright_white")

        table.add_row("Session ID", str(lifecycle.session_id))
        table.add_row("Gambler ID", str(lifecycle.gambler_id))
        table.add_row("Games Played", str(lifecycle.games_played))
        table.add_row("Wins / Losses", f"{summary.total_wins} / {summary.total_losses}")
        table.add_row("Current Stake", self._money(summary.current_stake))
        table.add_row("Starting Stake", self._money(summary.starting_stake))
        table.add_row("Peak / Lowest", f"{self._money(summary.peak_stake)} / {self._money(summary.lowest_stake)}")
        table.add_row("Lower / Upper Limit", f"{self._money(summary.parameters.lower_limit)} / {self._money(summary.parameters.upper_limit)}")
        table.add_row("Min / Max Bet", f"{self._money(summary.parameters.min_bet)} / {self._money(summary.parameters.max_bet)}")
        table.add_row("Default Win Probability", self._rate(summary.parameters.default_win_probability))
        table.add_row("Max Games", str(lifecycle.max_games))
        table.add_row("Max Session Minutes", str(summary.parameters.max_session_minutes))
        table.add_row("Strict Mode", "On" if summary.parameters.strict_mode else "Off")
        table.add_row(
            "Durations (total/active/pause)",
            (
                f"{summary.duration_metrics.total_duration_seconds}s / "
                f"{summary.duration_metrics.active_duration_seconds}s / "
                f"{summary.duration_metrics.pause_duration_seconds}s"
            ),
        )

        status_panel = Panel(
            table,
            border_style=title_style,
            title="Live Session View",
        )
        self._console.print(status_panel)

    def show_bet_outcome(self, result: BetSettlementResult) -> None:
        outcome_style = "bold green" if result.outcome == "WIN" else "bold red"
        table = Table(title="Bet Outcome", show_header=True, header_style="bold cyan")
        table.add_column("Field", style="white")
        table.add_column("Value", style="bright_white")
        table.add_row("Bet ID", str(result.bet_id))
        table.add_row("Game ID", str(result.game_id))
        table.add_row("Strategy", result.strategy_code)
        table.add_row("Outcome", Text(result.outcome, style=outcome_style))
        table.add_row("Payout", self._money(result.payout_amount))
        table.add_row("Loss", self._money(result.loss_amount))
        table.add_row("Net Change", self._money(result.net_change))
        table.add_row("Stake Before", self._money(result.stake_before))
        table.add_row("Stake After", self._money(result.stake_after))
        table.add_row("Session Status", result.session_status)
        table.add_row("End Reason", result.end_reason or "NOT_ENDED")
        self._console.print(table)

    def show_validation_feedback(self, result: ValidationResult | None) -> None:
        if result is None:
            return
        if not result.issues:
            return

        table = Table(title="Validation Feedback", show_header=True, header_style="bold cyan")
        table.add_column("Severity", style="white")
        table.add_column("Field", style="white")
        table.add_column("Message", style="bright_white")

        for issue in result.issues:
            style = "yellow" if issue.severity == ValidationSeverity.WARNING else "red"
            table.add_row(issue.severity.value, issue.field_name, Text(issue.user_message, style=style))

        self._console.print(table)

    def show_info(self, message: str) -> None:
        self._console.print(f"[cyan]{message}[/cyan]")

    def show_warning(self, message: str) -> None:
        self._console.print(f"[yellow]{message}[/yellow]")

    def show_error(self, message: str) -> None:
        self._console.print(f"[bold red]{message}[/bold red]")

    @staticmethod
    def _money(value: Decimal) -> str:
        return f"{value:.2f}"

    @staticmethod
    def _rate(value: Decimal) -> str:
        return f"{value:.4f}"
