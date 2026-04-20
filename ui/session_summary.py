from __future__ import annotations

from decimal import Decimal

from models.session_models import SessionSummary
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from tracking_and_reports.gambler_statistics import GamblerStatistics
from tracking_and_reports.win_loss_statistics import WinLossStatistics


class SessionSummaryRenderer:
    def __init__(self, console: Console) -> None:
        self._console = console

    def render_end_of_session(
        self,
        *,
        session_summary: SessionSummary,
        win_loss_statistics: WinLossStatistics | None,
        gambler_statistics: GamblerStatistics | None = None,
    ) -> None:
        self._console.print(
            Panel(
                "End-of-session report generated from service-layer reporting outputs.",
                title="Session Summary",
                border_style="green",
            )
        )

        lifecycle_table = Table(show_header=True, header_style="bold cyan", title="Lifecycle")
        lifecycle_table.add_column("Metric", style="white")
        lifecycle_table.add_column("Value", style="bright_white")

        lifecycle = session_summary.lifecycle
        lifecycle_table.add_row("Session ID", str(lifecycle.session_id))
        lifecycle_table.add_row("Gambler ID", str(lifecycle.gambler_id))
        lifecycle_table.add_row("Status", lifecycle.status.value)
        lifecycle_table.add_row("End Reason", lifecycle.end_reason.value if lifecycle.end_reason else "NOT_ENDED")
        lifecycle_table.add_row("Games Played", str(lifecycle.games_played))
        lifecycle_table.add_row("Started At", str(lifecycle.started_at))
        lifecycle_table.add_row("Ended At", str(lifecycle.ended_at) if lifecycle.ended_at else "ACTIVE")

        duration = session_summary.duration_metrics
        lifecycle_table.add_row("Total Seconds", str(duration.total_duration_seconds))
        lifecycle_table.add_row("Active Seconds", str(duration.active_duration_seconds))
        lifecycle_table.add_row("Pause Seconds", str(duration.pause_duration_seconds))
        self._console.print(lifecycle_table)

        financial_table = Table(show_header=True, header_style="bold cyan", title="Financial Snapshot")
        financial_table.add_column("Metric", style="white")
        financial_table.add_column("Value", style="bright_white")
        financial_table.add_row("Starting Stake", self._money(session_summary.starting_stake))
        financial_table.add_row("Current Stake", self._money(session_summary.current_stake))
        financial_table.add_row("Ending Stake", self._money(session_summary.ending_stake))
        financial_table.add_row("Peak Stake", self._money(session_summary.peak_stake))
        financial_table.add_row("Lowest Stake", self._money(session_summary.lowest_stake))
        financial_table.add_row("Total Wins", str(session_summary.total_wins))
        financial_table.add_row("Total Losses", str(session_summary.total_losses))
        self._console.print(financial_table)

        if win_loss_statistics is not None:
            win_loss_table = Table(show_header=True, header_style="bold cyan", title="Win/Loss Statistics")
            win_loss_table.add_column("Metric", style="white")
            win_loss_table.add_column("Value", style="bright_white")

            win_loss_table.add_row("Total Games", str(win_loss_statistics.total_games))
            win_loss_table.add_row("Wins", str(win_loss_statistics.total_wins))
            win_loss_table.add_row("Losses", str(win_loss_statistics.total_losses))
            win_loss_table.add_row("Win Rate", self._rate(win_loss_statistics.win_rate))
            win_loss_table.add_row("Loss Rate", self._rate(win_loss_statistics.loss_rate))
            win_loss_table.add_row(
                "Win/Loss Ratio",
                "N/A" if win_loss_statistics.win_loss_ratio is None else self._rate(win_loss_statistics.win_loss_ratio),
            )
            win_loss_table.add_row("Total Winnings", self._money(win_loss_statistics.total_winnings))
            win_loss_table.add_row("Total Losses", self._money(win_loss_statistics.total_losses_amount))
            win_loss_table.add_row("Net Profit", self._money(win_loss_statistics.net_profit))
            win_loss_table.add_row("ROI", self._rate(win_loss_statistics.roi))
            win_loss_table.add_row("Profit Factor", self._rate(win_loss_statistics.profit_factor))
            win_loss_table.add_row("Largest Win", self._money(win_loss_statistics.largest_win))
            win_loss_table.add_row("Largest Loss", self._money(win_loss_statistics.largest_loss))
            win_loss_table.add_row("Current Win Streak", str(win_loss_statistics.current_win_streak))
            win_loss_table.add_row("Current Loss Streak", str(win_loss_statistics.current_loss_streak))
            win_loss_table.add_row("Longest Win Streak", str(win_loss_statistics.longest_win_streak))
            win_loss_table.add_row("Longest Loss Streak", str(win_loss_statistics.longest_loss_streak))
            self._console.print(win_loss_table)

            if win_loss_statistics.running_totals:
                progression = Table(
                    show_header=True,
                    header_style="bold cyan",
                    title="Running Totals By Game (Most Recent 10)",
                )
                progression.add_column("Game", style="white")
                progression.add_column("Wins", style="green")
                progression.add_column("Losses", style="red")
                progression.add_column("Net Profit", style="bright_white")
                progression.add_column("ROI", style="bright_white")

                for row in win_loss_statistics.running_totals[-10:]:
                    progression.add_row(
                        str(row.game_index if row.game_index is not None else row.snapshot_id),
                        str(row.total_wins),
                        str(row.total_losses),
                        self._money(row.net_profit),
                        self._rate(row.roi),
                    )

                self._console.print(progression)

        if gambler_statistics is not None:
            gambler_table = Table(show_header=True, header_style="bold cyan", title="Profile-Level Totals")
            gambler_table.add_column("Metric", style="white")
            gambler_table.add_column("Value", style="bright_white")
            gambler_table.add_row("Username", gambler_statistics.username)
            gambler_table.add_row("Current Stake", self._money(gambler_statistics.current_stake))
            gambler_table.add_row("Initial Stake", self._money(gambler_statistics.initial_stake))
            gambler_table.add_row("Total Bets", str(gambler_statistics.total_bets))
            gambler_table.add_row("Total Wins", str(gambler_statistics.total_wins))
            gambler_table.add_row("Total Losses", str(gambler_statistics.total_losses))
            gambler_table.add_row("Net Profit", self._money(gambler_statistics.net_profit))
            gambler_table.add_row("Win Rate", self._rate(gambler_statistics.win_rate))
            self._console.print(gambler_table)

    @staticmethod
    def _money(value: Decimal) -> str:
        return f"{value:.2f}"

    @staticmethod
    def _rate(value: Decimal) -> str:
        return f"{value:.4f}"
