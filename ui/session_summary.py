from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any, Mapping

from config.cache_manager import RedisCacheManager
from models.session_models import SessionSummary
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from tracking_and_reports.gambler_statistics import GamblerStatistics
from tracking_and_reports.report_payloads import SessionReportBundle, session_report_from_payload
from tracking_and_reports.stake_history_report import StakeHistoryReport
from tracking_and_reports.win_loss_statistics import WinLossStatistics


class SessionSummaryRenderer:
    def __init__(self, console: Console, cache_manager: RedisCacheManager) -> None:
        self._console = console
        self._cache_manager = cache_manager

    async def present_end_of_session(self, session_id: int) -> SessionReportBundle | None:
        cached_payload = await self._cache_manager.get_session_report_bundle(session_id)
        if cached_payload is not None:
            bundle = session_report_from_payload(cached_payload)
            self.render_end_of_session(
                session_summary=bundle.session_summary,
                win_loss_statistics=bundle.win_loss_statistics,
                gambler_statistics=bundle.gambler_statistics,
                stake_history_report=bundle.stake_history_report,
                report_source="cached Redis payload",
            )
            return bundle

        progress_payload = await self._cache_manager.get_report_progress(session_id)
        task_result = None
        if progress_payload is None or str(progress_payload.get("state")) in {"SUCCESS", "FAILURE"}:
            from tasks.report_tasks import generate_session_report

            task_result = generate_session_report.apply_async(kwargs={"session_id": session_id})
            progress_payload = None

        with self._console.status(
            f"Generating session report for session {session_id}...",
            spinner="dots",
        ) as status:
            while True:
                progress_payload = await self._cache_manager.get_report_progress(session_id)
                if progress_payload is None:
                    status.update(f"Waiting for report worker for session {session_id}...")
                else:
                    status.update(self._format_progress_message(progress_payload))
                    if str(progress_payload.get("state")) in {"SUCCESS", "FAILURE"}:
                        break

                if task_result is not None and task_result.ready() and progress_payload is None:
                    break

                await asyncio.sleep(0.5)

        if progress_payload is not None and str(progress_payload.get("state")) == "FAILURE":
            raise RuntimeError(str(progress_payload.get("message", "Report generation failed.")))

        if task_result is not None and task_result.failed():
            raise RuntimeError("Report generation failed in the worker.")

        cached_payload = await self._cache_manager.get_session_report_bundle(session_id)
        if cached_payload is None:
            raise RuntimeError("Report worker finished without caching a session report.")

        bundle = session_report_from_payload(cached_payload)
        self.render_end_of_session(
            session_summary=bundle.session_summary,
            win_loss_statistics=bundle.win_loss_statistics,
            gambler_statistics=bundle.gambler_statistics,
            stake_history_report=bundle.stake_history_report,
            report_source="background worker output",
        )
        return bundle

    def render_end_of_session(
        self,
        *,
        session_summary: SessionSummary,
        win_loss_statistics: WinLossStatistics | None,
        gambler_statistics: GamblerStatistics | None = None,
        stake_history_report: StakeHistoryReport | None = None,
        report_source: str = "service-layer reporting outputs",
    ) -> None:
        self._console.print(
            Panel(
                f"End-of-session report loaded from {report_source}.",
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

        if stake_history_report is not None:
            stake_table = Table(show_header=True, header_style="bold cyan", title="Stake History Report")
            stake_table.add_column("Metric", style="white")
            stake_table.add_column("Value", style="bright_white")
            stake_table.add_row("Transaction Count", str(stake_history_report.transaction_count))
            stake_table.add_row("Starting Balance", self._money(stake_history_report.starting_balance))
            stake_table.add_row("Ending Balance", self._money(stake_history_report.ending_balance))
            stake_table.add_row("Net Change", self._money(stake_history_report.net_change))

            monitor = stake_history_report.monitor_summary
            stake_table.add_row("Session Status", monitor.session_status)
            stake_table.add_row("End Reason", "N/A" if monitor.end_reason is None else monitor.end_reason)
            stake_table.add_row("Current Stake", self._money(monitor.current_stake))
            stake_table.add_row("Volatility", self._rate(monitor.volatility))
            stake_table.add_row("Total Changes", str(monitor.total_changes))
            self._console.print(stake_table)

            boundary = monitor.boundary_validation
            boundary_table = Table(show_header=True, header_style="bold cyan", title="Stake Boundary Validation")
            boundary_table.add_column("Metric", style="white")
            boundary_table.add_column("Value", style="bright_white")
            boundary_table.add_row("Lower Limit", self._money(boundary.lower_limit))
            boundary_table.add_row("Upper Limit", self._money(boundary.upper_limit))
            boundary_table.add_row("Warning Lower", self._money(boundary.warning_lower))
            boundary_table.add_row("Warning Upper", self._money(boundary.warning_upper))
            boundary_table.add_row("Current Balance", self._money(boundary.current_balance))
            boundary_table.add_row("Within Bounds", str(boundary.is_within_bounds))
            boundary_table.add_row("Approaching Lower Warning", str(boundary.approaching_lower_warning))
            boundary_table.add_row("Approaching Upper Warning", str(boundary.approaching_upper_warning))
            boundary_table.add_row("Reached Lower Limit", str(boundary.reached_lower_limit))
            boundary_table.add_row("Reached Upper Limit", str(boundary.reached_upper_limit))
            self._console.print(boundary_table)

            if stake_history_report.transaction_breakdown:
                breakdown_table = Table(show_header=True, header_style="bold cyan", title="Stake Transaction Breakdown")
                breakdown_table.add_column("Transaction Type", style="white")
                breakdown_table.add_column("Count", style="bright_white")
                for transaction_type, count in sorted(stake_history_report.transaction_breakdown.items()):
                    breakdown_table.add_row(transaction_type, str(count))
                self._console.print(breakdown_table)

            if stake_history_report.transactions:
                transactions_table = Table(
                    show_header=True,
                    header_style="bold cyan",
                    title="Recent Stake Transactions (Most Recent 10)",
                )
                transactions_table.add_column("Transaction ID", style="white", justify="right")
                transactions_table.add_column("Type", style="bright_white")
                transactions_table.add_column("Amount", style="bright_white", justify="right")
                transactions_table.add_column("Before", style="bright_white", justify="right")
                transactions_table.add_column("After", style="bright_white", justify="right")
                transactions_table.add_column("Created At", style="bright_white")

                for item in stake_history_report.transactions[-10:]:
                    transactions_table.add_row(
                        str(item.transaction_id),
                        item.transaction_type,
                        self._money(item.amount),
                        self._money(item.balance_before),
                        self._money(item.balance_after),
                        str(item.created_at),
                    )

                self._console.print(transactions_table)

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
    def _format_progress_message(progress_payload: Mapping[str, Any]) -> str:
        phase = str(progress_payload.get("phase", "REPORT")).replace("_", " ").title()
        message = str(progress_payload.get("message", "Generating report..."))
        percentage = progress_payload.get("percentage")
        if percentage is None:
            return f"{phase}: {message}"

        return f"{phase}: {message} ({float(percentage):.0f}%)"

    @staticmethod
    def _money(value: Decimal) -> str:
        return f"{value:.2f}"

    @staticmethod
    def _rate(value: Decimal) -> str:
        return f"{value:.4f}"
