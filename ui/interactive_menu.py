from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

from config.database import Database
from config.settings import Settings
from models.gambler_profile import BettingPreferences, GamblerProfile
from models.stake_management import SessionEndReason, SessionStatus
from rich.console import Console
from rich.prompt import Confirm, Prompt
from rich.table import Table
from services.betting_service import BettingService
from services.game_session_manager import GameSessionManager
from services.gambler_profile_service import GamblerProfileService
from services.stake_management_service import StakeManagementService
from services.win_loss_calculator import WinLossCalculator
from ui.game_status_display import GameStatusDisplay
from ui.session_summary import SessionSummaryRenderer
from utils.exceptions import DataAccessException, NotFoundException, ValidationException
from utils.input_validator import get_last_validation_result


class InteractiveMenu:
    def __init__(self, *, database: Database, settings: Settings, console: Console) -> None:
        self._database = database
        self._settings = settings
        self._console = console

        self._stake_service = StakeManagementService(database=database, settings=settings)
        self._betting_service = BettingService(
            database=database,
            settings=settings,
            stake_management_service=self._stake_service,
        )
        self._session_manager = GameSessionManager(
            database=database,
            settings=settings,
            betting_service=self._betting_service,
            stake_management_service=self._stake_service,
        )
        self._profile_service = GamblerProfileService(database=database, settings=settings)
        self._win_loss_calculator = WinLossCalculator(database=database, settings=settings)

        self._status_display = GameStatusDisplay(console=console)
        self._summary_renderer = SessionSummaryRenderer(console=console)

    async def run(self) -> None:
        self._status_display.show_banner()

        gambler_id = self._resolve_gambler_id()
        if gambler_id is None:
            self._status_display.show_info("No profile selected. See you next time.")
            return

        session_id = self._resolve_session_id(gambler_id)
        if session_id is None:
            self._status_display.show_info("No session selected. See you next time.")
            return

        await self._session_loop(gambler_id=gambler_id, session_id=session_id)

    def _resolve_gambler_id(self) -> int | None:
        while True:
            self._console.print("\n[bold cyan]Player Setup[/bold cyan]")
            self._console.print("[white]1.[/white] Create a new player profile")
            self._console.print("[white]2.[/white] Load an existing profile")
            self._console.print("[white]0.[/white] Exit")
            choice = Prompt.ask(
                "Choose an option",
                choices=["1", "2", "0"],
                default="1",
            )

            if choice == "1":
                gambler_id = self._create_gambler_profile()
                if gambler_id is not None:
                    return gambler_id
            elif choice == "2":
                gambler_id = self._load_existing_gambler()
                if gambler_id is not None:
                    return gambler_id
            else:
                return None

    def _create_gambler_profile(self) -> int | None:
        try:
            username = self._prompt_text("Choose a username")
            full_name = self._prompt_text("Your full name")
            email = self._prompt_text("Email address")

            initial_stake = self._prompt_decimal(
                "Starting stake",
                minimum=self._settings.min_initial_stake,
            )
            win_threshold = self._prompt_decimal(
                "Profit target (auto-end when reached)",
                default=(initial_stake * Decimal("1.20")),
                minimum=initial_stake,
            )
            loss_threshold = self._prompt_decimal(
                "Stop-loss amount",
                default=(initial_stake * Decimal("0.80")),
                minimum=Decimal("0.00"),
            )
            min_required_stake = self._prompt_decimal(
                "Minimum required stake",
                default=Decimal("0.00"),
                minimum=Decimal("0.00"),
            )

            min_bet = self._prompt_decimal("Minimum bet amount", minimum=Decimal("0.01"))
            max_bet = self._prompt_decimal("Maximum bet amount", minimum=min_bet)
            preferred_game_type = self._prompt_text(
                "Preferred game type",
                default="STANDARD",
            )

            auto_play_enabled = Confirm.ask("Enable auto-play mode?", default=False)
            auto_play_max_games = 0
            if auto_play_enabled:
                auto_play_max_games = self._prompt_int(
                    "Auto-play game limit",
                    default=self._settings.session_default_max_games,
                    minimum=1,
                )

            profile = GamblerProfile(
                gambler_id=None,
                username=username,
                full_name=full_name,
                email=email,
                initial_stake=initial_stake,
                current_stake=initial_stake,
                win_threshold=win_threshold,
                loss_threshold=loss_threshold,
                min_required_stake=min_required_stake,
                is_active=True,
            )
            preferences = BettingPreferences(
                profile_id=None,
                min_bet=min_bet,
                max_bet=max_bet,
                preferred_game_type=preferred_game_type,
                auto_play_enabled=auto_play_enabled,
                auto_play_max_games=auto_play_max_games,
            )

            created_profile = self._profile_service.create_profile(profile=profile, preferences=preferences)
            self._status_display.show_profile(created_profile)
            self._status_display.show_info(
                f"Profile created successfully. Player ID: {created_profile.gambler_id}."
            )
            return created_profile.gambler_id
        except Exception as exc:
            self._display_exception(exc)
            return None

    def _load_existing_gambler(self) -> int | None:
        try:
            available_count = self._show_available_players()
            if available_count == 0:
                return None

            gambler_id = self._prompt_int("Enter player ID", minimum=1)
            profile = self._profile_service.get_profile(gambler_id)
            self._status_display.show_profile(profile)
            return gambler_id
        except Exception as exc:
            self._display_exception(exc)
            return None

    def _resolve_session_id(self, gambler_id: int) -> int | None:
        while True:
            open_sessions = self._session_manager.list_sessions(
                gambler_id=gambler_id,
                include_closed=False,
                limit=20,
            )
            if open_sessions:
                self._console.print("\n[bold cyan]Session Setup[/bold cyan]")
                self._status_display.show_warning(
                    "An open session already exists for this player."
                )
                self._console.print("[white]1.[/white] Continue an open session")
                self._console.print("[white]0.[/white] Exit")
                choice = Prompt.ask(
                    "Choose an option",
                    choices=["1", "0"],
                    default="1",
                )
                if choice == "0":
                    return None

                session_id = self._use_existing_session(
                    gambler_id,
                    include_closed=False,
                    id_prompt="Enter open session ID",
                )
                if session_id is not None:
                    return session_id
                continue

            self._console.print("\n[bold cyan]Session Setup[/bold cyan]")
            self._console.print("[white]1.[/white] Start a new session")
            self._console.print("[white]2.[/white] Continue an existing session")
            self._console.print("[white]0.[/white] Exit")
            choice = Prompt.ask(
                "Choose an option",
                choices=["1", "2", "0"],
                default="1",
            )

            if choice == "1":
                session_id = self._start_new_session(gambler_id)
                if session_id is not None:
                    return session_id
            elif choice == "2":
                session_id = self._use_existing_session(gambler_id)
                if session_id is not None:
                    return session_id
            else:
                return None

    def _start_new_session(self, gambler_id: int) -> int | None:
        try:
            use_defaults = Confirm.ask("Use recommended session settings?", default=True)
            if use_defaults:
                lifecycle = self._session_manager.start_new_session(gambler_id=gambler_id)
            else:
                starting_stake = self._prompt_decimal(
                    "Starting stake for this session",
                    minimum=self._settings.min_initial_stake,
                )
                lower_limit = self._prompt_decimal(
                    "Lower stake limit",
                    minimum=Decimal("0.00"),
                )
                upper_limit = self._prompt_decimal(
                    "Upper stake limit",
                    minimum=starting_stake,
                )
                min_bet = self._prompt_decimal("Minimum bet amount", minimum=Decimal("0.01"))
                max_bet = self._prompt_decimal("Maximum bet amount", minimum=min_bet)
                max_games = self._prompt_int(
                    "Maximum number of games",
                    default=self._settings.session_default_max_games,
                    minimum=1,
                )
                max_session_minutes = self._prompt_int(
                    "Maximum session length (minutes)",
                    default=self._settings.session_default_max_minutes,
                    minimum=1,
                )
                default_win_probability = self._prompt_decimal(
                    "Default win probability (0 to 1)",
                    default=self._settings.session_default_win_probability,
                    minimum=Decimal("0.00"),
                    maximum=Decimal("1.00"),
                )
                strict_mode = Confirm.ask(
                    "Enable strict validation mode?",
                    default=self._settings.validation_strict_mode,
                )

                lifecycle = self._session_manager.start_new_session(
                    gambler_id=gambler_id,
                    starting_stake=starting_stake,
                    lower_limit=lower_limit,
                    upper_limit=upper_limit,
                    min_bet=min_bet,
                    max_bet=max_bet,
                    max_games=max_games,
                    max_session_minutes=max_session_minutes,
                    default_win_probability=default_win_probability,
                    strict_mode=strict_mode,
                )

            self._show_validation_feedback(self._session_manager)
            self._status_display.show_info(
                f"Session started. Session ID: {lifecycle.session_id}."
            )
            return lifecycle.session_id
        except Exception as exc:
            self._display_exception(exc)
            self._show_validation_feedback(self._session_manager)
            return None

    def _use_existing_session(
        self,
        gambler_id: int,
        *,
        include_closed: bool = True,
        id_prompt: str = "Enter session ID",
    ) -> int | None:
        try:
            available_count = self._show_available_sessions(
                gambler_id,
                include_closed=include_closed,
            )
            if available_count == 0:
                return None

            session_id = self._prompt_int(id_prompt, minimum=1)
            summary = self._session_manager.get_session_summary(session_id)
            if summary.lifecycle.gambler_id != gambler_id:
                self._status_display.show_warning(
                    "That session belongs to a different player and cannot be attached."
                )
                return None

            if not include_closed and summary.lifecycle.status not in {
                SessionStatus.INITIALIZED,
                SessionStatus.ACTIVE,
                SessionStatus.PAUSED,
            }:
                self._status_display.show_warning(
                    "That session is already closed. Please choose an open session."
                )
                return None

            self._status_display.show_session_status(summary)
            return session_id
        except Exception as exc:
            self._display_exception(exc)
            return None

    async def _session_loop(self, *, gambler_id: int, session_id: int) -> None:
        while True:
            try:
                summary = self._session_manager.get_session_summary(session_id)
            except Exception as exc:
                self._display_exception(exc)
                return

            self._status_display.show_session_status(summary)
            status = summary.lifecycle.status

            if status in {
                SessionStatus.ENDED_WIN,
                SessionStatus.ENDED_LOSS,
                SessionStatus.ENDED_MANUAL,
                SessionStatus.ENDED_TIMEOUT,
            }:
                self._render_final_report(session_id)
                return

            if status == SessionStatus.PAUSED:
                self._console.print("\n[bold cyan]Paused Session Actions[/bold cyan]")
                self._console.print("[white]1.[/white] Resume session")
                self._console.print("[white]2.[/white] End session")
                self._console.print("[white]3.[/white] View current report")
                self._console.print("[white]0.[/white] Exit menu")
                choice = Prompt.ask(
                    "Choose what to do next",
                    choices=["1", "2", "3", "0"],
                    default="1",
                )
                if choice == "1":
                    self._handle_resume(session_id)
                elif choice == "2":
                    self._handle_end_session(session_id)
                elif choice == "3":
                    self._render_final_report(session_id)
                else:
                    self._status_display.show_warning("Leaving the session paused.")
                    return
            else:
                self._console.print("\n[bold cyan]Active Session Actions[/bold cyan]")
                self._console.print("[white]1.[/white] Place a manual bet")
                self._console.print("[white]2.[/white] Place a strategy-based bet")
                self._console.print("[white]3.[/white] Run multiple games")
                self._console.print("[white]4.[/white] Pause session")
                self._console.print("[white]5.[/white] End session")
                self._console.print("[white]6.[/white] Refresh status")
                self._console.print("[white]0.[/white] Exit menu")
                choice = Prompt.ask(
                    "Choose what to do next",
                    choices=["1", "2", "3", "4", "5", "6", "0"],
                    default="6",
                )
                if choice == "1":
                    await self._handle_manual_bet(gambler_id, session_id)
                elif choice == "2":
                    await self._handle_strategy_bet(gambler_id, session_id)
                elif choice == "3":
                    await self._handle_continue_session(session_id)
                elif choice == "4":
                    self._handle_pause(session_id)
                elif choice == "5":
                    self._handle_end_session(session_id)
                elif choice == "6":
                    continue
                else:
                    should_end = Confirm.ask(
                        "Would you like to end this session before exiting?",
                        default=False,
                    )
                    if should_end:
                        self._handle_end_session(session_id)
                    return

    async def _handle_manual_bet(self, gambler_id: int, session_id: int) -> None:
        try:
            bet_amount = self._prompt_decimal("Bet amount", minimum=Decimal("0.01"))
            use_default_probability = Confirm.ask(
                "Use session default win probability?",
                default=True,
            )
            win_probability = None
            if not use_default_probability:
                win_probability = self._prompt_decimal(
                    "Win probability (0 to 1)",
                    minimum=Decimal("0.00"),
                    maximum=Decimal("1.00"),
                )

            payout_multiplier = self._prompt_decimal(
                "Payout multiplier",
                default=Decimal("1.00"),
                minimum=Decimal("0.0001"),
            )

            result = await self._betting_service.place_bet(
                gambler_id=gambler_id,
                session_id=session_id,
                bet_amount=bet_amount,
                win_probability=win_probability,
                payout_multiplier=payout_multiplier,
            )
            self._status_display.show_bet_outcome(result)
            self._show_validation_feedback(self._betting_service)
        except Exception as exc:
            self._display_exception(exc)
            self._show_validation_feedback(self._betting_service)

    async def _handle_strategy_bet(self, gambler_id: int, session_id: int) -> None:
        try:
            self._console.print("\n[bold cyan]Strategy Options[/bold cyan]")
            self._console.print("[white]1.[/white] Fixed amount")
            self._console.print("[white]2.[/white] Percentage of current stake")
            self._console.print("[white]3.[/white] Martingale")
            strategy_choice = Prompt.ask(
                "Choose a strategy",
                choices=["1", "2", "3"],
                default="1",
            )
            strategy_code = {
                "1": "FIXED_AMOUNT",
                "2": "PERCENTAGE",
                "3": "MARTINGALE",
            }[strategy_choice]

            win_probability = None
            if not Confirm.ask("Use session default win probability?", default=True):
                win_probability = self._prompt_decimal(
                    "Win probability (0 to 1)",
                    minimum=Decimal("0.00"),
                    maximum=Decimal("1.00"),
                )

            payout_multiplier = self._prompt_decimal(
                "Payout multiplier",
                default=Decimal("1.00"),
                minimum=Decimal("0.0001"),
            )

            fixed_amount = None
            percentage = None
            base_amount = None

            if strategy_code == "FIXED_AMOUNT":
                fixed_amount = self._prompt_decimal("Fixed amount", minimum=Decimal("0.01"))
            elif strategy_code == "PERCENTAGE":
                percentage = self._prompt_decimal(
                    "Percentage (0-1 or 0-100)",
                    default=Decimal("0.05"),
                    minimum=Decimal("0.0001"),
                )
            else:
                base_amount = self._prompt_decimal(
                    "Martingale base amount",
                    minimum=Decimal("0.01"),
                )

            result = await self._betting_service.place_bet_with_strategy(
                gambler_id=gambler_id,
                session_id=session_id,
                strategy_code=strategy_code,
                win_probability=win_probability,
                payout_multiplier=payout_multiplier,
                fixed_amount=fixed_amount,
                percentage=percentage,
                base_amount=base_amount,
            )
            self._status_display.show_bet_outcome(result)
            self._show_validation_feedback(self._betting_service)
        except Exception as exc:
            self._display_exception(exc)
            self._show_validation_feedback(self._betting_service)

    async def _handle_continue_session(self, session_id: int) -> None:
        try:
            total_games = self._prompt_int("How many games would you like to run", minimum=1)
            self._console.print("\n[bold cyan]Game Run Mode[/bold cyan]")
            self._console.print("[white]1.[/white] Manual amount for each game")
            self._console.print("[white]2.[/white] Fixed amount strategy")
            self._console.print("[white]3.[/white] Percentage strategy")
            self._console.print("[white]4.[/white] Martingale strategy")
            strategy_choice = Prompt.ask(
                "Choose a run mode",
                choices=["1", "2", "3", "4"],
                default="1",
            )
            strategy_code = {
                "1": "MANUAL",
                "2": "FIXED_AMOUNT",
                "3": "PERCENTAGE",
                "4": "MARTINGALE",
            }[strategy_choice]

            bet_amount = None
            fixed_amount = None
            percentage = None
            base_amount = None

            if strategy_code == "MANUAL":
                bet_amount = self._prompt_decimal("Manual bet amount", minimum=Decimal("0.01"))
            elif strategy_code == "FIXED_AMOUNT":
                fixed_amount = self._prompt_decimal("Fixed amount", minimum=Decimal("0.01"))
            elif strategy_code == "PERCENTAGE":
                percentage = self._prompt_decimal(
                    "Percentage (0-1 or 0-100)",
                    default=Decimal("0.05"),
                    minimum=Decimal("0.0001"),
                )
            else:
                base_amount = self._prompt_decimal(
                    "Martingale base amount",
                    minimum=Decimal("0.01"),
                )

            win_probability = None
            if not Confirm.ask("Use session default win probability?", default=True):
                win_probability = self._prompt_decimal(
                    "Win probability (0 to 1)",
                    minimum=Decimal("0.00"),
                    maximum=Decimal("1.00"),
                )

            payout_multiplier = self._prompt_decimal(
                "Payout multiplier",
                default=Decimal("1.00"),
                minimum=Decimal("0.0001"),
            )

            continuation = await self._session_manager.continue_session(
                session_id=session_id,
                total_games=total_games,
                strategy_code=strategy_code,
                bet_amount=bet_amount,
                win_probability=win_probability,
                payout_multiplier=payout_multiplier,
                fixed_amount=fixed_amount,
                percentage=percentage,
                base_amount=base_amount,
            )

            for result in continuation.results:
                self._status_display.show_bet_outcome(result)

            self._status_display.show_info(
                (
                    f"Executed {continuation.executed_games} game(s) out of "
                    f"{continuation.requested_games} requested."
                )
            )
        except Exception as exc:
            self._display_exception(exc)

    def _handle_pause(self, session_id: int) -> None:
        try:
            reason = self._prompt_text(
                "Pause reason",
                default="Taking a short break",
            )
            self._session_manager.pause_session(session_id=session_id, pause_reason=reason)
            self._status_display.show_info("Session paused.")
        except Exception as exc:
            self._display_exception(exc)

    def _handle_resume(self, session_id: int) -> None:
        try:
            self._session_manager.resume_session(session_id=session_id)
            self._status_display.show_info("Session resumed. You are back in the game.")
        except Exception as exc:
            self._display_exception(exc)

    def _handle_end_session(self, session_id: int) -> None:
        try:
            self._console.print("\n[bold cyan]End Session[/bold cyan]")
            self._console.print("[white]1.[/white] End manually now")
            self._console.print("[white]2.[/white] End as timeout")
            reason_choice = Prompt.ask(
                "Choose an end reason",
                choices=["1", "2"],
                default="1",
            )
            end_reason = (
                SessionEndReason.TIMEOUT if reason_choice == "2" else SessionEndReason.MANUAL_STOP
            )
            self._session_manager.end_session(session_id=session_id, end_reason=end_reason)
            self._status_display.show_info("Session ended.")
            self._render_final_report(session_id)
        except Exception as exc:
            self._display_exception(exc)

    def _render_final_report(self, session_id: int) -> None:
        try:
            session_summary = self._session_manager.get_session_summary(session_id)
            win_loss_statistics = self._win_loss_calculator.get_win_loss_statistics(session_id)
            gambler_statistics = self._profile_service.retrieve_profile_statistics(
                session_summary.lifecycle.gambler_id
            )
            self._summary_renderer.render_end_of_session(
                session_summary=session_summary,
                win_loss_statistics=win_loss_statistics,
                gambler_statistics=gambler_statistics,
            )
        except Exception as exc:
            self._display_exception(exc)

    def _show_available_players(self) -> int:
        profiles = self._profile_service.list_profiles(limit=20)
        if not profiles:
            self._status_display.show_warning("No player profiles found yet. Create one first.")
            return 0

        table = Table(
            title="Available Players (Most Recent 20)",
            show_header=True,
            header_style="bold cyan",
        )
        table.add_column("Player ID", style="white", justify="right")
        table.add_column("Username", style="bright_white")
        table.add_column("Name", style="bright_white")
        table.add_column("Active", style="bright_white")
        table.add_column("Current Stake", style="bright_white", justify="right")

        for profile in profiles:
            table.add_row(
                str(profile.gambler_id),
                profile.username,
                profile.full_name,
                "Yes" if profile.is_active else "No",
                f"{profile.current_stake:.2f}",
            )

        self._console.print(table)
        return len(profiles)

    def _show_available_sessions(self, gambler_id: int, *, include_closed: bool = True) -> int:
        sessions = self._session_manager.list_sessions(
            gambler_id=gambler_id,
            include_closed=include_closed,
            limit=20,
        )
        if not sessions:
            if include_closed:
                self._status_display.show_warning(
                    "No sessions found for this player yet. Start a new session."
                )
            else:
                self._status_display.show_warning(
                    "No open sessions found for this player right now."
                )
            return 0

        title_prefix = "Available Sessions" if include_closed else "Open Sessions"

        table = Table(
            title=f"{title_prefix} For Player {gambler_id} (Most Recent 20)",
            show_header=True,
            header_style="bold cyan",
        )
        table.add_column("Session ID", style="white", justify="right")
        table.add_column("Status", style="bright_white")
        table.add_column("Games", style="bright_white", justify="right")
        table.add_column("Current Stake", style="bright_white", justify="right")
        table.add_column("Started At", style="bright_white")
        table.add_column("Ended At", style="bright_white")

        for session in sessions:
            table.add_row(
                str(session.session_id),
                session.status.value,
                str(session.games_played),
                f"{session.current_stake:.2f}",
                str(session.started_at),
                "-" if session.ended_at is None else str(session.ended_at),
            )

        self._console.print(table)
        return len(sessions)

    def _show_validation_feedback(self, service_instance: Any) -> None:
        result = get_last_validation_result(service_instance)
        self._status_display.show_validation_feedback(result)

    def _display_exception(self, exc: Exception) -> None:
        if isinstance(exc, ValidationException):
            message = exc.user_message if exc.user_message else exc.message
            if exc.is_recoverable:
                self._status_display.show_warning(message)
            else:
                self._status_display.show_error(message)
            return

        if isinstance(exc, NotFoundException):
            self._status_display.show_warning(str(exc))
            return

        if isinstance(exc, DataAccessException):
            self._status_display.show_error(str(exc))
            return

        self._status_display.show_error(f"Unexpected error: {exc}")

    def _prompt_text(
        self,
        label: str,
        *,
        default: str | None = None,
    ) -> str:
        while True:
            value = Prompt.ask(label, default=default)
            normalized = value.strip()
            if normalized:
                return normalized
            self._status_display.show_warning(f"Please enter a value for: {label}.")

    def _prompt_int(
        self,
        label: str,
        *,
        default: int | None = None,
        minimum: int | None = None,
    ) -> int:
        while True:
            default_value = None if default is None else str(default)
            raw = Prompt.ask(label, default=default_value)
            try:
                parsed = int(raw)
            except ValueError:
                self._status_display.show_warning(f"{label} must be an integer.")
                continue

            if minimum is not None and parsed < minimum:
                self._status_display.show_warning(f"{label} must be >= {minimum}.")
                continue

            return parsed

    def _prompt_decimal(
        self,
        label: str,
        *,
        default: Decimal | None = None,
        minimum: Decimal | None = None,
        maximum: Decimal | None = None,
    ) -> Decimal:
        while True:
            default_value = None if default is None else str(default)
            raw = Prompt.ask(label, default=default_value)
            try:
                parsed = Decimal(raw)
            except (InvalidOperation, ValueError):
                self._status_display.show_warning(f"{label} must be a decimal number.")
                continue

            if minimum is not None and parsed < minimum:
                self._status_display.show_warning(f"{label} must be >= {minimum}.")
                continue

            if maximum is not None and parsed > maximum:
                self._status_display.show_warning(f"{label} must be <= {maximum}.")
                continue

            return parsed

