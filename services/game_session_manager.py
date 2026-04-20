from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Mapping, Optional

from config.database import Database
from config.settings import Settings
from models.session_models import (
    PauseRecord,
    SessionContinuationResult,
    SessionDurationMetrics,
    SessionListItem,
    SessionLifecycleState,
    SessionParameters,
    SessionSummary,
)
from models.stake_management import SessionEndReason, SessionStatus, TransactionType
from services.betting_service import BettingService
from services.stake_management_service import StakeManagementService, _to_money
from utils.exceptions import NotFoundException, ValidationErrorType, ValidationException
from utils.input_validator import validation_guard

_RATE_QUANTUM = Decimal("0.0001")
_ZERO = Decimal("0.00")


class GameSessionManager:
    def __init__(
        self,
        database: Database,
        settings: Settings,
        betting_service: Optional[BettingService] = None,
        stake_management_service: Optional[StakeManagementService] = None,
    ) -> None:
        self._database = database
        self._settings = settings
        self._last_validation_result = None
        self._stake_service = stake_management_service or StakeManagementService(
            database=database,
            settings=settings,
        )
        self._betting_service = betting_service or BettingService(
            database=database,
            settings=settings,
            stake_management_service=self._stake_service,
        )

    @validation_guard(
        operation_name="START_SESSION",
        validator_method="validate_session_start_request",
    )
    def start_new_session(
        self,
        gambler_id: int,
        *,
        starting_stake: Decimal | int | float | str | None = None,
        lower_limit: Decimal | int | float | str | None = None,
        upper_limit: Decimal | int | float | str | None = None,
        min_bet: Decimal | int | float | str | None = None,
        max_bet: Decimal | int | float | str | None = None,
        max_games: int | None = None,
        max_session_minutes: int | None = None,
        default_win_probability: Decimal | int | float | str | None = None,
        strict_mode: bool | None = None,
    ) -> SessionLifecycleState:
        self._validate_positive_id(gambler_id, "gambler_id")

        with self._database.session(dictionary=True) as (connection, cursor):
            self._assert_no_open_session(cursor=cursor, gambler_id=gambler_id)

            gambler_row = self._fetch_gambler_row(
                cursor=cursor,
                gambler_id=gambler_id,
                for_update=True,
            )
            if gambler_row is None:
                raise NotFoundException(f"Gambler profile not found for id={gambler_id}.")

            if not bool(gambler_row["is_active"]):
                raise ValidationException(
                    error_type=ValidationErrorType.STAKE_ERROR,
                    field_name="is_active",
                    attempted_value=gambler_row["is_active"],
                    message="Cannot start a session for an inactive gambler.",
                )

            preference_row = self._fetch_preference_row(
                cursor=cursor,
                gambler_id=gambler_id,
                for_update=True,
            )
            if preference_row is None:
                raise NotFoundException(
                    f"Betting preferences not found for gambler id={gambler_id}."
                )

            current_stake = _to_money(gambler_row["current_stake"], "current_stake")
            resolved_starting = (
                current_stake
                if starting_stake is None
                else _to_money(starting_stake, "starting_stake")
            )
            resolved_lower = (
                _to_money(gambler_row["loss_threshold"], "loss_threshold")
                if lower_limit is None
                else _to_money(lower_limit, "lower_limit")
            )
            resolved_upper = (
                _to_money(gambler_row["win_threshold"], "win_threshold")
                if upper_limit is None
                else _to_money(upper_limit, "upper_limit")
            )

            resolved_min_bet = (
                _to_money(preference_row["min_bet"], "min_bet")
                if min_bet is None
                else _to_money(min_bet, "min_bet")
            )
            resolved_max_bet = (
                _to_money(preference_row["max_bet"], "max_bet")
                if max_bet is None
                else _to_money(max_bet, "max_bet")
            )

            resolved_max_games = (
                self._settings.session_default_max_games
                if max_games is None
                else self._to_positive_int(max_games, "max_games")
            )
            resolved_max_minutes = (
                self._settings.session_default_max_minutes
                if max_session_minutes is None
                else self._to_positive_int(max_session_minutes, "max_session_minutes")
            )
            resolved_probability = self._normalize_probability(default_win_probability)
            resolved_strict_mode = (
                self._settings.validation_strict_mode
                if strict_mode is None
                else self._to_bool(strict_mode, "strict_mode")
            )

            self._validate_session_inputs(
                starting_stake=resolved_starting,
                lower_limit=resolved_lower,
                upper_limit=resolved_upper,
                min_bet=resolved_min_bet,
                max_bet=resolved_max_bet,
                max_games=resolved_max_games,
                max_session_minutes=resolved_max_minutes,
            )

            cursor.execute(
                """
                INSERT INTO SESSIONS (
                    gambler_id,
                    status,
                    end_reason,
                    starting_stake,
                    ending_stake,
                    peak_stake,
                    lowest_stake,
                    lower_limit,
                    upper_limit,
                    max_games,
                    games_played,
                    total_pause_seconds
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    gambler_id,
                    SessionStatus.ACTIVE.value,
                    SessionEndReason.NOT_ENDED.value,
                    resolved_starting,
                    None,
                    resolved_starting,
                    resolved_starting,
                    resolved_lower,
                    resolved_upper,
                    resolved_max_games,
                    0,
                    0,
                ),
            )
            session_id = int(cursor.lastrowid)

            cursor.execute(
                """
                INSERT INTO SESSION_PARAMETERS (
                    session_id,
                    lower_limit,
                    upper_limit,
                    min_bet,
                    max_bet,
                    default_win_probability,
                    max_session_minutes,
                    strict_mode
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    session_id,
                    resolved_lower,
                    resolved_upper,
                    resolved_min_bet,
                    resolved_max_bet,
                    resolved_probability,
                    resolved_max_minutes,
                    resolved_strict_mode,
                ),
            )

            if resolved_starting != current_stake:
                cursor.execute(
                    """
                    UPDATE GAMBLERS
                    SET current_stake = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE gambler_id = %s
                    """,
                    (resolved_starting, gambler_id),
                )

            delta = _to_money(resolved_starting - current_stake, "session_start_delta")
            self._stake_service._insert_transaction(
                cursor=cursor,
                session_id=session_id,
                gambler_id=gambler_id,
                transaction_type=TransactionType.ADJUSTMENT,
                amount=delta,
                balance_before=current_stake,
                balance_after=resolved_starting,
                transaction_ref=self._stake_service._transaction_ref(
                    prefix=TransactionType.ADJUSTMENT.value,
                    gambler_id=gambler_id,
                    session_id=session_id,
                ),
            )
            self._stake_service._insert_running_snapshot(
                cursor=cursor,
                session_id=session_id,
                starting_stake=resolved_starting,
            )

            connection.commit()

        return self.get_session_lifecycle_state(session_id)

    def continue_session(
        self,
        session_id: int,
        total_games: int,
        *,
        strategy_code: str = "MANUAL",
        bet_amount: Decimal | int | float | str | None = None,
        win_probability: Decimal | int | float | str | None = None,
        payout_multiplier: Decimal | int | float | str = Decimal("1.00"),
        fixed_amount: Decimal | int | float | str | None = None,
        percentage: Decimal | int | float | str | None = None,
        base_amount: Decimal | int | float | str | None = None,
    ) -> SessionContinuationResult:
        self._validate_positive_id(session_id, "session_id")
        if total_games <= 0:
            raise ValidationException(
                error_type=ValidationErrorType.RANGE_ERROR,
                field_name="total_games",
                attempted_value=total_games,
                message="total_games must be a positive integer.",
            )

        strategy = strategy_code.strip().upper()
        results: list = []
        gambler_id: int | None = None

        for _ in range(total_games):
            lifecycle = self.get_session_lifecycle_state(session_id)
            gambler_id = lifecycle.gambler_id

            if lifecycle.status != SessionStatus.ACTIVE:
                break

            if self._is_session_timed_out(session_id):
                self._end_as_timeout(session_id)
                break

            if strategy == "MANUAL":
                if bet_amount is None:
                    raise ValidationException(
                        error_type=ValidationErrorType.NULL_ERROR,
                        field_name="bet_amount",
                        attempted_value=bet_amount,
                        message="bet_amount is required for MANUAL strategy.",
                    )
                result = self._betting_service.place_bet(
                    gambler_id=gambler_id,
                    session_id=session_id,
                    bet_amount=bet_amount,
                    win_probability=win_probability,
                    payout_multiplier=payout_multiplier,
                )
            else:
                result = self._betting_service.place_bet_with_strategy(
                    gambler_id=gambler_id,
                    session_id=session_id,
                    strategy_code=strategy,
                    win_probability=win_probability,
                    payout_multiplier=payout_multiplier,
                    fixed_amount=fixed_amount,
                    percentage=percentage,
                    base_amount=base_amount,
                )

            results.append(result)
            if result.session_status != SessionStatus.ACTIVE.value:
                break

        summary = self.get_session_summary(session_id)

        return SessionContinuationResult(
            session_id=session_id,
            gambler_id=summary.lifecycle.gambler_id,
            requested_games=total_games,
            executed_games=len(results),
            results=tuple(results),
            summary=summary,
        )

    def pause_session(
        self,
        session_id: int,
        *,
        pause_reason: str = "User requested pause",
    ) -> SessionLifecycleState:
        self._validate_positive_id(session_id, "session_id")
        reason = pause_reason.strip() or "User requested pause"

        with self._database.session(dictionary=True) as (connection, cursor):
            session_row = self._fetch_session_row(cursor, session_id, for_update=True)
            if session_row is None:
                raise NotFoundException(f"Session not found for id={session_id}.")

            status = SessionStatus(str(session_row["status"]))
            if status != SessionStatus.ACTIVE:
                raise ValidationException(
                    error_type=ValidationErrorType.RANGE_ERROR,
                    field_name="status",
                    attempted_value=status.value,
                    message="Only ACTIVE sessions can be paused.",
                )

            now = self._utc_now_naive()
            cursor.execute(
                """
                INSERT INTO PAUSE_RECORDS (
                    session_id,
                    pause_reason,
                    paused_at,
                    resumed_at,
                    pause_seconds
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                (session_id, reason, now, None, None),
            )

            cursor.execute(
                """
                UPDATE SESSIONS
                SET status = %s
                WHERE session_id = %s
                """,
                (SessionStatus.PAUSED.value, session_id),
            )

            connection.commit()

        return self.get_session_lifecycle_state(session_id)

    def resume_session(self, session_id: int) -> SessionLifecycleState:
        self._validate_positive_id(session_id, "session_id")

        with self._database.session(dictionary=True) as (connection, cursor):
            session_row = self._fetch_session_row(cursor, session_id, for_update=True)
            if session_row is None:
                raise NotFoundException(f"Session not found for id={session_id}.")

            status = SessionStatus(str(session_row["status"]))
            if status != SessionStatus.PAUSED:
                raise ValidationException(
                    error_type=ValidationErrorType.RANGE_ERROR,
                    field_name="status",
                    attempted_value=status.value,
                    message="Only PAUSED sessions can be resumed.",
                )

            open_pause = self._fetch_open_pause_record(cursor, session_id, for_update=True)
            if open_pause is None:
                raise ValidationException(
                    error_type=ValidationErrorType.RANGE_ERROR,
                    field_name="pause_record",
                    attempted_value=session_id,
                    message="No open pause record exists for this session.",
                )

            now = self._utc_now_naive()
            pause_seconds = max(
                int((now - open_pause["paused_at"]).total_seconds()),
                0,
            )

            cursor.execute(
                """
                UPDATE PAUSE_RECORDS
                SET resumed_at = %s,
                    pause_seconds = %s
                WHERE pause_id = %s
                """,
                (now, pause_seconds, int(open_pause["pause_id"])),
            )

            cursor.execute(
                """
                UPDATE SESSIONS
                SET status = %s,
                    total_pause_seconds = total_pause_seconds + %s
                WHERE session_id = %s
                """,
                (SessionStatus.ACTIVE.value, pause_seconds, session_id),
            )

            connection.commit()

        return self.get_session_lifecycle_state(session_id)

    def end_session(
        self,
        session_id: int,
        *,
        end_reason: SessionEndReason | str = SessionEndReason.MANUAL_STOP,
    ) -> SessionLifecycleState:
        self._validate_positive_id(session_id, "session_id")
        normalized_reason = self._normalize_end_reason(end_reason)

        with self._database.session(dictionary=True) as (connection, cursor):
            session_row = self._fetch_session_row(cursor, session_id, for_update=True)
            if session_row is None:
                raise NotFoundException(f"Session not found for id={session_id}.")

            status = SessionStatus(str(session_row["status"]))
            if status in {
                SessionStatus.ENDED_WIN,
                SessionStatus.ENDED_LOSS,
                SessionStatus.ENDED_MANUAL,
                SessionStatus.ENDED_TIMEOUT,
            }:
                return self._lifecycle_from_row(session_row)

            pause_increment = 0
            if status == SessionStatus.PAUSED:
                open_pause = self._fetch_open_pause_record(cursor, session_id, for_update=True)
                if open_pause is not None:
                    now = self._utc_now_naive()
                    pause_increment = max(
                        int((now - open_pause["paused_at"]).total_seconds()),
                        0,
                    )
                    cursor.execute(
                        """
                        UPDATE PAUSE_RECORDS
                        SET resumed_at = %s,
                            pause_seconds = %s
                        WHERE pause_id = %s
                        """,
                        (now, pause_increment, int(open_pause["pause_id"])),
                    )

            cursor.execute(
                """
                SELECT current_stake
                FROM GAMBLERS
                WHERE gambler_id = %s
                FOR UPDATE
                """,
                (int(session_row["gambler_id"]),),
            )
            gambler_row = cursor.fetchone()
            if gambler_row is None:
                raise NotFoundException(
                    f"Gambler profile not found for id={int(session_row['gambler_id'])}."
                )

            now = self._utc_now_naive()
            ending_stake = _to_money(gambler_row["current_stake"], "current_stake")

            cursor.execute(
                """
                UPDATE SESSIONS
                SET status = %s,
                    end_reason = %s,
                    ending_stake = %s,
                    ended_at = %s,
                    total_pause_seconds = total_pause_seconds + %s
                WHERE session_id = %s
                """,
                (
                    SessionStatus.ENDED_MANUAL.value,
                    normalized_reason.value,
                    ending_stake,
                    now,
                    pause_increment,
                    session_id,
                ),
            )

            connection.commit()

        return self.get_session_lifecycle_state(session_id)

    def get_session_lifecycle_state(self, session_id: int) -> SessionLifecycleState:
        self._validate_positive_id(session_id, "session_id")

        with self._database.session(dictionary=True) as (_, cursor):
            row = self._fetch_session_row(cursor, session_id, for_update=False)

        if row is None:
            raise NotFoundException(f"Session not found for id={session_id}.")

        return self._lifecycle_from_row(row)

    def list_sessions(
        self,
        *,
        gambler_id: int | None = None,
        include_closed: bool = True,
        limit: int = 20,
    ) -> tuple[SessionListItem, ...]:
        if gambler_id is not None:
            self._validate_positive_id(gambler_id, "gambler_id")

        bounded_limit = min(self._to_positive_int(limit, "limit"), 100)

        query = """
        SELECT
            s.session_id,
            s.gambler_id,
            s.status,
            s.games_played,
            s.started_at,
            s.ended_at,
            g.current_stake
        FROM SESSIONS s
        JOIN GAMBLERS g ON g.gambler_id = s.gambler_id
        WHERE 1 = 1
        """
        params: list[Any] = []

        if gambler_id is not None:
            query += " AND s.gambler_id = %s"
            params.append(gambler_id)

        if not include_closed:
            query += " AND s.status IN (%s, %s, %s)"
            params.extend(
                [
                    SessionStatus.INITIALIZED.value,
                    SessionStatus.ACTIVE.value,
                    SessionStatus.PAUSED.value,
                ]
            )

        query += " ORDER BY s.session_id DESC LIMIT %s"
        params.append(bounded_limit)

        with self._database.session(dictionary=True) as (_, cursor):
            cursor.execute(query, tuple(params))
            rows = cursor.fetchall()

        return tuple(
            SessionListItem(
                session_id=int(row["session_id"]),
                gambler_id=int(row["gambler_id"]),
                status=SessionStatus(str(row["status"])),
                games_played=int(row["games_played"]),
                started_at=row["started_at"],
                ended_at=row["ended_at"],
                current_stake=_to_money(row["current_stake"], "current_stake"),
            )
            for row in rows
        )

    def get_pause_history(self, session_id: int) -> tuple[PauseRecord, ...]:
        self._validate_positive_id(session_id, "session_id")

        with self._database.session(dictionary=True) as (_, cursor):
            cursor.execute(
                """
                SELECT
                    pause_id,
                    session_id,
                    pause_reason,
                    paused_at,
                    resumed_at,
                    pause_seconds
                FROM PAUSE_RECORDS
                WHERE session_id = %s
                ORDER BY pause_id
                """,
                (session_id,),
            )
            rows = cursor.fetchall()

        return tuple(
            PauseRecord(
                pause_id=int(row["pause_id"]),
                session_id=int(row["session_id"]),
                pause_reason=str(row["pause_reason"]),
                paused_at=row["paused_at"],
                resumed_at=row["resumed_at"],
                pause_seconds=(
                    None if row["pause_seconds"] is None else int(row["pause_seconds"])
                ),
            )
            for row in rows
        )

    def get_session_summary(self, session_id: int) -> SessionSummary:
        self._validate_positive_id(session_id, "session_id")

        with self._database.session(dictionary=True) as (_, cursor):
            cursor.execute(
                """
                SELECT
                    s.session_id,
                    s.gambler_id,
                    s.status,
                    s.end_reason,
                    s.games_played,
                    s.max_games,
                    s.started_at,
                    s.ended_at,
                    s.total_pause_seconds,
                    s.starting_stake,
                    s.ending_stake,
                    s.peak_stake,
                    s.lowest_stake,
                    g.current_stake
                FROM SESSIONS s
                JOIN GAMBLERS g ON g.gambler_id = s.gambler_id
                WHERE s.session_id = %s
                """,
                (session_id,),
            )
            session_row = cursor.fetchone()
            if session_row is None:
                raise NotFoundException(f"Session not found for id={session_id}.")

            parameters = self._fetch_session_parameters(cursor, session_id)
            if parameters is None:
                raise NotFoundException(
                    f"Session parameters not found for session id={session_id}."
                )

            cursor.execute(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN outcome = 'WIN' THEN 1 ELSE 0 END), 0) AS total_wins,
                    COALESCE(SUM(CASE WHEN outcome = 'LOSS' THEN 1 ELSE 0 END), 0) AS total_losses
                FROM GAME_RECORDS
                WHERE session_id = %s
                """,
                (session_id,),
            )
            outcome_row = cursor.fetchone()
            if outcome_row is None:
                total_wins = 0
                total_losses = 0
            else:
                total_wins = int(outcome_row["total_wins"])
                total_losses = int(outcome_row["total_losses"])

            open_pause = self._fetch_open_pause_record(cursor, session_id, for_update=False)

        lifecycle = self._lifecycle_from_row(session_row)

        now = self._utc_now_naive()
        ended_at = lifecycle.ended_at or now
        total_duration_seconds = max(
            int((ended_at - lifecycle.started_at).total_seconds()),
            0,
        )

        pause_seconds = int(session_row["total_pause_seconds"])
        if lifecycle.status == SessionStatus.PAUSED and open_pause is not None:
            pause_seconds += max(
                int((now - open_pause["paused_at"]).total_seconds()),
                0,
            )

        active_seconds = max(total_duration_seconds - pause_seconds, 0)

        current_stake = _to_money(session_row["current_stake"], "current_stake")
        ending_stake = (
            current_stake
            if session_row["ending_stake"] is None
            else _to_money(session_row["ending_stake"], "ending_stake")
        )

        return SessionSummary(
            lifecycle=lifecycle,
            parameters=parameters,
            duration_metrics=SessionDurationMetrics(
                total_duration_seconds=total_duration_seconds,
                active_duration_seconds=active_seconds,
                pause_duration_seconds=pause_seconds,
            ),
            current_stake=current_stake,
            starting_stake=_to_money(session_row["starting_stake"], "starting_stake"),
            ending_stake=ending_stake,
            peak_stake=_to_money(session_row["peak_stake"], "peak_stake"),
            lowest_stake=_to_money(session_row["lowest_stake"], "lowest_stake"),
            total_wins=total_wins,
            total_losses=total_losses,
        )

    def _is_session_timed_out(self, session_id: int) -> bool:
        summary = self.get_session_summary(session_id)
        if summary.lifecycle.status != SessionStatus.ACTIVE:
            return False

        max_active_seconds = summary.parameters.max_session_minutes * 60
        return summary.duration_metrics.active_duration_seconds >= max_active_seconds

    def _end_as_timeout(self, session_id: int) -> None:
        with self._database.session(dictionary=True) as (connection, cursor):
            session_row = self._fetch_session_row(cursor, session_id, for_update=True)
            if session_row is None:
                raise NotFoundException(f"Session not found for id={session_id}.")

            status = SessionStatus(str(session_row["status"]))
            if status != SessionStatus.ACTIVE:
                return

            cursor.execute(
                """
                SELECT current_stake
                FROM GAMBLERS
                WHERE gambler_id = %s
                FOR UPDATE
                """,
                (int(session_row["gambler_id"]),),
            )
            gambler_row = cursor.fetchone()
            if gambler_row is None:
                raise NotFoundException(
                    f"Gambler profile not found for id={int(session_row['gambler_id'])}."
                )

            now = self._utc_now_naive()
            ending_stake = _to_money(gambler_row["current_stake"], "current_stake")

            cursor.execute(
                """
                UPDATE SESSIONS
                SET status = %s,
                    end_reason = %s,
                    ending_stake = %s,
                    ended_at = %s
                WHERE session_id = %s
                """,
                (
                    SessionStatus.ENDED_TIMEOUT.value,
                    SessionEndReason.TIMEOUT.value,
                    ending_stake,
                    now,
                    session_id,
                ),
            )

            connection.commit()

    def _assert_no_open_session(self, *, cursor: Any, gambler_id: int) -> None:
        cursor.execute(
            """
            SELECT session_id, status
            FROM SESSIONS
            WHERE gambler_id = %s
              AND status IN (%s, %s, %s)
            ORDER BY session_id DESC
            LIMIT 1
            """,
            (
                gambler_id,
                SessionStatus.INITIALIZED.value,
                SessionStatus.ACTIVE.value,
                SessionStatus.PAUSED.value,
            ),
        )
        row = cursor.fetchone()
        if row is not None:
            raise ValidationException(
                error_type=ValidationErrorType.RANGE_ERROR,
                field_name="session_status",
                attempted_value=str(row["status"]),
                message=(
                    "An initialized, active, or paused session already exists for this gambler."
                ),
            )

    def _validate_session_inputs(
        self,
        *,
        starting_stake: Decimal,
        lower_limit: Decimal,
        upper_limit: Decimal,
        min_bet: Decimal,
        max_bet: Decimal,
        max_games: int,
        max_session_minutes: int,
    ) -> None:
        if starting_stake < self._settings.min_initial_stake:
            raise ValidationException(
                error_type=ValidationErrorType.STAKE_ERROR,
                field_name="starting_stake",
                attempted_value=starting_stake,
                message=(
                    "starting_stake is below configured minimum "
                    f"{self._settings.min_initial_stake}."
                ),
            )

        if starting_stake > self._settings.max_initial_stake:
            raise ValidationException(
                error_type=ValidationErrorType.STAKE_ERROR,
                field_name="starting_stake",
                attempted_value=starting_stake,
                message=(
                    "starting_stake is above configured maximum "
                    f"{self._settings.max_initial_stake}."
                ),
            )

        if lower_limit < _ZERO:
            raise ValidationException(
                error_type=ValidationErrorType.LIMIT_ERROR,
                field_name="lower_limit",
                attempted_value=lower_limit,
                message="lower_limit cannot be negative.",
            )

        if upper_limit <= lower_limit:
            raise ValidationException(
                error_type=ValidationErrorType.LIMIT_ERROR,
                field_name="upper_limit",
                attempted_value=upper_limit,
                message="upper_limit must be greater than lower_limit.",
            )

        if starting_stake < lower_limit or starting_stake > upper_limit:
            raise ValidationException(
                error_type=ValidationErrorType.LIMIT_ERROR,
                field_name="starting_stake",
                attempted_value=starting_stake,
                message="starting_stake must be between lower_limit and upper_limit.",
            )

        if min_bet <= _ZERO:
            raise ValidationException(
                error_type=ValidationErrorType.BET_ERROR,
                field_name="min_bet",
                attempted_value=min_bet,
                message="min_bet must be greater than zero.",
            )

        if max_bet < min_bet:
            raise ValidationException(
                error_type=ValidationErrorType.BET_ERROR,
                field_name="max_bet",
                attempted_value=max_bet,
                message="max_bet must be greater than or equal to min_bet.",
            )

        if max_games <= 0:
            raise ValidationException(
                error_type=ValidationErrorType.RANGE_ERROR,
                field_name="max_games",
                attempted_value=max_games,
                message="max_games must be a positive integer.",
            )

        if max_session_minutes <= 0:
            raise ValidationException(
                error_type=ValidationErrorType.RANGE_ERROR,
                field_name="max_session_minutes",
                attempted_value=max_session_minutes,
                message="max_session_minutes must be a positive integer.",
            )

    def _normalize_probability(
        self,
        value: Decimal | int | float | str | None,
    ) -> Decimal:
        source = (
            self._settings.session_default_win_probability
            if value is None
            else value
        )

        try:
            normalized = Decimal(str(source)).quantize(_RATE_QUANTUM, rounding=ROUND_HALF_UP)
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise ValidationException(
                error_type=ValidationErrorType.PROBABILITY_ERROR,
                field_name="default_win_probability",
                attempted_value=value,
                message="Invalid default win probability.",
            ) from exc

        if normalized < Decimal("0.0000") or normalized > Decimal("1.0000"):
            raise ValidationException(
                error_type=ValidationErrorType.PROBABILITY_ERROR,
                field_name="default_win_probability",
                attempted_value=normalized,
                message="default_win_probability must be between 0 and 1.",
            )

        return normalized

    @staticmethod
    def _to_positive_int(value: Any, field_name: str) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise ValidationException(
                error_type=ValidationErrorType.NUMERIC_ERROR,
                field_name=field_name,
                attempted_value=value,
                message=f"{field_name} must be a valid integer.",
            ) from exc

        if parsed <= 0:
            raise ValidationException(
                error_type=ValidationErrorType.RANGE_ERROR,
                field_name=field_name,
                attempted_value=parsed,
                message=f"{field_name} must be a positive integer.",
            )

        return parsed

    @staticmethod
    def _to_bool(value: Any, field_name: str) -> bool:
        if isinstance(value, bool):
            return value

        normalized = str(value).strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off"}:
            return False

        raise ValidationException(
            error_type=ValidationErrorType.RANGE_ERROR,
            field_name=field_name,
            attempted_value=value,
            message=f"{field_name} must be a boolean value.",
        )

    @staticmethod
    def _normalize_end_reason(value: SessionEndReason | str) -> SessionEndReason:
        if isinstance(value, SessionEndReason):
            if value == SessionEndReason.NOT_ENDED:
                return SessionEndReason.MANUAL_STOP
            return value

        normalized = str(value).strip().upper()
        if normalized == SessionEndReason.NOT_ENDED.value:
            normalized = SessionEndReason.MANUAL_STOP.value

        try:
            return SessionEndReason(normalized)
        except ValueError as exc:
            raise ValidationException(
                error_type=ValidationErrorType.RANGE_ERROR,
                field_name="end_reason",
                attempted_value=value,
                message="Invalid session end reason.",
            ) from exc

    @staticmethod
    def _validate_positive_id(value: int, field_name: str) -> None:
        if value <= 0:
            raise ValidationException(
                error_type=ValidationErrorType.RANGE_ERROR,
                field_name=field_name,
                attempted_value=value,
                message=f"{field_name} must be a positive integer.",
            )

    @staticmethod
    def _lifecycle_from_row(row: Mapping[str, Any]) -> SessionLifecycleState:
        raw_end_reason = row["end_reason"]
        end_reason = None
        if raw_end_reason is not None:
            end_reason = SessionEndReason(str(raw_end_reason))

        return SessionLifecycleState(
            session_id=int(row["session_id"]),
            gambler_id=int(row["gambler_id"]),
            status=SessionStatus(str(row["status"])),
            end_reason=end_reason,
            games_played=int(row["games_played"]),
            max_games=int(row["max_games"]),
            started_at=row["started_at"],
            ended_at=row["ended_at"],
        )

    @staticmethod
    def _fetch_session_row(
        cursor: Any,
        session_id: int,
        *,
        for_update: bool,
    ) -> Mapping[str, Any] | None:
        query = """
        SELECT
            session_id,
            gambler_id,
            status,
            end_reason,
            games_played,
            max_games,
            started_at,
            ended_at,
            total_pause_seconds,
            starting_stake,
            ending_stake,
            peak_stake,
            lowest_stake
        FROM SESSIONS
        WHERE session_id = %s
        """
        if for_update:
            query += " FOR UPDATE"

        cursor.execute(query, (session_id,))
        return cursor.fetchone()

    @staticmethod
    def _fetch_gambler_row(
        cursor: Any,
        gambler_id: int,
        *,
        for_update: bool,
    ) -> Mapping[str, Any] | None:
        query = """
        SELECT
            gambler_id,
            is_active,
            current_stake,
            win_threshold,
            loss_threshold
        FROM GAMBLERS
        WHERE gambler_id = %s
        """
        if for_update:
            query += " FOR UPDATE"

        cursor.execute(query, (gambler_id,))
        return cursor.fetchone()

    @staticmethod
    def _fetch_preference_row(
        cursor: Any,
        gambler_id: int,
        *,
        for_update: bool,
    ) -> Mapping[str, Any] | None:
        query = """
        SELECT min_bet, max_bet
        FROM BETTING_PREFERENCES
        WHERE gambler_id = %s
        """
        if for_update:
            query += " FOR UPDATE"

        cursor.execute(query, (gambler_id,))
        return cursor.fetchone()

    @staticmethod
    def _fetch_open_pause_record(
        cursor: Any,
        session_id: int,
        *,
        for_update: bool,
    ) -> Mapping[str, Any] | None:
        query = """
        SELECT pause_id, paused_at
        FROM PAUSE_RECORDS
        WHERE session_id = %s
          AND resumed_at IS NULL
        ORDER BY pause_id DESC
        LIMIT 1
        """
        if for_update:
            query += " FOR UPDATE"

        cursor.execute(query, (session_id,))
        return cursor.fetchone()

    @staticmethod
    def _fetch_session_parameters(
        cursor: Any,
        session_id: int,
    ) -> SessionParameters | None:
        cursor.execute(
            """
            SELECT
                parameter_id,
                session_id,
                lower_limit,
                upper_limit,
                min_bet,
                max_bet,
                default_win_probability,
                max_session_minutes,
                strict_mode,
                created_at
            FROM SESSION_PARAMETERS
            WHERE session_id = %s
            """,
            (session_id,),
        )
        row = cursor.fetchone()
        if row is None:
            return None

        return SessionParameters(
            parameter_id=int(row["parameter_id"]),
            session_id=int(row["session_id"]),
            lower_limit=_to_money(row["lower_limit"], "lower_limit"),
            upper_limit=_to_money(row["upper_limit"], "upper_limit"),
            min_bet=_to_money(row["min_bet"], "min_bet"),
            max_bet=_to_money(row["max_bet"], "max_bet"),
            default_win_probability=Decimal(str(row["default_win_probability"])).quantize(
                _RATE_QUANTUM,
                rounding=ROUND_HALF_UP,
            ),
            max_session_minutes=int(row["max_session_minutes"]),
            strict_mode=bool(row["strict_mode"]),
            created_at=row["created_at"],
        )

    @staticmethod
    def _utc_now_naive() -> datetime:
        return datetime.now(timezone.utc).replace(tzinfo=None)
