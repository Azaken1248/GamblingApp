from __future__ import annotations

import inspect
import json
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from functools import wraps
from typing import Any, Callable, Mapping

from config.database import Database
from config.settings import Settings
from utils.exceptions import (
    ValidationErrorType,
    ValidationException,
    ValidationIssue,
    ValidationResult,
    ValidationSeverity,
)

_MONEY_QUANTUM = Decimal("0.01")
_RATE_QUANTUM = Decimal("0.0001")
_ZERO = Decimal("0.00")


def validation_guard(
    *,
    operation_name: str,
    validator_method: str,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator that runs pre-execution input validation."""

    def decorator(function: Callable[..., Any]) -> Callable[..., Any]:
        function_signature = inspect.signature(function)

        @wraps(function)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            bound = function_signature.bind(*args, **kwargs)
            bound.apply_defaults()

            service_instance = bound.arguments.get("self")
            if service_instance is None:
                return function(*args, **kwargs)

            validator = _resolve_validator(service_instance)
            payload = {
                key: value
                for key, value in bound.arguments.items()
                if key != "self"
            }

            validate_callable = getattr(validator, validator_method, None)
            if not callable(validate_callable):
                raise ValidationException(
                    error_type=ValidationErrorType.RANGE_ERROR,
                    field_name="validator_method",
                    attempted_value=validator_method,
                    message="Configured validator method does not exist.",
                    user_message="Validation pipeline is not configured for this operation.",
                )

            result: ValidationResult = validate_callable(
                operation_name=operation_name,
                payload=payload,
            )
            setattr(service_instance, "_last_validation_result", result)

            validator.log_validation_events(
                result=result,
                operation_name=operation_name,
                service_name=service_instance.__class__.__name__,
                method_name=function.__name__,
                payload=payload,
            )

            if not result.is_valid:
                first_error = result.first_error
                if first_error is None:
                    raise ValidationException(
                        error_type=ValidationErrorType.RANGE_ERROR,
                        field_name="validation_result",
                        attempted_value=None,
                        message="Validation failed with no error details.",
                        user_message="Unable to continue because one or more inputs are invalid.",
                    )
                raise first_error.to_exception()

            return function(*args, **kwargs)

        return wrapper

    return decorator


def get_last_validation_result(service_instance: Any) -> ValidationResult | None:
    result = getattr(service_instance, "_last_validation_result", None)
    return result if isinstance(result, ValidationResult) else None


class InputValidator:
    def __init__(self, database: Database, settings: Settings) -> None:
        self._database = database
        self._settings = settings

    def validate_bet_request(
        self,
        *,
        operation_name: str,
        payload: Mapping[str, Any],
    ) -> ValidationResult:
        issues: list[ValidationIssue] = []

        gambler_id = self._to_positive_int(
            payload.get("gambler_id"),
            field_name="gambler_id",
            issues=issues,
        )
        session_id = self._to_positive_int(
            payload.get("session_id"),
            field_name="session_id",
            issues=issues,
        )

        gambler_row: Mapping[str, Any] | None = None
        session_row: Mapping[str, Any] | None = None
        parameter_row: Mapping[str, Any] | None = None
        bets_row: Mapping[str, Any] | None = None

        if gambler_id is not None and session_id is not None:
            try:
                with self._database.session(dictionary=True) as (_, cursor):
                    cursor.execute(
                        """
                        SELECT gambler_id, is_active, current_stake
                        FROM GAMBLERS
                        WHERE gambler_id = %s
                        """,
                        (gambler_id,),
                    )
                    gambler_row = cursor.fetchone()

                    cursor.execute(
                        """
                        SELECT
                            session_id,
                            gambler_id,
                            status,
                            games_played,
                            max_games,
                            lower_limit,
                            upper_limit
                        FROM SESSIONS
                        WHERE session_id = %s
                        """,
                        (session_id,),
                    )
                    session_row = cursor.fetchone()

                    cursor.execute(
                        """
                        SELECT min_bet, max_bet, default_win_probability
                        FROM SESSION_PARAMETERS
                        WHERE session_id = %s
                        """,
                        (session_id,),
                    )
                    parameter_row = cursor.fetchone()

                    cursor.execute(
                        """
                        SELECT
                            COALESCE(MAX(game_index), 0) AS last_game_index,
                            COALESCE(SUM(CASE WHEN is_settled = FALSE THEN 1 ELSE 0 END), 0)
                                AS unsettled_bets
                        FROM BETS
                        WHERE session_id = %s
                        """,
                        (session_id,),
                    )
                    bets_row = cursor.fetchone()
            except Exception as exc:
                issues.append(
                    self._error_issue(
                        field_name="database",
                        attempted_value=str(exc),
                        error_type=ValidationErrorType.RANGE_ERROR,
                        message="Validation lookup failed for betting request.",
                        user_message="Unable to validate this request right now. Please try again.",
                        is_recoverable=True,
                    )
                )

        if gambler_id is not None and gambler_row is None:
            issues.append(
                self._error_issue(
                    field_name="gambler_id",
                    attempted_value=gambler_id,
                    error_type=ValidationErrorType.RANGE_ERROR,
                    message="Gambler does not exist.",
                    user_message="The selected gambler profile was not found.",
                    is_recoverable=True,
                )
            )

        if session_id is not None and session_row is None:
            issues.append(
                self._error_issue(
                    field_name="session_id",
                    attempted_value=session_id,
                    error_type=ValidationErrorType.RANGE_ERROR,
                    message="Session does not exist.",
                    user_message="The selected session was not found.",
                    is_recoverable=True,
                )
            )

        current_stake: Decimal | None = None
        lower_limit: Decimal | None = None
        max_games: int | None = None
        games_played: int | None = None

        if gambler_row is not None:
            if not bool(gambler_row["is_active"]):
                issues.append(
                    self._error_issue(
                        field_name="is_active",
                        attempted_value=gambler_row["is_active"],
                        error_type=ValidationErrorType.STAKE_ERROR,
                        message="Inactive gambler cannot place bets.",
                        user_message="This gambler account is inactive and cannot place bets.",
                        is_recoverable=True,
                    )
                )
            current_stake = self._to_money(
                gambler_row["current_stake"],
                field_name="current_stake",
                issues=issues,
            )

        if session_row is not None:
            if gambler_id is not None and int(session_row["gambler_id"]) != gambler_id:
                issues.append(
                    self._error_issue(
                        field_name="session_id",
                        attempted_value=session_id,
                        error_type=ValidationErrorType.RANGE_ERROR,
                        message="Session belongs to another gambler.",
                        user_message="The selected session does not belong to this gambler.",
                        is_recoverable=True,
                    )
                )

            status_value = str(session_row["status"])
            if status_value != "ACTIVE":
                issues.append(
                    self._error_issue(
                        field_name="status",
                        attempted_value=status_value,
                        error_type=ValidationErrorType.RANGE_ERROR,
                        message="Session must be ACTIVE for betting.",
                        user_message="Betting is only allowed when the session is active.",
                        is_recoverable=True,
                    )
                )

            lower_limit = self._to_money(
                session_row["lower_limit"],
                field_name="lower_limit",
                issues=issues,
            )
            max_games = int(session_row["max_games"])
            games_played = int(session_row["games_played"])

            next_game_index = games_played + 1
            if next_game_index > max_games:
                issues.append(
                    self._error_issue(
                        field_name="max_games",
                        attempted_value=max_games,
                        error_type=ValidationErrorType.LIMIT_ERROR,
                        message="Session has no remaining game slots.",
                        user_message="This session has reached the maximum number of games.",
                        is_recoverable=True,
                    )
                )
            elif next_game_index == max_games:
                issues.append(
                    self._warning_issue(
                        field_name="max_games",
                        attempted_value=max_games,
                        error_type=ValidationErrorType.LIMIT_ERROR,
                        message="Next bet reaches the configured max_games boundary.",
                        user_message="This bet will consume the final available game in this session.",
                    )
                )

        min_bet: Decimal | None = None
        max_bet: Decimal | None = None
        default_probability: Decimal | None = None

        if parameter_row is None and session_row is not None:
            issues.append(
                self._error_issue(
                    field_name="session_parameters",
                    attempted_value=session_id,
                    error_type=ValidationErrorType.RANGE_ERROR,
                    message="SESSION_PARAMETERS row is missing for session.",
                    user_message="Session configuration is incomplete. Please restart the session.",
                    is_recoverable=True,
                )
            )

        if parameter_row is not None:
            min_bet = self._to_money(
                parameter_row["min_bet"],
                field_name="min_bet",
                issues=issues,
            )
            max_bet = self._to_money(
                parameter_row["max_bet"],
                field_name="max_bet",
                issues=issues,
            )
            default_probability = self._to_rate(
                parameter_row["default_win_probability"],
                field_name="default_win_probability",
                issues=issues,
            )

        if bets_row is not None and games_played is not None:
            last_game_index = int(bets_row["last_game_index"])
            unsettled_bets = int(bets_row["unsettled_bets"])
            if last_game_index > games_played:
                issues.append(
                    self._warning_issue(
                        field_name="game_index",
                        attempted_value=last_game_index,
                        error_type=ValidationErrorType.RANGE_ERROR,
                        message="BETS game index is ahead of session games_played.",
                        user_message="Recent game records are still synchronizing. Retry if this persists.",
                    )
                )
            if unsettled_bets > 0:
                issues.append(
                    self._warning_issue(
                        field_name="is_settled",
                        attempted_value=unsettled_bets,
                        error_type=ValidationErrorType.BET_ERROR,
                        message="Found unsettled bets for this session.",
                        user_message="Some prior bets are still unsettled. Results may update shortly.",
                    )
                )

        has_bet_input = "bet_amount" in payload
        raw_bet_amount = payload.get("bet_amount")
        normalized_bet_amount: Decimal | None = None
        if has_bet_input:
            normalized_bet_amount = self._to_money(
                raw_bet_amount,
                field_name="bet_amount",
                issues=issues,
            )
            if normalized_bet_amount is not None:
                if normalized_bet_amount <= _ZERO:
                    issues.append(
                        self._error_issue(
                            field_name="bet_amount",
                            attempted_value=raw_bet_amount,
                            error_type=ValidationErrorType.BET_ERROR,
                            message="bet_amount must be greater than zero.",
                            user_message="Bet amount must be greater than zero.",
                            is_recoverable=True,
                        )
                    )

                if current_stake is not None and normalized_bet_amount > current_stake:
                    issues.append(
                        self._error_issue(
                            field_name="bet_amount",
                            attempted_value=normalized_bet_amount,
                            error_type=ValidationErrorType.BET_ERROR,
                            message="bet_amount cannot exceed current stake.",
                            user_message="Bet amount cannot be greater than the current stake.",
                            is_recoverable=True,
                        )
                    )

                if min_bet is not None and normalized_bet_amount < min_bet:
                    issues.append(
                        self._error_issue(
                            field_name="bet_amount",
                            attempted_value=normalized_bet_amount,
                            error_type=ValidationErrorType.BET_ERROR,
                            message="bet_amount is below the configured session minimum.",
                            user_message=f"Bet amount must be at least {min_bet}.",
                            is_recoverable=True,
                        )
                    )

                if max_bet is not None and normalized_bet_amount > max_bet:
                    issues.append(
                        self._error_issue(
                            field_name="bet_amount",
                            attempted_value=normalized_bet_amount,
                            error_type=ValidationErrorType.BET_ERROR,
                            message="bet_amount is above the configured session maximum.",
                            user_message=f"Bet amount must be at most {max_bet}.",
                            is_recoverable=True,
                        )
                    )

                if current_stake is not None:
                    projected_balance = (current_stake - normalized_bet_amount).quantize(
                        _MONEY_QUANTUM,
                        rounding=ROUND_HALF_UP,
                    )
                    if projected_balance < _ZERO:
                        issues.append(
                            self._error_issue(
                                field_name="projected_balance",
                                attempted_value=projected_balance,
                                error_type=ValidationErrorType.STAKE_ERROR,
                                message="Projected balance cannot be negative.",
                                user_message="This bet would result in a negative balance.",
                                is_recoverable=True,
                            )
                        )
                    if lower_limit is not None and projected_balance <= lower_limit:
                        issues.append(
                            self._warning_issue(
                                field_name="projected_balance",
                                attempted_value=projected_balance,
                                error_type=ValidationErrorType.LIMIT_ERROR,
                                message="Projected balance is at/below session lower limit.",
                                user_message="This bet may immediately hit the session lower limit.",
                            )
                        )
                    if current_stake > _ZERO:
                        exposure_ratio = (normalized_bet_amount / current_stake).quantize(
                            _RATE_QUANTUM,
                            rounding=ROUND_HALF_UP,
                        )
                        if exposure_ratio >= Decimal("0.5000"):
                            issues.append(
                                self._warning_issue(
                                    field_name="bet_amount",
                                    attempted_value=normalized_bet_amount,
                                    error_type=ValidationErrorType.BET_ERROR,
                                    message="Bet size is 50% or more of current stake.",
                                    user_message="High-risk bet: this wager uses at least half of your current balance.",
                                )
                            )

        raw_probability = payload.get("win_probability")
        resolved_probability = raw_probability if raw_probability is not None else default_probability
        if resolved_probability is not None:
            normalized_probability = self._to_rate(
                resolved_probability,
                field_name="win_probability",
                issues=issues,
            )
            if normalized_probability is not None:
                if normalized_probability < Decimal("0.0000") or normalized_probability > Decimal("1.0000"):
                    issues.append(
                        self._error_issue(
                            field_name="win_probability",
                            attempted_value=resolved_probability,
                            error_type=ValidationErrorType.PROBABILITY_ERROR,
                            message="win_probability must be between 0 and 1.",
                            user_message="Win probability must be in the range 0 to 1.",
                            is_recoverable=True,
                        )
                    )
                elif (
                    normalized_probability <= Decimal("0.0500")
                    or normalized_probability >= Decimal("0.9500")
                ):
                    issues.append(
                        self._warning_issue(
                            field_name="win_probability",
                            attempted_value=normalized_probability,
                            error_type=ValidationErrorType.PROBABILITY_ERROR,
                            message="Probability is near an extreme boundary.",
                            user_message="The selected win probability is very extreme and may skew outcomes.",
                        )
                    )

        return ValidationResult(
            operation_name=operation_name,
            issues=tuple(issues),
        )

    def validate_session_start_request(
        self,
        *,
        operation_name: str,
        payload: Mapping[str, Any],
    ) -> ValidationResult:
        issues: list[ValidationIssue] = []

        gambler_id = self._to_positive_int(
            payload.get("gambler_id"),
            field_name="gambler_id",
            issues=issues,
        )

        gambler_row: Mapping[str, Any] | None = None
        open_session_count = 0

        if gambler_id is not None:
            try:
                with self._database.session(dictionary=True) as (_, cursor):
                    cursor.execute(
                        """
                        SELECT
                            gambler_id,
                            is_active,
                            current_stake,
                            loss_threshold,
                            win_threshold
                        FROM GAMBLERS
                        WHERE gambler_id = %s
                        """,
                        (gambler_id,),
                    )
                    gambler_row = cursor.fetchone()

                    cursor.execute(
                        """
                        SELECT COUNT(*) AS open_sessions
                        FROM SESSIONS
                        WHERE gambler_id = %s
                          AND status IN ('INITIALIZED', 'ACTIVE', 'PAUSED')
                        """,
                        (gambler_id,),
                    )
                    row = cursor.fetchone()
                    open_session_count = 0 if row is None else int(row["open_sessions"])
            except Exception as exc:
                issues.append(
                    self._error_issue(
                        field_name="database",
                        attempted_value=str(exc),
                        error_type=ValidationErrorType.RANGE_ERROR,
                        message="Validation lookup failed for session-start request.",
                        user_message="Unable to validate this session request right now. Please try again.",
                        is_recoverable=True,
                    )
                )

        if gambler_id is not None and gambler_row is None:
            issues.append(
                self._error_issue(
                    field_name="gambler_id",
                    attempted_value=gambler_id,
                    error_type=ValidationErrorType.RANGE_ERROR,
                    message="Gambler does not exist.",
                    user_message="The selected gambler profile was not found.",
                    is_recoverable=True,
                )
            )

        current_stake: Decimal | None = None
        default_lower_limit: Decimal | None = None
        default_upper_limit: Decimal | None = None

        if gambler_row is not None:
            if not bool(gambler_row["is_active"]):
                issues.append(
                    self._error_issue(
                        field_name="is_active",
                        attempted_value=gambler_row["is_active"],
                        error_type=ValidationErrorType.STAKE_ERROR,
                        message="Inactive gambler cannot start session.",
                        user_message="This gambler account is inactive and cannot start a session.",
                        is_recoverable=True,
                    )
                )

            current_stake = self._to_money(
                gambler_row["current_stake"],
                field_name="current_stake",
                issues=issues,
            )
            default_lower_limit = self._to_money(
                gambler_row["loss_threshold"],
                field_name="loss_threshold",
                issues=issues,
            )
            default_upper_limit = self._to_money(
                gambler_row["win_threshold"],
                field_name="win_threshold",
                issues=issues,
            )

        if open_session_count > 0:
            issues.append(
                self._error_issue(
                    field_name="status",
                    attempted_value=open_session_count,
                    error_type=ValidationErrorType.LIMIT_ERROR,
                    message="Gambler already has an open session.",
                    user_message="Please close or pause the active session before starting a new one.",
                    is_recoverable=True,
                )
            )

        resolved_starting = (
            payload.get("starting_stake")
            if payload.get("starting_stake") is not None
            else current_stake
        )
        starting_stake = self._to_money(
            resolved_starting,
            field_name="starting_stake",
            issues=issues,
        )

        resolved_lower = (
            payload.get("lower_limit")
            if payload.get("lower_limit") is not None
            else default_lower_limit
        )
        lower_limit = self._to_money(
            resolved_lower,
            field_name="lower_limit",
            issues=issues,
        )

        resolved_upper = (
            payload.get("upper_limit")
            if payload.get("upper_limit") is not None
            else default_upper_limit
        )
        upper_limit = self._to_money(
            resolved_upper,
            field_name="upper_limit",
            issues=issues,
        )

        min_bet = None
        if payload.get("min_bet") is not None:
            min_bet = self._to_money(
                payload.get("min_bet"),
                field_name="min_bet",
                issues=issues,
            )

        max_bet = None
        if payload.get("max_bet") is not None:
            max_bet = self._to_money(
                payload.get("max_bet"),
                field_name="max_bet",
                issues=issues,
            )

        if starting_stake is not None and starting_stake <= _ZERO:
            issues.append(
                self._error_issue(
                    field_name="starting_stake",
                    attempted_value=starting_stake,
                    error_type=ValidationErrorType.STAKE_ERROR,
                    message="starting_stake must be greater than zero.",
                    user_message="Starting stake must be greater than zero.",
                    is_recoverable=True,
                )
            )

        if lower_limit is not None and lower_limit < _ZERO:
            issues.append(
                self._error_issue(
                    field_name="lower_limit",
                    attempted_value=lower_limit,
                    error_type=ValidationErrorType.LIMIT_ERROR,
                    message="lower_limit cannot be negative.",
                    user_message="Lower limit cannot be negative.",
                    is_recoverable=True,
                )
            )

        if lower_limit is not None and upper_limit is not None and upper_limit <= lower_limit:
            issues.append(
                self._error_issue(
                    field_name="upper_limit",
                    attempted_value=upper_limit,
                    error_type=ValidationErrorType.LIMIT_ERROR,
                    message="upper_limit must be greater than lower_limit.",
                    user_message="Upper limit must be greater than lower limit.",
                    is_recoverable=True,
                )
            )

        if (
            starting_stake is not None
            and lower_limit is not None
            and upper_limit is not None
            and (starting_stake < lower_limit or starting_stake > upper_limit)
        ):
            issues.append(
                self._error_issue(
                    field_name="starting_stake",
                    attempted_value=starting_stake,
                    error_type=ValidationErrorType.LIMIT_ERROR,
                    message="starting_stake must be between lower_limit and upper_limit.",
                    user_message="Starting stake must be inside the configured session limits.",
                    is_recoverable=True,
                )
            )

        if min_bet is not None and min_bet <= _ZERO:
            issues.append(
                self._error_issue(
                    field_name="min_bet",
                    attempted_value=min_bet,
                    error_type=ValidationErrorType.BET_ERROR,
                    message="min_bet must be greater than zero.",
                    user_message="Minimum bet must be greater than zero.",
                    is_recoverable=True,
                )
            )

        if max_bet is not None and max_bet <= _ZERO:
            issues.append(
                self._error_issue(
                    field_name="max_bet",
                    attempted_value=max_bet,
                    error_type=ValidationErrorType.BET_ERROR,
                    message="max_bet must be greater than zero.",
                    user_message="Maximum bet must be greater than zero.",
                    is_recoverable=True,
                )
            )

        if min_bet is not None and max_bet is not None and max_bet < min_bet:
            issues.append(
                self._error_issue(
                    field_name="max_bet",
                    attempted_value=max_bet,
                    error_type=ValidationErrorType.BET_ERROR,
                    message="max_bet must be greater than or equal to min_bet.",
                    user_message="Maximum bet must be greater than or equal to minimum bet.",
                    is_recoverable=True,
                )
            )

        if starting_stake is not None and max_bet is not None and max_bet > starting_stake:
            issues.append(
                self._warning_issue(
                    field_name="max_bet",
                    attempted_value=max_bet,
                    error_type=ValidationErrorType.BET_ERROR,
                    message="max_bet exceeds starting_stake.",
                    user_message="Maximum bet is above starting stake and may be unreachable early in the session.",
                )
            )

        for integer_field in ("max_games", "max_session_minutes"):
            if payload.get(integer_field) is None:
                continue

            parsed_value = self._to_positive_int(
                payload.get(integer_field),
                field_name=integer_field,
                issues=issues,
            )
            if parsed_value is not None and parsed_value <= 0:
                issues.append(
                    self._error_issue(
                        field_name=integer_field,
                        attempted_value=payload.get(integer_field),
                        error_type=ValidationErrorType.RANGE_ERROR,
                        message=f"{integer_field} must be a positive integer.",
                        user_message=f"{integer_field} must be greater than zero.",
                        is_recoverable=True,
                    )
                )

        if payload.get("default_win_probability") is not None:
            probability = self._to_rate(
                payload.get("default_win_probability"),
                field_name="default_win_probability",
                issues=issues,
            )
            if probability is not None and (
                probability < Decimal("0.0000") or probability > Decimal("1.0000")
            ):
                issues.append(
                    self._error_issue(
                        field_name="default_win_probability",
                        attempted_value=payload.get("default_win_probability"),
                        error_type=ValidationErrorType.PROBABILITY_ERROR,
                        message="default_win_probability must be between 0 and 1.",
                        user_message="Default win probability must be between 0 and 1.",
                        is_recoverable=True,
                    )
                )

        if current_stake is not None and starting_stake is not None and current_stake > _ZERO:
            delta_ratio = abs((starting_stake - current_stake) / current_stake).quantize(
                _RATE_QUANTUM,
                rounding=ROUND_HALF_UP,
            )
            if delta_ratio >= Decimal("0.5000"):
                issues.append(
                    self._warning_issue(
                        field_name="starting_stake",
                        attempted_value=starting_stake,
                        error_type=ValidationErrorType.STAKE_ERROR,
                        message="starting_stake differs from current stake by at least 50%.",
                        user_message="Starting stake differs significantly from current stake; review limits before continuing.",
                    )
                )

        return ValidationResult(
            operation_name=operation_name,
            issues=tuple(issues),
        )

    def log_validation_events(
        self,
        *,
        result: ValidationResult,
        operation_name: str,
        service_name: str,
        method_name: str,
        payload: Mapping[str, Any],
    ) -> None:
        context_json = self._safe_context_json(payload)

        try:
            with self._database.session() as (connection, cursor):
                if result.issues:
                    for issue in result.issues:
                        cursor.execute(
                            """
                            INSERT INTO VALIDATION_EVENTS (
                                operation_name,
                                service_name,
                                method_name,
                                severity,
                                error_type,
                                field_name,
                                attempted_value,
                                message,
                                user_message,
                                is_recoverable,
                                context_json
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                operation_name,
                                service_name,
                                method_name,
                                issue.severity.value,
                                issue.error_type.value,
                                issue.field_name,
                                self._trim(str(issue.attempted_value), 255),
                                self._trim(issue.message, 512),
                                self._trim(issue.user_message, 512),
                                issue.is_recoverable,
                                context_json,
                            ),
                        )
                else:
                    cursor.execute(
                        """
                        INSERT INTO VALIDATION_EVENTS (
                            operation_name,
                            service_name,
                            method_name,
                            severity,
                            error_type,
                            field_name,
                            attempted_value,
                            message,
                            user_message,
                            is_recoverable,
                            context_json
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            operation_name,
                            service_name,
                            method_name,
                            "INFO",
                            None,
                            None,
                            None,
                            "Validation passed.",
                            "Validation passed.",
                            True,
                            context_json,
                        ),
                    )
                connection.commit()
        except Exception:
            return

    @staticmethod
    def _safe_context_json(payload: Mapping[str, Any]) -> str:
        safe_payload = {
            key: str(value)
            for key, value in payload.items()
        }
        return json.dumps(safe_payload, ensure_ascii=True)

    @staticmethod
    def _trim(value: str, max_length: int) -> str:
        if len(value) <= max_length:
            return value
        return value[: max_length - 3] + "..."

    def _to_positive_int(
        self,
        value: Any,
        *,
        field_name: str,
        issues: list[ValidationIssue],
    ) -> int | None:
        if value is None:
            issues.append(
                self._error_issue(
                    field_name=field_name,
                    attempted_value=value,
                    error_type=ValidationErrorType.NULL_ERROR,
                    message=f"{field_name} is required.",
                    user_message=f"{field_name} is required.",
                    is_recoverable=True,
                )
            )
            return None

        if isinstance(value, bool):
            issues.append(
                self._error_issue(
                    field_name=field_name,
                    attempted_value=value,
                    error_type=ValidationErrorType.NUMERIC_ERROR,
                    message=f"{field_name} must be an integer.",
                    user_message=f"{field_name} must be an integer value.",
                    is_recoverable=True,
                )
            )
            return None

        try:
            parsed = int(str(value))
        except (TypeError, ValueError):
            issues.append(
                self._error_issue(
                    field_name=field_name,
                    attempted_value=value,
                    error_type=ValidationErrorType.NUMERIC_ERROR,
                    message=f"{field_name} must be an integer.",
                    user_message=f"{field_name} must be an integer value.",
                    is_recoverable=True,
                )
            )
            return None

        if parsed <= 0:
            issues.append(
                self._error_issue(
                    field_name=field_name,
                    attempted_value=value,
                    error_type=ValidationErrorType.RANGE_ERROR,
                    message=f"{field_name} must be positive.",
                    user_message=f"{field_name} must be greater than zero.",
                    is_recoverable=True,
                )
            )
            return None

        return parsed

    def _to_money(
        self,
        value: Any,
        *,
        field_name: str,
        issues: list[ValidationIssue],
    ) -> Decimal | None:
        if value is None:
            issues.append(
                self._error_issue(
                    field_name=field_name,
                    attempted_value=value,
                    error_type=ValidationErrorType.NULL_ERROR,
                    message=f"{field_name} is required.",
                    user_message=f"{field_name} is required.",
                    is_recoverable=True,
                )
            )
            return None

        try:
            return Decimal(str(value)).quantize(_MONEY_QUANTUM, rounding=ROUND_HALF_UP)
        except (InvalidOperation, TypeError, ValueError):
            issues.append(
                self._error_issue(
                    field_name=field_name,
                    attempted_value=value,
                    error_type=ValidationErrorType.NUMERIC_ERROR,
                    message=f"{field_name} must be numeric.",
                    user_message=f"{field_name} must be a numeric amount.",
                    is_recoverable=True,
                )
            )
            return None

    def _to_rate(
        self,
        value: Any,
        *,
        field_name: str,
        issues: list[ValidationIssue],
    ) -> Decimal | None:
        if value is None:
            issues.append(
                self._error_issue(
                    field_name=field_name,
                    attempted_value=value,
                    error_type=ValidationErrorType.NULL_ERROR,
                    message=f"{field_name} is required.",
                    user_message=f"{field_name} is required.",
                    is_recoverable=True,
                )
            )
            return None

        try:
            return Decimal(str(value)).quantize(_RATE_QUANTUM, rounding=ROUND_HALF_UP)
        except (InvalidOperation, TypeError, ValueError):
            issues.append(
                self._error_issue(
                    field_name=field_name,
                    attempted_value=value,
                    error_type=ValidationErrorType.PROBABILITY_ERROR,
                    message=f"{field_name} must be numeric.",
                    user_message=f"{field_name} must be a numeric value between 0 and 1.",
                    is_recoverable=True,
                )
            )
            return None

    @staticmethod
    def _error_issue(
        *,
        field_name: str,
        attempted_value: Any,
        error_type: ValidationErrorType,
        message: str,
        user_message: str,
        is_recoverable: bool,
    ) -> ValidationIssue:
        return ValidationIssue(
            severity=ValidationSeverity.ERROR,
            error_type=error_type,
            field_name=field_name,
            attempted_value=attempted_value,
            message=message,
            user_message=user_message,
            is_recoverable=is_recoverable,
        )

    @staticmethod
    def _warning_issue(
        *,
        field_name: str,
        attempted_value: Any,
        error_type: ValidationErrorType,
        message: str,
        user_message: str,
    ) -> ValidationIssue:
        return ValidationIssue(
            severity=ValidationSeverity.WARNING,
            error_type=error_type,
            field_name=field_name,
            attempted_value=attempted_value,
            message=message,
            user_message=user_message,
            is_recoverable=True,
        )


def _resolve_validator(service_instance: Any) -> InputValidator:
    cached = getattr(service_instance, "_input_validator", None)
    if isinstance(cached, InputValidator):
        return cached

    database = getattr(service_instance, "_database", None)
    settings = getattr(service_instance, "_settings", None)
    if not isinstance(database, Database) or not isinstance(settings, Settings):
        raise ValidationException(
            error_type=ValidationErrorType.RANGE_ERROR,
            field_name="service_instance",
            attempted_value=service_instance.__class__.__name__,
            message="Service does not expose _database/_settings for validation.",
            user_message="Validation is unavailable for this operation.",
            is_recoverable=False,
        )

    validator = InputValidator(database=database, settings=settings)
    setattr(service_instance, "_input_validator", validator)
    return validator
