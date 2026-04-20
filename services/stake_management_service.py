from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Mapping

from config.database import Database
from config.settings import Settings
from models.stake_management import (
    SessionEndReason,
    SessionStatus,
    StakeBoundary,
    TransactionType,
)
from tracking_and_reports.stake_history_report import (
    StakeBoundaryValidation,
    StakeHistoryItem,
    StakeHistoryReport,
    StakeMonitorSummary,
)
from utils.exceptions import NotFoundException, ValidationErrorType, ValidationException
from utils.input_validator import validation_guard

_MONEY_QUANTUM = Decimal("0.01")
_RATE_QUANTUM = Decimal("0.0001")
_ZERO = Decimal("0.00")


def _to_money(value: Any, field_name: str) -> Decimal:
    try:
        return Decimal(str(value)).quantize(_MONEY_QUANTUM, rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValidationException(
            error_type=ValidationErrorType.NUMERIC_ERROR,
            field_name=field_name,
            attempted_value=value,
            message="Value cannot be converted to a monetary decimal.",
        ) from exc


class StakeManagementService:
    _ADJUSTMENT_TYPES = {
        TransactionType.DEPOSIT,
        TransactionType.WITHDRAWAL,
        TransactionType.ADJUSTMENT,
    }

    def __init__(self, database: Database, settings: Settings) -> None:
        self._database = database
        self._settings = settings
        self._last_validation_result = None

    @validation_guard(
        operation_name="INITIALIZE_STAKE_SESSION",
        validator_method="validate_session_start_request",
    )
    def initialize_stake_session(
        self,
        gambler_id: int,
        starting_stake: Decimal | int | float | str,
        lower_limit: Decimal | int | float | str,
        upper_limit: Decimal | int | float | str,
        *,
        max_games: int | None = None,
    ) -> StakeMonitorSummary:
        self._validate_gambler_id(gambler_id)

        normalized_starting = _to_money(starting_stake, "starting_stake")
        normalized_lower = _to_money(lower_limit, "lower_limit")
        normalized_upper = _to_money(upper_limit, "upper_limit")

        effective_max_games = (
            self._settings.session_default_max_games if max_games is None else max_games
        )
        if effective_max_games <= 0:
            raise ValidationException(
                error_type=ValidationErrorType.RANGE_ERROR,
                field_name="max_games",
                attempted_value=effective_max_games,
                message="max_games must be a positive integer.",
            )

        self._validate_stake_bounds(
            stake=normalized_starting,
            lower_limit=normalized_lower,
            upper_limit=normalized_upper,
        )

        with self._database.session(dictionary=True) as (connection, cursor):
            gambler_row = self._fetch_gambler_row(cursor, gambler_id, for_update=True)
            if gambler_row is None:
                raise NotFoundException(f"Gambler profile not found for id={gambler_id}.")

            if not bool(gambler_row["is_active"]):
                raise ValidationException(
                    error_type=ValidationErrorType.STAKE_ERROR,
                    field_name="is_active",
                    attempted_value=gambler_row["is_active"],
                    message="Cannot initialize a session for an inactive gambler.",
                )

            previous_stake = _to_money(gambler_row["current_stake"], "current_stake")

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
                    normalized_starting,
                    None,
                    normalized_starting,
                    normalized_starting,
                    normalized_lower,
                    normalized_upper,
                    effective_max_games,
                    0,
                    0,
                ),
            )
            session_id = int(cursor.lastrowid)

            cursor.execute(
                """
                UPDATE GAMBLERS
                SET current_stake = %s, updated_at = CURRENT_TIMESTAMP
                WHERE gambler_id = %s
                """,
                (normalized_starting, gambler_id),
            )

            delta = _to_money(normalized_starting - previous_stake, "adjustment_delta")
            self._insert_transaction(
                cursor=cursor,
                session_id=session_id,
                gambler_id=gambler_id,
                transaction_type=TransactionType.ADJUSTMENT,
                amount=delta,
                balance_before=previous_stake,
                balance_after=normalized_starting,
                transaction_ref=self._transaction_ref(
                    prefix=TransactionType.ADJUSTMENT.value,
                    gambler_id=gambler_id,
                    session_id=session_id,
                ),
            )

            self._insert_running_snapshot(
                cursor=cursor,
                session_id=session_id,
                starting_stake=normalized_starting,
            )

            connection.commit()

        return self.monitor_stake(session_id)

    def track_current_stake(self, gambler_id: int) -> Decimal:
        self._validate_gambler_id(gambler_id)

        with self._database.session(dictionary=True) as (_, cursor):
            cursor.execute(
                """
                SELECT current_stake
                FROM GAMBLERS
                WHERE gambler_id = %s
                """,
                (gambler_id,),
            )
            row = cursor.fetchone()

        if row is None:
            raise NotFoundException(f"Gambler profile not found for id={gambler_id}.")

        return _to_money(row["current_stake"], "current_stake")

    @validation_guard(
        operation_name="PROCESS_BET_OUTCOME",
        validator_method="validate_bet_request",
    )
    def process_bet_outcome(
        self,
        gambler_id: int,
        session_id: int,
        bet_amount: Decimal | int | float | str,
        *,
        is_win: bool,
        payout_multiplier: Decimal | int | float | str = Decimal("1.00"),
    ) -> StakeMonitorSummary:
        self._validate_gambler_id(gambler_id)
        self._validate_session_id(session_id)

        normalized_bet = _to_money(bet_amount, "bet_amount")
        if normalized_bet <= _ZERO:
            raise ValidationException(
                error_type=ValidationErrorType.BET_ERROR,
                field_name="bet_amount",
                attempted_value=normalized_bet,
                message="Bet amount must be greater than zero.",
            )

        normalized_multiplier = _to_money(payout_multiplier, "payout_multiplier")
        if normalized_multiplier <= _ZERO:
            raise ValidationException(
                error_type=ValidationErrorType.PROBABILITY_ERROR,
                field_name="payout_multiplier",
                attempted_value=normalized_multiplier,
                message="Payout multiplier must be greater than zero.",
            )

        with self._database.session(dictionary=True) as (connection, cursor):
            session_row = self._fetch_session_row(cursor, session_id, for_update=True)
            if session_row is None:
                raise NotFoundException(f"Session not found for id={session_id}.")
            if int(session_row["gambler_id"]) != gambler_id:
                raise ValidationException(
                    error_type=ValidationErrorType.RANGE_ERROR,
                    field_name="session_id",
                    attempted_value=session_id,
                    message="Session does not belong to gambler.",
                )
            if session_row["status"] != SessionStatus.ACTIVE.value:
                raise ValidationException(
                    error_type=ValidationErrorType.RANGE_ERROR,
                    field_name="status",
                    attempted_value=session_row["status"],
                    message="Session is not active.",
                )

            gambler_row = self._fetch_gambler_row(cursor, gambler_id, for_update=True)
            if gambler_row is None:
                raise NotFoundException(f"Gambler profile not found for id={gambler_id}.")

            current_stake = _to_money(gambler_row["current_stake"], "current_stake")
            if normalized_bet > current_stake:
                raise ValidationException(
                    error_type=ValidationErrorType.BET_ERROR,
                    field_name="bet_amount",
                    attempted_value=normalized_bet,
                    message="Bet amount cannot exceed current stake.",
                )

            if is_win:
                delta = _to_money(normalized_bet * normalized_multiplier, "delta")
                outcome_type = TransactionType.BET_WIN
            else:
                delta = _to_money(-normalized_bet, "delta")
                outcome_type = TransactionType.BET_LOSS

            new_stake = _to_money(current_stake + delta, "new_stake")
            if new_stake < _ZERO:
                raise ValidationException(
                    error_type=ValidationErrorType.STAKE_ERROR,
                    field_name="new_stake",
                    attempted_value=new_stake,
                    message="Stake cannot become negative.",
                )

            self._insert_transaction(
                cursor=cursor,
                session_id=session_id,
                gambler_id=gambler_id,
                transaction_type=TransactionType.BET_PLACED,
                amount=normalized_bet,
                balance_before=current_stake,
                balance_after=current_stake,
                transaction_ref=self._transaction_ref(
                    prefix=TransactionType.BET_PLACED.value,
                    gambler_id=gambler_id,
                    session_id=session_id,
                ),
            )

            self._insert_transaction(
                cursor=cursor,
                session_id=session_id,
                gambler_id=gambler_id,
                transaction_type=outcome_type,
                amount=delta,
                balance_before=current_stake,
                balance_after=new_stake,
                transaction_ref=self._transaction_ref(
                    prefix=outcome_type.value,
                    gambler_id=gambler_id,
                    session_id=session_id,
                ),
            )

            cursor.execute(
                """
                UPDATE GAMBLERS
                SET current_stake = %s, updated_at = CURRENT_TIMESTAMP
                WHERE gambler_id = %s
                """,
                (new_stake, gambler_id),
            )

            self._update_session_stake_state(
                cursor=cursor,
                session_row=session_row,
                new_stake=new_stake,
                increment_games=True,
            )

            self._insert_running_snapshot(
                cursor=cursor,
                session_id=session_id,
                starting_stake=_to_money(session_row["starting_stake"], "starting_stake"),
            )

            connection.commit()

        return self.monitor_stake(session_id)

    def apply_manual_adjustment(
        self,
        gambler_id: int,
        session_id: int,
        amount: Decimal | int | float | str,
        *,
        transaction_type: TransactionType | str,
    ) -> StakeMonitorSummary:
        self._validate_gambler_id(gambler_id)
        self._validate_session_id(session_id)

        normalized_type = self._to_transaction_type(transaction_type)
        if normalized_type not in self._ADJUSTMENT_TYPES:
            raise ValidationException(
                error_type=ValidationErrorType.RANGE_ERROR,
                field_name="transaction_type",
                attempted_value=str(transaction_type),
                message="transaction_type must be DEPOSIT, WITHDRAWAL, or ADJUSTMENT.",
            )

        normalized_amount = _to_money(amount, "amount")
        if normalized_amount <= _ZERO:
            raise ValidationException(
                error_type=ValidationErrorType.STAKE_ERROR,
                field_name="amount",
                attempted_value=normalized_amount,
                message="Adjustment amount must be greater than zero.",
            )

        with self._database.session(dictionary=True) as (connection, cursor):
            session_row = self._fetch_session_row(cursor, session_id, for_update=True)
            if session_row is None:
                raise NotFoundException(f"Session not found for id={session_id}.")
            if int(session_row["gambler_id"]) != gambler_id:
                raise ValidationException(
                    error_type=ValidationErrorType.RANGE_ERROR,
                    field_name="session_id",
                    attempted_value=session_id,
                    message="Session does not belong to gambler.",
                )

            gambler_row = self._fetch_gambler_row(cursor, gambler_id, for_update=True)
            if gambler_row is None:
                raise NotFoundException(f"Gambler profile not found for id={gambler_id}.")

            current_stake = _to_money(gambler_row["current_stake"], "current_stake")
            delta = normalized_amount
            if normalized_type == TransactionType.WITHDRAWAL:
                delta = _to_money(-normalized_amount, "delta")

            new_stake = _to_money(current_stake + delta, "new_stake")
            if new_stake < _ZERO:
                raise ValidationException(
                    error_type=ValidationErrorType.STAKE_ERROR,
                    field_name="new_stake",
                    attempted_value=new_stake,
                    message="Adjustment would result in a negative stake.",
                )

            self._insert_transaction(
                cursor=cursor,
                session_id=session_id,
                gambler_id=gambler_id,
                transaction_type=normalized_type,
                amount=delta,
                balance_before=current_stake,
                balance_after=new_stake,
                transaction_ref=self._transaction_ref(
                    prefix=normalized_type.value,
                    gambler_id=gambler_id,
                    session_id=session_id,
                ),
            )

            cursor.execute(
                """
                UPDATE GAMBLERS
                SET current_stake = %s, updated_at = CURRENT_TIMESTAMP
                WHERE gambler_id = %s
                """,
                (new_stake, gambler_id),
            )

            self._update_session_stake_state(
                cursor=cursor,
                session_row=session_row,
                new_stake=new_stake,
                increment_games=False,
            )

            self._insert_running_snapshot(
                cursor=cursor,
                session_id=session_id,
                starting_stake=_to_money(session_row["starting_stake"], "starting_stake"),
            )

            connection.commit()

        return self.monitor_stake(session_id)

    def validate_stake_boundaries(
        self,
        session_id: int,
        *,
        current_balance: Decimal | None = None,
    ) -> StakeBoundaryValidation:
        self._validate_session_id(session_id)

        with self._database.session(dictionary=True) as (_, cursor):
            cursor.execute(
                """
                SELECT
                    s.lower_limit,
                    s.upper_limit,
                    g.current_stake
                FROM SESSIONS s
                JOIN GAMBLERS g ON g.gambler_id = s.gambler_id
                WHERE s.session_id = %s
                """,
                (session_id,),
            )
            row = cursor.fetchone()

        if row is None:
            raise NotFoundException(f"Session not found for id={session_id}.")

        lower_limit = _to_money(row["lower_limit"], "lower_limit")
        upper_limit = _to_money(row["upper_limit"], "upper_limit")
        boundary = StakeBoundary(lower_limit=lower_limit, upper_limit=upper_limit)
        resolved_current = (
            _to_money(row["current_stake"], "current_stake")
            if current_balance is None
            else _to_money(current_balance, "current_balance")
        )

        reached_lower = resolved_current <= boundary.lower_limit
        reached_upper = resolved_current >= boundary.upper_limit

        return StakeBoundaryValidation(
            lower_limit=boundary.lower_limit,
            upper_limit=boundary.upper_limit,
            warning_lower=boundary.warning_lower.quantize(
                _MONEY_QUANTUM,
                rounding=ROUND_HALF_UP,
            ),
            warning_upper=boundary.warning_upper.quantize(
                _MONEY_QUANTUM,
                rounding=ROUND_HALF_UP,
            ),
            current_balance=resolved_current,
            is_within_bounds=not reached_lower and not reached_upper,
            approaching_lower_warning=(
                resolved_current <= boundary.warning_lower and not reached_lower
            ),
            approaching_upper_warning=(
                resolved_current >= boundary.warning_upper and not reached_upper
            ),
            reached_lower_limit=reached_lower,
            reached_upper_limit=reached_upper,
        )

    def monitor_stake(self, session_id: int) -> StakeMonitorSummary:
        self._validate_session_id(session_id)

        with self._database.session(dictionary=True) as (_, cursor):
            cursor.execute(
                """
                SELECT
                    s.session_id,
                    s.gambler_id,
                    s.status,
                    s.end_reason,
                    s.starting_stake,
                    s.peak_stake,
                    s.lowest_stake,
                    g.current_stake
                FROM SESSIONS s
                JOIN GAMBLERS g ON g.gambler_id = s.gambler_id
                WHERE s.session_id = %s
                """,
                (session_id,),
            )
            row = cursor.fetchone()

            if row is None:
                raise NotFoundException(f"Session not found for id={session_id}.")

            cursor.execute(
                """
                SELECT COUNT(*) AS total_changes
                FROM STAKE_TRANSACTIONS
                WHERE session_id = %s
                """,
                (session_id,),
            )
            changes_row = cursor.fetchone()

        current_stake = _to_money(row["current_stake"], "current_stake")
        starting_stake = _to_money(row["starting_stake"], "starting_stake")
        peak_stake = _to_money(row["peak_stake"], "peak_stake")
        lowest_stake = _to_money(row["lowest_stake"], "lowest_stake")

        volatility = Decimal("0.0000")
        if starting_stake > _ZERO:
            volatility = (
                (peak_stake - lowest_stake) / starting_stake
            ).quantize(_RATE_QUANTUM, rounding=ROUND_HALF_UP)

        boundaries = self.validate_stake_boundaries(
            session_id=session_id,
            current_balance=current_stake,
        )

        return StakeMonitorSummary(
            session_id=int(row["session_id"]),
            gambler_id=int(row["gambler_id"]),
            session_status=str(row["status"]),
            end_reason=(None if row["end_reason"] is None else str(row["end_reason"])),
            current_stake=current_stake,
            starting_stake=starting_stake,
            peak_stake=peak_stake,
            lowest_stake=lowest_stake,
            volatility=volatility,
            total_changes=int(changes_row["total_changes"]) if changes_row else 0,
            boundary_validation=boundaries,
        )

    def generate_stake_history_report(
        self,
        session_id: int,
        *,
        transaction_type: TransactionType | str | None = None,
    ) -> StakeHistoryReport:
        self._validate_session_id(session_id)

        normalized_type: TransactionType | None = None
        if transaction_type is not None:
            normalized_type = self._to_transaction_type(transaction_type)

        with self._database.session(dictionary=True) as (_, cursor):
            cursor.execute(
                """
                SELECT session_id, gambler_id, starting_stake
                FROM SESSIONS
                WHERE session_id = %s
                """,
                (session_id,),
            )
            session_row = cursor.fetchone()
            if session_row is None:
                raise NotFoundException(f"Session not found for id={session_id}.")

            query = """
                SELECT
                    transaction_id,
                    transaction_type,
                    amount,
                    balance_before,
                    balance_after,
                    transaction_ref,
                    created_at
                FROM STAKE_TRANSACTIONS
                WHERE session_id = %s
            """
            params: list[Any] = [session_id]
            if normalized_type is not None:
                query += " AND transaction_type = %s"
                params.append(normalized_type.value)
            query += " ORDER BY transaction_id"

            cursor.execute(query, tuple(params))
            rows = cursor.fetchall()

        items = tuple(
            StakeHistoryItem(
                transaction_id=int(row["transaction_id"]),
                transaction_type=str(row["transaction_type"]),
                amount=_to_money(row["amount"], "amount"),
                balance_before=_to_money(row["balance_before"], "balance_before"),
                balance_after=_to_money(row["balance_after"], "balance_after"),
                transaction_ref=str(row["transaction_ref"]),
                created_at=row["created_at"],
            )
            for row in rows
        )

        monitor_summary = self.monitor_stake(session_id)

        starting_balance = (
            items[0].balance_before
            if items
            else _to_money(session_row["starting_stake"], "starting_stake")
        )
        ending_balance = items[-1].balance_after if items else monitor_summary.current_stake

        breakdown: dict[str, int] = {}
        for item in items:
            breakdown[item.transaction_type] = breakdown.get(item.transaction_type, 0) + 1

        return StakeHistoryReport(
            session_id=int(session_row["session_id"]),
            gambler_id=int(session_row["gambler_id"]),
            transaction_count=len(items),
            starting_balance=starting_balance,
            ending_balance=ending_balance,
            net_change=_to_money(ending_balance - starting_balance, "net_change"),
            transaction_breakdown=breakdown,
            monitor_summary=monitor_summary,
            transactions=items,
        )

    def _validate_gambler_id(self, gambler_id: int) -> None:
        if gambler_id <= 0:
            raise ValidationException(
                error_type=ValidationErrorType.RANGE_ERROR,
                field_name="gambler_id",
                attempted_value=gambler_id,
                message="gambler_id must be a positive integer.",
            )

    def _validate_session_id(self, session_id: int) -> None:
        if session_id <= 0:
            raise ValidationException(
                error_type=ValidationErrorType.RANGE_ERROR,
                field_name="session_id",
                attempted_value=session_id,
                message="session_id must be a positive integer.",
            )

    def _validate_stake_bounds(
        self,
        *,
        stake: Decimal,
        lower_limit: Decimal,
        upper_limit: Decimal,
    ) -> None:
        if stake < self._settings.min_initial_stake or stake > self._settings.max_initial_stake:
            raise ValidationException(
                error_type=ValidationErrorType.STAKE_ERROR,
                field_name="starting_stake",
                attempted_value=stake,
                message=(
                    "starting_stake must be within configured range "
                    f"[{self._settings.min_initial_stake}, {self._settings.max_initial_stake}]"
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

        if stake < lower_limit or stake > upper_limit:
            raise ValidationException(
                error_type=ValidationErrorType.LIMIT_ERROR,
                field_name="starting_stake",
                attempted_value=stake,
                message="starting_stake must be between lower_limit and upper_limit.",
            )

    @staticmethod
    def _to_transaction_type(value: TransactionType | str) -> TransactionType:
        if isinstance(value, TransactionType):
            return value

        normalized = str(value).strip().upper()
        try:
            return TransactionType(normalized)
        except ValueError as exc:
            raise ValidationException(
                error_type=ValidationErrorType.RANGE_ERROR,
                field_name="transaction_type",
                attempted_value=value,
                message="Invalid transaction type.",
            ) from exc

    @staticmethod
    def _fetch_gambler_row(
        cursor: Any,
        gambler_id: int,
        *,
        for_update: bool = False,
    ) -> Mapping[str, Any] | None:
        query = """
        SELECT gambler_id, is_active, current_stake
        FROM GAMBLERS
        WHERE gambler_id = %s
        """
        if for_update:
            query += " FOR UPDATE"

        cursor.execute(query, (gambler_id,))
        return cursor.fetchone()

    @staticmethod
    def _fetch_session_row(
        cursor: Any,
        session_id: int,
        *,
        for_update: bool = False,
    ) -> Mapping[str, Any] | None:
        query = """
        SELECT
            session_id,
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
            started_at,
            ended_at
        FROM SESSIONS
        WHERE session_id = %s
        """
        if for_update:
            query += " FOR UPDATE"

        cursor.execute(query, (session_id,))
        return cursor.fetchone()

    @staticmethod
    def _insert_transaction(
        *,
        cursor: Any,
        session_id: int,
        gambler_id: int,
        transaction_type: TransactionType,
        amount: Decimal,
        balance_before: Decimal,
        balance_after: Decimal,
        transaction_ref: str,
    ) -> None:
        cursor.execute(
            """
            INSERT INTO STAKE_TRANSACTIONS (
                session_id,
                gambler_id,
                bet_id,
                game_id,
                transaction_type,
                amount,
                balance_before,
                balance_after,
                transaction_ref
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                session_id,
                gambler_id,
                None,
                None,
                transaction_type.value,
                amount,
                balance_before,
                balance_after,
                transaction_ref,
            ),
        )

    def _update_session_stake_state(
        self,
        *,
        cursor: Any,
        session_row: Mapping[str, Any],
        new_stake: Decimal,
        increment_games: bool,
    ) -> None:
        current_games = int(session_row["games_played"])
        next_games = current_games + (1 if increment_games else 0)

        peak_stake = _to_money(session_row["peak_stake"], "peak_stake")
        lowest_stake = _to_money(session_row["lowest_stake"], "lowest_stake")
        updated_peak = peak_stake if peak_stake >= new_stake else new_stake
        updated_lowest = lowest_stake if lowest_stake <= new_stake else new_stake

        status = str(session_row["status"])
        end_reason = session_row["end_reason"]
        ending_stake = session_row["ending_stake"]
        ended_at = session_row["ended_at"]

        lower_limit = _to_money(session_row["lower_limit"], "lower_limit")
        upper_limit = _to_money(session_row["upper_limit"], "upper_limit")
        max_games = int(session_row["max_games"])

        if status == SessionStatus.ACTIVE.value:
            if new_stake >= upper_limit:
                status = SessionStatus.ENDED_WIN.value
                end_reason = SessionEndReason.UPPER_LIMIT_REACHED.value
                ending_stake = new_stake
                ended_at = datetime.now(timezone.utc).replace(tzinfo=None)
            elif new_stake <= lower_limit:
                status = SessionStatus.ENDED_LOSS.value
                end_reason = SessionEndReason.LOWER_LIMIT_REACHED.value
                ending_stake = new_stake
                ended_at = datetime.now(timezone.utc).replace(tzinfo=None)
            elif next_games >= max_games:
                status = SessionStatus.ENDED_TIMEOUT.value
                end_reason = SessionEndReason.TIMEOUT.value
                ending_stake = new_stake
                ended_at = datetime.now(timezone.utc).replace(tzinfo=None)

        cursor.execute(
            """
            UPDATE SESSIONS
            SET
                status = %s,
                end_reason = %s,
                ending_stake = %s,
                peak_stake = %s,
                lowest_stake = %s,
                games_played = %s,
                ended_at = %s
            WHERE session_id = %s
            """,
            (
                status,
                end_reason,
                ending_stake,
                updated_peak,
                updated_lowest,
                next_games,
                ended_at,
                int(session_row["session_id"]),
            ),
        )

    def _insert_running_snapshot(
        self,
        *,
        cursor: Any,
        session_id: int,
        starting_stake: Decimal,
        game_id: int | None = None,
    ) -> None:
        cursor.execute(
            """
            SELECT
                COALESCE(SUM(CASE WHEN transaction_type = 'BET_PLACED' THEN 1 ELSE 0 END), 0) AS total_games,
                COALESCE(SUM(CASE WHEN transaction_type = 'BET_WIN' THEN 1 ELSE 0 END), 0) AS total_wins,
                COALESCE(SUM(CASE WHEN transaction_type = 'BET_LOSS' THEN 1 ELSE 0 END), 0) AS total_losses,
                COALESCE(SUM(CASE WHEN transaction_type = 'BET_WIN' THEN amount ELSE 0 END), 0) AS total_winnings,
                COALESCE(SUM(CASE WHEN transaction_type = 'BET_LOSS' THEN ABS(amount) ELSE 0 END), 0) AS total_losses_amount
            FROM STAKE_TRANSACTIONS
            WHERE session_id = %s
            """,
            (session_id,),
        )
        aggregate = cursor.fetchone()
        if aggregate is None:
            raise NotFoundException(
                f"Unable to aggregate stake transactions for session id={session_id}."
            )

        total_games = int(aggregate["total_games"])
        total_wins = int(aggregate["total_wins"])
        total_losses = int(aggregate["total_losses"])
        total_winnings = _to_money(aggregate["total_winnings"], "total_winnings")
        total_losses_amount = _to_money(
            aggregate["total_losses_amount"],
            "total_losses_amount",
        )

        net_profit = _to_money(total_winnings - total_losses_amount, "net_profit")
        win_rate = Decimal("0.0000")
        if total_games > 0:
            win_rate = (
                Decimal(total_wins) / Decimal(total_games)
            ).quantize(_RATE_QUANTUM, rounding=ROUND_HALF_UP)

        profit_factor = Decimal("0.0000")
        if total_losses_amount > _ZERO:
            profit_factor = (
                total_winnings / total_losses_amount
            ).quantize(_RATE_QUANTUM, rounding=ROUND_HALF_UP)

        roi = Decimal("0.0000")
        if starting_stake > _ZERO:
            roi = (net_profit / starting_stake).quantize(
                _RATE_QUANTUM,
                rounding=ROUND_HALF_UP,
            )

        cursor.execute(
            """
            SELECT transaction_type
            FROM STAKE_TRANSACTIONS
            WHERE session_id = %s
              AND transaction_type IN ('BET_WIN', 'BET_LOSS')
            ORDER BY transaction_id
            """,
            (session_id,),
        )
        outcome_rows = cursor.fetchall()

        longest_win_streak = 0
        longest_loss_streak = 0
        current_win_streak = 0
        current_loss_streak = 0

        for row in outcome_rows:
            transaction_type = str(row["transaction_type"])
            if transaction_type == TransactionType.BET_WIN.value:
                current_win_streak += 1
                current_loss_streak = 0
                if current_win_streak > longest_win_streak:
                    longest_win_streak = current_win_streak
            elif transaction_type == TransactionType.BET_LOSS.value:
                current_loss_streak += 1
                current_win_streak = 0
                if current_loss_streak > longest_loss_streak:
                    longest_loss_streak = current_loss_streak

        cursor.execute(
            """
            INSERT INTO RUNNING_TOTALS_SNAPSHOTS (
                session_id,
                game_id,
                total_games,
                total_wins,
                total_losses,
                total_pushes,
                total_winnings,
                total_losses_amount,
                net_profit,
                win_rate,
                profit_factor,
                roi,
                longest_win_streak,
                longest_loss_streak
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                session_id,
                game_id,
                total_games,
                total_wins,
                total_losses,
                0,
                total_winnings,
                total_losses_amount,
                net_profit,
                win_rate,
                profit_factor,
                roi,
                longest_win_streak,
                longest_loss_streak,
            ),
        )

    @staticmethod
    def _transaction_ref(*, prefix: str, gambler_id: int, session_id: int) -> str:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
        return f"{prefix}-{gambler_id}-{session_id}-{timestamp}"
