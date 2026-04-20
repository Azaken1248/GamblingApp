from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from random import Random
from typing import Any, Mapping, Optional

from config.database import Database
from config.settings import Settings
from models.betting import BetSettlementResult, ConsecutiveBetSummary
from models.stake_management import SessionStatus, TransactionType
from services.stake_management_service import StakeManagementService, _to_money
from strategies.base_strategy import BettingStrategy, StrategyContext
from strategies.fixed_amount_strategy import FixedAmountStrategy
from strategies.martingale_strategy import MartingaleStrategy
from strategies.percentage_strategy import PercentageStrategy
from utils.exceptions import NotFoundException, ValidationErrorType, ValidationException

_RATE_QUANTUM = Decimal("0.0001")
_ZERO = Decimal("0.00")


class BettingService:
    def __init__(
        self,
        database: Database,
        settings: Settings,
        stake_management_service: Optional[StakeManagementService] = None,
        rng: Optional[Random] = None,
    ) -> None:
        self._database = database
        self._settings = settings
        self._stake_service = stake_management_service or StakeManagementService(
            database=database,
            settings=settings,
        )
        self._rng = rng or Random()

    def place_bet(
        self,
        gambler_id: int,
        session_id: int,
        bet_amount: Decimal | int | float | str,
        *,
        win_probability: Decimal | int | float | str | None = None,
        payout_multiplier: Decimal | int | float | str = Decimal("1.00"),
    ) -> BetSettlementResult:
        normalized_bet = _to_money(bet_amount, "bet_amount")
        return self._execute_bet(
            gambler_id=gambler_id,
            session_id=session_id,
            strategy_code="MANUAL",
            explicit_bet_amount=normalized_bet,
            win_probability=self._normalize_probability(win_probability),
            payout_multiplier=self._normalize_multiplier(payout_multiplier),
            fixed_amount=None,
            percentage=None,
            base_amount=None,
        )

    def place_bet_with_strategy(
        self,
        gambler_id: int,
        session_id: int,
        strategy_code: str,
        *,
        win_probability: Decimal | int | float | str | None = None,
        payout_multiplier: Decimal | int | float | str = Decimal("1.00"),
        fixed_amount: Decimal | int | float | str | None = None,
        percentage: Decimal | int | float | str | None = None,
        base_amount: Decimal | int | float | str | None = None,
    ) -> BetSettlementResult:
        return self._execute_bet(
            gambler_id=gambler_id,
            session_id=session_id,
            strategy_code=self._normalize_strategy_code(strategy_code),
            explicit_bet_amount=None,
            win_probability=self._normalize_probability(win_probability),
            payout_multiplier=self._normalize_multiplier(payout_multiplier),
            fixed_amount=fixed_amount,
            percentage=percentage,
            base_amount=base_amount,
        )

    def place_consecutive_bets(
        self,
        gambler_id: int,
        session_id: int,
        total_bets: int,
        *,
        strategy_code: str,
        win_probability: Decimal | int | float | str | None = None,
        payout_multiplier: Decimal | int | float | str = Decimal("1.00"),
        fixed_amount: Decimal | int | float | str | None = None,
        percentage: Decimal | int | float | str | None = None,
        base_amount: Decimal | int | float | str | None = None,
    ) -> ConsecutiveBetSummary:
        if total_bets <= 0:
            raise ValidationException(
                error_type=ValidationErrorType.RANGE_ERROR,
                field_name="total_bets",
                attempted_value=total_bets,
                message="total_bets must be a positive integer.",
            )

        normalized_code = self._normalize_strategy_code(strategy_code)
        normalized_probability = self._normalize_probability(win_probability)
        normalized_multiplier = self._normalize_multiplier(payout_multiplier)

        results: list[BetSettlementResult] = []
        wins = 0
        losses = 0

        for _ in range(total_bets):
            result = self._execute_bet(
                gambler_id=gambler_id,
                session_id=session_id,
                strategy_code=normalized_code,
                explicit_bet_amount=None,
                win_probability=normalized_probability,
                payout_multiplier=normalized_multiplier,
                fixed_amount=fixed_amount,
                percentage=percentage,
                base_amount=base_amount,
            )
            results.append(result)

            if result.outcome == "WIN":
                wins += 1
            else:
                losses += 1

            if result.session_status != SessionStatus.ACTIVE.value:
                break

        final_stake = results[-1].stake_after if results else self._stake_service.track_current_stake(gambler_id)

        return ConsecutiveBetSummary(
            session_id=session_id,
            gambler_id=gambler_id,
            total_bets=len(results),
            total_wins=wins,
            total_losses=losses,
            final_stake=final_stake,
            results=tuple(results),
        )

    def determine_bet_outcome(self, win_probability: Decimal) -> bool:
        threshold = float(win_probability)
        return self._rng.random() < threshold

    def _execute_bet(
        self,
        *,
        gambler_id: int,
        session_id: int,
        strategy_code: str,
        explicit_bet_amount: Decimal | None,
        win_probability: Decimal,
        payout_multiplier: Decimal,
        fixed_amount: Decimal | int | float | str | None,
        percentage: Decimal | int | float | str | None,
        base_amount: Decimal | int | float | str | None,
    ) -> BetSettlementResult:
        self._validate_positive_id(gambler_id, "gambler_id")
        self._validate_positive_id(session_id, "session_id")

        with self._database.session(dictionary=True) as (connection, cursor):
            session_row = self._stake_service._fetch_session_row(
                cursor,
                session_id,
                for_update=True,
            )
            if session_row is None:
                raise NotFoundException(f"Session not found for id={session_id}.")

            if int(session_row["gambler_id"]) != gambler_id:
                raise ValidationException(
                    error_type=ValidationErrorType.RANGE_ERROR,
                    field_name="session_id",
                    attempted_value=session_id,
                    message="Session does not belong to gambler.",
                )

            if str(session_row["status"]) != SessionStatus.ACTIVE.value:
                raise ValidationException(
                    error_type=ValidationErrorType.RANGE_ERROR,
                    field_name="status",
                    attempted_value=session_row["status"],
                    message="Session must be ACTIVE before placing bets.",
                )

            gambler_row = self._stake_service._fetch_gambler_row(
                cursor,
                gambler_id,
                for_update=True,
            )
            if gambler_row is None:
                raise NotFoundException(f"Gambler profile not found for id={gambler_id}.")

            current_stake = _to_money(gambler_row["current_stake"], "current_stake")
            preference_row = self._fetch_preferences_row(cursor, gambler_id)
            if preference_row is None:
                raise NotFoundException(
                    f"Betting preferences not found for gambler id={gambler_id}."
                )

            min_bet = _to_money(preference_row["min_bet"], "min_bet")
            max_bet = _to_money(preference_row["max_bet"], "max_bet")

            if explicit_bet_amount is None:
                strategy = self._build_strategy(
                    strategy_code=strategy_code,
                    fallback_amount=min_bet,
                    fixed_amount=fixed_amount,
                    percentage=percentage,
                    base_amount=base_amount,
                )
                context = self._load_strategy_context(
                    cursor=cursor,
                    session_id=session_id,
                    next_step_index=int(session_row["games_played"]) + 1,
                )
                bet_amount = _to_money(
                    strategy.compute_bet_amount(
                        current_stake=current_stake,
                        context=context,
                    ),
                    "bet_amount",
                )
            else:
                bet_amount = explicit_bet_amount

            self._validate_bet_amount(
                bet_amount=bet_amount,
                current_stake=current_stake,
                min_bet=min_bet,
                max_bet=max_bet,
            )

            strategy_row = self._fetch_strategy_row(cursor, strategy_code)
            if strategy_row is None:
                raise NotFoundException(
                    f"Strategy {strategy_code!r} was not found or is inactive."
                )

            strategy_id = int(strategy_row["strategy_id"])
            potential_win = _to_money(bet_amount * payout_multiplier, "potential_win")

            is_win = self.determine_bet_outcome(win_probability)
            outcome = "WIN" if is_win else "LOSS"
            payout_amount = potential_win if is_win else _ZERO
            loss_amount = _ZERO if is_win else bet_amount
            net_change = payout_amount if is_win else _to_money(-bet_amount, "net_change")
            new_stake = _to_money(current_stake + net_change, "new_stake")

            if new_stake < _ZERO:
                raise ValidationException(
                    error_type=ValidationErrorType.STAKE_ERROR,
                    field_name="new_stake",
                    attempted_value=new_stake,
                    message="Stake cannot become negative.",
                )

            game_index = int(session_row["games_played"]) + 1
            placed_at = datetime.now(timezone.utc).replace(tzinfo=None)

            cursor.execute(
                """
                INSERT INTO BETS (
                    session_id,
                    gambler_id,
                    strategy_id,
                    game_index,
                    bet_amount,
                    win_probability,
                    odds_type,
                    odds_value,
                    potential_win,
                    stake_before,
                    stake_after,
                    is_settled,
                    placed_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    session_id,
                    gambler_id,
                    strategy_id,
                    game_index,
                    bet_amount,
                    win_probability,
                    "FIXED",
                    payout_multiplier,
                    potential_win,
                    current_stake,
                    new_stake,
                    True,
                    placed_at,
                ),
            )
            bet_id = int(cursor.lastrowid)

            win_streak, loss_streak = self._compute_consecutive_streaks(
                cursor=cursor,
                session_id=session_id,
                outcome=outcome,
            )

            resolved_at = datetime.now(timezone.utc).replace(tzinfo=None)
            cursor.execute(
                """
                INSERT INTO GAME_RECORDS (
                    session_id,
                    bet_id,
                    outcome,
                    payout_amount,
                    loss_amount,
                    net_change,
                    stake_before,
                    stake_after,
                    consecutive_win_streak,
                    consecutive_loss_streak,
                    game_duration_ms,
                    resolved_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    session_id,
                    bet_id,
                    outcome,
                    payout_amount,
                    loss_amount,
                    net_change,
                    current_stake,
                    new_stake,
                    win_streak,
                    loss_streak,
                    0,
                    resolved_at,
                ),
            )
            game_id = int(cursor.lastrowid)

            self._insert_stake_transaction(
                cursor=cursor,
                session_id=session_id,
                gambler_id=gambler_id,
                bet_id=bet_id,
                game_id=game_id,
                transaction_type=TransactionType.BET_PLACED,
                amount=bet_amount,
                balance_before=current_stake,
                balance_after=current_stake,
                transaction_ref=self._stake_service._transaction_ref(
                    prefix=TransactionType.BET_PLACED.value,
                    gambler_id=gambler_id,
                    session_id=session_id,
                ),
            )

            outcome_type = TransactionType.BET_WIN if is_win else TransactionType.BET_LOSS
            self._insert_stake_transaction(
                cursor=cursor,
                session_id=session_id,
                gambler_id=gambler_id,
                bet_id=bet_id,
                game_id=game_id,
                transaction_type=outcome_type,
                amount=net_change,
                balance_before=current_stake,
                balance_after=new_stake,
                transaction_ref=self._stake_service._transaction_ref(
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

            self._stake_service._update_session_stake_state(
                cursor=cursor,
                session_row=session_row,
                new_stake=new_stake,
                increment_games=True,
            )

            self._stake_service._insert_running_snapshot(
                cursor=cursor,
                session_id=session_id,
                starting_stake=_to_money(session_row["starting_stake"], "starting_stake"),
                game_id=game_id,
            )

            cursor.execute(
                """
                SELECT status, end_reason
                FROM SESSIONS
                WHERE session_id = %s
                """,
                (session_id,),
            )
            updated_session = cursor.fetchone()
            if updated_session is None:
                raise NotFoundException(f"Session not found for id={session_id}.")

            connection.commit()

            return BetSettlementResult(
                bet_id=bet_id,
                game_id=game_id,
                session_id=session_id,
                gambler_id=gambler_id,
                strategy_code=strategy_code,
                outcome=outcome,
                payout_amount=payout_amount,
                loss_amount=loss_amount,
                net_change=net_change,
                stake_before=current_stake,
                stake_after=new_stake,
                session_status=str(updated_session["status"]),
                end_reason=(
                    None
                    if updated_session["end_reason"] is None
                    else str(updated_session["end_reason"])
                ),
            )

    @staticmethod
    def _insert_stake_transaction(
        *,
        cursor: Any,
        session_id: int,
        gambler_id: int,
        bet_id: int,
        game_id: int,
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
                bet_id,
                game_id,
                transaction_type.value,
                amount,
                balance_before,
                balance_after,
                transaction_ref,
            ),
        )

    @staticmethod
    def _fetch_preferences_row(cursor: Any, gambler_id: int) -> Mapping[str, Any] | None:
        cursor.execute(
            """
            SELECT min_bet, max_bet
            FROM BETTING_PREFERENCES
            WHERE gambler_id = %s
            """,
            (gambler_id,),
        )
        return cursor.fetchone()

    @staticmethod
    def _fetch_strategy_row(cursor: Any, strategy_code: str) -> Mapping[str, Any] | None:
        cursor.execute(
            """
            SELECT strategy_id, strategy_code
            FROM BETTING_STRATEGIES
            WHERE strategy_code = %s
              AND is_active = TRUE
            """,
            (strategy_code,),
        )
        return cursor.fetchone()

    @staticmethod
    def _compute_consecutive_streaks(
        *,
        cursor: Any,
        session_id: int,
        outcome: str,
    ) -> tuple[int, int]:
        cursor.execute(
            """
            SELECT outcome, consecutive_win_streak, consecutive_loss_streak
            FROM GAME_RECORDS
            WHERE session_id = %s
            ORDER BY game_id DESC
            LIMIT 1
            """,
            (session_id,),
        )
        previous = cursor.fetchone()

        if previous is None:
            return (1, 0) if outcome == "WIN" else (0, 1)

        previous_outcome = str(previous["outcome"])
        previous_win_streak = int(previous["consecutive_win_streak"])
        previous_loss_streak = int(previous["consecutive_loss_streak"])

        if outcome == "WIN":
            return (
                previous_win_streak + 1 if previous_outcome == "WIN" else 1,
                0,
            )

        return (
            0,
            previous_loss_streak + 1 if previous_outcome == "LOSS" else 1,
        )

    @staticmethod
    def _validate_positive_id(value: int, field_name: str) -> None:
        if value <= 0:
            raise ValidationException(
                error_type=ValidationErrorType.RANGE_ERROR,
                field_name=field_name,
                attempted_value=value,
                message=f"{field_name} must be a positive integer.",
            )

    def _validate_bet_amount(
        self,
        *,
        bet_amount: Decimal,
        current_stake: Decimal,
        min_bet: Decimal,
        max_bet: Decimal,
    ) -> None:
        if bet_amount <= _ZERO:
            raise ValidationException(
                error_type=ValidationErrorType.BET_ERROR,
                field_name="bet_amount",
                attempted_value=bet_amount,
                message="bet_amount must be greater than zero.",
            )
        if bet_amount > current_stake:
            raise ValidationException(
                error_type=ValidationErrorType.BET_ERROR,
                field_name="bet_amount",
                attempted_value=bet_amount,
                message="bet_amount cannot exceed current stake.",
            )
        if bet_amount < min_bet:
            raise ValidationException(
                error_type=ValidationErrorType.BET_ERROR,
                field_name="bet_amount",
                attempted_value=bet_amount,
                message=f"bet_amount must be >= min_bet ({min_bet}).",
            )
        if bet_amount > max_bet:
            raise ValidationException(
                error_type=ValidationErrorType.BET_ERROR,
                field_name="bet_amount",
                attempted_value=bet_amount,
                message=f"bet_amount must be <= max_bet ({max_bet}).",
            )

    def _normalize_probability(
        self,
        win_probability: Decimal | int | float | str | None,
    ) -> Decimal:
        value = (
            self._settings.session_default_win_probability
            if win_probability is None
            else win_probability
        )
        try:
            normalized = Decimal(str(value)).quantize(_RATE_QUANTUM, rounding=ROUND_HALF_UP)
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise ValidationException(
                error_type=ValidationErrorType.PROBABILITY_ERROR,
                field_name="win_probability",
                attempted_value=value,
                message="Invalid win_probability value.",
            ) from exc

        if normalized < Decimal("0.0000") or normalized > Decimal("1.0000"):
            raise ValidationException(
                error_type=ValidationErrorType.PROBABILITY_ERROR,
                field_name="win_probability",
                attempted_value=normalized,
                message="win_probability must be between 0 and 1.",
            )

        return normalized

    def _normalize_multiplier(
        self,
        payout_multiplier: Decimal | int | float | str,
    ) -> Decimal:
        try:
            normalized = Decimal(str(payout_multiplier)).quantize(
                _RATE_QUANTUM,
                rounding=ROUND_HALF_UP,
            )
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise ValidationException(
                error_type=ValidationErrorType.NUMERIC_ERROR,
                field_name="payout_multiplier",
                attempted_value=payout_multiplier,
                message="Invalid payout_multiplier value.",
            ) from exc

        if normalized <= Decimal("0.0000"):
            raise ValidationException(
                error_type=ValidationErrorType.RANGE_ERROR,
                field_name="payout_multiplier",
                attempted_value=normalized,
                message="payout_multiplier must be greater than zero.",
            )

        return normalized

    @staticmethod
    def _normalize_strategy_code(strategy_code: str) -> str:
        normalized = strategy_code.strip().upper()
        if not normalized:
            raise ValidationException(
                error_type=ValidationErrorType.NULL_ERROR,
                field_name="strategy_code",
                attempted_value=strategy_code,
                message="strategy_code is required.",
            )
        return normalized

    def _load_strategy_context(
        self,
        *,
        cursor: Any,
        session_id: int,
        next_step_index: int,
    ) -> StrategyContext:
        cursor.execute(
            """
            SELECT b.bet_amount, g.outcome
            FROM GAME_RECORDS g
            JOIN BETS b ON b.bet_id = g.bet_id
            WHERE g.session_id = %s
            ORDER BY g.game_id DESC
            LIMIT 1
            """,
            (session_id,),
        )
        row = cursor.fetchone()

        if row is None:
            return StrategyContext(
                step_index=next_step_index,
                last_bet_amount=None,
                last_outcome=None,
            )

        return StrategyContext(
            step_index=next_step_index,
            last_bet_amount=_to_money(row["bet_amount"], "last_bet_amount"),
            last_outcome=str(row["outcome"]),
        )

    def _build_strategy(
        self,
        *,
        strategy_code: str,
        fallback_amount: Decimal,
        fixed_amount: Decimal | int | float | str | None,
        percentage: Decimal | int | float | str | None,
        base_amount: Decimal | int | float | str | None,
    ) -> BettingStrategy:
        if strategy_code in {"MANUAL", "FIXED_AMOUNT"}:
            resolved_amount = (
                fallback_amount if fixed_amount is None else _to_money(fixed_amount, "fixed_amount")
            )
            return FixedAmountStrategy(amount=resolved_amount)

        if strategy_code == "PERCENTAGE":
            resolved_percentage = self._normalize_percentage(percentage)
            return PercentageStrategy(percent=resolved_percentage)

        if strategy_code == "MARTINGALE":
            resolved_base = fallback_amount
            if base_amount is not None:
                resolved_base = _to_money(base_amount, "base_amount")
            elif fixed_amount is not None:
                resolved_base = _to_money(fixed_amount, "fixed_amount")
            return MartingaleStrategy(base_amount=resolved_base)

        raise ValidationException(
            error_type=ValidationErrorType.RANGE_ERROR,
            field_name="strategy_code",
            attempted_value=strategy_code,
            message="Unsupported strategy_code.",
        )

    @staticmethod
    def _normalize_percentage(
        percentage: Decimal | int | float | str | None,
    ) -> Decimal:
        raw = Decimal("0.05") if percentage is None else Decimal(str(percentage))

        if raw > Decimal("1") and raw <= Decimal("100"):
            raw = raw / Decimal("100")

        normalized = raw.quantize(_RATE_QUANTUM, rounding=ROUND_HALF_UP)
        if normalized <= Decimal("0.0000") or normalized > Decimal("1.0000"):
            raise ValidationException(
                error_type=ValidationErrorType.RANGE_ERROR,
                field_name="percentage",
                attempted_value=percentage,
                message="percentage must be in (0, 1] or (0, 100] format.",
            )

        return normalized
