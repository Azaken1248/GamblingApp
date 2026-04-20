from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from random import Random
from typing import Any

from config.database import Database
from config.settings import Settings
from tracking_and_reports.win_loss_statistics import (
    OddsConfiguration,
    RunningTotalsByGame,
    WinLossStatistics,
)
from utils.exceptions import NotFoundException, ValidationErrorType, ValidationException

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
            message="Value cannot be converted to monetary decimal.",
        ) from exc


class WinLossCalculator:
    def __init__(
        self,
        database: Database,
        settings: Settings,
        rng: Random | None = None,
    ) -> None:
        self._database = database
        self._settings = settings
        self._rng = rng or Random()

    def determine_outcome(
        self,
        *,
        win_probability: Decimal | int | float | str,
        mode: str = "RANDOM",
        house_edge: Decimal | int | float | str | None = None,
    ) -> bool:
        normalized_probability = self._normalize_probability(win_probability)
        normalized_mode = mode.strip().upper()

        effective_probability = normalized_probability
        if normalized_mode == "WEIGHTED":
            edge = self._normalize_house_edge(house_edge)
            effective_probability = (normalized_probability * (Decimal("1") - edge)).quantize(
                _RATE_QUANTUM,
                rounding=ROUND_HALF_UP,
            )
        elif normalized_mode != "RANDOM":
            raise ValidationException(
                error_type=ValidationErrorType.RANGE_ERROR,
                field_name="mode",
                attempted_value=mode,
                message="mode must be RANDOM or WEIGHTED.",
            )

        return self._rng.random() < float(effective_probability)

    def calculate_winnings(
        self,
        *,
        bet_amount: Decimal | int | float | str,
        odds_type: str,
        odds_value: Decimal | int | float | str | None = None,
        win_probability: Decimal | int | float | str | None = None,
    ) -> Decimal:
        normalized_bet = _to_money(bet_amount, "bet_amount")
        if normalized_bet <= _ZERO:
            raise ValidationException(
                error_type=ValidationErrorType.BET_ERROR,
                field_name="bet_amount",
                attempted_value=normalized_bet,
                message="bet_amount must be greater than zero.",
            )

        normalized_type = odds_type.strip().upper()

        if normalized_type == "FIXED":
            multiplier = self._normalize_positive_decimal(odds_value, "odds_value", default=Decimal("1.0"))
            return _to_money(normalized_bet * multiplier, "winnings")

        if normalized_type == "PROBABILITY_BASED":
            probability = self._normalize_probability(win_probability)
            if probability <= Decimal("0.0000"):
                raise ValidationException(
                    error_type=ValidationErrorType.PROBABILITY_ERROR,
                    field_name="win_probability",
                    attempted_value=probability,
                    message="win_probability must be greater than zero for probability-based odds.",
                )
            factor = self._normalize_positive_decimal(
                odds_value,
                "odds_value",
                default=Decimal("1.0"),
            )
            payout_multiplier = ((Decimal("1") / probability) - Decimal("1")) * factor
            return _to_money(normalized_bet * payout_multiplier, "winnings")

        if normalized_type == "AMERICAN":
            if odds_value is None:
                raise ValidationException(
                    error_type=ValidationErrorType.NULL_ERROR,
                    field_name="odds_value",
                    attempted_value=odds_value,
                    message="odds_value is required for AMERICAN odds.",
                )
            try:
                american = int(str(odds_value))
            except (TypeError, ValueError) as exc:
                raise ValidationException(
                    error_type=ValidationErrorType.NUMERIC_ERROR,
                    field_name="odds_value",
                    attempted_value=odds_value,
                    message="odds_value must be an integer for AMERICAN odds.",
                ) from exc

            if american == 0:
                raise ValidationException(
                    error_type=ValidationErrorType.RANGE_ERROR,
                    field_name="odds_value",
                    attempted_value=american,
                    message="American odds cannot be zero.",
                )

            if american > 0:
                payout_multiplier = Decimal(american) / Decimal("100")
            else:
                payout_multiplier = Decimal("100") / abs(Decimal(american))
            return _to_money(normalized_bet * payout_multiplier, "winnings")

        if normalized_type == "DECIMAL":
            decimal_odds = self._normalize_positive_decimal(
                odds_value,
                "odds_value",
                default=Decimal("2.0"),
            )
            if decimal_odds <= Decimal("1"):
                raise ValidationException(
                    error_type=ValidationErrorType.RANGE_ERROR,
                    field_name="odds_value",
                    attempted_value=decimal_odds,
                    message="Decimal odds must be greater than 1.",
                )
            payout_multiplier = decimal_odds - Decimal("1")
            return _to_money(normalized_bet * payout_multiplier, "winnings")

        raise ValidationException(
            error_type=ValidationErrorType.RANGE_ERROR,
            field_name="odds_type",
            attempted_value=odds_type,
            message="Unsupported odds_type.",
        )

    def calculate_loss(self, *, bet_amount: Decimal | int | float | str) -> Decimal:
        normalized_bet = _to_money(bet_amount, "bet_amount")
        if normalized_bet <= _ZERO:
            raise ValidationException(
                error_type=ValidationErrorType.BET_ERROR,
                field_name="bet_amount",
                attempted_value=normalized_bet,
                message="bet_amount must be greater than zero.",
            )
        return normalized_bet

    def list_odds_configurations(self) -> tuple[OddsConfiguration, ...]:
        with self._database.session(dictionary=True) as (_, cursor):
            cursor.execute(
                """
                SELECT
                    odds_config_id,
                    odds_type,
                    fixed_multiplier,
                    american_odds,
                    decimal_odds,
                    probability_payout_factor,
                    house_edge,
                    is_default
                FROM ODDS_CONFIGURATIONS
                ORDER BY odds_config_id
                """,
            )
            rows = cursor.fetchall()

        return tuple(self._odds_row_to_model(row) for row in rows)

    def get_odds_configuration(self, odds_config_id: int) -> OddsConfiguration:
        self._validate_positive_id(odds_config_id, "odds_config_id")

        with self._database.session(dictionary=True) as (_, cursor):
            cursor.execute(
                """
                SELECT
                    odds_config_id,
                    odds_type,
                    fixed_multiplier,
                    american_odds,
                    decimal_odds,
                    probability_payout_factor,
                    house_edge,
                    is_default
                FROM ODDS_CONFIGURATIONS
                WHERE odds_config_id = %s
                """,
                (odds_config_id,),
            )
            row = cursor.fetchone()

        if row is None:
            raise NotFoundException(
                f"Odds configuration not found for id={odds_config_id}."
            )

        return self._odds_row_to_model(row)

    def get_running_totals_by_game(
        self,
        session_id: int,
        *,
        include_non_game_snapshots: bool = False,
    ) -> tuple[RunningTotalsByGame, ...]:
        self._validate_positive_id(session_id, "session_id")

        query = """
            SELECT
                rs.snapshot_id,
                rs.session_id,
                rs.game_id,
                b.game_index,
                rs.total_games,
                rs.total_wins,
                rs.total_losses,
                rs.total_pushes,
                rs.total_winnings,
                rs.total_losses_amount,
                rs.net_profit,
                rs.win_rate,
                rs.profit_factor,
                rs.roi,
                rs.longest_win_streak,
                rs.longest_loss_streak
            FROM RUNNING_TOTALS_SNAPSHOTS rs
            LEFT JOIN GAME_RECORDS gr ON gr.game_id = rs.game_id
            LEFT JOIN BETS b ON b.bet_id = gr.bet_id
            WHERE rs.session_id = %s
        """
        params: list[Any] = [session_id]

        if not include_non_game_snapshots:
            query += " AND rs.game_id IS NOT NULL"

        query += " ORDER BY rs.snapshot_id"

        with self._database.session(dictionary=True) as (_, cursor):
            cursor.execute(query, tuple(params))
            rows = cursor.fetchall()

        return tuple(self._snapshot_row_to_model(row) for row in rows)

    def get_win_loss_statistics(self, session_id: int) -> WinLossStatistics:
        self._validate_positive_id(session_id, "session_id")

        with self._database.session(dictionary=True) as (_, cursor):
            cursor.execute(
                """
                SELECT
                    s.session_id,
                    s.gambler_id,
                    s.starting_stake,
                    COALESCE(COUNT(gr.game_id), 0) AS total_games,
                    COALESCE(SUM(CASE WHEN gr.outcome = 'WIN' THEN 1 ELSE 0 END), 0) AS total_wins,
                    COALESCE(SUM(CASE WHEN gr.outcome = 'LOSS' THEN 1 ELSE 0 END), 0) AS total_losses,
                    COALESCE(SUM(gr.payout_amount), 0) AS total_winnings,
                    COALESCE(SUM(gr.loss_amount), 0) AS total_losses_amount,
                    COALESCE(MAX(gr.payout_amount), 0) AS largest_win,
                    COALESCE(MAX(gr.loss_amount), 0) AS largest_loss,
                    COALESCE(MAX(gr.consecutive_win_streak), 0) AS longest_win_streak,
                    COALESCE(MAX(gr.consecutive_loss_streak), 0) AS longest_loss_streak
                FROM SESSIONS s
                LEFT JOIN GAME_RECORDS gr ON gr.session_id = s.session_id
                WHERE s.session_id = %s
                GROUP BY s.session_id, s.gambler_id, s.starting_stake
                """,
                (session_id,),
            )
            base_row = cursor.fetchone()

            if base_row is None:
                raise NotFoundException(f"Session not found for id={session_id}.")

            cursor.execute(
                """
                SELECT
                    outcome,
                    consecutive_win_streak,
                    consecutive_loss_streak
                FROM GAME_RECORDS
                WHERE session_id = %s
                ORDER BY game_id DESC
                LIMIT 1
                """,
                (session_id,),
            )
            current_streak_row = cursor.fetchone()

        total_games = int(base_row["total_games"])
        total_wins = int(base_row["total_wins"])
        total_losses = int(base_row["total_losses"])

        total_winnings = _to_money(base_row["total_winnings"], "total_winnings")
        total_losses_amount = _to_money(base_row["total_losses_amount"], "total_losses_amount")
        net_profit = _to_money(total_winnings - total_losses_amount, "net_profit")

        win_rate = Decimal("0.0000")
        loss_rate = Decimal("0.0000")
        if total_games > 0:
            win_rate = (Decimal(total_wins) / Decimal(total_games)).quantize(
                _RATE_QUANTUM,
                rounding=ROUND_HALF_UP,
            )
            loss_rate = (Decimal(total_losses) / Decimal(total_games)).quantize(
                _RATE_QUANTUM,
                rounding=ROUND_HALF_UP,
            )

        win_loss_ratio: Decimal | None = None
        if total_losses > 0:
            win_loss_ratio = (Decimal(total_wins) / Decimal(total_losses)).quantize(
                _RATE_QUANTUM,
                rounding=ROUND_HALF_UP,
            )

        starting_stake = _to_money(base_row["starting_stake"], "starting_stake")
        roi = Decimal("0.0000")
        if starting_stake > _ZERO:
            roi = (net_profit / starting_stake).quantize(
                _RATE_QUANTUM,
                rounding=ROUND_HALF_UP,
            )

        profit_factor = Decimal("0.0000")
        if total_losses_amount > _ZERO:
            profit_factor = (total_winnings / total_losses_amount).quantize(
                _RATE_QUANTUM,
                rounding=ROUND_HALF_UP,
            )

        current_win_streak = 0
        current_loss_streak = 0
        if current_streak_row is not None:
            if str(current_streak_row["outcome"]) == "WIN":
                current_win_streak = int(current_streak_row["consecutive_win_streak"])
            else:
                current_loss_streak = int(current_streak_row["consecutive_loss_streak"])

        running_totals = self.get_running_totals_by_game(
            session_id=session_id,
            include_non_game_snapshots=False,
        )
        if not running_totals:
            running_totals = self.get_running_totals_by_game(
                session_id=session_id,
                include_non_game_snapshots=True,
            )

        return WinLossStatistics(
            session_id=int(base_row["session_id"]),
            gambler_id=int(base_row["gambler_id"]),
            total_games=total_games,
            total_wins=total_wins,
            total_losses=total_losses,
            win_rate=win_rate,
            loss_rate=loss_rate,
            win_loss_ratio=win_loss_ratio,
            total_winnings=total_winnings,
            total_losses_amount=total_losses_amount,
            net_profit=net_profit,
            roi=roi,
            profit_factor=profit_factor,
            largest_win=_to_money(base_row["largest_win"], "largest_win"),
            largest_loss=_to_money(base_row["largest_loss"], "largest_loss"),
            current_win_streak=current_win_streak,
            current_loss_streak=current_loss_streak,
            longest_win_streak=int(base_row["longest_win_streak"]),
            longest_loss_streak=int(base_row["longest_loss_streak"]),
            running_totals=running_totals,
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

    def _normalize_probability(self, value: Decimal | int | float | str) -> Decimal:
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

    def _normalize_house_edge(self, value: Decimal | int | float | str | None) -> Decimal:
        source = Decimal("0.0000") if value is None else value

        try:
            normalized = Decimal(str(source)).quantize(_RATE_QUANTUM, rounding=ROUND_HALF_UP)
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise ValidationException(
                error_type=ValidationErrorType.PROBABILITY_ERROR,
                field_name="house_edge",
                attempted_value=value,
                message="Invalid house_edge value.",
            ) from exc

        if normalized < Decimal("0.0000") or normalized >= Decimal("1.0000"):
            raise ValidationException(
                error_type=ValidationErrorType.PROBABILITY_ERROR,
                field_name="house_edge",
                attempted_value=normalized,
                message="house_edge must be in [0, 1).",
            )

        return normalized

    @staticmethod
    def _normalize_positive_decimal(
        value: Decimal | int | float | str | None,
        field_name: str,
        *,
        default: Decimal,
    ) -> Decimal:
        source = default if value is None else value

        try:
            normalized = Decimal(str(source)).quantize(_RATE_QUANTUM, rounding=ROUND_HALF_UP)
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise ValidationException(
                error_type=ValidationErrorType.NUMERIC_ERROR,
                field_name=field_name,
                attempted_value=value,
                message=f"Invalid {field_name} value.",
            ) from exc

        if normalized <= Decimal("0.0000"):
            raise ValidationException(
                error_type=ValidationErrorType.RANGE_ERROR,
                field_name=field_name,
                attempted_value=normalized,
                message=f"{field_name} must be greater than zero.",
            )

        return normalized

    @staticmethod
    def _odds_row_to_model(row: dict[str, Any]) -> OddsConfiguration:
        return OddsConfiguration(
            odds_config_id=int(row["odds_config_id"]),
            odds_type=str(row["odds_type"]),
            fixed_multiplier=(
                None
                if row["fixed_multiplier"] is None
                else Decimal(str(row["fixed_multiplier"])).quantize(
                    _RATE_QUANTUM,
                    rounding=ROUND_HALF_UP,
                )
            ),
            american_odds=(None if row["american_odds"] is None else int(row["american_odds"])),
            decimal_odds=(
                None
                if row["decimal_odds"] is None
                else Decimal(str(row["decimal_odds"])).quantize(
                    _RATE_QUANTUM,
                    rounding=ROUND_HALF_UP,
                )
            ),
            probability_payout_factor=(
                None
                if row["probability_payout_factor"] is None
                else Decimal(str(row["probability_payout_factor"])).quantize(
                    _RATE_QUANTUM,
                    rounding=ROUND_HALF_UP,
                )
            ),
            house_edge=Decimal(str(row["house_edge"])).quantize(
                _RATE_QUANTUM,
                rounding=ROUND_HALF_UP,
            ),
            is_default=bool(row["is_default"]),
        )

    @staticmethod
    def _snapshot_row_to_model(row: dict[str, Any]) -> RunningTotalsByGame:
        return RunningTotalsByGame(
            snapshot_id=int(row["snapshot_id"]),
            session_id=int(row["session_id"]),
            game_id=(None if row["game_id"] is None else int(row["game_id"])),
            game_index=(None if row["game_index"] is None else int(row["game_index"])),
            total_games=int(row["total_games"]),
            total_wins=int(row["total_wins"]),
            total_losses=int(row["total_losses"]),
            total_pushes=int(row["total_pushes"]),
            total_winnings=_to_money(row["total_winnings"], "total_winnings"),
            total_losses_amount=_to_money(row["total_losses_amount"], "total_losses_amount"),
            net_profit=_to_money(row["net_profit"], "net_profit"),
            win_rate=Decimal(str(row["win_rate"])).quantize(
                _RATE_QUANTUM,
                rounding=ROUND_HALF_UP,
            ),
            profit_factor=Decimal(str(row["profit_factor"])).quantize(
                _RATE_QUANTUM,
                rounding=ROUND_HALF_UP,
            ),
            roi=Decimal(str(row["roi"])).quantize(
                _RATE_QUANTUM,
                rounding=ROUND_HALF_UP,
            ),
            longest_win_streak=int(row["longest_win_streak"]),
            longest_loss_streak=int(row["longest_loss_streak"]),
        )
