from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Mapping

from config.database import Database
from config.settings import Settings
from models.gambler_profile import BettingPreferences, GamblerProfile
from tracking_and_reports.gambler_statistics import EligibilityStatus, GamblerStatistics
from utils.exceptions import NotFoundException, ValidationErrorType, ValidationException

_MONEY_QUANTUM = Decimal("0.01")
_WIN_RATE_QUANTUM = Decimal("0.0001")
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


class GamblerProfileService:
    _PROFILE_UPDATE_FIELDS = {
        "full_name",
        "email",
        "win_threshold",
        "loss_threshold",
        "min_required_stake",
        "is_active",
    }
    _PREFERENCE_UPDATE_FIELDS = {
        "min_bet",
        "max_bet",
        "preferred_game_type",
        "auto_play_enabled",
        "auto_play_max_games",
        "session_loss_limit",
        "session_win_target",
    }

    def __init__(self, database: Database, settings: Settings) -> None:
        self._database = database
        self._settings = settings

    def create_profile(
        self,
        profile: GamblerProfile,
        preferences: BettingPreferences,
    ) -> GamblerProfile:
        normalized_profile = self._normalize_profile(profile)
        normalized_preferences = self._normalize_preferences(preferences)

        self._validate_profile(normalized_profile, enforce_threshold_position=True)
        self._validate_preferences(normalized_preferences)

        with self._database.session(dictionary=True) as (connection, cursor):
            cursor.execute(
                """
                INSERT INTO GAMBLERS (
                    username,
                    full_name,
                    email,
                    is_active,
                    initial_stake,
                    current_stake,
                    win_threshold,
                    loss_threshold,
                    min_required_stake
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    normalized_profile.username,
                    normalized_profile.full_name,
                    normalized_profile.email,
                    normalized_profile.is_active,
                    normalized_profile.initial_stake,
                    normalized_profile.current_stake,
                    normalized_profile.win_threshold,
                    normalized_profile.loss_threshold,
                    normalized_profile.min_required_stake,
                ),
            )

            gambler_id = int(cursor.lastrowid)

            cursor.execute(
                """
                INSERT INTO BETTING_PREFERENCES (
                    gambler_id,
                    min_bet,
                    max_bet,
                    preferred_game_type,
                    auto_play_enabled,
                    auto_play_max_games,
                    session_loss_limit,
                    session_win_target
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    gambler_id,
                    normalized_preferences.min_bet,
                    normalized_preferences.max_bet,
                    normalized_preferences.preferred_game_type,
                    normalized_preferences.auto_play_enabled,
                    normalized_preferences.auto_play_max_games,
                    normalized_preferences.session_loss_limit,
                    normalized_preferences.session_win_target,
                ),
            )

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
                    None,
                    gambler_id,
                    None,
                    None,
                    "INITIAL_STAKE",
                    normalized_profile.initial_stake,
                    _ZERO,
                    normalized_profile.current_stake,
                    self._transaction_ref("INITIAL", gambler_id),
                ),
            )

            connection.commit()

        return self.get_profile(gambler_id)

    def update_profile(
        self,
        gambler_id: int,
        profile_updates: Mapping[str, Any] | None = None,
        preference_updates: Mapping[str, Any] | None = None,
    ) -> GamblerProfile:
        self._validate_gambler_id(gambler_id)

        profile_updates = dict(profile_updates or {})
        preference_updates = dict(preference_updates or {})

        if not profile_updates and not preference_updates:
            return self.get_profile(gambler_id)

        self._validate_update_fields(
            profile_updates,
            self._PROFILE_UPDATE_FIELDS,
            "profile",
        )
        self._validate_update_fields(
            preference_updates,
            self._PREFERENCE_UPDATE_FIELDS,
            "preferences",
        )

        with self._database.session(dictionary=True) as (connection, cursor):
            current_profile_row = self._fetch_gambler_row(cursor, gambler_id, for_update=True)
            if current_profile_row is None:
                raise NotFoundException(f"Gambler profile not found for id={gambler_id}.")

            current_preferences_row = self._fetch_preferences_row(
                cursor,
                gambler_id,
                for_update=True,
            )
            if current_preferences_row is None:
                raise NotFoundException(
                    f"Betting preferences not found for gambler id={gambler_id}."
                )

            merged_profile = {**current_profile_row, **profile_updates}
            merged_preferences = {**current_preferences_row, **preference_updates}

            normalized_profile = self._profile_from_mapping(merged_profile)
            normalized_preferences = self._preferences_from_mapping(merged_preferences)

            self._validate_profile(
                normalized_profile,
                enforce_threshold_position=(
                    "win_threshold" in profile_updates
                    or "loss_threshold" in profile_updates
                ),
            )
            self._validate_preferences(normalized_preferences)

            if profile_updates:
                profile_values = self._profile_to_db_values(normalized_profile)
                payload = {
                    field: profile_values[field]
                    for field in profile_updates
                    if field in profile_values
                }
                self._execute_update(
                    cursor=cursor,
                    table_name="GAMBLERS",
                    id_column="gambler_id",
                    id_value=gambler_id,
                    updates=payload,
                    append_updated_at=True,
                )

            if preference_updates:
                preference_values = self._preferences_to_db_values(normalized_preferences)
                payload = {
                    field: preference_values[field]
                    for field in preference_updates
                    if field in preference_values
                }
                self._execute_update(
                    cursor=cursor,
                    table_name="BETTING_PREFERENCES",
                    id_column="gambler_id",
                    id_value=gambler_id,
                    updates=payload,
                    append_updated_at=True,
                )

            connection.commit()

        return self.get_profile(gambler_id)

    def get_profile(self, gambler_id: int) -> GamblerProfile:
        self._validate_gambler_id(gambler_id)

        with self._database.session(dictionary=True) as (_, cursor):
            row = self._fetch_gambler_row(cursor, gambler_id)

        if row is None:
            raise NotFoundException(f"Gambler profile not found for id={gambler_id}.")

        return self._profile_from_mapping(row)

    def retrieve_profile_statistics(self, gambler_id: int) -> GamblerStatistics:
        self._validate_gambler_id(gambler_id)

        with self._database.session(dictionary=True) as (_, cursor):
            cursor.execute(
                """
                SELECT
                    g.gambler_id,
                    g.username,
                    g.current_stake,
                    g.initial_stake,
                    g.win_threshold,
                    g.loss_threshold,
                    COALESCE(
                        SUM(CASE WHEN st.transaction_type = 'BET_PLACED' THEN 1 ELSE 0 END),
                        0
                    ) AS total_bets,
                    COALESCE(
                        SUM(CASE WHEN st.transaction_type = 'BET_WIN' THEN 1 ELSE 0 END),
                        0
                    ) AS total_wins,
                    COALESCE(
                        SUM(CASE WHEN st.transaction_type = 'BET_LOSS' THEN 1 ELSE 0 END),
                        0
                    ) AS total_losses,
                    COALESCE(
                        SUM(CASE WHEN st.transaction_type = 'BET_WIN' THEN st.amount ELSE 0 END),
                        0
                    ) AS total_winnings,
                    COALESCE(
                        SUM(CASE WHEN st.transaction_type = 'BET_LOSS' THEN ABS(st.amount) ELSE 0 END),
                        0
                    ) AS total_losses_amount
                FROM GAMBLERS g
                LEFT JOIN STAKE_TRANSACTIONS st ON st.gambler_id = g.gambler_id
                WHERE g.gambler_id = %s
                GROUP BY
                    g.gambler_id,
                    g.username,
                    g.current_stake,
                    g.initial_stake,
                    g.win_threshold,
                    g.loss_threshold
                """,
                (gambler_id,),
            )
            row = cursor.fetchone()

        if row is None:
            raise NotFoundException(f"Gambler profile not found for id={gambler_id}.")

        total_bets = int(row["total_bets"])
        total_wins = int(row["total_wins"])
        total_losses = int(row["total_losses"])
        total_winnings = _to_money(row["total_winnings"], "total_winnings")
        total_losses_amount = _to_money(row["total_losses_amount"], "total_losses_amount")

        win_rate = _ZERO
        if total_bets > 0:
            win_rate = (
                Decimal(total_wins) / Decimal(total_bets)
            ).quantize(_WIN_RATE_QUANTUM, rounding=ROUND_HALF_UP)

        current_stake = _to_money(row["current_stake"], "current_stake")
        initial_stake = _to_money(row["initial_stake"], "initial_stake")
        win_threshold = _to_money(row["win_threshold"], "win_threshold")
        loss_threshold = _to_money(row["loss_threshold"], "loss_threshold")

        return GamblerStatistics(
            gambler_id=int(row["gambler_id"]),
            username=str(row["username"]),
            current_stake=current_stake,
            initial_stake=initial_stake,
            total_bets=total_bets,
            total_wins=total_wins,
            total_losses=total_losses,
            total_winnings=total_winnings,
            total_losses_amount=total_losses_amount,
            win_rate=win_rate,
            net_profit=(total_winnings - total_losses_amount).quantize(
                _MONEY_QUANTUM,
                rounding=ROUND_HALF_UP,
            ),
            reached_win_threshold=current_stake >= win_threshold,
            reached_loss_threshold=current_stake <= loss_threshold,
        )

    def validate_eligibility(self, gambler_id: int) -> EligibilityStatus:
        profile = self.get_profile(gambler_id)

        reasons: list[str] = []
        if not profile.is_active:
            reasons.append("Account is inactive.")
        if profile.current_stake < profile.min_required_stake:
            reasons.append("Current stake is below minimum required stake.")
        if profile.current_stake <= profile.loss_threshold:
            reasons.append("Current stake reached or crossed the loss threshold.")

        return EligibilityStatus(
            gambler_id=gambler_id,
            is_eligible=len(reasons) == 0,
            current_stake=profile.current_stake,
            min_required_stake=profile.min_required_stake,
            reasons=tuple(reasons),
        )

    def reset_profile_for_new_session(
        self,
        gambler_id: int,
        new_initial_stake: Decimal | int | float | str | None = None,
    ) -> GamblerProfile:
        self._validate_gambler_id(gambler_id)

        with self._database.session(dictionary=True) as (connection, cursor):
            current_row = self._fetch_gambler_row(cursor, gambler_id, for_update=True)
            if current_row is None:
                raise NotFoundException(f"Gambler profile not found for id={gambler_id}.")

            profile = self._profile_from_mapping(current_row)
            target_initial_stake = (
                profile.initial_stake
                if new_initial_stake is None
                else _to_money(new_initial_stake, "new_initial_stake")
            )
            self._validate_initial_stake(target_initial_stake)

            if profile.initial_stake <= _ZERO:
                win_delta_ratio = Decimal("0")
                loss_delta_ratio = Decimal("0")
            else:
                win_delta_ratio = (
                    profile.win_threshold - profile.initial_stake
                ) / profile.initial_stake
                loss_delta_ratio = (
                    profile.initial_stake - profile.loss_threshold
                ) / profile.initial_stake

            new_win_threshold = _to_money(
                target_initial_stake * (Decimal("1") + win_delta_ratio),
                "win_threshold",
            )

            new_loss_candidate = target_initial_stake * (Decimal("1") - loss_delta_ratio)
            new_loss_threshold = _to_money(
                new_loss_candidate if new_loss_candidate > _ZERO else _ZERO,
                "loss_threshold",
            )

            if new_win_threshold <= target_initial_stake:
                raise ValidationException(
                    error_type=ValidationErrorType.LIMIT_ERROR,
                    field_name="win_threshold",
                    attempted_value=new_win_threshold,
                    message="Win threshold must remain above current stake after reset.",
                )

            if new_loss_threshold >= target_initial_stake:
                raise ValidationException(
                    error_type=ValidationErrorType.LIMIT_ERROR,
                    field_name="loss_threshold",
                    attempted_value=new_loss_threshold,
                    message="Loss threshold must remain below current stake after reset.",
                )

            cursor.execute(
                """
                UPDATE GAMBLERS
                SET
                    initial_stake = %s,
                    current_stake = %s,
                    win_threshold = %s,
                    loss_threshold = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE gambler_id = %s
                """,
                (
                    target_initial_stake,
                    target_initial_stake,
                    new_win_threshold,
                    new_loss_threshold,
                    gambler_id,
                ),
            )

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
                    None,
                    gambler_id,
                    None,
                    None,
                    "RESET",
                    _to_money(target_initial_stake - profile.current_stake, "reset_delta"),
                    profile.current_stake,
                    target_initial_stake,
                    self._transaction_ref("RESET", gambler_id),
                ),
            )

            connection.commit()

        return self.get_profile(gambler_id)

    def _validate_gambler_id(self, gambler_id: int) -> None:
        if gambler_id <= 0:
            raise ValidationException(
                error_type=ValidationErrorType.RANGE_ERROR,
                field_name="gambler_id",
                attempted_value=gambler_id,
                message="Gambler id must be a positive integer.",
            )

    def _validate_initial_stake(self, initial_stake: Decimal) -> None:
        if initial_stake < self._settings.min_initial_stake:
            raise ValidationException(
                error_type=ValidationErrorType.STAKE_ERROR,
                field_name="initial_stake",
                attempted_value=initial_stake,
                message=(
                    "Initial stake is below configured minimum "
                    f"{self._settings.min_initial_stake}."
                ),
            )
        if initial_stake > self._settings.max_initial_stake:
            raise ValidationException(
                error_type=ValidationErrorType.STAKE_ERROR,
                field_name="initial_stake",
                attempted_value=initial_stake,
                message=(
                    "Initial stake is above configured maximum "
                    f"{self._settings.max_initial_stake}."
                ),
            )

    def _validate_profile(
        self,
        profile: GamblerProfile,
        *,
        enforce_threshold_position: bool,
    ) -> None:
        if not profile.username:
            raise ValidationException(
                error_type=ValidationErrorType.NULL_ERROR,
                field_name="username",
                attempted_value=profile.username,
                message="Username is required.",
            )
        if not profile.full_name:
            raise ValidationException(
                error_type=ValidationErrorType.NULL_ERROR,
                field_name="full_name",
                attempted_value=profile.full_name,
                message="Full name is required.",
            )
        if not profile.email:
            raise ValidationException(
                error_type=ValidationErrorType.NULL_ERROR,
                field_name="email",
                attempted_value=profile.email,
                message="Email is required.",
            )

        self._validate_initial_stake(profile.initial_stake)

        if profile.current_stake != profile.initial_stake and profile.gambler_id is None:
            raise ValidationException(
                error_type=ValidationErrorType.STAKE_ERROR,
                field_name="current_stake",
                attempted_value=profile.current_stake,
                message=(
                    "When creating a profile, current stake must match initial stake."
                ),
            )

        if profile.current_stake < _ZERO:
            raise ValidationException(
                error_type=ValidationErrorType.STAKE_ERROR,
                field_name="current_stake",
                attempted_value=profile.current_stake,
                message="Current stake cannot be negative.",
            )
        if profile.min_required_stake < _ZERO:
            raise ValidationException(
                error_type=ValidationErrorType.STAKE_ERROR,
                field_name="min_required_stake",
                attempted_value=profile.min_required_stake,
                message="Minimum required stake cannot be negative.",
            )

        if profile.win_threshold <= profile.loss_threshold:
            raise ValidationException(
                error_type=ValidationErrorType.LIMIT_ERROR,
                field_name="win_threshold",
                attempted_value=profile.win_threshold,
                message="Win threshold must be greater than loss threshold.",
            )

        if enforce_threshold_position:
            if profile.win_threshold <= profile.current_stake:
                raise ValidationException(
                    error_type=ValidationErrorType.LIMIT_ERROR,
                    field_name="win_threshold",
                    attempted_value=profile.win_threshold,
                    message="Win threshold must be above current stake.",
                )
            if profile.loss_threshold >= profile.current_stake:
                raise ValidationException(
                    error_type=ValidationErrorType.LIMIT_ERROR,
                    field_name="loss_threshold",
                    attempted_value=profile.loss_threshold,
                    message="Loss threshold must be below current stake.",
                )

    def _validate_preferences(self, preferences: BettingPreferences) -> None:
        if not preferences.preferred_game_type:
            raise ValidationException(
                error_type=ValidationErrorType.NULL_ERROR,
                field_name="preferred_game_type",
                attempted_value=preferences.preferred_game_type,
                message="Preferred game type is required.",
            )
        if preferences.min_bet <= _ZERO:
            raise ValidationException(
                error_type=ValidationErrorType.BET_ERROR,
                field_name="min_bet",
                attempted_value=preferences.min_bet,
                message="Minimum bet must be greater than zero.",
            )
        if preferences.max_bet < preferences.min_bet:
            raise ValidationException(
                error_type=ValidationErrorType.BET_ERROR,
                field_name="max_bet",
                attempted_value=preferences.max_bet,
                message="Maximum bet must be greater than or equal to minimum bet.",
            )
        if preferences.auto_play_max_games < 0:
            raise ValidationException(
                error_type=ValidationErrorType.RANGE_ERROR,
                field_name="auto_play_max_games",
                attempted_value=preferences.auto_play_max_games,
                message="Auto-play max games cannot be negative.",
            )
        if preferences.session_loss_limit is not None and preferences.session_loss_limit < _ZERO:
            raise ValidationException(
                error_type=ValidationErrorType.RANGE_ERROR,
                field_name="session_loss_limit",
                attempted_value=preferences.session_loss_limit,
                message="Session loss limit cannot be negative.",
            )
        if preferences.session_win_target is not None and preferences.session_win_target <= _ZERO:
            raise ValidationException(
                error_type=ValidationErrorType.RANGE_ERROR,
                field_name="session_win_target",
                attempted_value=preferences.session_win_target,
                message="Session win target must be greater than zero.",
            )

    def _validate_update_fields(
        self,
        updates: Mapping[str, Any],
        allowed_fields: set[str],
        section: str,
    ) -> None:
        unknown_fields = set(updates).difference(allowed_fields)
        if unknown_fields:
            raise ValidationException(
                error_type=ValidationErrorType.RANGE_ERROR,
                field_name=f"{section}_updates",
                attempted_value=sorted(unknown_fields),
                message="One or more update fields are not allowed.",
            )

    def _normalize_profile(self, profile: GamblerProfile) -> GamblerProfile:
        return GamblerProfile(
            gambler_id=profile.gambler_id,
            username=profile.username.strip(),
            full_name=profile.full_name.strip(),
            email=profile.email.strip().lower(),
            initial_stake=_to_money(profile.initial_stake, "initial_stake"),
            current_stake=_to_money(profile.current_stake, "current_stake"),
            win_threshold=_to_money(profile.win_threshold, "win_threshold"),
            loss_threshold=_to_money(profile.loss_threshold, "loss_threshold"),
            min_required_stake=_to_money(
                profile.min_required_stake,
                "min_required_stake",
            ),
            is_active=bool(profile.is_active),
            created_at=profile.created_at,
            updated_at=profile.updated_at,
        )

    def _normalize_preferences(self, preferences: BettingPreferences) -> BettingPreferences:
        return BettingPreferences(
            profile_id=preferences.profile_id,
            min_bet=_to_money(preferences.min_bet, "min_bet"),
            max_bet=_to_money(preferences.max_bet, "max_bet"),
            preferred_game_type=preferences.preferred_game_type.strip(),
            auto_play_enabled=bool(preferences.auto_play_enabled),
            auto_play_max_games=int(preferences.auto_play_max_games),
            session_loss_limit=(
                None
                if preferences.session_loss_limit is None
                else _to_money(preferences.session_loss_limit, "session_loss_limit")
            ),
            session_win_target=(
                None
                if preferences.session_win_target is None
                else _to_money(preferences.session_win_target, "session_win_target")
            ),
            updated_at=preferences.updated_at,
        )

    def _profile_from_mapping(self, mapping: Mapping[str, Any]) -> GamblerProfile:
        normalized = GamblerProfile(
            gambler_id=(
                int(mapping["gambler_id"])
                if mapping.get("gambler_id") is not None
                else None
            ),
            username=str(mapping["username"]),
            full_name=str(mapping["full_name"]),
            email=str(mapping["email"]),
            initial_stake=_to_money(mapping["initial_stake"], "initial_stake"),
            current_stake=_to_money(mapping["current_stake"], "current_stake"),
            win_threshold=_to_money(mapping["win_threshold"], "win_threshold"),
            loss_threshold=_to_money(mapping["loss_threshold"], "loss_threshold"),
            min_required_stake=_to_money(
                mapping.get("min_required_stake", _ZERO),
                "min_required_stake",
            ),
            is_active=bool(mapping.get("is_active", True)),
            created_at=mapping.get("created_at"),
            updated_at=mapping.get("updated_at"),
        )
        return self._normalize_profile(normalized)

    def _preferences_from_mapping(self, mapping: Mapping[str, Any]) -> BettingPreferences:
        normalized = BettingPreferences(
            profile_id=(
                int(mapping["gambler_id"]) if mapping.get("gambler_id") is not None else None
            ),
            min_bet=_to_money(mapping["min_bet"], "min_bet"),
            max_bet=_to_money(mapping["max_bet"], "max_bet"),
            preferred_game_type=str(mapping["preferred_game_type"]),
            auto_play_enabled=bool(mapping.get("auto_play_enabled", False)),
            auto_play_max_games=int(mapping.get("auto_play_max_games", 0)),
            session_loss_limit=(
                None
                if mapping.get("session_loss_limit") is None
                else _to_money(mapping["session_loss_limit"], "session_loss_limit")
            ),
            session_win_target=(
                None
                if mapping.get("session_win_target") is None
                else _to_money(mapping["session_win_target"], "session_win_target")
            ),
            updated_at=mapping.get("updated_at"),
        )
        return self._normalize_preferences(normalized)

    @staticmethod
    def _profile_to_db_values(profile: GamblerProfile) -> dict[str, Any]:
        return {
            "username": profile.username,
            "full_name": profile.full_name,
            "email": profile.email,
            "win_threshold": profile.win_threshold,
            "loss_threshold": profile.loss_threshold,
            "min_required_stake": profile.min_required_stake,
            "is_active": profile.is_active,
        }

    @staticmethod
    def _preferences_to_db_values(preferences: BettingPreferences) -> dict[str, Any]:
        return {
            "min_bet": preferences.min_bet,
            "max_bet": preferences.max_bet,
            "preferred_game_type": preferences.preferred_game_type,
            "auto_play_enabled": preferences.auto_play_enabled,
            "auto_play_max_games": preferences.auto_play_max_games,
            "session_loss_limit": preferences.session_loss_limit,
            "session_win_target": preferences.session_win_target,
        }

    @staticmethod
    def _execute_update(
        *,
        cursor: Any,
        table_name: str,
        id_column: str,
        id_value: int,
        updates: Mapping[str, Any],
        append_updated_at: bool,
    ) -> None:
        if not updates and not append_updated_at:
            return

        set_clauses: list[str] = []
        values: list[Any] = []

        for column, value in updates.items():
            set_clauses.append(f"{column} = %s")
            values.append(value)

        if append_updated_at:
            set_clauses.append("updated_at = CURRENT_TIMESTAMP")

        query = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE {id_column} = %s"
        values.append(id_value)
        cursor.execute(query, tuple(values))

    @staticmethod
    def _fetch_gambler_row(
        cursor: Any,
        gambler_id: int,
        *,
        for_update: bool = False,
    ) -> Mapping[str, Any] | None:
        query = """
        SELECT
            gambler_id,
            username,
            full_name,
            email,
            is_active,
            initial_stake,
            current_stake,
            win_threshold,
            loss_threshold,
            min_required_stake,
            created_at,
            updated_at
        FROM GAMBLERS
        WHERE gambler_id = %s
        """
        if for_update:
            query += " FOR UPDATE"

        cursor.execute(query, (gambler_id,))
        return cursor.fetchone()

    @staticmethod
    def _fetch_preferences_row(
        cursor: Any,
        gambler_id: int,
        *,
        for_update: bool = False,
    ) -> Mapping[str, Any] | None:
        query = """
        SELECT
            gambler_id,
            min_bet,
            max_bet,
            preferred_game_type,
            auto_play_enabled,
            auto_play_max_games,
            session_loss_limit,
            session_win_target,
            updated_at
        FROM BETTING_PREFERENCES
        WHERE gambler_id = %s
        """
        if for_update:
            query += " FOR UPDATE"

        cursor.execute(query, (gambler_id,))
        return cursor.fetchone()

    @staticmethod
    def _transaction_ref(prefix: str, gambler_id: int) -> str:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
        return f"{prefix}-{gambler_id}-{timestamp}"
