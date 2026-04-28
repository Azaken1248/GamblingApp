from __future__ import annotations

import os
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path

from dotenv import load_dotenv


class SettingsError(ValueError):
    """Raised when configuration is missing or invalid."""


@dataclass(frozen=True, slots=True)
class Settings:
    app_name: str
    app_env: str
    app_debug: bool
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    db_charset: str
    db_autocommit: bool
    session_default_win_probability: Decimal
    session_default_max_games: int
    session_default_max_minutes: int
    validation_strict_mode: bool
    min_initial_stake: Decimal
    max_initial_stake: Decimal
    redis_host: str
    redis_port: int
    redis_db: int
    celery_broker_url: str
    celery_result_backend: str


_ENV_LOADED = False


def _load_env_once() -> None:
    global _ENV_LOADED
    if _ENV_LOADED:
        return

    project_root = Path(__file__).resolve().parents[1]
    root_env = project_root / ".env"
    legacy_env = Path(__file__).resolve().parent / ".env"

    if root_env.exists():
        load_dotenv(dotenv_path=root_env, override=False)
    elif legacy_env.exists():
        load_dotenv(dotenv_path=legacy_env, override=False)

    _ENV_LOADED = True


def _required_str(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise SettingsError(f"Missing required environment variable: {name}")
    return value.strip()


def _optional_str(name: str, default: str) -> str:
    value = os.getenv(name)
    return default if value is None or value.strip() == "" else value.strip()


def _bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default

    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False

    raise SettingsError(
        f"Invalid boolean environment value for {name}: {value!r}. "
        "Use true/false."
    )


def _int(name: str, default: int | None = None, *, min_value: int | None = None) -> int:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        if default is None:
            raise SettingsError(f"Missing required integer environment variable: {name}")
        parsed = default
    else:
        try:
            parsed = int(value.strip())
        except ValueError as exc:
            raise SettingsError(
                f"Invalid integer environment value for {name}: {value!r}"
            ) from exc

    if min_value is not None and parsed < min_value:
        raise SettingsError(
            f"Environment variable {name} must be >= {min_value}, got {parsed}."
        )

    return parsed


def _decimal(
    name: str,
    default: Decimal | None = None,
    *,
    min_value: Decimal | None = None,
    max_value: Decimal | None = None,
) -> Decimal:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        if default is None:
            raise SettingsError(f"Missing required decimal environment variable: {name}")
        parsed = default
    else:
        try:
            parsed = Decimal(value.strip())
        except (InvalidOperation, ValueError) as exc:
            raise SettingsError(
                f"Invalid decimal environment value for {name}: {value!r}"
            ) from exc

    if min_value is not None and parsed < min_value:
        raise SettingsError(
            f"Environment variable {name} must be >= {min_value}, got {parsed}."
        )

    if max_value is not None and parsed > max_value:
        raise SettingsError(
            f"Environment variable {name} must be <= {max_value}, got {parsed}."
        )

    return parsed


def load_settings() -> Settings:
    _load_env_once()

    settings = Settings(
        app_name=_optional_str("APP_NAME", "Gambling App"),
        app_env=_optional_str("APP_ENV", "dev"),
        app_debug=_bool("APP_DEBUG", False),
        db_host=_required_str("DB_HOST"),
        db_port=_int("DB_PORT", default=3306, min_value=1),
        db_name=_required_str("DB_NAME"),
        db_user=_required_str("DB_USER"),
        db_password=_required_str("DB_PASSWORD"),
        db_charset=_optional_str("DB_CHARSET", "utf8mb4"),
        db_autocommit=_bool("DB_AUTOCOMMIT", False),
        session_default_win_probability=_decimal(
            "SESSION_DEFAULT_WIN_PROBABILITY",
            default=Decimal("0.50"),
            min_value=Decimal("0.00"),
            max_value=Decimal("1.00"),
        ),
        session_default_max_games=_int("SESSION_DEFAULT_MAX_GAMES", default=100, min_value=1),
        session_default_max_minutes=_int(
            "SESSION_DEFAULT_MAX_MINUTES", default=120, min_value=1
        ),
        validation_strict_mode=_bool("VALIDATION_STRICT_MODE", True),
        min_initial_stake=_decimal("MIN_INITIAL_STAKE", default=Decimal("1.00")),
        max_initial_stake=_decimal("MAX_INITIAL_STAKE", default=Decimal("1000000.00")),
        redis_host=_optional_str("REDIS_HOST", "localhost"),
        redis_port=_int("REDIS_PORT", default=6379, min_value=1),
        redis_db=_int("REDIS_DB", default=0, min_value=0),
        celery_broker_url=_optional_str(
            "CELERY_BROKER_URL",
            "redis://localhost:6379/0",
        ),
        celery_result_backend=_optional_str(
            "CELERY_RESULT_BACKEND",
            "redis://localhost:6379/0",
        ),
    )

    if settings.min_initial_stake > settings.max_initial_stake:
        raise SettingsError(
            "MIN_INITIAL_STAKE cannot be greater than MAX_INITIAL_STAKE."
        )

    return settings
