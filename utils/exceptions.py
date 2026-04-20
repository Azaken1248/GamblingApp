from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class ValidationErrorType(str, Enum):
    STAKE_ERROR = "STAKE_ERROR"
    BET_ERROR = "BET_ERROR"
    LIMIT_ERROR = "LIMIT_ERROR"
    PROBABILITY_ERROR = "PROBABILITY_ERROR"
    NUMERIC_ERROR = "NUMERIC_ERROR"
    RANGE_ERROR = "RANGE_ERROR"
    NULL_ERROR = "NULL_ERROR"


@dataclass(slots=True)
class ValidationException(Exception):
    error_type: ValidationErrorType
    field_name: str
    attempted_value: Any
    message: str

    def __str__(self) -> str:
        return (
            f"{self.error_type.value} on '{self.field_name}' "
            f"(value={self.attempted_value!r}): {self.message}"
        )


class NotFoundException(Exception):
    """Raised when a requested entity cannot be found."""


class DataAccessException(Exception):
    """Raised when database access fails."""
