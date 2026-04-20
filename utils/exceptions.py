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


class ValidationSeverity(str, Enum):
    WARNING = "WARNING"
    ERROR = "ERROR"


@dataclass(slots=True)
class ValidationException(Exception):
    error_type: ValidationErrorType
    field_name: str
    attempted_value: Any
    message: str
    severity: ValidationSeverity = ValidationSeverity.ERROR
    user_message: str | None = None
    is_recoverable: bool = True

    def __str__(self) -> str:
        feedback = self.user_message if self.user_message is not None else self.message
        return (
            f"{self.severity.value} {self.error_type.value} on '{self.field_name}' "
            f"(value={self.attempted_value!r}): {feedback}"
        )

    def to_issue(self) -> ValidationIssue:
        return ValidationIssue(
            severity=self.severity,
            error_type=self.error_type,
            field_name=self.field_name,
            attempted_value=self.attempted_value,
            message=self.message,
            user_message=(self.user_message if self.user_message is not None else self.message),
            is_recoverable=self.is_recoverable,
        )


@dataclass(slots=True, frozen=True)
class ValidationIssue:
    severity: ValidationSeverity
    error_type: ValidationErrorType
    field_name: str
    attempted_value: Any
    message: str
    user_message: str
    is_recoverable: bool = True

    def to_exception(self) -> ValidationException:
        return ValidationException(
            error_type=self.error_type,
            field_name=self.field_name,
            attempted_value=self.attempted_value,
            message=self.message,
            severity=self.severity,
            user_message=self.user_message,
            is_recoverable=self.is_recoverable,
        )


@dataclass(slots=True, frozen=True)
class ValidationResult:
    operation_name: str
    issues: tuple[ValidationIssue, ...] = ()

    @property
    def is_valid(self) -> bool:
        return len(self.errors) == 0

    @property
    def has_warnings(self) -> bool:
        return len(self.warnings) > 0

    @property
    def errors(self) -> tuple[ValidationIssue, ...]:
        return tuple(
            issue for issue in self.issues if issue.severity == ValidationSeverity.ERROR
        )

    @property
    def warnings(self) -> tuple[ValidationIssue, ...]:
        return tuple(
            issue for issue in self.issues if issue.severity == ValidationSeverity.WARNING
        )

    @property
    def first_error(self) -> ValidationIssue | None:
        return self.errors[0] if self.errors else None

    def feedback_messages(self, *, include_warnings: bool = True) -> tuple[str, ...]:
        if include_warnings:
            target = self.issues
        else:
            target = self.errors

        return tuple(issue.user_message for issue in target)


class NotFoundException(Exception):
    """Raised when a requested entity cannot be found."""


class DataAccessException(Exception):
    """Raised when database access fails."""
