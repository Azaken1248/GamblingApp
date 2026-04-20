from typing import TYPE_CHECKING, Any

from utils.exceptions import (
	DataAccessException,
	NotFoundException,
	ValidationIssue,
	ValidationErrorType,
	ValidationException,
	ValidationResult,
	ValidationSeverity,
)

if TYPE_CHECKING:
	from utils.input_validator import (
		InputValidator,
		get_last_validation_result,
		validation_guard,
	)

__all__ = [
	"DataAccessException",
	"InputValidator",
	"NotFoundException",
	"ValidationIssue",
	"ValidationErrorType",
	"ValidationException",
	"ValidationResult",
	"ValidationSeverity",
	"get_last_validation_result",
	"validation_guard",
]


def __getattr__(name: str) -> Any:
	if name in {"InputValidator", "get_last_validation_result", "validation_guard"}:
		from utils.input_validator import (
			InputValidator,
			get_last_validation_result,
			validation_guard,
		)
		lookup = {
			"InputValidator": InputValidator,
			"get_last_validation_result": get_last_validation_result,
			"validation_guard": validation_guard,
		}
		return lookup[name]

	raise AttributeError(f"module 'utils' has no attribute {name!r}")