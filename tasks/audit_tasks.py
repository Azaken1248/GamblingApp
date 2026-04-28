from __future__ import annotations

from typing import Any, Mapping

from config.database import Database
from . import app, settings


def _trim(value: str, max_length: int) -> str:
    return value[:max_length] if len(value) > max_length else value


def _normalize_issue(issue: Mapping[str, Any]) -> dict[str, Any]:
    attempted_value = issue.get("attempted_value")
    attempted_value_text = None if attempted_value is None else _trim(str(attempted_value), 255)

    return {
        "severity": str(issue.get("severity", "INFO")),
        "error_type": issue.get("error_type"),
        "field_name": issue.get("field_name"),
        "attempted_value": attempted_value_text,
        "message": _trim(str(issue.get("message", "Validation event.")), 512),
        "user_message": _trim(str(issue.get("user_message", "Validation event.")), 512),
        "is_recoverable": bool(issue.get("is_recoverable", True)),
    }


def _insert_validation_event(
    *,
    cursor: Any,
    operation_name: str,
    service_name: str,
    method_name: str,
    severity: str,
    error_type: str | None,
    field_name: str | None,
    attempted_value: str | None,
    message: str,
    user_message: str,
    is_recoverable: bool,
    context_json: str,
) -> None:
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
            severity,
            error_type,
            field_name,
            attempted_value,
            message,
            user_message,
            is_recoverable,
            context_json,
        ),
    )


@app.task(
    name="tasks.audit_tasks.persist_validation_events",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_jitter=True,
    max_retries=5,
)
def persist_validation_events(self, audit_payload: Mapping[str, Any]) -> int:
    operation_name = str(audit_payload.get("operation_name", "UNKNOWN"))
    service_name = str(audit_payload.get("service_name", "UNKNOWN"))
    method_name = str(audit_payload.get("method_name", "UNKNOWN"))
    context_json = str(audit_payload.get("context_json", "{}"))
    issues = list(audit_payload.get("issues", []))

    database = Database(settings=settings)
    inserted_count = 0

    with database.session() as (connection, cursor):
        if issues:
            for issue in issues:
                normalized_issue = _normalize_issue(issue)
                _insert_validation_event(
                    cursor=cursor,
                    operation_name=operation_name,
                    service_name=service_name,
                    method_name=method_name,
                    severity=normalized_issue["severity"],
                    error_type=normalized_issue["error_type"],
                    field_name=normalized_issue["field_name"],
                    attempted_value=normalized_issue["attempted_value"],
                    message=normalized_issue["message"],
                    user_message=normalized_issue["user_message"],
                    is_recoverable=normalized_issue["is_recoverable"],
                    context_json=context_json,
                )
                inserted_count += 1
        else:
            _insert_validation_event(
                cursor=cursor,
                operation_name=operation_name,
                service_name=service_name,
                method_name=method_name,
                severity="INFO",
                error_type=None,
                field_name=None,
                attempted_value=None,
                message="Validation passed.",
                user_message="Validation passed.",
                is_recoverable=True,
                context_json=context_json,
            )
            inserted_count = 1

        connection.commit()

    return inserted_count