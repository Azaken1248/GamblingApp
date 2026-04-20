from __future__ import annotations

import re
from contextlib import contextmanager
from typing import Any, Iterator

import mysql.connector
from mysql.connector.connection import MySQLConnection
from mysql.connector.cursor import MySQLCursor
from mysql.connector.errors import Error as MySQLError

from config.settings import Settings
from utils.exceptions import DataAccessException


_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class Database:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    @staticmethod
    def _validate_identifier(value: str, field_name: str) -> str:
        if not _IDENTIFIER_PATTERN.match(value):
            raise DataAccessException(
                f"Invalid SQL identifier for {field_name}: {value!r}"
            )
        return value

    def _connection_args(self, *, include_database: bool) -> dict[str, Any]:
        args: dict[str, Any] = {
            "host": self._settings.db_host,
            "port": self._settings.db_port,
            "user": self._settings.db_user,
            "password": self._settings.db_password,
            "charset": self._settings.db_charset,
            "use_unicode": True,
            "autocommit": self._settings.db_autocommit if include_database else True,
        }
        if include_database:
            args["database"] = self._settings.db_name
        return args

    def ensure_database_exists(self) -> None:
        db_name = self._validate_identifier(self._settings.db_name, "DB_NAME")
        charset = self._validate_identifier(self._settings.db_charset, "DB_CHARSET")

        connection = mysql.connector.connect(**self._connection_args(include_database=False))
        cursor = connection.cursor()
        try:
            cursor.execute(
                f"CREATE DATABASE IF NOT EXISTS `{db_name}` CHARACTER SET {charset}"
            )
            connection.commit()
        except MySQLError as exc:
            connection.rollback()
            raise DataAccessException("Failed to create or validate database.") from exc
        finally:
            cursor.close()
            connection.close()

    def get_connection(self) -> MySQLConnection:
        try:
            return mysql.connector.connect(**self._connection_args(include_database=True))
        except MySQLError as exc:
            raise DataAccessException("Failed to open database connection.") from exc

    @contextmanager
    def session(
        self,
        *,
        dictionary: bool = False,
    ) -> Iterator[tuple[MySQLConnection, MySQLCursor]]:
        connection = self.get_connection()
        cursor = connection.cursor(dictionary=dictionary)
        try:
            yield connection, cursor
        except MySQLError as exc:
            connection.rollback()
            raise DataAccessException("Database operation failed.") from exc
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()
            connection.close()
