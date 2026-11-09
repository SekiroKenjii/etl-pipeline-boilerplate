"""What the drivers' errors tell: their codes, SQLSTATEs, duplicate keys.

Whatever the database: which codes are transient is each dialect's
(`dialects`).
"""

from __future__ import annotations

import re

from sqlalchemy import exc as sa_exc

# SQLSTATEs (ISO SQL, ODBC, Postgres) of transient failures: connection
# errors (08xxx), serialization failures and deadlocks, insufficient
# resources (53xxx: too many connections...), lock timeouts, statements
# cancelled by another session (a statement timeout is not:
# `dialects.PostgresDialect`), servers shutting down or starting, ODBC
# timeouts. Any other SQLSTATE is fatal: authorization (28), bad SQL or a
# missing table or column (42), a missing database (3D) or schema (3F), bad
# data (22), a constraint (23)...
_RETRYABLE_STATES = (
    "01002",
    "08",
    "40001",
    "40003",
    "40P01",
    "53",
    "55P03",
    "57014",
    "57P01",
    "57P02",
    "57P03",
    "HYT00",
    "HYT01",
)
# "General error": the SQLSTATE of anything else, which tells nothing.
_UNTOLD_STATES = frozenset({"HY000"})
_SQLSTATE = re.compile(r"^[0-9A-Z]{5}$")
# The native error number in pyodbc messages: `... (1205) (SQLExecDirectW)`.
_ODBC_NATIVE = re.compile(r"\((-?\d+)\) \(SQL\w*\)")
_BULK_NATIVE = re.compile(r"Sql Error: (\d+)")


def state_is_transient(state: str | None) -> bool | None:
    """Whether a SQLSTATE tells a transient failure; None if it tells
    nothing (none, or "general error")."""
    if state is None or state in _UNTOLD_STATES:
        return None
    return state.startswith(_RETRYABLE_STATES)


# mssql-python gives the SQLSTATE of an error only as its description.
_MSSQL_PYTHON_STATES = {
    "Disconnect error": "01002",
    "Client unable to establish connection": "08001",
    "Connection not open": "08003",
    "Server rejected the connection": "08004",
    "Connection failure during transaction": "08007",
    "Communication link failure": "08S01",
    "Invalid authorization specification": "28000",
    "Invalid catalog name": "3D000",
    "Serialization failure": "40001",
    "Statement completion unknown": "40003",
    "Syntax error or access violation": "42000",
    "Base table or view not found": "42S02",
    "Column not found": "42S22",
    "Operation canceled": "HY008",
    "Timeout expired": "HYT00",
    "Connection timeout expired": "HYT01",
}


def error_code(orig: BaseException | None) -> str | None:
    """Returns the database's own code of a driver error, if it has one.

    `ORA-00060` or `DPY-4011` (python-oracledb), `1213` (MySQL drivers,
    pymssql, the number in pyodbc's and bulk copy's messages),
    `SQLITE_BUSY` (sqlite3, without the extended code).
    """
    if orig is None:
        return None
    name = getattr(orig, "sqlite_errorname", None)
    if isinstance(name, str):
        return "_".join(name.split("_")[:2])
    args = getattr(orig, "args", ())
    first = args[0] if args else None
    full_code = getattr(first, "full_code", None)
    if isinstance(full_code, str):
        return full_code
    if isinstance(first, int) and not isinstance(first, bool):
        return str(first)
    found = _ODBC_NATIVE.search(str(orig)) or _BULK_NATIVE.search(str(orig))
    return found.group(1) if found else None


def sqlstate(orig: BaseException | None) -> str | None:
    """Returns the SQLSTATE of a driver error, if it tells it.

    psycopg and PyMySQL give it, pyodbc first in its arguments, mssql-python
    as its description.
    """
    state = getattr(orig, "sqlstate", None)
    if isinstance(state, str) and _SQLSTATE.match(state):
        return state
    args = getattr(orig, "args", ())
    if args and isinstance(args[0], str) and _SQLSTATE.match(args[0]):
        return args[0]
    description = getattr(orig, "driver_error", None)
    if isinstance(description, str):
        return _MSSQL_PYTHON_STATES.get(description)
    return None


# How each database reports a duplicate key: SQLSTATE 23505 (Postgres),
# errors 2627 and 2601 (SQL Server, whose drivers do not all give the number),
# ORA-00001, MySQL 1062, SQLite.
_UNIQUE_VIOLATIONS = (
    "23505",
    "2627",
    "2601",
    "Cannot insert duplicate key",
    "ORA-00001",
    "1062",
    "Duplicate entry",
    "UNIQUE constraint failed",
)


# The duplicate value quoted by MySQL (1062: `Duplicate entry 'v' for key
# 'k'`) and SQL Server (2627, 2601: `The duplicate key value is (v).`): a
# value of the batch.
_QUOTED_DUPLICATES = (
    (
        re.compile(r"Duplicate entry '.*' for key"),
        "Duplicate entry '...' for key",
    ),
    (
        re.compile(r"The duplicate key value is \(.*\)"),
        "The duplicate key value is (...)",
    ),
)


def without_duplicate_values(message: str) -> str:
    """Returns the message of a driver error without the duplicate key value
    some databases quote in it."""
    for quoted, hidden in _QUOTED_DUPLICATES:
        message = quoted.sub(hidden, message)
    return message


def is_unique_violation(exc: BaseException | None) -> bool:
    """Whether a database error is a duplicate key, whatever the database.

    A writer racing another one on the same key gets one: trying again then
    finds the row, and updates it. Any other constraint (a foreign key, NOT
    NULL) is not.
    """
    if not isinstance(exc, sa_exc.IntegrityError):
        return False
    if getattr(exc.orig, "sqlstate", None) == "23505":
        return True
    text = str(exc.orig)
    return any(marker in text for marker in _UNIQUE_VIOLATIONS)
