"""Which database errors are worth a retry, and what errors may tell.

The tables of each database are checked here, without it; the errors that can
be provoked are checked on the databases themselves
(tests/integrations/databases/test_errors.py).
"""

from __future__ import annotations

import contextlib
import pathlib
import sqlite3
import traceback
from typing import Any

from mssql_python import exceptions as mssql_errors
from psycopg import errors as pg_errors
import pytest
from sqlalchemy import event as sa_event
from sqlalchemy import exc as sa_exc
import sqlalchemy as sa
from sqlalchemy.dialects import mssql as sa_mssql

from core import contract
from infra.database import relational
from infra.database.relational import database
from infra.database.relational import dialects

MSSQL = dialects.MssqlDialect()
ORACLE = dialects.OracleDialect()
MYSQL = dialects.MysqlDialect()
POSTGRES = dialects.PostgresDialect()
GENERIC = dialects.Dialect()


class _OracleError:
    """What python-oracledb puts first in the arguments of its errors."""

    def __init__(self, full_code: str, isrecoverable: bool = False) -> None:
        self.full_code = full_code
        self.isrecoverable = isrecoverable


def _oracle(full_code: str, isrecoverable: bool = False) -> Exception:
    return Exception(_OracleError(full_code, isrecoverable))


def _mssql(description: str, message: str) -> Exception:
    return mssql_errors.OperationalError(
        driver_error=description, ddbc_error=message
    )


def _sqlite(message: str, name: str) -> Exception:
    error = sqlite3.OperationalError(message)
    error.sqlite_errorname = name
    return error


OPERATIONAL = sa_exc.OperationalError
PROGRAMMING = sa_exc.ProgrammingError
DATABASE = sa_exc.DatabaseError
INTERFACE = sa_exc.InterfaceError


@pytest.mark.parametrize(
    ("dialect", "kind", "orig", "transient"),
    [
        # Postgres: by SQLSTATE; at connection, by message.
        (POSTGRES, OPERATIONAL, pg_errors.DeadlockDetected(), True),
        (POSTGRES, OPERATIONAL, pg_errors.SerializationFailure(), True),
        (POSTGRES, OPERATIONAL, pg_errors.AdminShutdown(), True),
        (POSTGRES, OPERATIONAL, pg_errors.CannotConnectNow(), True),
        (POSTGRES, OPERATIONAL, pg_errors.TooManyConnections(), True),
        (POSTGRES, OPERATIONAL, pg_errors.LockNotAvailable(), True),
        (POSTGRES, OPERATIONAL, pg_errors.QueryCanceled(), True),
        (POSTGRES, OPERATIONAL, pg_errors.ConnectionFailure(), True),
        (POSTGRES, PROGRAMMING, pg_errors.UndefinedTable(), False),
        (POSTGRES, PROGRAMMING, pg_errors.UndefinedColumn(), False),
        (POSTGRES, PROGRAMMING, pg_errors.InsufficientPrivilege(), False),
        (POSTGRES, OPERATIONAL, pg_errors.InvalidPassword(), False),
        (POSTGRES, OPERATIONAL, pg_errors.InvalidCatalogName(), False),
        (
            POSTGRES,
            sa_exc.DataError,
            pg_errors.StringDataRightTruncation(),
            False,
        ),
        (
            POSTGRES,
            OPERATIONAL,
            Exception(
                "connection failed: FATAL:  password authentication failed for"
                ' user "etl"'
            ),
            False,
        ),
        (
            POSTGRES,
            OPERATIONAL,
            Exception(
                'connection failed: FATAL:  database "crm" does not exist'
            ),
            False,
        ),
        (
            POSTGRES,
            OPERATIONAL,
            Exception("connection failed: Connection refused"),
            True,
        ),
        # SQL Server, mssql-python: by SQLSTATE description, then message.
        (
            MSSQL,
            OPERATIONAL,
            _mssql(
                "Serialization failure",
                "Transaction (Process ID 55) was deadlocked on lock resources",
            ),
            True,
        ),
        (
            MSSQL,
            OPERATIONAL,
            _mssql("General error", "Lock request time out period exceeded."),
            True,
        ),
        (MSSQL, OPERATIONAL, _mssql("Communication link failure", "TCP"), True),
        (MSSQL, OPERATIONAL, _mssql("Timeout expired", "Query timeout"), True),
        (MSSQL, OPERATIONAL, _mssql("Connection timeout expired", "x"), True),
        (
            MSSQL,
            OPERATIONAL,
            _mssql(
                "Invalid authorization specification",
                "Login failed for user 'etl'.",
            ),
            False,
        ),
        (
            MSSQL,
            PROGRAMMING,
            _mssql("Base table or view not found", "Invalid object name 'x'."),
            False,
        ),
        (
            MSSQL,
            OPERATIONAL,
            _mssql("General error", 'Cannot open database "crm"'),
            False,
        ),
        # SQL Server, pyodbc: SQLSTATE first, the error number in the text.
        (
            MSSQL,
            sa_exc.DBAPIError,
            Exception(
                "40001",
                "[40001] [SQL Server]Transaction was deadlocked (1205)"
                " (SQLExecDirectW)",
            ),
            True,
        ),
        (
            MSSQL,
            OPERATIONAL,
            Exception(
                "HY000",
                "[HY000] [SQL Server]Lock request time out period exceeded."
                " (1222) (SQLExecDirectW)",
            ),
            True,
        ),
        (
            MSSQL,
            INTERFACE,
            Exception(
                "28000",
                "[28000] [SQL Server]Login failed for user 'etl'. (18456)"
                " (SQLDriverConnect)",
            ),
            False,
        ),
        # SQL Server, pymssql: the error number first.
        (MSSQL, OPERATIONAL, Exception(1205, b"deadlocked"), True),
        (MSSQL, OPERATIONAL, Exception(18456, b"Login failed"), False),
        (MSSQL, OPERATIONAL, Exception(4060, b"Cannot open database"), False),
        # SQL Server, bulk copy (mssql-python's own connection).
        (
            MSSQL,
            sa_exc.DBAPIError,
            RuntimeError("Sql Error: 1205: Class 13: State 51: deadlocked"),
            True,
        ),
        (
            MSSQL,
            sa_exc.DBAPIError,
            RuntimeError(
                "Failed to retrieve destination metadata: Sql Error: 208:"
                " Class 16: State 0: Invalid object name '##x'."
            ),
            False,
        ),
        (
            MSSQL,
            sa_exc.DBAPIError,
            RuntimeError("Io error: Connection reset by peer (os error 104)"),
            True,
        ),
        (
            MSSQL,
            sa_exc.DBAPIError,
            ValueError("String length (19) exceeds schema size (5)"),
            False,
        ),
        # Oracle: by ORA- and DPY- codes, and python-oracledb's verdict.
        *(
            (ORACLE, DATABASE, _oracle(code), True)
            for code in (
                "ORA-00060",
                "ORA-00054",
                "ORA-03113",
                "ORA-03114",
                "ORA-12541",
                "ORA-12170",
                "ORA-01033",
                "DPY-4011",
                "DPY-6005",
            )
        ),
        (ORACLE, DATABASE, _oracle("ORA-03135", isrecoverable=True), True),
        (ORACLE, DATABASE, _oracle("ORA-99999", isrecoverable=True), True),
        *(
            (ORACLE, DATABASE, _oracle(code), False)
            for code in ("ORA-00942", "ORA-00904", "ORA-01017", "ORA-28000")
        ),
        (ORACLE, DATABASE, _oracle("ORA-00001"), False),
        # MySQL: by error number, first in the arguments (PyMySQL).
        *(
            (MYSQL, OPERATIONAL, Exception(code, "x"), True)
            for code in (1205, 1213, 2003, 2006, 2013, 1040, 4031)
        ),
        *(
            (MYSQL, kind, Exception(code, "x"), False)
            for kind, code in (
                (OPERATIONAL, 1045),
                (OPERATIONAL, 1049),
                (PROGRAMMING, 1146),
                (OPERATIONAL, 1054),
            )
        ),
        # SQLite: by result code; a missing table is not transient.
        (
            GENERIC,
            OPERATIONAL,
            _sqlite("no such table: t", "SQLITE_ERROR"),
            False,
        ),
        (
            GENERIC,
            OPERATIONAL,
            _sqlite("database is locked", "SQLITE_BUSY"),
            True,
        ),
        (
            GENERIC,
            OPERATIONAL,
            _sqlite("database is locked", "SQLITE_BUSY_SNAPSHOT"),
            True,
        ),
        (
            GENERIC,
            OPERATIONAL,
            _sqlite("attempt to write a readonly database", "SQLITE_READONLY"),
            False,
        ),
        # Telling nothing: an operational or interface error is transient.
        (GENERIC, OPERATIONAL, Exception("server went away"), True),
        (GENERIC, INTERFACE, Exception("connection closed"), True),
        (GENERIC, PROGRAMMING, Exception("bad"), False),
    ],
)
def test_errors_are_transient_by_code_not_by_class(
    dialect: dialects.Dialect,
    kind: type[sa_exc.DBAPIError],
    orig: Exception,
    transient: bool,
) -> None:
    error = kind("SELECT 1", None, orig)

    assert dialect.transient(error) is transient


def test_a_lost_connection_is_transient_whatever_the_error() -> None:
    error = sa_exc.ProgrammingError(
        "SELECT 1",
        None,
        pg_errors.UndefinedTable(),
        connection_invalidated=True,
    )

    assert POSTGRES.transient(error)


def test_without_a_dialect_the_driver_tells_the_database() -> None:
    # Only SQL Server's message tells: "General error" tells nothing.
    timeout = mssql_errors.ProgrammingError(
        driver_error="General error",
        ddbc_error="Lock request time out period exceeded.",
    )
    error = sa_exc.ProgrammingError("x", None, timeout)

    assert dialects.is_transient(error)
    assert not dialects.is_transient(error, dialects.Dialect())


@pytest.mark.parametrize(
    "state",
    [
        "01002",
        "08001",
        "08003",
        "08004",
        "08007",
        "08S01",
        "28000",
        "3D000",
        "40001",
        "40003",
        "42000",
        "42S02",
        "42S22",
        "HY008",
        "HYT00",
        "HYT01",
    ],
)
def test_mssql_python_tells_its_sqlstates(state: str) -> None:
    # Breaks if a version of mssql-python renames a description.
    error = mssql_errors.sqlstate_to_exception(state, "")

    assert dialects.sqlstate(error) == state


def _execute(
    connect: contextlib.AbstractContextManager[sa.Connection],
    statement: sa.Executable,
    parameters: list[dict[str, Any]] | None = None,
) -> None:
    """Runs a statement on a connection opened for it."""
    with connect as connection:
        connection.execute(statement, parameters)


def test_a_missing_table_is_fatal() -> None:
    db = relational.Database("sqlite://")
    connect = db.connect("reading")
    statement = sa.text("SELECT * FROM nope")

    try:
        with pytest.raises(contract.FatalError, match="no such table"):
            _execute(connect, statement)
    finally:
        db.engine.dispose()


def test_an_exhausted_pool_is_retryable(tmp_path: pathlib.Path) -> None:
    db = relational.Database(
        f"sqlite:///{tmp_path / 'etl.db'}",
        poolclass=sa.QueuePool,
        pool_size=1,
        max_overflow=0,
        pool_timeout=0.05,
    )
    try:
        with db.connect("holding"):
            with pytest.raises(contract.RetryableError, match="QueuePool"):
                with db.connect("waiting"):
                    pass
    finally:
        db.engine.dispose()


def _text(error: BaseException) -> str:
    """Everything a log of the error shows: the chained causes included."""
    return "".join(traceback.format_exception(error))


def test_errors_never_show_the_parameters_of_a_statement(
    tmp_path: pathlib.Path,
) -> None:
    db = relational.Database(f"sqlite:///{tmp_path / 'etl.db'}")
    table = sa.Table(
        "person",
        sa.MetaData(),
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("email", sa.Unicode(50)),
    )
    table.metadata.create_all(db.engine)
    rows = [
        {"id": 1, "email": "jane.doe@bank.example"},
        {"id": 1, "email": "123-45-6789"},
    ]
    connect = db.begin("inserting")
    statement = table.insert()

    with pytest.raises(contract.FatalError) as caught:
        _execute(connect, statement, rows)

    text = _text(caught.value)
    assert "UNIQUE constraint failed" in text
    assert "jane.doe@bank.example" not in text
    assert "123-45-6789" not in text


class TestStatements:

    def test_sql_server_sees_trailing_blanks(self) -> None:
        table = sa.Table("t", sa.MetaData(), sa.Column("b", sa.Unicode(10)))
        stage = table.alias("s")
        dialect: Any = sa_mssql.dialect()  # type: ignore[no-untyped-call]

        changed = str(
            MSSQL.changed(table.c.b, stage.c.b).compile(dialect=dialect)
        )

        assert "datalength(t.b)" in changed.lower()
        assert "COLLATE Latin1_General_100_BIN2" in changed


class TestBatches:

    def _target(self, mode: contract.WriteMode) -> contract.TableTarget:
        return contract.TableTarget("t", key=("id", "tenant"), mode=mode)

    @pytest.mark.parametrize(
        "mode",
        [
            contract.WriteMode.UPSERT,
            contract.WriteMode.UPDATE,
            contract.WriteMode.INSERT_MISSING,
        ],
    )
    def test_null_in_a_key_is_refused(self, mode: contract.WriteMode) -> None:
        rows: list[contract.Row] = [
            {"id": 1, "tenant": "a", "secret": "jane.doe@bank.example"},
            {"id": 2, "tenant": None, "secret": "jane.doe@bank.example"},
        ]
        writer = relational.RelationalTableWriter("sqlite://")
        target = self._target(mode)

        try:
            with pytest.raises(contract.FatalError) as caught:
                writer.write(target, rows)
        finally:
            writer.db.close()

        assert "Row 1" in str(caught.value)
        assert "['tenant']" in str(caught.value)
        assert "jane.doe" not in str(caught.value)

    def test_plain_inserts_take_null_keys(self) -> None:
        db = relational.Database("sqlite://")
        sa.Table(
            "t",
            sa.MetaData(),
            sa.Column("id", sa.Integer),
            sa.Column("tenant", sa.Unicode(10)),
        ).metadata.create_all(db.engine)
        writer = relational.RelationalTableWriter(db)

        try:
            result = writer.write(
                self._target(contract.WriteMode.INSERT),
                [{"id": 1, "tenant": None}],
            )
        finally:
            db.close()

        assert result.created == 1


def _counting(engine: sa.Engine) -> list[str]:
    """Returns the list the engine's SELECTs are appended to."""
    selects: list[str] = []

    def count(*args: Any) -> None:
        statement: str = args[2]
        if statement.lstrip().upper().startswith("SELECT"):
            selects.append(statement)

    sa_event.listen(engine, "before_cursor_execute", count)
    return selects


def test_nulls_in_the_order_are_looked_for_once_per_reader(
    tmp_path: pathlib.Path,
) -> None:
    db = relational.Database(f"sqlite:///{tmp_path / 'etl.db'}")
    table = sa.Table(
        "event",
        sa.MetaData(),
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("at", sa.Integer, nullable=False),
    )
    table.metadata.create_all(db.engine)
    with db.engine.begin() as connection:
        connection.execute(
            table.insert(), [{"id": i, "at": i} for i in range(9)]
        )
    reader = relational.RelationalTableReader(db, page_size=100)
    selects = _counting(db.engine)

    def nulls_checks() -> int:
        return sum(1 for s in selects if "IS NULL" in s)

    for lower, upper in [(0, 2), (3, 5), (6, 8), (0, 8)]:
        query = contract.TableQuery(
            "event", order_by=("at", "id"), lower=lower, upper=upper
        )
        assert len(list(reader.read(query))) == upper - lower + 1
    assert nulls_checks() == 1
    next_run = relational.RelationalTableReader(db, page_size=100)
    list(next_run.read(contract.TableQuery("event", order_by=("at", "id"))))
    assert nulls_checks() == 2


def test_the_key_registry_is_read_in_large_pages(
    tmp_path: pathlib.Path,
) -> None:
    db = relational.Database(f"sqlite:///{tmp_path / 'etl.db'}")
    relational.create_etl_tables(db)
    registry = relational.RelationalKeyRegistry(db)
    registry.register([
        contract.KeyEntry("customer", f"s{i:05}", (f"n{i}",))
        for i in range(2500)
    ])
    selects = _counting(db.engine)

    entries = list(registry.entries("customer"))

    assert len(entries) == 2500
    assert len(selects) == 1


def test_errors_translate_without_a_dialect() -> None:
    error = sa_exc.OperationalError(
        "SELECT", None, _sqlite("no such table: t", "SQLITE_ERROR")
    )

    with pytest.raises(contract.FatalError, match="no such table"):
        with database.errors("reading"):
            raise error
