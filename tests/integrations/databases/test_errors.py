"""Database errors, provoked on each database: transient ones are retryable,
the others fatal, and none shows the rows of a batch.

Killing a session on Oracle takes a privileged account: set
ETL_TEST_ORACLE_ADMIN_URL (e.g. the `system` user) to run that test there.
"""

from __future__ import annotations

import contextlib
import datetime
import os
import threading
import time
import traceback
from typing import Any

import pytest
from sqlalchemy import exc as sa_exc
import sqlalchemy as sa
from sqlalchemy.dialects import mysql
from sqlalchemy.dialects import oracle

from core import contract
from infra.database import relational
from tests.integrations.databases import conftest

# A summer time: 06:00 UTC in Paris, 03:00 UTC at +05:00.
T0 = datetime.datetime(2026, 7, 1, 8, 0, 0)
PII = ("jane.doe@bank.example", "123-45-6789")


def _text(error: BaseException) -> str:
    """Everything a log of the error shows: the chained causes included."""
    return "".join(traceback.format_exception(error))


def _server(backend: conftest.Backend) -> None:
    if backend.name == "sqlite":
        pytest.skip("SQLite has no server")


def _execute(
    connect: contextlib.AbstractContextManager[sa.Connection],
    statement: sa.Executable,
    parameters: list[dict[str, Any]] | None = None,
) -> None:
    """Runs a statement on a connection opened for it."""
    with connect as connection:
        connection.execute(statement, parameters)


def test_a_missing_table_is_fatal(backend: conftest.Backend) -> None:
    connect = backend.db.connect("reading")
    statement = sa.text("SELECT 1 FROM etl_no_such_table")

    with pytest.raises(contract.FatalError, match="^reading: "):
        _execute(connect, statement)


def test_a_bad_login_is_fatal(backend: conftest.Backend) -> None:
    _server(backend)
    # A user that does not exist: failed logins of a real one may lock it.
    url = sa.make_url(backend.url).set(
        username="etl_nobody", password="wrong-password"
    )
    db = relational.Database(url.render_as_string(hide_password=False))

    try:
        with pytest.raises(contract.FatalError, match="^connecting: "):
            with db.connect("connecting"):
                pass
    finally:
        db.engine.dispose()


def test_the_victim_of_a_deadlock_is_retryable(
    backend: conftest.Backend,
) -> None:
    _server(backend)
    table = backend.table(
        "deadlock",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("v", sa.Integer),
    )
    backend.create()
    backend.insert(table, {"id": 1, "v": 0}, {"id": 2, "v": 0})
    both_locked = threading.Barrier(2)
    errors: list[BaseException] = []

    def lock(first: int, second: int) -> None:
        try:
            with backend.db.begin("updating") as connection:
                update = table.update().values(v=first)
                connection.execute(update.where(table.c.id == first))
                both_locked.wait(timeout=10)
                time.sleep(0.2)
                connection.execute(update.where(table.c.id == second))
        except contract.PipelineError as exc:
            errors.append(exc)

    threads = [
        threading.Thread(target=lock, args=(1, 2)),
        threading.Thread(target=lock, args=(2, 1)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(errors) == 1
    assert isinstance(errors[0], contract.RetryableError), errors[0]


# How a session finds its own id, waits, and how another one kills it.
_SESSION = {
    "postgresql": (
        "SELECT pg_backend_pid()",
        "SELECT pg_sleep(5)",
        "SELECT pg_terminate_backend({})",
    ),
    "mssql": ("SELECT @@SPID", "WAITFOR DELAY '00:00:05'", "KILL {}"),
    "mysql": ("SELECT CONNECTION_ID()", "SELECT SLEEP(5)", "KILL {}"),
    "oracle": (
        "SELECT SYS_CONTEXT('USERENV', 'SID') FROM dual",
        "BEGIN DBMS_SESSION.SLEEP(5); END;",
        "",
    ),
}


def _wait_until_killed(backend: conftest.Backend) -> None:
    """Waits in a query until another session kills this one."""
    find, wait, _ = _SESSION[backend.name]
    with backend.db.connect("reading") as victim:
        session = victim.execute(sa.text(find)).scalar()
        killer = threading.Timer(0.5, _kill, (backend, session))
        killer.start()
        try:
            victim.execute(sa.text(wait))
        finally:
            killer.join()


def _kill(backend: conftest.Backend, session: Any) -> None:
    """Kills a session of the database, from another one."""
    if backend.name != "oracle":
        with backend.db.connect("killing") as killer:
            killer.execute(sa.text(_SESSION[backend.name][2].format(session)))
        return
    admin = sa.create_engine(os.environ["ETL_TEST_ORACLE_ADMIN_URL"])
    try:
        with admin.connect() as killer:
            serial = killer.execute(
                sa.text("SELECT serial# FROM v$session WHERE sid = :sid"),
                {"sid": int(session)},
            ).scalar()
            # The killer is told ORA-00031 (marked for kill), with its own
            # connection said to be closed, even when the kill is done.
            with contextlib.suppress(sa_exc.DBAPIError):
                killer.execute(
                    sa.text(
                        f"ALTER SYSTEM KILL SESSION '{session},{serial}'"
                        " IMMEDIATE"
                    )
                )
    finally:
        admin.dispose()


def test_a_connection_killed_in_a_query_is_retryable(
    backend: conftest.Backend,
) -> None:
    # Killed while idle, SQL Server's driver reconnects on its own.
    _server(backend)
    if backend.name == "oracle" and not os.environ.get(
        "ETL_TEST_ORACLE_ADMIN_URL"
    ):
        pytest.skip("set ETL_TEST_ORACLE_ADMIN_URL to kill Oracle sessions")
    probe = sa.select(sa.literal(1))

    with pytest.raises(contract.RetryableError, match="^reading: "):
        _wait_until_killed(backend)

    # The pool replaced the dead connection.
    with backend.db.connect("reading again") as connection:
        assert connection.execute(probe).scalar() == 1


class TestPostgresCancels:
    """A statement cancelled by another session is retried; one past its
    `statement_timeout` is not, it would time out again."""

    @pytest.fixture
    def postgres(self, backend: conftest.Backend) -> conftest.Backend:
        if backend.name != "postgresql":
            pytest.skip("Postgres's statement_timeout and pg_cancel_backend")
        return backend

    def test_a_statement_timeout_is_fatal(
        self, postgres: conftest.Backend
    ) -> None:
        def sleep_past_the_timeout() -> None:
            with postgres.db.connect("reading") as connection:
                connection.exec_driver_sql("SET statement_timeout = '50ms'")
                connection.exec_driver_sql("SELECT pg_sleep(5)")

        with pytest.raises(contract.FatalError, match="statement timeout"):
            sleep_past_the_timeout()

    def test_a_statement_cancelled_by_another_session_is_retryable(
        self, postgres: conftest.Backend
    ) -> None:
        pids: list[int] = []

        def cancel() -> None:
            while not pids:
                time.sleep(0.01)
            time.sleep(0.2)
            with postgres.db.engine.connect() as other:
                other.execute(
                    sa.text("SELECT pg_cancel_backend(:pid)"), {"pid": pids[0]}
                )

        def sleep_until_cancelled() -> None:
            with postgres.db.connect("reading") as connection:
                pids.append(
                    connection.exec_driver_sql(
                        "SELECT pg_backend_pid()"
                    ).scalar_one()
                )
                connection.exec_driver_sql("SELECT pg_sleep(5)")

        canceller = threading.Thread(target=cancel)
        canceller.start()
        try:
            with pytest.raises(contract.RetryableError, match="user request"):
                sleep_until_cancelled()
        finally:
            canceller.join()

    def test_a_lock_timeout_is_retryable_under_a_statement_timeout(
        self, postgres: conftest.Backend
    ) -> None:
        table = postgres.table("locked", sa.Column("id", sa.Integer))
        postgres.create()

        def wait_for_the_lock() -> None:
            with postgres.db.connect("reading") as connection:
                connection.exec_driver_sql("SET statement_timeout = '5s'")
                connection.exec_driver_sql("SET lock_timeout = '50ms'")
                connection.execute(sa.select(table))

        with postgres.db.engine.connect() as holder:
            holder.execute(sa.text(f"LOCK TABLE {table.name}"))
            with pytest.raises(contract.RetryableError):
                wait_for_the_lock()
            holder.rollback()


def test_errors_never_show_the_rows_of_a_batch(
    backend: conftest.Backend,
) -> None:
    table = backend.table(
        "person",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("email", sa.Unicode(50)),
    )
    backend.create()
    connect = backend.db.begin("inserting")
    statement = table.insert()
    rows = [{"id": 1, "email": PII[0]}, {"id": 1, "email": PII[1]}]

    with pytest.raises(contract.FatalError) as caught:
        _execute(connect, statement, rows)

    text = _text(caught.value)
    assert all(value not in text for value in PII), text


def test_a_duplicate_key_message_keeps_the_value_out(
    backend: conftest.Backend,
) -> None:
    """MySQL (1062) and SQL Server (2627) quote the duplicate value."""
    table = backend.table(
        "person",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("email", sa.Unicode(50), unique=True),
        sa.Column("ssn", sa.Unicode(20)),
    )
    backend.create()
    writer = relational.RelationalTableWriter(backend.db)
    target = contract.TableTarget(
        table.name, key=(), mode=contract.WriteMode.INSERT
    )
    writer.write(target, [{"id": 1, "email": PII[0], "ssn": PII[1]}])

    with pytest.raises(contract.FatalError) as caught:
        writer.write(target, [{"id": 2, "email": PII[0], "ssn": PII[1]}])

    message = str(caught.value)
    assert message.startswith(f"writing 1 row(s) to {table.name}: ")
    assert all(value not in message for value in PII), message
    assert "uplicate" in message or "unique" in message.lower(), message


def test_bulk_loads_fail_like_any_statement(backend: conftest.Backend) -> None:
    # Large enough for COPY (Postgres) and bulk copy (SQL Server), which call
    # their drivers directly.
    if backend.name == "sqlite":
        pytest.skip("SQLite does not enforce the length of text")
    table = backend.table(
        "short",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("code", sa.Unicode(5)),
    )
    backend.create()
    rows: list[contract.Row] = [{"id": i, "code": "ok"} for i in range(2500)]
    rows[7] = {"id": 7, "code": PII[0]}
    writer = relational.RelationalTableWriter(backend.db)
    target = contract.TableTarget(table.name, key=("id",))

    with pytest.raises(contract.FatalError) as caught:
        writer.write(target, rows)

    message = str(caught.value)
    assert message.startswith(f"writing 2500 row(s) to {table.name}: ")
    assert PII[0] not in message
    assert backend.rows(table, "id") == []


# The instant of a value, in UTC, whatever the session's time zone.
_UTC = {
    "postgresql": "SELECT stamp AT TIME ZONE 'UTC' FROM {}",
    "oracle": "SELECT SYS_EXTRACT_UTC(stamp) FROM {}",
    "mysql": "SELECT CONVERT_TZ(stamp, @@session.time_zone, '+00:00') FROM {}",
    "mssql": "SELECT CONVERT(datetime2, SWITCHOFFSET(stamp, '+00:00')) FROM {}",
    "sqlite": "SELECT stamp FROM {}",
}


def test_naive_datetimes_are_utc_whatever_the_session(
    backend: conftest.Backend, monkeypatch: pytest.MonkeyPatch
) -> None:
    table = backend.table(
        "moment",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column(
            "stamp",
            sa.DateTime(timezone=True)
            .with_variant(mysql.TIMESTAMP(), "mysql")
            .with_variant(oracle.TIMESTAMP(timezone=True), "oracle"),
        ),
    )
    backend.create()
    # Sessions in another time zone than the server's: the client's
    # environment (Postgres, Oracle), the connection's setup (MySQL).
    monkeypatch.setenv("PGTZ", "Europe/Paris")
    monkeypatch.setenv("ORA_SDTZ", "Europe/Paris")
    options: dict[str, Any] = {}
    if backend.name == "mysql":
        options["connect_args"] = {"init_command": "SET time_zone = '+05:00'"}
    db = relational.Database(backend.url, **options)
    try:
        writer = relational.RelationalTableWriter(db)
        target = contract.TableTarget(table.name, key=("id",))

        writer.write(target, [{"id": 1, "stamp": T0}])
        with db.connect("reading") as connection:
            utc = connection.execute(
                sa.text(_UTC[backend.name].format(table.name))
            ).scalar()
            stored = connection.execute(sa.select(table.c.stamp)).scalar()
    finally:
        db.close()

    assert utc is not None
    if isinstance(utc, str):
        utc = datetime.datetime.fromisoformat(utc)
    assert utc.replace(tzinfo=None) == T0
    if backend.name == "sqlite":
        return
    assert stored is not None
    if stored.tzinfo is not None:
        stored = stored.astimezone(datetime.UTC).replace(tzinfo=None)
    assert stored == T0
