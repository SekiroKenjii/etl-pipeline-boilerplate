"""Every session keeps its setup (isolation level, time zone) whatever the
mix of autocommit reads and transactions on its pooled connection."""

from __future__ import annotations

from collections.abc import Callable, Generator
import contextlib

import pytest
from sqlalchemy import exc as sa_exc
import sqlalchemy as sa

from core import contract
from infra.database import relational
from tests.integrations.databases import conftest

# What a session of each database reports: its isolation level, and its time
# zone where it has one.
_PROBES = {
    "postgresql": (
        "SELECT current_setting('transaction_isolation'),"
        " current_setting('TimeZone')"
    ),
    "mysql": "SELECT @@transaction_isolation, @@session.time_zone",
    "mssql": (
        "SELECT CASE transaction_isolation_level WHEN 1 THEN 'READ UNCOMMITTED'"
        " WHEN 2 THEN 'READ COMMITTED' WHEN 3 THEN 'REPEATABLE READ'"
        " WHEN 4 THEN 'SERIALIZABLE' WHEN 5 THEN 'SNAPSHOT' END, NULL"
        " FROM sys.dm_exec_sessions WHERE session_id = @@SPID"
    ),
    "oracle": "SELECT NULL, SESSIONTIMEZONE FROM dual",
}
_EXPECTED = {
    "postgresql": ("read committed", "UTC"),
    "mysql": ("READ-COMMITTED", "+00:00"),
    "mssql": ("READ COMMITTED", None),
    "oracle": (None, "+00:00"),
}
_SERIALIZABLE = {
    "postgresql": "serializable",
    "mysql": "SERIALIZABLE",
    "mssql": "SERIALIZABLE",
}

type _Opening = Callable[
    [str], contextlib.AbstractContextManager[sa.Connection]
]


@pytest.fixture
def server(backend: conftest.Backend) -> conftest.Backend:
    if backend.name not in _PROBES:
        pytest.skip(f"{backend.name} has no session to set up")
    return backend


@pytest.fixture
def one_connection(
    server: conftest.Backend,
) -> Generator[relational.Database]:
    """A database whose pool holds one connection, reused by every use."""
    db = relational.Database(server.url, pool_size=1, max_overflow=0)
    yield db
    db.close()


def _session(db: relational.Database, opening: _Opening) -> tuple[str, ...]:
    with opening("probing the session") as connection:
        row = connection.exec_driver_sql(_PROBES[db.name]).one()
    return tuple(row)


def test_transactions_after_autocommit_reads_keep_the_session_setup(
    server: conftest.Backend, one_connection: relational.Database
) -> None:
    db = one_connection
    seen = []
    for _ in range(3):
        seen.append(_session(db, db.begin))
        seen.append(_session(db, db.connect))

    transactions = seen[::2]
    assert transactions == [_EXPECTED[server.name]] * 3
    zones = {zone for _, zone in seen}
    assert zones == {_EXPECTED[server.name][1]}


def test_a_failed_transaction_keeps_the_session_setup(
    server: conftest.Backend, one_connection: relational.Database
) -> None:
    db = one_connection

    def query_a_missing_table() -> None:
        with db.begin("failing") as connection:
            connection.exec_driver_sql("SELECT * FROM etl_no_such_table")

    with pytest.raises(contract.FatalError):
        query_a_missing_table()
    _session(db, db.connect)

    assert _session(db, db.begin) == _EXPECTED[server.name]


def test_a_new_connection_after_a_lost_one_is_set_up(
    server: conftest.Backend, one_connection: relational.Database
) -> None:
    db = one_connection
    _session(db, db.connect)
    with db.engine.connect() as connection:
        connection.invalidate()

    assert _session(db, db.begin) == _EXPECTED[server.name]
    _session(db, db.connect)
    assert _session(db, db.begin) == _EXPECTED[server.name]


def test_the_isolation_level_of_the_settings_wins(
    server: conftest.Backend,
) -> None:
    if server.name not in _SERIALIZABLE:
        pytest.skip(f"{server.name}: the isolation level is not probed")
    db = relational.Database(
        server.url, pool_size=1, max_overflow=0, isolation_level="SERIALIZABLE"
    )
    try:
        first = _session(db, db.begin)
        _session(db, db.connect)
        second = _session(db, db.begin)
    finally:
        db.close()

    expected = (_SERIALIZABLE[server.name], _EXPECTED[server.name][1])
    assert [first, second] == [expected, expected]


def test_sessions_with_english_messages_have_no_warning(
    backend: conftest.Backend,
) -> None:
    with backend.db.connect("checking") as connection:
        assert not backend.db.dialect.session_warnings(connection)


@pytest.mark.parametrize("locale", ["C", "C.utf8"])
def test_postgres_messages_in_c_have_no_warning(
    backend: conftest.Backend, locale: str
) -> None:
    if backend.name != "postgresql":
        pytest.skip("lc_messages is Postgres'")
    with backend.db.connect("checking") as connection:
        connection.execute(sa.text(f"SET lc_messages = '{locale}'"))
        found = backend.db.dialect.session_warnings(connection)
        connection.execute(sa.text("RESET lc_messages"))

    assert not found


def test_postgres_messages_in_german_are_warned_of(
    backend: conftest.Backend,
) -> None:
    if backend.name != "postgresql":
        pytest.skip("lc_messages is Postgres'")
    with backend.db.connect("checking") as connection:
        try:
            connection.execute(sa.text("SET lc_messages = 'de_DE.utf8'"))
        except sa_exc.DBAPIError as exc:
            pytest.skip(f"the server has no de_DE.utf8 locale: {exc.orig}")
        found = backend.db.dialect.session_warnings(connection)
        connection.execute(sa.text("RESET lc_messages"))

    assert len(found) == 1
    assert "'de_DE.utf8'" in found[0]
