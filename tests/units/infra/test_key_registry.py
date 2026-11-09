"""The relational key registry's statements, on SQLite: a batch is one
INSERT, with no read before it, whatever the database."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
import pathlib
import sqlite3

import pytest
from sqlalchemy import event as sa_event
from sqlalchemy import exc as sa_exc
import sqlalchemy as sa

from core import contract
from infra.database import relational
from infra.database.relational import dialects

ENTITY = "crm.customer"


@pytest.fixture
def db(tmp_path: pathlib.Path) -> Iterator[relational.Database]:
    database = relational.Database(f"sqlite:///{tmp_path / 'etl.db'}")
    relational.create_etl_tables(database)
    yield database
    database.close()


def _entries(count: int, start: int = 0) -> list[contract.KeyEntry]:
    return [
        contract.KeyEntry(ENTITY, f"k{i}", (str(i),))
        for i in range(start, start + count)
    ]


def _keys(entries: Sequence[contract.KeyEntry]) -> list[str]:
    return [e.surrogate_key for e in entries]


def _statements(
    db: relational.Database, action: Callable[[], None]
) -> list[str]:
    """The first word of each statement `action` runs."""
    seen: list[str] = []

    def record(*args: object) -> None:
        seen.append(str(args[2]).split(None, 1)[0].upper())

    sa_event.listen(db.engine, "before_cursor_execute", record)
    try:
        action()
    finally:
        sa_event.remove(db.engine, "before_cursor_execute", record)
    return seen


def test_a_batch_is_one_insert_and_no_select(db: relational.Database) -> None:
    registry = relational.RelationalKeyRegistry(db)
    registry.register(_entries(3))
    mixed = _entries(5, start=1)  # Two keys known, three new.

    statements = _statements(db, lambda: registry.register(mixed))

    assert statements == ["INSERT"]
    assert registry.known(ENTITY, _keys(_entries(6))) == set(_keys(_entries(6)))
    assert registry.natural_key(ENTITY, "k1") == ("1",)


def test_registering_no_key_runs_no_statement(db: relational.Database) -> None:
    registry = relational.RelationalKeyRegistry(db)

    assert _statements(db, lambda: registry.register([])) == []


def test_a_key_taken_by_another_writer_meanwhile_is_tried_again(
    db: relational.Database,
) -> None:
    """Two writers inserting the same new key at once, where the database
    refuses the second one (SQL Server, Oracle): the batch goes in on the
    next try."""

    class Racing(dialects.SqliteDialect):
        raced = False

        def insert_missing(
            self,
            engine: sa.Engine,
            table: sa.Table,
            rows: Sequence[contract.Row],
            key: Sequence[str],
        ) -> None:
            if not self.raced:
                self.raced = True
                raise sa_exc.IntegrityError(
                    "INSERT",
                    None,
                    sqlite3.IntegrityError(
                        "UNIQUE constraint failed: etl_key_registry.entity,"
                        " etl_key_registry.surrogate_key"
                    ),
                )
            super().insert_missing(engine, table, rows, key)

    racing = Racing()
    db.dialect = racing
    registry = relational.RelationalKeyRegistry(db)

    registry.register(_entries(3))

    assert racing.raced
    assert registry.known(ENTITY, _keys(_entries(3))) == set(_keys(_entries(3)))


def test_a_database_without_a_skipping_insert_stages_the_batch(
    db: relational.Database,
) -> None:
    """The base dialect (any other database) inserts what the staging table
    holds and the target does not, still without reading first."""
    db.dialect = dialects.Dialect()
    registry = relational.RelationalKeyRegistry(db)
    registry.register(_entries(3))

    statements = _statements(db, lambda: registry.register(_entries(5)))

    assert "SELECT" not in statements
    assert statements.count("INSERT") == 2  # The staging table, the target.
    assert registry.known(ENTITY, _keys(_entries(5))) == set(_keys(_entries(5)))
    assert [e.surrogate_key for e in registry.entries(ENTITY)] == _keys(
        _entries(5)
    )
