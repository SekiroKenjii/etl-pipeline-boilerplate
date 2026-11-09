"""Writes at once into one table, and the key registry's bulk inserts, on
every database."""

from __future__ import annotations

from collections.abc import Callable
import concurrent.futures
from typing import Any
import uuid

import pytest
from sqlalchemy import event as sa_event
import sqlalchemy as sa

from core import contract
from infra.database import relational
from infra.database.relational import bookkeeping
from infra.database.relational import database
from tests.integrations.databases import conftest

ENTITY = "crm.customer"


def _key(writer: int, batch: int, row: int) -> str:
    # UUIDs spread every batch over the whole key index, as uuid5 keys do.
    return str(uuid.uuid5(uuid.NAMESPACE_OID, f"{writer}/{batch}/{row}"))


def test_batches_of_disjoint_keys_written_at_once_never_deadlock(
    backend: conftest.Backend,
) -> None:
    table = backend.table(
        "customer",
        sa.Column("customer_key", sa.Unicode(36), primary_key=True),
        sa.Column("name", sa.Unicode(50), nullable=False),
    )
    backend.create()
    relational.create_etl_tables(backend.db, prefix=backend.prefix)
    registry = relational.RelationalKeyRegistry(
        backend.db, prefix=backend.prefix
    )
    target = contract.TableTarget(table.name, key=("customer_key",))
    writer = relational.RelationalTableWriter(backend.db)
    writers, batches, rows = 4, 12, 1000

    def write(index: int, name: str) -> tuple[int, int]:
        created = updated = 0
        for batch in range(batches):
            keys = [_key(index, batch, row) for row in range(rows)]
            # An autocommit read on the pooled connection first, as a run
            # does (state, key registry): its reset must not undo the
            # isolation level of the next merge.
            registry.known(ENTITY, keys[:1])
            result = writer.write(
                target,
                [{"customer_key": key, "name": name} for key in keys],
            )
            created += result.created
            updated += result.updated
        return created, updated

    with concurrent.futures.ThreadPoolExecutor(writers) as threads:
        inserted = list(threads.map(write, range(writers), ["a"] * writers))
        changed = list(threads.map(write, range(writers), ["b"] * writers))

    total = writers * batches * rows
    assert [sum(c) for c in zip(*inserted)] == [total, 0]
    assert [sum(c) for c in zip(*changed)] == [0, total]
    assert {
        name for _, name in backend.rows(table, "customer_key", "name")
    } == {"b"}


def _rows_per_insert(backend: conftest.Backend) -> int:
    dialect = backend.db.engine.dialect
    columns = len(bookkeeping.etl_tables(None, backend.prefix).key_registry.c)
    return min(
        dialect.insertmanyvalues_page_size,
        dialect.insertmanyvalues_max_parameters // columns,
    )


def _entries(count: int, start: int = 0) -> list[contract.KeyEntry]:
    return [
        contract.KeyEntry(ENTITY, _key(0, 0, i), (str(i),))
        for i in range(start, start + count)
    ]


@pytest.fixture
def registry(backend: conftest.Backend) -> relational.RelationalKeyRegistry:
    relational.create_etl_tables(backend.db, prefix=backend.prefix)
    return relational.RelationalKeyRegistry(backend.db, prefix=backend.prefix)


# INSERT statements a batch of keys takes, by database: one where the
# database skips the keys there (MySQL in the driver's executemany, Postgres
# with the batch as arrays); Oracle fills its staging table in one
# executemany, then one INSERT ... SELECT. SQLite sends one multi-row
# INSERT ... RETURNING a page of `_rows_per_insert` rows; so does SQL Server
# for the first batch of a table, which the online choice of the staging
# sends as parameters (the second is bulk copied, which no cursor runs),
# then one INSERT ... SELECT.
_INSERTS = {"postgresql": 1, "mysql": 1, "oracle": 2}


def _expected_inserts(backend: conftest.Backend, rows: int) -> int:
    if backend.name == "sqlite":
        return -(-rows // _rows_per_insert(backend))
    if backend.name == "mssql":
        return -(-rows // _rows_per_insert(backend)) + 1
    return _INSERTS[backend.name]


def _registry_table(backend: conftest.Backend) -> sa.Table:
    return bookkeeping.etl_tables(None, backend.prefix).key_registry


def _recording(
    backend: conftest.Backend, action: Callable[[], None]
) -> list[str]:
    """The statements `action` runs on the backend."""
    statements: list[str] = []

    def record(*args: Any) -> None:
        statement: str = args[2]
        statements.append(statement.lstrip())

    sa_event.listen(backend.db.engine, "before_cursor_execute", record)
    try:
        action()
    finally:
        sa_event.remove(backend.db.engine, "before_cursor_execute", record)
    return statements


class TestKeyRegistryInserts:

    def test_a_batch_is_inserted_without_a_read_first(
        self,
        backend: conftest.Backend,
        registry: relational.RelationalKeyRegistry,
    ) -> None:
        entries = _entries(_rows_per_insert(backend) * 2 + 7)
        table = _registry_table(backend).name

        statements = _recording(backend, lambda: registry.register(entries))

        inserts = [s for s in statements if s.upper().startswith("INSERT")]
        # No key is read before writing (SQL Server reads its catalog once,
        # for the plan of the merge).
        reads = [
            s
            for s in statements
            if s.upper().startswith("SELECT") and table in s
        ]
        assert (len(inserts), reads) == (
            _expected_inserts(backend, len(entries)),
            [],
        )
        keys = [e.surrogate_key for e in entries]
        assert registry.known(ENTITY, keys) == set(keys)
        assert registry.natural_key(ENTITY, keys[-1]) == entries[-1].natural_key

    def test_keys_registered_already_are_kept(
        self,
        backend: conftest.Backend,
        registry: relational.RelationalKeyRegistry,
    ) -> None:
        per_insert = _rows_per_insert(backend)
        # Across the end of the first insert and into the second.
        earlier = [
            contract.KeyEntry(e.entity, e.surrogate_key, ("earlier",))
            for e in _entries(10, start=per_insert - 5)
        ]
        registry.register(earlier)
        entries = _entries(per_insert * 2)

        registry.register(entries)
        registry.register(entries)

        keys = [e.surrogate_key for e in entries]
        assert registry.known(ENTITY, keys) == set(keys)
        assert registry.natural_key(ENTITY, earlier[0].surrogate_key) == (
            "earlier",
        )

    def test_overlapping_batches_keep_the_first_natural_key(
        self,
        backend: conftest.Backend,
        registry: relational.RelationalKeyRegistry,
    ) -> None:
        first = _entries(700)
        second = [
            contract.KeyEntry(ENTITY, e.surrogate_key, ("second",))
            for e in _entries(700, start=350)
        ]

        registry.register(first)
        registry.register(second)
        registry.register(first)

        keys = [e.surrogate_key for e in _entries(1050)]
        assert (
            len(backend.rows(_registry_table(backend), "surrogate_key")) == 1050
        )
        assert registry.natural_key(ENTITY, _key(0, 0, 400)) == ("400",)
        assert registry.natural_key(ENTITY, _key(0, 0, 1000)) == ("second",)
        assert (
            {e.surrogate_key for e in registry.entries(ENTITY)}
            == (registry.known(ENTITY, keys))
            == set(keys)
        )

    def test_a_concurrent_writer_taking_keys_first(
        self,
        backend: conftest.Backend,
        registry: relational.RelationalKeyRegistry,
    ) -> None:
        """Another writer registers some keys of the batch just before this
        one writes it: every key once, the first writer's natural key kept."""
        per_insert = _rows_per_insert(backend)
        entries = _entries(per_insert + 50)
        taken = [
            contract.KeyEntry(e.entity, e.surrogate_key, ("taken",))
            for e in entries[per_insert : per_insert + 10]
        ]
        table = _registry_table(backend)
        raced: list[str] = []

        def race(*args: Any) -> None:
            statement: str = args[2]
            if (
                not raced
                and statement.lstrip().upper().startswith("INSERT")
                and table.name in statement
            ):
                raced.append(statement)
                relational.RelationalKeyRegistry(
                    backend.db, prefix=backend.prefix
                ).register(taken)

        sa_event.listen(backend.db.engine, "before_cursor_execute", race)
        try:
            registry.register(entries)
        finally:
            sa_event.remove(backend.db.engine, "before_cursor_execute", race)

        keys = [e.surrogate_key for e in entries]
        assert len(raced) == 1
        assert registry.known(ENTITY, keys) == set(keys)
        assert registry.natural_key(ENTITY, taken[0].surrogate_key) == (
            "taken",
        )
        assert len(backend.rows(table, "surrogate_key")) == len(entries)

    def test_the_same_keys_registered_by_four_threads_at_once(
        self,
        backend: conftest.Backend,
        registry: relational.RelationalKeyRegistry,
    ) -> None:
        entries = _entries(1000)
        keys = [e.surrogate_key for e in entries]

        with concurrent.futures.ThreadPoolExecutor(4) as threads:
            list(threads.map(lambda _: registry.register(entries), range(4)))

        assert (
            len(backend.rows(_registry_table(backend), "surrogate_key")) == 1000
        )
        assert registry.known(ENTITY, keys) == set(keys)
        assert {
            e.surrogate_key: e.natural_key for e in registry.entries(ENTITY)
        } == {e.surrogate_key: e.natural_key for e in entries}


def test_inserting_no_rows_does_nothing(
    backend: conftest.Backend,
) -> None:
    table = backend.table(
        "empty", sa.Column("id", sa.Integer, primary_key=True)
    )
    backend.create()

    with backend.db.engine.begin() as connection:
        database.insert_many(connection, table, [])

    assert backend.rows(table, "id") == []
