"""Writes at once into one table, and the key registry's bulk inserts, on
every database."""

from __future__ import annotations

import concurrent.futures
import math
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
    target = contract.TableTarget(table.name, key=("customer_key",))
    writer = relational.RelationalTableWriter(backend.db)
    writers, batches, rows = 4, 12, 1000

    def write(index: int, name: str) -> tuple[int, int]:
        created = updated = 0
        for batch in range(batches):
            result = writer.write(
                target,
                [
                    {"customer_key": _key(index, batch, row), "name": name}
                    for row in range(rows)
                ],
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


class TestKeyRegistryInserts:

    def test_more_keys_than_one_insert_holds(
        self,
        backend: conftest.Backend,
        registry: relational.RelationalKeyRegistry,
    ) -> None:
        per_insert = _rows_per_insert(backend)
        entries = _entries(per_insert * 2 + 7)
        inserts: list[str] = []

        def count(*args: Any) -> None:
            statement: str = args[2]
            if statement.lstrip().upper().startswith("INSERT"):
                inserts.append(statement)

        sa_event.listen(backend.db.engine, "before_cursor_execute", count)
        try:
            registry.register(entries)
        finally:
            sa_event.remove(backend.db.engine, "before_cursor_execute", count)

        dialect = backend.db.engine.dialect
        if dialect.use_insertmanyvalues and (
            dialect.use_insertmanyvalues_wo_returning
            or dialect.insert_executemany_returning
        ):
            # Multi-row INSERTs, never one statement per row.
            assert len(inserts) == math.ceil(len(entries) / per_insert)
        else:
            # The driver's own (MySQL: multi-row INSERTs; Oracle: arrays).
            assert len(inserts) == 1
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

    def test_a_concurrent_writer_taking_keys_first(
        self, backend: conftest.Backend
    ) -> None:
        relational.create_etl_tables(backend.db, prefix=backend.prefix)
        per_insert = _rows_per_insert(backend)
        entries = _entries(per_insert + 50)
        taken = entries[per_insert : per_insert + 10]

        class Racing(relational.RelationalKeyRegistry):
            """Another writer registers some keys after the first read."""

            raced = False

            def _known(
                self, connection: sa.Connection, entity: str, keys: Any
            ) -> set[str]:
                found = super()._known(connection, entity, keys)
                if not self.raced:
                    self.raced = True
                    relational.RelationalKeyRegistry(
                        backend.db, prefix=backend.prefix
                    ).register(taken)
                return found

        racing = Racing(backend.db, prefix=backend.prefix)

        racing.register(entries)

        keys = [e.surrogate_key for e in entries]
        assert racing.raced
        assert racing.known(ENTITY, keys) == set(keys)


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
