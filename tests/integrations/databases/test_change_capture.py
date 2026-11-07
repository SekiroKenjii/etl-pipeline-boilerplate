"""Deletes and change data capture on real databases.

The conformance part (deletes, UPDATE, reconciliation) runs on every
database; each change source runs on its own: SQL Server Change Tracking,
Postgres logical replication.
"""

from __future__ import annotations

from collections.abc import Generator
import os
import uuid

import pytest
import sqlalchemy as sa

from core import contract
from core import operators
from core import pipeline
from core import state as core_state
from infra.database import relational
from tests.factories import fakes
from tests.integrations.databases import conftest

INSERT = contract.ChangeOperation.INSERT
UPDATE = contract.ChangeOperation.UPDATE
DELETE = contract.ChangeOperation.DELETE


@pytest.fixture
def items(backend: conftest.Backend) -> sa.Table:
    table = backend.table(
        "item",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("parent", sa.Integer, nullable=False),
        sa.Column("name", sa.Unicode(20)),
        sa.Column("is_deleted", sa.Boolean),
    )
    backend.create()
    backend.insert(
        table,
        *(
            {"id": i, "parent": i // 10, "name": f"n{i}", "is_deleted": False}
            for i in (1, 2, 11, 12, 21)
        ),
    )
    return table


class TestDeletes:

    def test_by_key_and_by_parent(
        self, backend: conftest.Backend, items: sa.Table
    ) -> None:
        writer = relational.RelationalTableWriter(backend.db)

        by_key = writer.delete(items.name, [{"id": 1}, {"id": 404}])
        by_parent = writer.delete(items.name, [{"parent": 1}])

        assert (by_key, by_parent) == (1, 2)
        assert backend.rows(items, "id") == [(2,), (21,)]

    def test_update_mode_never_inserts(
        self, backend: conftest.Backend, items: sa.Table
    ) -> None:
        writer = relational.RelationalTableWriter(backend.db)
        target = contract.TableTarget(
            items.name,
            key=("parent",),
            mode=contract.WriteMode.UPDATE,
            update_columns=("is_deleted",),
        )

        result = writer.write(
            target,
            [
                {"parent": 1, "is_deleted": True},
                {"parent": 9, "is_deleted": True},
            ],
        )

        assert result.created == 0
        assert result.updated == 2
        assert backend.rows(items, "id", "is_deleted") == [
            (1, False),
            (2, False),
            (11, True),
            (12, True),
            (21, False),
        ]

    def test_the_crosswalk_lists_its_keys(
        self, backend: conftest.Backend
    ) -> None:
        relational.create_etl_tables(backend.db, prefix=backend.prefix)
        registry = relational.RelationalKeyRegistry(
            backend.db, prefix=backend.prefix
        )
        entries = [
            contract.KeyEntry("crm.customer", f"k{i:04}", (str(i),))
            for i in range(1500)
        ]

        def use(r: relational.RelationalKeyRegistry) -> list[contract.KeyEntry]:
            r.register(entries)
            return list(r.entries("crm.customer"))

        listed = use(registry)

        assert sorted(listed, key=lambda e: e.surrogate_key) == entries


def test_deletes_are_found_by_reconciliation(
    backend: conftest.Backend, items: sa.Table
) -> None:
    """A source without a change log: its deletes, soft-deleted in a target."""
    relational.create_etl_tables(backend.db, prefix=backend.prefix)
    registry = relational.RelationalKeyRegistry(
        backend.db, prefix=backend.prefix
    )
    target = backend.table(
        "target",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("is_deleted", sa.Boolean, nullable=False),
    )
    backend.create()
    backend.insert(target, *({"id": i, "is_deleted": False} for i in (1, 2, 3)))
    registry.register(
        [contract.KeyEntry("item", str(i), (str(i),)) for i in (1, 2, 3)]
    )
    backend.execute(items.delete().where(items.c.id == 1))
    job = pipeline.ETLJob(
        operators.MissingSourceRows(
            relational.RelationalTableReader(backend.db),
            items.name,
            key=["id"],
            registry=registry,
            entity="item",
            max_share=None,  # 2 of 3 rows.
        ),
        operators.Passthrough[contract.ChangeEvent](),
        operators.ChangeLoader[contract.ChangeEvent](
            relational.RelationalTableWriter(backend.db),
            contract.TableTarget(target.name, key=("id",)),
            row=lambda e: {"id": int(str(e.row["id"]))},
            deleted=lambda e: e.deleted,
            deletes=operators.Deletes.SOFT,
        ),
    )

    result = job(fakes.make_context("reconcile"), {})

    # 3 was never in the source table: it was registered by another load.
    assert result.stats.deleted == 2
    assert backend.rows(target, "id", "is_deleted") == [
        (1, True),
        (2, False),
        (3, True),
    ]


def _run(
    source: contract.IChangeSource,
    state: contract.IStateStore,
    snapshot: operators.Snapshot | None = None,
    loader: fakes.CollectingLoader[contract.ChangeEvent] | None = None,
    batch_size: int = 1,
) -> list[tuple[str, object, object]]:
    loader = loader or fakes.CollectingLoader[contract.ChangeEvent]()
    pipeline.ETLJob(
        operators.ChangeExtractor(source, state=state, snapshot=snapshot),
        operators.Passthrough[contract.ChangeEvent](),
        loader,
        batch_size=batch_size,
    )(fakes.make_context("sync"), {})
    return [
        (e.operation.value, e.key[0], e.row.get("name")) for e in loader.rows
    ]


@pytest.fixture
def tracked() -> Generator[relational.Database]:
    """A SQL Server database with Change Tracking (never on `master`)."""
    url = os.environ.get("ETL_TEST_MSSQL_URL")
    if not url:
        pytest.skip("set ETL_TEST_MSSQL_URL to run the SQL Server tests")
    with sa.create_engine(url).connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.exec_driver_sql(
            "IF DB_ID('etl_cdc') IS NULL CREATE DATABASE etl_cdc"
        )
    db = relational.Database(
        sa.make_url(url).set(database="etl_cdc").render_as_string(False)
    )
    try:
        yield db
    finally:
        db.close()


def test_sql_server_change_tracking(tracked: relational.Database) -> None:
    name = f"t{uuid.uuid4().hex[:8]}"
    table = sa.Table(
        name,
        sa.MetaData(),
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("name", sa.Unicode(20)),
    )
    table.create(tracked.engine)
    try:
        relational.enable_change_tracking(tracked, name)
        with tracked.engine.begin() as conn:
            conn.execute(table.insert(), [{"id": 1, "name": "a"}])
        source = relational.SqlServerChangeTracking(tracked, name)
        snapshot = operators.Snapshot(
            relational.RelationalTableReader(tracked),
            contract.TableQuery(name, order_by=("id",)),
        )
        state = core_state.InMemoryStateStore()

        assert _run(source, state, snapshot) == [("insert", 1, "a")]

        # Three rows in one transaction: one version, cut by the batches.
        with tracked.engine.begin() as conn:
            conn.execute(
                table.insert(),
                [{"id": i, "name": f"n{i}"} for i in (2, 3, 4)],
            )
        with tracked.engine.begin() as conn:
            conn.execute(table.update().where(table.c.id == 1).values(name="A"))
            conn.execute(table.delete().where(table.c.id == 2))
        failing = fakes.CollectingLoader[contract.ChangeEvent](fail_on_batch=2)
        with pytest.raises(contract.RetryableError):
            _run(source, state, loader=failing)

        # The first batch (3) was loaded; 2, inserted then deleted, is a
        # delete: Change Tracking keeps the net change of each row.
        assert _run(source, state) == [
            ("insert", 4, "n4"),
            ("update", 1, "A"),
            ("delete", 2, None),
        ]
        assert _run(source, state) == []
    finally:
        table.drop(tracked.engine)


@pytest.fixture
def replicated(
    backend: conftest.Backend,
) -> Generator[tuple[conftest.Backend, sa.Table, str, str]]:
    if backend.name != "postgresql":
        pytest.skip("logical replication is Postgres only")
    table = backend.table(
        "replicated",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("name", sa.Unicode(20)),
        sa.Column("doc", sa.Text),
    )
    backend.create()
    slot, publication = f"{backend.prefix}slot", f"{backend.prefix}pub"
    relational.create_replication(
        backend.db, table.name, slot=slot, publication=publication
    )
    try:
        yield backend, table, slot, publication
    finally:
        relational.drop_replication(
            backend.db, slot=slot, publication=publication
        )


def test_postgres_logical_replication(
    replicated: tuple[conftest.Backend, sa.Table, str, str],
) -> None:
    backend, table, slot, publication = replicated
    backend.insert(table, {"id": 1, "name": "a", "doc": "x" * 5000})
    source = relational.PostgresLogicalReplication(
        backend.db, table.name, slot=slot, publication=publication
    )
    snapshot = operators.Snapshot(
        relational.RelationalTableReader(backend.db),
        contract.TableQuery(table.name, order_by=("id",)),
    )
    state = core_state.InMemoryStateStore()

    assert _run(source, state, snapshot) == [("insert", 1, "a")]

    # Concurrent transactions: T1 starts first, T2 commits first.
    with backend.db.engine.connect() as first:
        t1 = first.begin()
        first.execute(table.insert().values(id=2, name="t1"))
        backend.insert(table, {"id": 3, "name": "t2"})
        first.execute(table.insert().values(id=4, name="t1"))
        t1.commit()
    # An update leaving a large (TOASTed) value unchanged, then a delete.
    backend.execute(table.update().where(table.c.id == 1).values(name="A"))
    backend.execute(table.delete().where(table.c.id == 3))

    failing = fakes.CollectingLoader[contract.ChangeEvent](fail_on_batch=2)
    with pytest.raises(contract.RetryableError):
        _run(source, state, loader=failing)
    rest = _run(source, state)

    # T2's change came first, then T1's whose change LSN is lower: kept.
    assert rest == [
        ("insert", 2, "t1"),
        ("insert", 4, "t1"),
        ("update", 1, "A"),
        ("delete", 3, None),
    ]
    assert _run(source, state) == []

    loader = fakes.CollectingLoader[contract.ChangeEvent]()
    backend.execute(table.update().where(table.c.id == 1).values(name="AA"))
    _run(source, state, loader=loader)
    (update,) = loader.rows
    assert update.row["doc"] == "x" * 5000
