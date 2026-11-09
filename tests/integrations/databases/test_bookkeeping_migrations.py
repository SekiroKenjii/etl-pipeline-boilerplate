"""The ETL tables upgraded, the same way on every database (review D4): from
each earlier state of the code, by `etl-db upgrade` or by the SQL of
`etl-db ddl --from`, to tables like freshly created ones, records kept."""

from __future__ import annotations

from collections.abc import Iterator
import concurrent.futures
import contextlib
import datetime
import json
import multiprocessing
import re
from typing import Any
import uuid

import pytest
from sqlalchemy import event as sa_event
import sqlalchemy as sa

from core import contract
from infra.database import relational
from infra.database.relational import bookkeeping
from infra.database.relational import migrations
from tests.factories import fakes
from tests.factories import quarantines
from tests.integrations.databases import conftest
from tests.integrations.databases import legacy_etl_tables

NOW = datetime.datetime(2026, 3, 4, 5, 6, 7, tzinfo=datetime.UTC)
# What each state of the code is, to `upgrade`.
DETECTED = {
    "0001": "0001",
    "0001+steps": "0001",
    "0002": "0002",
    "0003": "0003",
}


def _detected(state: str, dialect: str) -> str:
    # On MySQL, 0004 changes nothing: the tables of 0003 are the latest.
    if dialect not in bookkeeping.OPEN_ENTRY_INDEX_DIALECTS and state == "0003":
        return migrations.HEAD
    return DETECTED[state]


def _record(key: str) -> dict[str, Any]:
    return {
        "mapping": "customer",
        "entity": "crm.customer",
        "source_key": [key],
        "part": 0,
        "deleted": False,
        "data": {"name": "Chi"},
    }


def _fill(backend: conftest.Backend, state: str, prefix: str) -> None:
    """Records as the code of `state` wrote them: a run, 3 quarantined
    records, one of them twice (rejected again before 0003), an issue."""
    tables = legacy_etl_tables.metadata(state, prefix=prefix).tables
    quarantine = tables[f"{prefix}quarantine"]
    rows: list[dict[str, Any]] = []
    for key in ("1003", "1003", "1004", "1005"):
        row: dict[str, Any] = {
            "pipeline": "crm.full_load",
            "run_id": "r1",
            "checkpoint": "after_transform",
            "record": json.dumps(_record(key)),
            "issues": json.dumps([{"message": "not a date"}]),
            "rejected_at": NOW,
        }
        if "record_key" in quarantine.c:
            row["record_key"] = contract.QuarantineEntry.key_of(
                "crm.full_load", "after_transform", _record(key)
            )
        rows.append(row)
    with backend.db.engine.begin() as connection:
        connection.execute(
            tables[f"{prefix}runs"].insert(),
            {
                "run_id": "r1",
                "pipeline": "crm.full_load",
                "status": "partial_success",
                "started_at": NOW,
                "completed_at": NOW,
                "host": "h",
                "dry_run": False,
            },
        )
        connection.execute(quarantine.insert(), rows)
        connection.execute(
            tables[f"{prefix}quality_issues"].insert(),
            {
                "pipeline": "crm.full_load",
                "run_id": "r1",
                "rule": "mapping",
                "checkpoint": "after_transform",
                "action": "reject",
                "message": "not a date",
                "record_id": "1003",
                "found_at": NOW,
            },
        )
        connection.execute(
            tables[f"{prefix}key_registry"].insert(),
            {
                "entity": "crm.customer",
                "surrogate_key": "k1",
                "natural_key": '["1003"]',
                "registered_at": NOW,
            },
        )


def _shape(
    backend: conftest.Backend, prefix: str, schema: str | None = None
) -> dict[str, Any]:
    """The tables of a prefix as the database tells them, prefix removed:
    columns, types, defaults, keys and indexes."""
    inspector = sa.inspect(backend.db.engine)
    shape: dict[str, Any] = {}

    def bare(name: str | None) -> str:
        return str(name).lower().removeprefix(prefix.lower())

    for table in inspector.get_table_names(schema):
        if not table.lower().startswith(prefix.lower()):
            continue
        columns = [
            (
                c["name"],
                str(c["type"]),
                c["nullable"],
                re.sub(r"[()N]", "", str(c.get("default") or "")),
            )
            for c in inspector.get_columns(table, schema)
        ]
        indexes = sorted(
            (
                bare(i["name"]),
                tuple(str(c) for c in i["column_names"]),
                tuple(str(e) for e in i.get("expressions", ()) or ()),
                bool(i["unique"]),
                json.dumps(
                    i.get("dialect_options", {}), sort_keys=True, default=str
                ),
            )
            for i in inspector.get_indexes(table, schema)
        )
        primary = inspector.get_pk_constraint(table, schema)
        shape[bare(table)] = {
            "columns": sorted(columns),
            "indexes": indexes,
            "primary": primary["constrained_columns"],
        }
    return shape


@contextlib.contextmanager
def _other_prefix(backend: conftest.Backend) -> Iterator[str]:
    prefix = f"f{uuid.uuid4().hex[:7]}_"
    try:
        yield prefix
    finally:
        relational.drop_etl_tables(backend.db, prefix=prefix)


@pytest.mark.parametrize("state", legacy_etl_tables.STATES)
def test_tables_of_earlier_code_are_told_upgraded_and_kept(
    backend: conftest.Backend, state: str
) -> None:
    prefix = backend.prefix
    legacy_etl_tables.metadata(state, prefix=prefix).create_all(
        backend.db.engine
    )
    _fill(backend, state, prefix)

    before = migrations.status(backend.db, prefix=prefix)
    assert (before.revision, before.detected) == (
        None,
        _detected(state, backend.name),
    )
    assert relational.create_etl_tables(backend.db, prefix=prefix) == before
    assert migrations.status(backend.db, prefix=prefix).up_to_date

    # As if created by this release.
    with _other_prefix(backend) as fresh:
        relational.create_etl_tables(backend.db, prefix=fresh)
        assert _shape(backend, prefix) == _shape(backend, fresh)

    # Every record kept; one open entry per record, the latest.
    quarantine = relational.RelationalQuarantine(backend.db, prefix=prefix)
    entries = list(quarantine.entries())
    assert [quarantines.fields(e)["source_key"] for e in entries] == [
        ["1003"],
        ["1003"],
        ["1004"],
        ["1005"],
    ]
    assert {e.status for e in entries} == {contract.QuarantineStatus.OPEN}
    rejected = contract.RejectedRecord(
        _record("1003"), contract.Checkpoint.AFTER_TRANSFORM, ()
    )
    quarantine.put([rejected], fakes.make_context("crm.full_load"))
    again = list(quarantine.entries())
    assert len(again) == 4
    assert again[1].issues == ()
    ledger = relational.RelationalRunLedger(backend.db, prefix=prefix)
    found = ledger.get("r1")
    assert found is not None
    assert (found.status, found.steps) == (
        contract.ETLStatus.PARTIAL_SUCCESS,
        {},
    )
    registry = relational.RelationalKeyRegistry(backend.db, prefix=prefix)
    assert registry.natural_key("crm.customer", "k1") == ("1003",)


def _statements(sql: str) -> list[str]:
    """The statements of `etl-db ddl`, as a DBA's tool splits them; its
    transactions left to the test."""
    statements = []
    for chunk in re.split(r"\n\s*\n", sql):
        lines = [
            line
            for line in chunk.splitlines()
            if line.strip() not in ("GO", "/") and not line.startswith("--")
        ]
        statement = "\n".join(lines).strip().removesuffix(";").strip()
        if statement and statement.upper() not in (
            "BEGIN",
            "BEGIN TRANSACTION",
            "COMMIT",
        ):
            statements.append(statement)
    return statements


def _apply(backend: conftest.Backend, sql: str) -> None:
    with backend.db.engine.begin() as connection:
        for statement in _statements(sql):
            connection.exec_driver_sql(statement)


@pytest.mark.parametrize("state", ["0001", "0002"])
def test_the_sql_for_dbas_upgrades_the_tables(
    backend: conftest.Backend, state: str
) -> None:
    prefix = backend.prefix
    legacy_etl_tables.metadata(state, prefix=prefix).create_all(
        backend.db.engine
    )
    _fill(backend, state, prefix)

    _apply(
        backend,
        relational.ddl(backend.name, prefix=prefix, start=state, legacy=True),
    )

    assert migrations.status(backend.db, prefix=prefix).up_to_date
    with _other_prefix(backend) as fresh:
        relational.create_etl_tables(backend.db, prefix=fresh)
        assert _shape(backend, prefix) == _shape(backend, fresh)
    # Offline, the records keep a key of their own: rejected again, a
    # record gets a new entry, once.
    quarantine = relational.RelationalQuarantine(backend.db, prefix=prefix)
    rejected = contract.RejectedRecord(
        _record("1004"), contract.Checkpoint.AFTER_TRANSFORM, ()
    )
    for _ in range(2):
        quarantine.put([rejected], fakes.make_context("crm.full_load"))
    assert len(list(quarantine.entries())) == 5


def test_the_sql_for_dbas_creates_the_tables(
    backend: conftest.Backend,
) -> None:
    prefix = backend.prefix

    _apply(backend, relational.ddl(backend.name, prefix=prefix))

    assert migrations.status(backend.db, prefix=prefix).up_to_date
    with _other_prefix(backend) as fresh:
        relational.create_etl_tables(backend.db, prefix=fresh)
        assert _shape(backend, prefix) == _shape(backend, fresh)


def test_the_revisions_make_the_declared_tables(
    backend: conftest.Backend,
) -> None:
    """From nothing, revision by revision: the baseline is 0.1.0, the
    latest is `bookkeeping.etl_tables`."""
    prefix = backend.prefix
    with backend.db.engine.connect() as connection:
        # pylint: disable-next=protected-access
        migrations._run(connection, "upgrade", "0001", None, prefix)
    with _other_prefix(backend) as old:
        legacy_etl_tables.metadata("0001", prefix=old).create_all(
            backend.db.engine
        )
        assert _shape(backend, prefix) == {
            **_shape(backend, old),
            "alembic_version": _shape(backend, prefix)["alembic_version"],
        }

    relational.create_etl_tables(backend.db, prefix=prefix)

    with _other_prefix(backend) as fresh:
        relational.create_etl_tables(backend.db, prefix=fresh)
        assert _shape(backend, prefix) == _shape(backend, fresh)


def test_tables_matching_no_revision_are_left_alone(
    backend: conftest.Backend,
) -> None:
    prefix = backend.prefix
    tables = legacy_etl_tables.metadata("0003", prefix=prefix)
    issues = tables.tables[f"{prefix}quality_issues"]
    issues.indexes = {
        i for i in issues.indexes if not str(i.name).endswith("_found")
    }
    tables.create_all(backend.db.engine)
    before = _shape(backend, prefix)

    with pytest.raises(contract.PipelineConfigError, match="not all: missing"):
        relational.create_etl_tables(backend.db, prefix=prefix)

    assert _shape(backend, prefix) == before
    assert not migrations.status(backend.db, prefix=prefix).known


def _upgrade(url: str, prefix: str) -> str | None:
    """Upgrades the ETL tables in a process of its own."""
    db = relational.Database(url)
    try:
        return relational.create_etl_tables(db, prefix=prefix).revision
    finally:
        db.engine.dispose()


@pytest.mark.parametrize("state", [None, "0001"])
def test_processes_starting_together_upgrade_the_tables_once(
    backend: conftest.Backend, state: str | None
) -> None:
    prefix = backend.prefix
    if state is not None:
        legacy_etl_tables.metadata(state, prefix=prefix).create_all(
            backend.db.engine
        )
    spawn = multiprocessing.get_context("spawn")
    with concurrent.futures.ProcessPoolExecutor(3, mp_context=spawn) as pool:
        list(pool.map(_upgrade, [backend.url] * 3, [prefix] * 3))

    assert migrations.status(backend.db, prefix=prefix).up_to_date
    with _other_prefix(backend) as fresh:
        relational.create_etl_tables(backend.db, prefix=fresh)
        assert _shape(backend, prefix) == _shape(backend, fresh)


def test_a_schema_and_a_prefix(backend: conftest.Backend) -> None:
    """In a schema of their own where the test may create one, else in the
    default schema named: the version table goes with the tables."""
    schema = backend.schema
    created = None
    if backend.name in ("postgresql", "mssql"):
        created = f"s{uuid.uuid4().hex[:7]}"
        backend.execute(sa.schema.CreateSchema(created))
        schema = created
    prefix = backend.prefix
    try:
        legacy_etl_tables.metadata(
            "0001", schema=schema, prefix=prefix
        ).create_all(backend.db.engine)

        before = relational.create_etl_tables(backend.db, schema, prefix)

        assert before.detected == "0001"
        inspector = sa.inspect(backend.db.engine)
        assert f"{prefix}alembic_version" in inspector.get_table_names(schema)
        assert migrations.status(backend.db, schema, prefix).up_to_date
        assert not migrations.status(backend.db, schema, "other_").up_to_date
        quarantine = relational.RelationalQuarantine(
            backend.db, schema=schema, prefix=prefix
        )
        rejected = contract.RejectedRecord(
            _record("1"), contract.Checkpoint.AFTER_TRANSFORM, ()
        )
        quarantine.put([rejected], fakes.make_context("crm.full_load"))
        assert len(list(quarantine.entries())) == 1
    finally:
        relational.drop_etl_tables(backend.db, schema, prefix)
        if created is not None:
            backend.execute(sa.schema.DropSchema(created))


def test_a_record_rejected_by_two_writers_at_once_has_one_entry(
    backend: conftest.Backend,
) -> None:
    """The second writer fails on the unique index of the open entries,
    tries again, and refreshes the entry of the first."""
    if backend.name not in bookkeeping.OPEN_ENTRY_INDEX_DIALECTS:
        pytest.skip("no unique index of the open entries")
    if backend.name == "sqlite":
        pytest.skip("SQLite writers wait for each other")
    prefix = backend.prefix
    relational.create_etl_tables(backend.db, prefix=prefix)
    quarantine = relational.RelationalQuarantine(backend.db, prefix=prefix)
    rejected = contract.RejectedRecord(
        _record("1003"), contract.Checkpoint.AFTER_TRANSFORM, ()
    )
    other = relational.RelationalQuarantine(backend.db, prefix=prefix)
    raced: list[bool] = []

    # Between the first writer's read and its insert, the other one inserts.
    @sa_event.listens_for(backend.db.engine, "before_cursor_execute")
    def race(*args: Any) -> None:
        statement = str(args[2])
        if not raced and statement.startswith(f"INSERT INTO {prefix}quar"):
            raced.append(True)
            with concurrent.futures.ThreadPoolExecutor(1) as thread:
                thread.submit(
                    other.put, [rejected], fakes.make_context("crm.full_load")
                ).result()

    try:
        quarantine.put([rejected], fakes.make_context("crm.full_load"))
    finally:
        sa_event.remove(backend.db.engine, "before_cursor_execute", race)

    assert raced
    assert len(list(quarantine.entries())) == 1
