"""SQL Server seeks the target's rows by key only where an index allows it.

The writer forces a nested loop from the staging table (`OPTION (LOOP
JOIN)`): with an index on the key, each staged row seeks its target row;
without one, each would scan the whole target.
"""

from __future__ import annotations

from collections.abc import Generator, Sequence
import concurrent.futures
import logging
from xml.etree import ElementTree

import pytest
from sqlalchemy import event as sa_event
import sqlalchemy as sa

from core import contract
from infra.database import relational
from tests.integrations.databases import conftest

_HINT = "OPTION (LOOP JOIN)"
_ROWS = 2000


@pytest.fixture
def mssql(backend: conftest.Backend) -> conftest.Backend:
    if backend.name != "mssql":
        pytest.skip("SQL Server's join hints")
    return backend


class _Statements:
    """The statements an engine runs, while listening."""

    def __init__(self, engine: sa.Engine) -> None:
        self.engine = engine
        self.seen: list[str] = []

    def __enter__(self) -> _Statements:
        sa_event.listen(self.engine, "before_cursor_execute", self._record)
        return self

    def __exit__(self, *exc: object) -> None:
        sa_event.remove(self.engine, "before_cursor_execute", self._record)

    def _record(
        self,
        connection: sa.Connection,
        cursor: object,
        statement: str,
        *args: object,
    ) -> None:
        del connection, cursor, args  # Unused.
        self.seen.append(statement)

    def joining(self, table: sa.Table) -> list[str]:
        """The UPDATEs, INSERT ... SELECTs and DELETEs of `table`."""
        return [
            s
            for s in self.seen
            if table.name in s
            and s.lstrip().upper().startswith(("UPDATE", "INSERT", "DELETE"))
            and "etl_stage" in s
        ]


def _customers(backend: conftest.Backend, *, primary: bool = False) -> sa.Table:
    table = backend.table(
        "customer",
        sa.Column("tenant", sa.Unicode(10), nullable=False),
        sa.Column("customer_key", sa.Unicode(36), primary_key=primary),
        sa.Column("name", sa.Unicode(50), nullable=False),
    )
    backend.create()
    backend.insert(
        table,
        *(
            {"tenant": "a", "customer_key": f"k{i:05}", "name": "old"}
            for i in range(_ROWS)
        ),
    )
    return table


def _upsert(
    backend: conftest.Backend,
    table: sa.Table,
    keys: Sequence[str] = ("customer_key",),
    rows: range = range(0, 100),
) -> contract.WriteResult:
    writer = relational.RelationalTableWriter(backend.db)
    return writer.write(
        contract.TableTarget(table.name, key=tuple(keys)),
        [
            {"tenant": "a", "customer_key": f"k{i:05}", "name": "new"}
            for i in rows
        ],
    )


@pytest.fixture
def warnings(
    caplog: pytest.LogCaptureFixture,
) -> Generator[pytest.LogCaptureFixture]:
    with caplog.at_level(logging.WARNING, "infra.database.relational"):
        yield caplog


def _no_index_warnings(
    caplog: pytest.LogCaptureFixture, table: sa.Table
) -> list[str]:
    return [
        r.getMessage()
        for r in caplog.records
        if table.name in r.getMessage() and "no index" in r.getMessage()
    ]


_SHOWPLAN = "{http://schemas.microsoft.com/sqlserver/2004/07/showplan}"


def _reads_of(backend: conftest.Backend, table: sa.Table) -> list[str]:
    """How the cached plans of the UPDATEs of `table` read it.

    The physical operators of the plans on `table`, its update aside: an
    `Index Seek` or a `Clustered Index Seek`, or a scan.
    """
    reads: list[str] = []
    for plan in _plans(backend, table):
        for op in ElementTree.fromstring(plan).iter(f"{_SHOWPLAN}RelOp"):
            physical = op.get("PhysicalOp", "")
            if "Update" in physical:
                continue
            reads.extend(
                physical
                for details in op
                for found in details.findall(f"{_SHOWPLAN}Object")
                if found.get("Table") == f"[{table.name}]"
            )
    return reads


def _plans(backend: conftest.Backend, table: sa.Table) -> list[str]:
    """The cached plans of the UPDATEs of `table`, as XML."""
    with backend.db.engine.connect() as connection:
        return [
            str(plan)
            for (plan,) in connection.execute(
                sa.text(
                    "SELECT CAST(qp.query_plan AS NVARCHAR(MAX))"
                    " FROM sys.dm_exec_query_stats qs"
                    " CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st"
                    " CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp"
                    " WHERE st.text LIKE :update"
                ),
                {"update": f"UPDATE {table.name} %"},
            )
        ]


class TestKeyWithoutIndex:

    def test_a_heap_is_merged_without_the_hint_and_warned_of_once(
        self,
        mssql: conftest.Backend,
        warnings: pytest.LogCaptureFixture,
    ) -> None:
        table = _customers(mssql)

        with _Statements(mssql.db.engine) as statements:
            first = _upsert(mssql, table, rows=range(0, 100))
            second = _upsert(mssql, table, rows=range(100, 200))

        assert (first.updated, second.updated) == (100, 100)
        joining = statements.joining(table)
        assert joining
        assert not [s for s in joining if _HINT in s]
        assert len(_no_index_warnings(warnings, table)) == 1

    def test_an_index_led_by_another_column_does_not_seek_the_key(
        self,
        mssql: conftest.Backend,
        warnings: pytest.LogCaptureFixture,
    ) -> None:
        table = _customers(mssql)
        mssql.execute(
            sa.text(
                f"CREATE INDEX ix_{table.name} ON {table.name}"
                " (name, customer_key)"
            )
        )

        with _Statements(mssql.db.engine) as statements:
            _upsert(mssql, table)

        assert not [s for s in statements.joining(table) if _HINT in s]
        assert len(_no_index_warnings(warnings, table)) == 1

    def test_a_filtered_index_does_not_seek_every_key(
        self,
        mssql: conftest.Backend,
        warnings: pytest.LogCaptureFixture,
    ) -> None:
        table = _customers(mssql)
        mssql.execute(
            sa.text(
                f"CREATE UNIQUE INDEX ux_{table.name} ON {table.name}"
                " (customer_key) WHERE name = 'old'"
            )
        )

        with _Statements(mssql.db.engine) as statements:
            _upsert(mssql, table)

        assert not [s for s in statements.joining(table) if _HINT in s]
        assert len(_no_index_warnings(warnings, table)) == 1

    def test_writers_at_once_warn_once(
        self,
        mssql: conftest.Backend,
        warnings: pytest.LogCaptureFixture,
    ) -> None:
        table = _customers(mssql)
        writer = relational.RelationalTableWriter(mssql.db)
        target = contract.TableTarget(table.name, key=("customer_key",))

        def write(start: int) -> int:
            return writer.write(
                target,
                [
                    {"tenant": "a", "customer_key": f"k{i:05}", "name": "new"}
                    for i in range(start, start + 50)
                ],
            ).updated

        with concurrent.futures.ThreadPoolExecutor(4) as threads:
            updated = list(threads.map(write, range(0, 200, 50)))

        assert updated == [50] * 4
        assert len(_no_index_warnings(warnings, table)) == 1

    def test_deleting_by_unindexed_columns_has_no_hint(
        self,
        mssql: conftest.Backend,
        warnings: pytest.LogCaptureFixture,
    ) -> None:
        table = _customers(mssql, primary=True)
        writer = relational.RelationalTableWriter(mssql.db)

        with _Statements(mssql.db.engine) as statements:
            deleted = writer.delete(table.name, [{"name": "old"}])

        assert deleted == _ROWS
        joining = statements.joining(table)
        assert joining
        assert not [s for s in joining if _HINT in s]
        assert len(_no_index_warnings(warnings, table)) == 1

    def test_a_table_named_with_and_without_its_schema_is_read_once(
        self,
        mssql: conftest.Backend,
        warnings: pytest.LogCaptureFixture,
    ) -> None:
        table = _customers(mssql)
        writer = relational.RelationalTableWriter(mssql.db)
        rows: list[contract.Row] = [
            {"tenant": "a", "customer_key": "k00001", "name": "new"}
        ]

        with _Statements(mssql.db.engine) as statements:
            for name in (table.name, f"{mssql.schema}.{table.name}"):
                writer.write(
                    contract.TableTarget(name, key=("customer_key",)), rows
                )

        reads = [s for s in statements.seen if "is_hypothetical" in s]
        assert len(reads) == 1
        assert len(_no_index_warnings(warnings, table)) == 1

    def test_writers_at_once_by_both_names_warn_once(
        self,
        mssql: conftest.Backend,
        warnings: pytest.LogCaptureFixture,
    ) -> None:
        table = _customers(mssql)
        writer = relational.RelationalTableWriter(mssql.db)
        names = [table.name, f"{mssql.schema}.{table.name}"] * 4

        def write(index: int) -> int:
            return writer.write(
                contract.TableTarget(names[index], key=("customer_key",)),
                [
                    {"tenant": "a", "customer_key": f"k{i:05}", "name": "new"}
                    for i in range(index * 25, index * 25 + 25)
                ],
            ).updated

        with concurrent.futures.ThreadPoolExecutor(4) as threads:
            updated = list(threads.map(write, range(len(names))))

        assert updated == [25] * len(names)
        assert len(_no_index_warnings(warnings, table)) == 1

    def test_a_non_unique_index_on_part_of_the_key_does_not_seek_it(
        self,
        mssql: conftest.Backend,
        warnings: pytest.LogCaptureFixture,
    ) -> None:
        table = _customers(mssql)
        mssql.execute(
            sa.text(f"CREATE INDEX ix_{table.name} ON {table.name} (tenant)")
        )

        with _Statements(mssql.db.engine) as statements:
            _upsert(mssql, table, keys=("tenant", "customer_key"))

        assert not [s for s in statements.joining(table) if _HINT in s]
        assert len(_no_index_warnings(warnings, table)) == 1


class TestKeyWithIndex:

    def test_a_primary_key_is_sought_by_a_nested_loop(
        self,
        mssql: conftest.Backend,
        warnings: pytest.LogCaptureFixture,
    ) -> None:
        table = _customers(mssql, primary=True)

        with _Statements(mssql.db.engine) as statements:
            result = _upsert(mssql, table)

        assert result.updated == 100
        joining = statements.joining(table)
        assert joining
        assert all(_HINT in s for s in joining)
        assert not _no_index_warnings(warnings, table)
        assert set(_reads_of(mssql, table)) == {"Clustered Index Seek"}

    def test_a_composite_key_led_by_its_columns_in_another_order(
        self,
        mssql: conftest.Backend,
        warnings: pytest.LogCaptureFixture,
    ) -> None:
        table = _customers(mssql)
        mssql.execute(
            sa.text(
                f"CREATE INDEX ix_{table.name} ON {table.name}"
                " (customer_key, tenant, name)"
            )
        )

        with _Statements(mssql.db.engine) as statements:
            _upsert(mssql, table, keys=("tenant", "customer_key"))

        assert all(_HINT in s for s in statements.joining(table))
        assert not _no_index_warnings(warnings, table)
        reads = set(_reads_of(mssql, table))
        assert "Index Seek" in reads
        assert not [r for r in reads if "Scan" in r]

    def test_a_unique_index_on_part_of_the_key(
        self,
        mssql: conftest.Backend,
        warnings: pytest.LogCaptureFixture,
    ) -> None:
        table = _customers(mssql)
        mssql.execute(
            sa.text(
                f"CREATE UNIQUE INDEX ux_{table.name} ON {table.name}"
                " (customer_key)"
            )
        )

        with _Statements(mssql.db.engine) as statements:
            _upsert(mssql, table, keys=("tenant", "customer_key"))

        assert all(_HINT in s for s in statements.joining(table))
        assert not _no_index_warnings(warnings, table)
