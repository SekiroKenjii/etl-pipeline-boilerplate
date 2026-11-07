"""The cutover diff and load verification, on every database.

What a database gives back (a `UUID` or its text, `Decimal` or `float`, `bool`
or `1`) differs; the reconciliation must not.
"""

from __future__ import annotations

import datetime
import decimal
from typing import Any
import uuid

import pytest
import sqlalchemy as sa

from core import contract
from core import cutover
from infra.database import relational
from tests.factories import fake_ports
from tests.factories import fakes
from tests.integrations.databases import conftest

ROWS = [
    {
        "id": uuid.UUID(int=i),
        "name": name,
        "total": decimal.Decimal(total),
        "born": datetime.date(2000, 1, i),
        "seen": datetime.datetime(2026, 10, 1, 8, i),
        "vip": i % 2 == 0,
    }
    for i, name, total in [
        (1, "Zoë Müller", "120.50"),
        (2, "Bruno", "75"),
        (3, "Chi", "0.10"),
    ]
]
COLUMNS = ("id", "name", "total", "born", "seen", "vip")


def _table(backend: conftest.Backend) -> tuple[sa.Table, list[dict[str, Any]]]:
    """The table, and the rows written to it."""
    table = backend.table(
        "client",
        sa.Column(
            "id",
            sa.Uuid().with_variant(sa.Unicode(36), "oracle", "sqlite", "mysql"),
            primary_key=True,
        ),
        sa.Column("name", sa.Unicode(50)),
        sa.Column("total", sa.Numeric(12, 2)),
        sa.Column("born", sa.Date),
        sa.Column("seen", sa.DateTime),
        sa.Column("vip", sa.Boolean),
    )
    backend.create()
    native = backend.db.name in ("postgresql", "mssql")
    written = [r if native else {**r, "id": str(r["id"])} for r in ROWS]
    writer = relational.RelationalTableWriter(backend.db)
    writer.write(contract.TableTarget(table.name, key=("id",)), written)
    return table, written


def _reconciliation(
    backend: conftest.Backend, table: sa.Table
) -> contract.Reconciliation:
    return contract.Reconciliation(
        entity="client",
        source=fake_ports.InMemoryTableReader({"legacy": ROWS}),
        source_query=contract.TableQuery("legacy", order_by=("born",)),
        expect=lambda row: contract.Expected(rows=[row]),
        target=relational.RelationalTableReader(backend.db),
        target_query=contract.TableQuery(table.name, order_by=("id",)),
        key=("id",),
        columns=COLUMNS,
    )


def test_what_was_written_reconciles(backend: conftest.Backend) -> None:
    table, _ = _table(backend)

    report = cutover.reconcile(_reconciliation(backend, table))

    assert report.status == "match", report.samples
    assert (report.matched, report.target_rows) == (3, 3)


def test_a_changed_value_is_found(backend: conftest.Backend) -> None:
    table, _ = _table(backend)
    backend.execute(
        table.update()
        .where(table.c.name == "Chi")
        .values(total=decimal.Decimal("0.11"))
    )

    report = cutover.reconcile(_reconciliation(backend, table))

    assert (report.matched, report.different) == (2, 1)
    (sample,) = report.samples
    assert sample.columns == {
        "total": (
            cutover.canonical_value(decimal.Decimal("0.1")),
            cutover.canonical_value(0.11),
        )
    }


def test_loads_are_verified(backend: conftest.Backend) -> None:
    table, written = _table(backend)
    check = cutover.LoadedRowsCheck[dict[str, Any]](
        relational.RelationalTableReader(backend.db),
        table.name,
        key=("id",),
        columns=COLUMNS,
        row=lambda row: row,
    )
    changed = {**written[0], "name": "Zoe Muller"}

    violations = list(
        check.check_batch([*written[1:], changed], fakes.make_context())
    )

    assert [v.message for v in violations] == [
        "differs in the target: name 'Zoe Muller' became 'Zoë Müller'"
    ]


class _Counting(relational.RelationalTableReader):
    """Counts the rows each query yields."""

    def __init__(self, db: relational.Database) -> None:
        super().__init__(db, page_size=97)
        self.read_rows: list[int] = []

    def read(self, query: contract.TableQuery) -> Any:
        self.read_rows.append(0)
        for row in super().read(query):
            self.read_rows[-1] += 1
            yield row


def _drifted(
    backend: conftest.Backend, key: sa.types.TypeEngine[Any], keys: Any
) -> tuple[contract.Reconciliation, _Counting]:
    """A target of 2,000 rows: one missing, one different, one extra."""
    table = backend.table(
        "drifted",
        sa.Column("id", key, primary_key=True, autoincrement=False),
        sa.Column("name", sa.Unicode(20)),
    )
    backend.create()
    source = [{"id": keys(i), "name": f"n{i}"} for i in range(2_000)]
    target = [dict(row) for row in source if row["id"] != keys(1_000)]
    target[700]["name"] = "changed"
    target.append({"id": keys(5_000), "name": "extra"})
    backend.insert(table, *target)
    reader = _Counting(backend.db)
    reconciliation = contract.Reconciliation(
        entity="drifted",
        source=fake_ports.InMemoryTableReader({"legacy": source}),
        source_query=contract.TableQuery("legacy", order_by=("id",)),
        expect=lambda row: contract.Expected(rows=[row]),
        target=reader,
        target_query=contract.TableQuery(table.name, order_by=("id",)),
        key=("id",),
        columns=("id", "name"),
    )
    return reconciliation, reader


def test_the_second_pass_reads_only_the_ranges_that_differ(
    backend: conftest.Backend,
) -> None:
    reconciliation, reader = _drifted(backend, sa.Integer(), lambda i: i)

    report = cutover.reconcile(reconciliation)

    assert report.partitioning == "range"
    assert (report.missing, report.different, report.extra) == (1, 1, 1)
    assert report.matched == 1_998
    # The target in full, then 3 ranges of it, in the database.
    full, *ranges = reader.read_rows
    assert full == 2_000
    assert len(ranges) == 3
    assert sum(ranges) <= 3 * (cutover.ROWS_PER_PARTITION + 1)


@pytest.mark.parametrize("ranges", [True, False])
def test_uuid_keys_reconcile_whatever_the_order_of_the_database(
    backend: conftest.Backend, ranges: bool
) -> None:
    # SQL Server orders a uniqueidentifier by its last bytes, not as its
    # text: there, the diff splits the keys by hash.
    native = backend.name in ("postgresql", "mssql")
    key: sa.types.TypeEngine[Any] = sa.Uuid() if native else sa.Unicode(36)
    reconciliation, _ = _drifted(
        backend,
        key,
        (lambda i: uuid.uuid5(uuid.NAMESPACE_OID, str(i))) if native else
        (lambda i: str(uuid.uuid5(uuid.NAMESPACE_OID, str(i)))),
    )  # fmt: skip

    report = cutover.reconcile(reconciliation, ranges=ranges)

    assert (report.missing, report.different, report.extra) == (1, 1, 1)
    assert report.matched == 1_998
    if not ranges or backend.name == "mssql":
        assert report.partitioning == "hash"
    else:
        assert report.partitioning == "range"


# The ticks per second each database keeps of a `sa.DateTime`: SQLAlchemy
# makes it a SQL Server `datetime`, a MySQL `DATETIME`, an Oracle `DATE`.
_RESOLUTIONS = {
    "sqlite": 1_000_000,
    "postgresql": 1_000_000,
    "mssql": 300,
    "mysql": 1,
    "oracle": 1,
}


def test_timestamps_compare_at_the_coarser_resolution(
    backend: conftest.Backend,
) -> None:
    table = backend.table(
        "moments",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("seen", sa.DateTime),
    )
    backend.create()
    source = [
        {"id": i, "seen": datetime.datetime(2026, 10, 1, 8, 0, i, micro)}
        # Fractions on no tick of any database, under half a second.
        for i, micro in enumerate([123456, 1700, 498765, 0])
    ]
    backend.insert(table, *source)
    reconciliation = contract.Reconciliation(
        entity="moments",
        source=fake_ports.InMemoryTableReader({"legacy": source}),
        source_query=contract.TableQuery("legacy", order_by=("id",)),
        expect=lambda row: contract.Expected(rows=[row]),
        target=relational.RelationalTableReader(backend.db),
        target_query=contract.TableQuery(table.name, order_by=("id",)),
        key=("id",),
        columns=("id", "seen"),
    )
    resolution = _RESOLUTIONS[backend.name]

    exact = cutover.reconcile(reconciliation)
    coarse = cutover.reconcile(reconciliation, resolution=resolution)

    # Coarser in the database: by default, the fractions differ, in one
    # direction; at its resolution, they match.
    assert exact.different == (0 if resolution == 1_000_000 else 3)
    assert coarse.status == "match", coarse.samples
    assert coarse.matched == 4
