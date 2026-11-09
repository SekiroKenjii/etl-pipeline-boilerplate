"""On SQL Server, the writer chooses how to stage a batch from what it
measures: bulk copy for CRM-sized batches of 1,000 rows, and the same rows
either way."""

from __future__ import annotations

import datetime
import decimal
import logging

import pytest
import sqlalchemy as sa

from core import contract
from infra.database import relational
from infra.database.relational import staging
from tests.integrations.databases import conftest

T0 = datetime.datetime(2026, 1, 1, 12, 0, 0)
BATCH = 1000
BATCHES = 4


def _rows(batch: int) -> list[contract.Row]:
    return [
        {
            "id": f"K-{batch * BATCH + i:08d}",
            "full_name": f"Customer {batch}-{i}",
            "status": "AI"[i % 2],
            "total": decimal.Decimal(f"{i % 1000}.50"),
            "updated_at": T0 + datetime.timedelta(seconds=i),
        }
        for i in range(BATCH)
    ]


@pytest.fixture
def customers(backend: conftest.Backend) -> str:
    backend.table(
        "customer",
        sa.Column("id", sa.Unicode(64), primary_key=True),
        sa.Column("full_name", sa.Unicode(100)),
        sa.Column("status", sa.Unicode(10)),
        sa.Column("total", sa.Numeric(12, 2)),
        sa.Column("updated_at", sa.DateTime),
    )
    backend.create()
    return backend.prefix + "customer"


def test_bulk_copy_is_chosen_for_batches_of_a_thousand_rows(
    backend: conftest.Backend,
    customers: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    if backend.name != "mssql":
        pytest.skip("the online choice is SQL Server's")
    dialect = backend.db.dialect
    assert dialect.bulk_copy_rows is None
    assert dialect.chooser is not None
    writer = relational.RelationalTableWriter(backend.db)
    target = contract.TableTarget(customers, key=("id",))

    with caplog.at_level(logging.INFO, logger="infra.database.relational"):
        results = [
            writer.write(target, _rows(batch)) for batch in range(BATCHES)
        ]
    again = writer.write(target, _rows(0))

    assert [r.created for r in results] == [BATCH] * BATCHES
    assert (again.created, again.updated, again.unchanged) == (0, 0, BATCH)
    table = backend.db.table(customers).fullname
    assert dialect.chooser.chosen(table) == staging.BULK_COPY
    (record,) = [r for r in caplog.records if "staging by" in r.message]
    assert f"{table}: staging by bulk copy chosen after 3 batch(es)" in (
        record.message
    )
    assert "bulk copy" in record.message
    assert "parameters" in record.message
    rows = backend.rows(backend.db.table(customers), "id", "full_name", "total")
    assert len(rows) == BATCH * BATCHES
    assert rows[0] == ("K-00000000", "Customer 0-0", decimal.Decimal("0.50"))
    assert rows[-1] == (
        f"K-{BATCH * BATCHES - 1:08d}",
        f"Customer {BATCHES - 1}-{BATCH - 1}",
        decimal.Decimal("999.50"),
    )


def test_a_set_threshold_is_kept(
    backend: conftest.Backend, customers: str
) -> None:
    """With `tuning.bulk_copy_rows` set, nothing is measured: today's
    behaviour, whatever the database."""
    db = relational.Database(backend.url, bulk_copy_rows=2000)
    try:
        writer = relational.RelationalTableWriter(db)
        target = contract.TableTarget(customers, key=("id",))

        results = [writer.write(target, _rows(batch)) for batch in range(2)]

        assert [r.created for r in results] == [BATCH, BATCH]
        assert db.dialect.chooser is None
        assert db.dialect.bulk_copy_rows == 2000
    finally:
        db.close()
