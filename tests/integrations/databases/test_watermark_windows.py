"""Extraction windows (`--since`, `--until`) against real watermark columns.

The CLI gives aware times; most databases return naive ones (SQL Server
`datetime`, Oracle `DATE`, PostgreSQL `timestamp`, MySQL `DATETIME`): naive
means UTC.
"""

from __future__ import annotations

import datetime

import pytest
import sqlalchemy as sa

from core import contract
from core import operators
from core import pipeline
from infra.database import relational
from tests.factories import fakes
from tests.integrations.databases import conftest

PARIS = datetime.timezone(datetime.timedelta(hours=2))


@pytest.mark.parametrize("snapshot", [True, False])
def test_a_window_on_a_naive_watermark(
    backend: conftest.Backend, snapshot: bool
) -> None:
    table = backend.table(
        "event",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("updated_at", sa.DateTime, nullable=False),
    )
    backend.create()
    backend.insert(
        table,
        *(
            {"id": i, "updated_at": datetime.datetime(2026, 10, 1, i)}
            for i in (1, 2, 3)
        ),
    )
    loader = fakes.CollectingLoader[contract.Row]()
    job = pipeline.ETLJob(
        operators.TableExtractor(
            relational.RelationalTableReader(backend.db),
            table.name,
            key=["id"],
            watermark="updated_at",
            snapshot=snapshot,
        ),
        operators.Passthrough[contract.Row](),
        loader,
    )
    window = contract.ETLContext(
        contract.ETLRunInfo(
            "events",
            # 02:00 to 03:00 UTC, as `etl run --since ... --until ...`.
            since=datetime.datetime(2026, 10, 1, 4, tzinfo=PARIS),
            until=datetime.datetime(2026, 10, 1, 3, tzinfo=datetime.UTC),
        )
    )

    job(window, {})

    assert [row["id"] for row in loader.rows] == [2, 3]
