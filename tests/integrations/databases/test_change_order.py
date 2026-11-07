"""Changes of one row in one batch, applied in their order, on real targets."""

from __future__ import annotations

from typing import Any

import pytest
import sqlalchemy as sa

from core import contract
from core import operators
from infra.database import relational
from tests.factories import fakes
from tests.integrations.databases import conftest

UPDATE = contract.ChangeOperation.UPDATE
DELETE = contract.ChangeOperation.DELETE


def _event(
    operation: contract.ChangeOperation, row: dict[str, Any]
) -> contract.ChangeEvent:
    return contract.ChangeEvent("t", operation, row, (row["id"],), 1)


@pytest.mark.parametrize(
    "deletes", [operators.Deletes.HARD, operators.Deletes.SOFT]
)
def test_the_last_change_of_a_row_wins(
    backend: conftest.Backend, deletes: operators.Deletes
) -> None:
    target = backend.table(
        "target",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("name", sa.Unicode(20)),
        sa.Column(
            "is_deleted", sa.Boolean, nullable=False, server_default=sa.false()
        ),
    )
    backend.create()
    backend.insert(
        target,
        {"id": 1, "name": "a", "is_deleted": False},
        {"id": 2, "name": "b", "is_deleted": False},
    )
    loader = operators.ChangeLoader[contract.ChangeEvent](
        relational.RelationalTableWriter(backend.db),
        contract.TableTarget(target.name, key=("id",)),
        row=lambda e: e.row,
        deleted=lambda e: e.deleted,
        deletes=deletes,
    )

    with loader.lifecycle(fakes.make_context()):
        loader.load(
            [
                _event(UPDATE, {"id": 1, "name": "A"}),
                _event(DELETE, {"id": 1}),
                _event(DELETE, {"id": 2}),
                _event(UPDATE, {"id": 2, "name": "B"}),
            ],
            fakes.make_context(),
        )

    rows = backend.rows(target, "id", "name", "is_deleted")
    if deletes is operators.Deletes.HARD:
        assert rows == [(2, "B", False)]
    else:
        assert rows == [(1, "A", True), (2, "B", False)]
