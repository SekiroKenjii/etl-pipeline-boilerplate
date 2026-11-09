"""A cutover reconciliation over in-memory tables: `src` mapped to `dst`
by id, the name upper-cased, a row without a name rejected.

Shared by the `test_cutover*.py` files.
"""

from __future__ import annotations

from typing import Any

from core import contract
from tests.factories import fake_ports


def expect_upper(row: contract.Row) -> contract.Expected:
    """A mapping: id and name, uppercase; a row without name is rejected."""
    if not row.get("name"):
        return contract.Expected(rejected=["name: required"])
    return contract.Expected(
        rows=[{"id": row["id"], "name": str(row["name"]).upper()}]
    )


def reconciliation(
    source: list[dict[str, Any]],
    target: list[dict[str, Any]],
    *,
    filters: tuple[contract.Condition, ...] = (),
) -> contract.Reconciliation:
    """The reconciliation of the entity `customer`, keyed by `id`."""
    reader = fake_ports.InMemoryTableReader({"src": source, "dst": target})
    return contract.Reconciliation(
        entity="customer",
        source=reader,
        source_query=contract.TableQuery("src", order_by=("id",)),
        expect=expect_upper,
        target=reader,
        target_query=contract.TableQuery(
            "dst", order_by=("id",), filters=filters
        ),
        key=("id",),
        columns=("id", "name"),
        parking_queue="customer",
    )
