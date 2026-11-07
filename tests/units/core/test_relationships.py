from __future__ import annotations

from typing import Any

import pytest

from core import contract
from core import keys
from tests.factories import fake_ports

_FAMILIES = {
    "integer": contract.TypeFamily.INTEGER,
    "bigint": contract.TypeFamily.INTEGER,
    "text": contract.TypeFamily.TEXT,
    "character varying(10)": contract.TypeFamily.TEXT,
}


def _table(
    name: str, *columns: tuple[str, str], primary_key: tuple[str, ...] = ()
) -> contract.TableInfo:
    return contract.TableInfo(
        name=f"legacy.{name}",
        columns=tuple(
            contract.ColumnInfo(n, t, True, _FAMILIES[t]) for n, t in columns
        ),
        primary_key=primary_key,
    )


TABLES = [
    _table(
        "customers",
        ("id", "integer"),
        ("cust_no", "character varying(10)"),
        primary_key=("id",),
    ),
    _table(
        "orders",
        ("ord_no", "text"),
        ("customer_id", "bigint"),
        ("cust_no", "text"),
        ("paid", "integer"),
    ),
    _table("addresses", ("id", "integer"), ("order_id", "integer")),
]
ROWS: dict[str, list[contract.Row]] = {
    "legacy.customers": [
        {"id": 1, "cust_no": "C1"},
        {"id": 2, "cust_no": "C2"},
    ],
    "legacy.orders": [
        {"ord_no": "O1", "customer_id": 1, "cust_no": "C1", "paid": 1},
        {"ord_no": "O2", "customer_id": 2, "cust_no": "C2", "paid": 1},
        {"ord_no": "O3", "customer_id": 9, "cust_no": None, "paid": 2},
        {"ord_no": "O4", "customer_id": 1, "cust_no": "C1", "paid": 2},
    ],
    "legacy.addresses": [{"id": 1, "order_id": 1}, {"id": 2, "order_id": 1}],
}


def _report(**options: Any) -> keys.RelationshipReport:
    inspector = fake_ports.InMemorySchemaInspector(TABLES, ROWS)
    return keys.discover_relationships(inspector, "legacy", **options)


def test_candidate_keys_are_declared_or_profiled() -> None:
    report = _report()

    assert [(k.table, k.columns, k.evidence) for k in report.keys] == [
        ("legacy.addresses", ("id",), keys.Evidence.PROFILED),
        ("legacy.customers", ("id",), keys.Evidence.DECLARED),
        ("legacy.customers", ("cust_no",), keys.Evidence.PROFILED),
        ("legacy.orders", ("ord_no",), keys.Evidence.PROFILED),
    ]


def test_foreign_keys_are_inferred_from_names_types_and_values() -> None:
    report = _report(min_coverage=0.5)

    assert [
        (r.table, r.columns[0], r.referenced_table, r.coverage, r.orphans)
        for r in report.relationships
    ] == [
        ("legacy.orders", "cust_no", "legacy.customers", 1.0, 0),
        (
            "legacy.orders",
            "customer_id",
            "legacy.customers",
            pytest.approx(2 / 3),
            1,
        ),
    ]


def test_foreign_keys_with_too_many_orphans_are_not_suggested() -> None:
    report = _report()

    assert [r.columns for r in report.relationships] == [("cust_no",)]


def test_declared_foreign_keys_come_first_and_are_not_checked() -> None:
    declared = contract.ForeignKeyInfo(
        "orders_customer_fk",
        "legacy.orders",
        ("customer_id",),
        "legacy.customers",
        ("id",),
    )
    inspector = fake_ports.InMemorySchemaInspector(TABLES, ROWS, [declared])

    report = keys.discover_relationships(inspector, "legacy")

    first = report.relationships[0]
    assert (first.evidence, first.name, first.coverage) == (
        keys.Evidence.DECLARED,
        "orders_customer_fk",
        None,
    )
    assert ("legacy.orders", "customer_id") not in {
        (t, c) for t, c, _, _ in inspector.inclusions
    }


def test_without_name_matching_every_compatible_pair_is_checked() -> None:
    inspector = fake_ports.InMemorySchemaInspector(TABLES, ROWS)

    report = keys.discover_relationships(inspector, "legacy", match_names=False)

    # `paid` (1, 1, 2) is in customers.id (1, 2): values alone cannot tell.
    assert ("legacy.orders", ("paid",), "legacy.customers") in {
        (r.table, r.columns, r.referenced_table) for r in report.relationships
    }
    # Text never compares with numbers.
    assert ("legacy.orders", "ord_no", "legacy.customers", "id") not in (
        inspector.inclusions
    )


def test_a_sample_is_reported() -> None:
    report = _report(sample_rows=1)

    assert report.sampled
    assert "confirm on the full table" in report.to_text()


def test_the_report_reads_as_text_and_json() -> None:
    report = _report(min_coverage=0.5)

    text = report.to_text()
    data = report.as_dict()

    assert (
        "legacy.orders (customer_id) -> legacy.customers (id)  inferred"
        "  coverage 66.7%, 1 orphan value(s)"
    ) in text
    assert data["schema"] == "legacy"
    assert isinstance(data["relationships"], list)


def test_min_coverage_is_a_share() -> None:
    with pytest.raises(ValueError, match="min_coverage"):
        _report(min_coverage=0)


def test_a_declared_key_is_a_parent_not_a_child_of_a_profiled_key() -> None:
    customers = contract.TableInfo(
        "legacy.customers",
        (
            contract.ColumnInfo(
                "cust_no", "text", False, contract.TypeFamily.TEXT
            ),
        ),
        unique=(("cust_no",),),
    )
    orders = _table("orders", ("cust_no", "text"))
    rows: dict[str, list[contract.Row]] = {
        "legacy.customers": [{"cust_no": "C1"}, {"cust_no": "C2"}],
        "legacy.orders": [{"cust_no": "C1"}],
    }
    inspector = fake_ports.InMemorySchemaInspector([customers, orders], rows)

    report = keys.discover_relationships(inspector, "legacy")

    assert [(r.table, r.referenced_table) for r in report.relationships] == [
        ("legacy.orders", "legacy.customers")
    ]


def test_tables_can_be_picked_by_name() -> None:
    report = _report(tables="ORDER*", min_coverage=0.5)

    assert {k.table for k in report.keys} == {"legacy.orders"}
    assert report.relationships == ()


def test_keys_without_described_columns_are_skipped() -> None:
    empty = contract.TableInfo("legacy.empty", (), primary_key=("_id",))
    inspector = fake_ports.InMemorySchemaInspector(
        [*TABLES, empty], {**ROWS, "legacy.empty": []}
    )

    report = keys.discover_relationships(inspector, "legacy")

    assert ("legacy.empty", ("_id",)) in {
        (k.table, k.columns) for k in report.keys
    }
