"""What changed between two catalogs: fields, keys, lookups, and reach."""

from __future__ import annotations

import dataclasses
import io
import pathlib

from cli import mappings as mappings_cli
from core import contract
from core import mapping
from tests.factories import mapping_specs

field = mapping_specs.field


def _catalog(
    *specs: contract.MappingSpec, **lookups: dict[str, str]
) -> contract.MappingCatalog:
    return mapping_specs.catalog(*specs, lookups=lookups)


CUSTOMER = mapping_specs.mapping(
    field("customer_key", "CUST_NO", primary_key=True),
    field("name", "NAME", transform="trim"),
    field("country", "CTRY", transform="lookup", params={"table": "countries"}),
)
ORDER = mapping_specs.mapping(
    field("order_key", "ORD_NO", primary_key=True),
    field("customer_key", "CUST_NO", references="customer"),
    name="order",
    source="legacy.ORD",
    target="crm.order",
)


def test_the_same_catalog_has_no_change() -> None:
    catalog = _catalog(CUSTOMER, ORDER, countries={"VN": "VN"})

    changes = mapping.compare_catalogs(catalog, catalog)

    assert not changes
    assert mapping.describe_changes(changes) == "No change.\n"


def test_changes_and_what_they_reach() -> None:
    old = _catalog(CUSTOMER, ORDER, countries={"VN": "VN", "US": "US"})
    fields = list(CUSTOMER.fields)
    fields[1] = dataclasses.replace(fields[1], transform="upper")
    fields.append(field("email", "MAIL", personal="email"))
    customer = dataclasses.replace(
        CUSTOMER,
        fields=tuple(fields),
        key_case=contract.KeyCase.UPPER,
        version=2,
    )
    new = _catalog(customer, ORDER, countries={"VN": "Viet Nam", "FR": "FR"})

    changes = mapping.compare_catalogs(old, new)
    text = mapping.describe_changes(changes)

    assert changes.lines == [
        "customer: version 1 → 2",
        "customer: key_case preserve → upper",
        "customer.name: transform 'trim' → 'upper'",
        "customer.email: added",
        "lookup countries: 'FR' - → 'FR'",
        "lookup countries: 'US' 'US' → -",
        "lookup countries: 'VN' 'VN' → 'Viet Nam'",
    ]
    assert changes.rekeyed == ["customer"]
    assert changes.affected == ["order"]
    assert "Surrogate keys change for: customer" in text
    assert "Also check, as they reference changed mappings: order" in text


def test_mappings_added_and_removed() -> None:
    changes = mapping.compare_catalogs(_catalog(CUSTOMER), _catalog(ORDER))

    assert changes.lines == ["customer: removed", "order: added"]


def test_the_command_compares_compiled_and_workbook() -> None:
    root = pathlib.Path(__file__).resolve().parents[3] / "mappings"
    out = io.StringIO()

    code = mappings_cli.main(
        ["diff", str(root / "compiled" / "crm.json"), str(root / "crm.xlsx")],
        out,
    )

    assert (code, out.getvalue()) == (0, "No change.\n")
