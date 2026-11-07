"""Mappings of documents: nested fields, and arrays exploded into rows."""

from __future__ import annotations

from typing import Any

import pytest

from core import contract
from core import mapping
from tests.factories import mapping_specs

CUSTOMER = mapping_specs.mapping(
    mapping_specs.field(
        "customer_key", "_id", target_type="UUID", primary_key=True
    ),
    mapping_specs.field("city", "address.city", row=3),
    mapping_specs.field("phone", "phones.0", row=4),
    name="customer",
)
# The same customer, leaving the addresses to their own mapping.
CUSTOMER_ONLY = mapping_specs.mapping(
    *CUSTOMER.fields, name="customer", ignored_source_fields=("addresses",)
)
ADDRESS = mapping_specs.mapping(
    mapping_specs.field(
        "address_key",
        "_id",
        "addresses#",
        target_type="UUID",
        primary_key=True,
    ),
    mapping_specs.field(
        "customer_key",
        "_id",
        target_type="UUID",
        references="customer",
        row=3,
    ),
    mapping_specs.field("street", "addresses.street", row=4),
    name="address",
    row_split="explode(addresses)",
    ignored_source_fields=("address", "phones"),
)


def _compiled(*mappings: Any) -> dict[str, mapping.CompiledMapping]:
    return mapping.compile_catalog(
        mapping_specs.catalog(*mappings), mapping_specs.registry
    )


DOCUMENT = {
    "_id": "c1",
    "address": {"city": "Genève"},
    "phones": ["0901", "0902"],
    "addresses": [{"street": "Königstraße"}, {"street": "Rua São Bento"}],
}


class TestNestedFields:

    def test_field_paths_read_nested_documents_and_arrays(self) -> None:
        customer = _compiled(CUSTOMER_ONLY)["customer"]

        (record,) = customer.map(DOCUMENT)

        assert (record.data["city"], record.data["phone"]) == ("Genève", "0901")
        assert record.ok

    def test_a_missing_nested_field_is_empty(self) -> None:
        customer = _compiled(
            mapping_specs.mapping(
                *CUSTOMER.fields,
                name="customer",
                unmapped_source_fields=contract.UnmappedPolicy.IGNORE,
            )
        )["customer"]

        (record,) = customer.map({"_id": "c2", "address": None, "phones": []})

        assert (record.data["city"], record.data["phone"]) == (None, None)

    def test_the_root_field_must_exist(self) -> None:
        customer = _compiled(CUSTOMER)["customer"]

        with pytest.raises(contract.FatalError, match=r"\['address.city'\]"):
            customer.map({"_id": "c3", "phones": []})

    def test_used_roots_are_not_unmapped(self) -> None:
        customer = _compiled(CUSTOMER)["customer"]

        with pytest.raises(contract.FatalError, match=r"\['addresses'\]"):
            customer.map(DOCUMENT)

    def test_a_column_named_with_a_dot_is_read_as_it_is(self) -> None:
        dotted = _compiled(
            mapping_specs.mapping(
                mapping_specs.field("v", "a.b"), name="dotted"
            )
        )["dotted"]

        (record,) = dotted.map({"a.b": 1})

        assert record.data["v"] == 1


class TestExplode:

    def test_one_row_per_element_with_its_position(self) -> None:
        compiled = _compiled(CUSTOMER_ONLY, ADDRESS)

        rows = compiled["address"].map(DOCUMENT)
        (customer,) = compiled["customer"].map(DOCUMENT)

        assert [r.data["street"] for r in rows] == [
            "Königstraße",
            "Rua São Bento",
        ]
        assert [r.natural_key for r in rows] == [("c1", "0"), ("c1", "1")]
        assert {r.data["customer_key"] for r in rows} == {customer.key}
        assert len({r.key for r in rows}) == 2

    def test_an_empty_or_missing_array_makes_no_row(self) -> None:
        transformer = mapping.MappingTransformer[dict[str, Any]](
            _compiled(CUSTOMER, ADDRESS)["address"]
        )

        rows = list(
            transformer.transform(
                [
                    {**DOCUMENT, "addresses": []},
                    {"_id": "c9", "address": None, "phones": None},
                ],
                contract.ETLContext(contract.ETLRunInfo("crm")),
            )
        )

        assert rows == []
        assert transformer.collect_stats().filtered == 2

    def test_a_value_that_is_not_an_array_is_an_issue_of_the_row(self) -> None:
        compiled = _compiled(CUSTOMER, ADDRESS)

        (failed,) = compiled["address"].map({**DOCUMENT, "addresses": "x"})

        assert "addresses is not an array" in failed.issues[0].message

    def test_nested_arrays(self) -> None:
        phones = _compiled(
            mapping_specs.mapping(
                mapping_specs.field("number", "profile.phones.number"),
                name="phone",
                row_split="explode(profile.phones)",
                unmapped_source_fields=contract.UnmappedPolicy.IGNORE,
            )
        )["phone"]

        rows = phones.map(
            {"profile": {"phones": [{"number": "1"}, {"number": "2"}]}}
        )

        assert [r.data["number"] for r in rows] == ["1", "2"]

    def test_explode_needs_no_registration_and_is_validated(self) -> None:
        bad = mapping_specs.mapping(
            mapping_specs.field("v", "a"),
            name="bad",
            row_split="explode(not a path)",
        )

        (problem,) = mapping.validate_catalog(
            mapping_specs.catalog(CUSTOMER, ADDRESS, bad),
            mapping_specs.registry,
        )

        assert "unknown row splitter 'explode(not a path)'" in str(problem)
