from __future__ import annotations

from collections.abc import Iterator
import dataclasses
from typing import Any

import pytest

from core import contract
from core import mapping
from core import pipeline
from core import quality
from core.mapping import catalog
from tests.factories import fakes
from tests.factories import mapping_specs

FIELDS = (
    mapping_specs.field(
        "customer_id", "CUST_NO", target_type="NUMBER(10)", required=True
    ),
    mapping_specs.field("name", "NAME", target_type="VARCHAR(50)", row=3),
)


def _transformer(**options: Any) -> mapping.MappingTransformer[Any]:
    spec = mapping_specs.mapping(*FIELDS, **options)
    return mapping.MappingTransformer(
        mapping.compile_catalog(
            mapping_specs.catalog(spec), mapping_specs.registry
        )["customer"]
    )


class TestSourceShape:

    def test_unmapped_source_columns_fail_the_run(self) -> None:
        transformer = _transformer()
        records = transformer.transform(
            [{"CUST_NO": 1, "NAME": "a", "LEGACY_FLAG": "x"}],
            fakes.make_context(),
        )

        with pytest.raises(
            contract.FatalError, match=r"\['LEGACY_FLAG'\] are not mapped"
        ):
            list(records)

    def test_ignored_columns_are_fine(self) -> None:
        transformer = _transformer(ignored_source_fields=("LEGACY_FLAG",))

        (record,) = transformer.transform(
            [{"CUST_NO": 1, "NAME": "a", "LEGACY_FLAG": "x"}],
            fakes.make_context(),
        )

        assert record.data == {"customer_id": 1, "name": "a"}

    def test_policy_ignore_accepts_extra_columns(self) -> None:
        transformer = _transformer(
            unmapped_source_fields=contract.UnmappedPolicy.IGNORE
        )

        assert (
            len(
                list(
                    transformer.transform(
                        [{"CUST_NO": 1, "NAME": "a", "X": 0}],
                        fakes.make_context(),
                    )
                )
            )
            == 1
        )

    def test_a_missing_source_column_fails_the_run(self) -> None:
        records = _transformer().transform(
            [{"CUST_NO": 1}], fakes.make_context()
        )

        with pytest.raises(
            contract.FatalError, match=r"has no column\(s\) \['NAME'\]"
        ):
            list(records)

    def test_rows_must_be_mappings(self) -> None:
        records = _transformer().transform(
            [("not", "a", "mapping")], fakes.make_context()
        )

        with pytest.raises(contract.FatalError, match="pass `row=`"):
            list(records)


def test_rows_and_keys_can_come_from_wrapped_records() -> None:
    @dataclasses.dataclass
    class Wrapped:
        key: str
        data: dict[str, Any]

    compiled = mapping.compile_catalog(
        mapping_specs.catalog(mapping_specs.mapping(*FIELDS)),
        mapping_specs.registry,
    )["customer"]
    transformer = mapping.MappingTransformer[Wrapped](
        compiled, row=lambda r: r.data, key=lambda r: r.key
    )

    (record,) = transformer.transform(
        [Wrapped("cust.csv#0", {"CUST_NO": "7", "NAME": "b"})],
        fakes.make_context(),
    )

    assert (record.data["customer_id"], record.source_key) == (7, "cust.csv#0")


class Rows(pipeline.IDataExtractor[dict[str, Any]]):

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def extract(self, ctx: contract.ETLContext) -> Iterator[dict[str, Any]]:
        yield from self.rows


def test_bad_legacy_rows_are_quarantined_by_the_gate() -> None:
    compiled = mapping.compile_catalog(
        mapping_specs.catalog(mapping_specs.mapping(*FIELDS)),
        mapping_specs.registry,
    )["customer"]
    quarantine = quality.InMemoryQuarantine()
    gate = quality.QualityGate(
        {
            contract.Checkpoint.AFTER_TRANSFORM: quality.CheckSet(
                [mapping.MappingIssuesRule()], record_id=lambda r: r.source_key
            )
        },
        quarantine=quarantine,
    )
    loader = fakes.CollectingLoader[mapping.MappedRecord]()
    rows = [
        {"CUST_NO": "1", "NAME": "Ana"},
        {"CUST_NO": "oops", "NAME": "Bo"},
        {"CUST_NO": "3", "NAME": "Chi"},
    ]
    job = pipeline.ETLJob(
        Rows(rows), mapping.MappingTransformer(compiled), loader, gate
    )

    result = job(fakes.make_context(), {})

    assert [record.data["customer_id"] for record in loader.rows] == [1, 3]
    (rejected,) = quarantine.records
    assert [(i.rule, i.field, i.message) for i in rejected.issues] == [
        ("mapping_issues", "customer_id", "'oops' is not a number (NUMBER(10))")
    ]
    assert result.stats.rejected == 1
    assert result.status is contract.ETLStatus.PARTIAL_SUCCESS


class TestCompiledCatalog:

    def test_json_round_trip(self) -> None:
        original = mapping_specs.catalog(
            mapping_specs.mapping(
                *FIELDS, version=4, ignored_source_fields=("X",)
            ),
            lookups={"countries": {"VN": "VN"}},
        )

        assert catalog.loads(catalog.dumps(original)) == original

    def test_output_is_stable(self) -> None:
        lookups = {
            "countries": {"VN": "Viet Nam", "FR": "France"},
            "status": {"A": "active", "I": "inactive"},
        }
        # The same lookups, built in the opposite order.
        reordered = {
            name: dict(reversed(table.items()))
            for name, table in reversed(lookups.items())
        }

        text = catalog.dumps(
            mapping_specs.catalog(
                mapping_specs.mapping(*FIELDS), lookups=lookups
            )
        )

        assert text == catalog.dumps(
            mapping_specs.catalog(
                mapping_specs.mapping(*FIELDS), lookups=reordered
            )
        )

    def test_locations_of_files_compiled_before_are_read(self) -> None:
        text = catalog.dumps(
            mapping_specs.catalog(mapping_specs.mapping(*FIELDS))
        ).replace(
            '"location": "crm.xlsx › Mappings › row 2"',
            '"location": {"file": "crm.xlsx", "row": 2, "sheet": "Mappings"}',
        )

        (read,) = catalog.loads(text).mappings

        assert read.location == "crm.xlsx › Mappings › row 2"

    def test_unknown_format_versions_are_refused(self) -> None:
        with pytest.raises(
            ValueError, match="Unsupported compiled mapping format"
        ):
            catalog.loads('{"format_version": 99}')
