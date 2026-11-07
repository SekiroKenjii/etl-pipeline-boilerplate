from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from core import contract
from core import mapping
from core import pipeline
from core import quality
from core.mapping import catalog
from tests.factories import fakes
from tests.factories import mapping_specs

CUSTOMER_FIELDS = (
    mapping_specs.field("customer_id", "CUST_NO", target_type="NUMBER(10)"),
)
PHONE_FIELDS = (
    mapping_specs.field("customer_id", "CUST_NO", target_type="NUMBER(10)"),
    mapping_specs.field("kind", "KIND", row=3),
    mapping_specs.field("number", "NUMBER", target_type="VARCHAR(20)", row=4),
)


def _transformer(
    *fields: Any, **options: Any
) -> mapping.MappingTransformer[Any]:
    spec = mapping_specs.mapping(*fields, **options)
    return mapping.MappingTransformer(
        mapping.compile_catalog(
            mapping_specs.catalog(spec), mapping_specs.registry
        )["customer"]
    )


def _map(
    transformer: mapping.MappingTransformer[Any], rows: list[dict[str, Any]]
) -> list[mapping.MappedRecord]:
    return list(transformer.transform(rows, fakes.make_context()))


class TestRowFilter:

    def test_filtered_rows_are_dropped_and_counted(self) -> None:
        transformer = _transformer(
            *CUSTOMER_FIELDS,
            row_filter="customer.not_deleted",
            ignored_source_fields=("DELETED",),
        )

        records = _map(
            transformer,
            [
                {"CUST_NO": "1", "DELETED": "N"},
                {"CUST_NO": "2", "DELETED": "Y"},
                {"CUST_NO": "3", "DELETED": "N"},
            ],
        )

        assert [r.data["customer_id"] for r in records] == [1, 3]
        assert transformer.collect_stats().filtered == 1
        assert transformer.collect_stats().filtered == 0  # reset once collected

    def test_a_failing_filter_turns_the_row_into_an_issue(self) -> None:
        transformer = _transformer(
            *CUSTOMER_FIELDS, row_filter="customer.broken_filter"
        )

        (record,) = _map(transformer, [{"CUST_NO": "1"}])

        assert [(i.field, i.message) for i in record.issues] == [(
            "*",
            "row filter 'customer.broken_filter' failed: KeyError: 'STATUS'",
        )]


class TestRowSplit:

    def test_one_source_row_becomes_several_target_rows(self) -> None:
        transformer = _transformer(*PHONE_FIELDS, row_split="customer.phones")

        records = _map(
            transformer,
            [{"CUST_NO": "7", "HOME_PHONE": "0281", "WORK_PHONE": "0909"}],
        )

        assert [(r.data, r.part) for r in records] == [
            ({"customer_id": 7, "kind": "home", "number": "0281"}, 0),
            ({"customer_id": 7, "kind": "work", "number": "0909"}, 1),
        ]

    def test_the_shape_check_applies_to_the_split_rows(self) -> None:
        # HOME_PHONE / WORK_PHONE are consumed by the splitter, not "unmapped"
        transformer = _transformer(*PHONE_FIELDS, row_split="customer.phones")

        assert (
            len(
                _map(
                    transformer,
                    [{"CUST_NO": "7", "HOME_PHONE": "0281", "WORK_PHONE": ""}],
                )
            )
            == 1
        )

    def test_a_row_split_into_nothing_counts_as_filtered(self) -> None:
        transformer = _transformer(*PHONE_FIELDS, row_split="customer.phones")

        assert (
            _map(
                transformer,
                [{"CUST_NO": "7", "HOME_PHONE": "", "WORK_PHONE": None}],
            )
            == []
        )
        assert transformer.collect_stats().filtered == 1

    def test_a_splitter_returning_non_rows_is_an_issue(self) -> None:
        transformer = _transformer(
            *CUSTOMER_FIELDS, row_split="customer.not_rows"
        )

        (record,) = _map(transformer, [{"CUST_NO": "1"}])

        assert (
            "every row it returns must be a mapping" in record.issues[0].message
        )


class Rows(pipeline.IDataExtractor[dict[str, Any]]):

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def extract(self, ctx: contract.ETLContext) -> Iterator[dict[str, Any]]:
        yield from self.rows


def test_in_a_job_filtered_rows_are_counted_and_failing_ones_quarantined() -> (
    None
):
    spec = mapping_specs.mapping(
        *PHONE_FIELDS,
        row_split="customer.phones",
        row_filter="customer.not_deleted",
        ignored_source_fields=("DELETED",),
    )
    compiled = mapping.compile_catalog(
        mapping_specs.catalog(spec), mapping_specs.registry
    )["customer"]
    quarantine = quality.InMemoryQuarantine()
    gate = quality.QualityGate(
        {
            contract.Checkpoint.AFTER_TRANSFORM: quality.CheckSet(
                [mapping.MappingIssuesRule()]
            )
        },
        quarantine=quarantine,
    )
    loader = fakes.CollectingLoader[mapping.MappedRecord]()
    rows = [
        {
            "CUST_NO": "1",
            "HOME_PHONE": "0281",
            "WORK_PHONE": "0909",
            "DELETED": "N",
        },
        {
            "CUST_NO": "2",
            "HOME_PHONE": "0282",
            "WORK_PHONE": "",
            "DELETED": "Y",
        },
        {
            "CUST_NO": "x",
            "HOME_PHONE": "0283",
            "WORK_PHONE": "",
            "DELETED": "N",
        },
    ]

    result = pipeline.ETLJob(
        Rows(rows), mapping.MappingTransformer(compiled), loader, gate
    )(fakes.make_context(), {})

    assert [r.data["number"] for r in loader.rows] == ["0281", "0909"]
    assert (
        result.stats.total,
        result.stats.filtered,
        result.stats.rejected,
    ) == (
        3,
        1,
        1,
    )
    assert quarantine.records[0].issues[0].field == "customer_id"


class TestValidation:

    def test_unknown_hooks_point_at_the_mappings_sheet(self) -> None:
        spec = mapping_specs.mapping(
            *CUSTOMER_FIELDS,
            row_filter="customer.not_delete",
            row_split="customer.phone",
        )

        problems = [
            str(p)
            for p in mapping.validate_catalog(
                mapping_specs.catalog(spec), mapping_specs.registry
            )
        ]

        assert problems == [
            (
                "crm.xlsx › Mappings › row 2 › Row filter: unknown row filter"
                " 'customer.not_delete'. Did you mean 'customer.not_deleted'?"
            ),
            (
                "crm.xlsx › Mappings › row 2 › Row split: unknown row splitter"
                " 'customer.phone'. Did you mean 'customer.phones'?"
            ),
        ]

    def test_a_hook_must_take_the_row_only(self) -> None:
        hooks = mapping.TransformRegistry()

        @hooks.register_filter("customer.two_args")
        def two_args(row: object, other: object) -> bool:
            return True

        spec = mapping_specs.mapping(
            *CUSTOMER_FIELDS, row_filter="customer.two_args"
        )

        (problem,) = mapping.validate_catalog(
            mapping_specs.catalog(spec), hooks
        )

        assert "must take the source row as its only argument" in str(problem)

    def test_hooks_survive_the_compiled_json(self) -> None:
        original = mapping_specs.catalog(
            mapping_specs.mapping(
                *CUSTOMER_FIELDS,
                row_filter="customer.not_deleted",
                row_split="customer.phones",
            )
        )

        assert catalog.loads(catalog.dumps(original)) == original


def test_registries_merge_their_hooks() -> None:
    merged = mapping.TransformRegistry()
    merged.include(mapping_specs.registry)

    assert "customer.not_deleted" in merged.filter_names()
    assert "customer.phones" in merged.splitter_names()
