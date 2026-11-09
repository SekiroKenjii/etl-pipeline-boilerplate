from __future__ import annotations

import dataclasses
import datetime
from typing import Any

import pytest

from core import contract
from core import mapping
from tests.factories import mapping_specs

LOOKUPS = {"countries": {"VN": "VN", "VIETNAM": "VN"}}


def _compile(
    *fields: Any, lookups: dict[str, dict[str, str]] | None = None
) -> mapping.CompiledMapping:
    built = mapping.compile_catalog(
        mapping_specs.catalog(
            mapping_specs.mapping(*fields), lookups=lookups or LOOKUPS
        ),
        mapping_specs.registry,
    )
    return built["customer"]


def _problems(*fields: Any, **options: Any) -> list[str]:
    lookups = options.pop("lookups", LOOKUPS)
    found = mapping.validate_catalog(
        mapping_specs.catalog(
            mapping_specs.mapping(*fields, **options), lookups=lookups
        ),
        mapping_specs.registry,
    )
    return [str(problem) for problem in found]


class TestApply:

    def test_one_target_column_composed_from_several_source_columns(
        self,
    ) -> None:
        compiled = _compile(
            mapping_specs.field(
                "address",
                "STREET",
                "WARD",
                "DISTRICT",
                transform="customer.full_address",
                target_type="VARCHAR(500)",
            )
        )

        record = compiled.apply(
            {"STREET": " 12 Königstraße ", "WARD": "", "DISTRICT": "District 1"}
        )

        assert record.data == {"address": "12 Königstraße, District 1"}
        assert record.ok

    def test_cast_with_the_source_format(self) -> None:
        compiled = _compile(
            mapping_specs.field(
                "created_at", "CRT_DT", target_type="DATE", format="dd/MM/yyyy"
            )
        )

        assert compiled.apply({"CRT_DT": "31/12/2020"}).data == {
            "created_at": datetime.date(2020, 12, 31)
        }

    def test_value_map_with_a_default(self) -> None:
        compiled = _compile(
            mapping_specs.field(
                "status",
                "STAT_CD",
                transform="value_map",
                params={"A": "active", "I": "inactive"},
                default="unknown",
            )
        )

        assert [
            compiled.apply({"STAT_CD": code}).data["status"]
            for code in ("A", "Z", "")
        ] == [
            "active",
            "unknown",
            "unknown",
        ]

    def test_untranslated_value_without_default_is_an_issue(self) -> None:
        compiled = _compile(
            mapping_specs.field(
                "country",
                "CTRY",
                transform="lookup",
                params={"table": "countries"},
            )
        )

        ok, bad = compiled.apply({"CTRY": "VIETNAM"}), compiled.apply(
            {"CTRY": "Narnia"}
        )

        assert ok.data == {"country": "VN"}
        assert bad.data == {"country": None}
        assert [(i.field, i.message, i.value) for i in bad.issues] == [
            ("country", "no translation for 'Narnia'", "Narnia")
        ]

    def test_required_and_cast_issues_are_collected_per_field(self) -> None:
        compiled = _compile(
            mapping_specs.field(
                "customer_id",
                "CUST_NO",
                target_type="NUMBER(10)",
                required=True,
            ),
            mapping_specs.field(
                "name", "NAME", target_type="VARCHAR(3)", row=3
            ),
        )

        record = compiled.apply({"CUST_NO": "  ", "NAME": "Alexandra"})

        assert [(i.field, i.message) for i in record.issues] == [
            ("customer_id", "is required but empty"),
            ("name", "9 characters, more than the 3 of VARCHAR(3)"),
        ]

    def test_constant_from_a_default(self) -> None:
        compiled = _compile(
            mapping_specs.field("source_system", default="LEGACY")
        )

        assert compiled.apply({}).data == {"source_system": "LEGACY"}

    def test_a_failing_custom_transform_is_an_issue_not_a_crash(self) -> None:
        compiled = _compile(
            mapping_specs.field("weird", "W", transform="customer.explode")
        )

        record = compiled.apply({"W": "x"})

        assert record.issues[0].message == (
            "transform 'customer.explode' failed: ValueError: cannot parse"
        )

    def test_the_error_of_a_transform_is_one_line(self) -> None:
        registry = mapping.TransformRegistry()

        @registry.register("customer.multiline")
        def multiline(value: object) -> object:
            raise ValueError(
                f"cannot parse {value!s}\n# PASSED\r\n| x |\x1b[0m"
            )

        compiled = mapping.compile_catalog(
            mapping_specs.catalog(
                mapping_specs.mapping(
                    mapping_specs.field(
                        "weird", "W", transform="customer.multiline"
                    )
                )
            ),
            registry,
        )["customer"]

        record = compiled.apply({"W": "x"})

        assert record.issues[0].message == (
            "transform 'customer.multiline' failed: ValueError: cannot parse"
            " x\\n# PASSED\\r\\n| x |\\x1b[0m"
        )

    def test_records_carry_their_lineage(self) -> None:
        compiled = mapping.compile_catalog(
            mapping_specs.catalog(
                mapping_specs.mapping(
                    mapping_specs.field("id", "ID"), version=7
                )
            ),
            mapping_specs.registry,
        )["customer"]

        record = compiled.apply({"ID": 1}, source_key="cust.csv#3")

        assert (record.mapping, record.version, record.source_key) == (
            "customer",
            7,
            "cust.csv#3",
        )


class TestValidation:

    def test_unknown_transform_points_at_the_cell_with_a_suggestion(
        self,
    ) -> None:
        assert _problems(
            mapping_specs.field("status", "S", transform="valuemap", row=4)
        ) == [
            "crm.xlsx › customer › row 4 › Transform: unknown transform"
            " 'valuemap'. Did you mean 'value_map'?"
        ]

    def test_custom_transform_signature_is_checked_against_the_sheet(
        self,
    ) -> None:
        problems = _problems(
            mapping_specs.field(
                "address",
                "STREET",
                "WARD",
                transform="customer.full_address",
                params={"sep": "-"},
            )
        )

        assert problems == [
            (
                "crm.xlsx › customer › row 2 › Source columns:"
                " 'customer.full_address' takes exactly 3 source column(s), 2"
                " given"
            ),
            (
                "crm.xlsx › customer › row 2 › Parameters:"
                " 'customer.full_address' has no parameter 'sep' (accepted:"
                " separator)"
            ),
        ]

    @pytest.mark.parametrize(
        ("spec", "expected"),
        [
            (
                mapping_specs.field("c", "CTRY", transform="lookup"),
                "needs the parameter 'table'",
            ),
            (
                mapping_specs.field(
                    "c", "CTRY", transform="lookup", params={"table": "nations"}
                ),
                "no lookup table 'nations' (known: countries)",
            ),
            (
                mapping_specs.field("s", "S", transform="value_map"),
                "value_map needs its translations",
            ),
            (
                mapping_specs.field("n", "FIRST", "LAST"),
                "several source columns need a transform",
            ),
            (mapping_specs.field("x"), "nothing to map"),
            (
                mapping_specs.field("x", "X", target_type="MONEY"),
                "unknown type 'MONEY'",
            ),
            (
                mapping_specs.field(
                    "x", "X", target_type="VARCHAR(10)", format="dd/MM/yyyy"
                ),
                "only applies to DATE",
            ),
            (
                mapping_specs.field(
                    "x", "X", target_type="NUMBER(3)", default="abc"
                ),
                "the default 'abc' is not valid",
            ),
        ],
    )
    def test_field_problems(self, spec: Any, expected: str) -> None:
        problems = _problems(spec)

        assert len(problems) == 1, problems
        assert expected in problems[0], problems

    def test_duplicate_target_columns(self) -> None:
        problems = _problems(
            mapping_specs.field("id", "A", row=2),
            mapping_specs.field("id", "B", row=5),
        )

        assert problems == [
            "crm.xlsx › customer › row 5 › Target column: target column 'id' is"
            " already mapped at crm.xlsx › customer › row 2"
        ]

    def test_a_column_cannot_be_used_and_ignored(self) -> None:
        problems = _problems(
            mapping_specs.field("id", "ID"), ignored_source_fields=("ID",)
        )

        assert "'ID' is both used and ignored" in problems[0]

    def test_mapping_names_are_unique(self) -> None:
        duplicated = mapping_specs.catalog(
            mapping_specs.mapping(mapping_specs.field("a", "A")),
            mapping_specs.mapping(mapping_specs.field("b", "B")),
        )

        assert [
            str(p)
            for p in mapping.validate_catalog(
                duplicated, mapping_specs.registry
            )
        ] == ["mapping 'customer' is defined more than once"]

    def test_labels_follow_the_workbook_headers(self) -> None:
        relabelled = dataclasses.replace(
            mapping_specs.catalog(
                mapping_specs.mapping(
                    mapping_specs.field("s", "S", transform="nope")
                )
            ),
            labels={"transform": "Rule"},
        )

        (problem,) = mapping.validate_catalog(
            relabelled, mapping_specs.registry
        )

        assert str(problem).startswith("crm.xlsx › customer › row 2 › Rule:")


class TestCompile:

    def test_drafts_only_run_when_allowed(self) -> None:
        drafts = mapping_specs.catalog(
            mapping_specs.mapping(
                mapping_specs.field("id", "ID"),
                status=contract.MappingStatus.DRAFT,
            )
        )

        assert not mapping.compile_catalog(drafts, mapping_specs.registry)
        assert "customer" in mapping.compile_catalog(
            drafts, mapping_specs.registry, allow_drafts=True
        )

    def test_every_problem_is_reported_at_once(self) -> None:
        broken = mapping_specs.catalog(
            mapping_specs.mapping(
                mapping_specs.field("a", "A", transform="nope"),
                mapping_specs.field("b", row=3),
            )
        )

        with pytest.raises(contract.MappingDefinitionError) as error:
            mapping.compile_catalog(broken, mapping_specs.registry)

        assert len(error.value.problems) == 2
        assert "2 mapping problem(s)" in str(error.value)


class TestRegistry:

    def test_custom_names_need_a_namespace(self) -> None:
        transforms = mapping.TransformRegistry()

        with pytest.raises(ValueError, match="must be namespaced"):
            transforms.add("full_address", lambda value: value)

    def test_a_name_is_registered_once(self) -> None:
        transforms = mapping.TransformRegistry()
        transforms.add("billing.round", lambda value: value)

        with pytest.raises(ValueError, match="already registered"):
            transforms.add("billing.round", lambda value: value)

    def test_include_merges_module_registries(self) -> None:
        merged = mapping.TransformRegistry()
        merged.include(mapping_specs.registry)

        assert {"customer.full_address", "concat"} <= set(merged.names())
        assert merged.get("customer.full_address") is not None
