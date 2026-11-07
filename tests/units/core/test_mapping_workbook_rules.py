"""Workbook footguns: duplicate headers, `;` in parameters, date formats,
drafts, corrections of any source column, and source keys as canonical text.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import dataclasses
import decimal
import io
import logging
import pathlib

import openpyxl
import pytest

from cli import mappings as mappings_cli
from core import contract
from core import mapping
from core import mapping_pipelines
from core import operators
from infra import mapping as infra_mapping
from modules.crm import settings as crm_settings
from tests.factories import mapping_specs

field = mapping_specs.field
INDEX = [
    ["Mapping", "Source table", "Target table", "Status"],
    ["customer", "legacy.CUST", "crm.customer", "approved"],
]


def _workbook(
    path: pathlib.Path, sheets: Mapping[str, Sequence[Sequence[object]]]
) -> pathlib.Path:
    workbook = openpyxl.Workbook()
    workbook.remove(workbook.active)  # type: ignore[arg-type]
    for title, rows in sheets.items():
        sheet = workbook.create_sheet(title)
        for row in rows:
            sheet.append(row)
    workbook.save(path)
    return path


def _problems(path: pathlib.Path) -> list[str]:
    reader = infra_mapping.XlsxMappingReader()
    with pytest.raises(contract.MappingDefinitionError) as error:
        reader.read(path)
    return [str(problem) for problem in error.value.problems]


class TestWorkbooks:

    def test_a_column_given_twice_is_a_problem(
        self, tmp_path: pathlib.Path
    ) -> None:
        path = _workbook(
            tmp_path / "crm.xlsx",
            {
                "Mappings": INDEX,
                "customer": [
                    ["Target column", "Source columns", "source column"],
                    ["name", "GOOD", "WRONG"],
                ],
            },
        )

        assert _problems(path) == [
            "crm.xlsx › customer › row 1: column 'source column' is there"
            " twice (as 'Source columns' too): keep one"
        ]

    def test_a_quoted_parameter_holds_semicolons(
        self, tmp_path: pathlib.Path
    ) -> None:
        path = _workbook(
            tmp_path / "crm.xlsx",
            {
                "Mappings": INDEX,
                "customer": [
                    ["Target column", "Source columns", "Transform", "Params"],
                    ["name", "A, B", "concat", 'separator="; " ; skip=x'],
                    ["code", "C", "value_map", 'A="a;b"; Q="say ""hi"""'],
                ],
            },
        )

        name, code = (
            infra_mapping.XlsxMappingReader().read(path).get("customer").fields
        )

        assert name.params == {"separator": "; ", "skip": "x"}
        assert code.params == {"A": "a;b", "Q": 'say "hi"'}


class TestDateFormats:

    def _problems(self, source_format: str) -> list[str]:
        catalog = mapping_specs.catalog(
            mapping_specs.mapping(
                field("born", "DOB", target_type="DATE", format=source_format)
            )
        )
        return [
            p.message
            for p in mapping.validate_catalog(catalog, mapping_specs.registry)
        ]

    @pytest.mark.parametrize(
        ("source_format", "expected"),
        [
            ("DD/MM/YYYY", "unknown token(s) DD, YYYY"),
            ("d/M/yyyy", "unknown token(s) M, d"),
            ("dd/mm/yyyy", "mm is minutes"),
            ("dd/MM", "has no year"),
        ],
    )
    def test_unknown_tokens_are_refused(
        self, source_format: str, expected: str
    ) -> None:
        problems = self._problems(source_format)

        assert len(problems) == 1
        assert expected in problems[0]

    @pytest.mark.parametrize(
        "source_format",
        ["dd/MM/yyyy", "yyyy-MM-dd'T'HH:mm:ss", "%d.%m.%Y", "dd/MM/yy"],
    )
    def test_known_tokens_are_accepted(self, source_format: str) -> None:
        assert self._problems(source_format) == []

    def test_quoted_letters_are_literals(self) -> None:
        cast = mapping.parse_type("TIMESTAMP").cast(
            "2020-01-02T03:04:05", "yyyy-MM-dd'T'HH:mm:ss"
        )

        assert str(cast) == "2020-01-02 03:04:05"

    def test_two_digit_years_are_warned_about(self) -> None:
        catalog = mapping_specs.catalog(
            mapping_specs.mapping(
                field("born", "DOB", target_type="DATE", format="dd/MM/yy")
            )
        )

        (warning,) = mapping.catalog_warnings(catalog)

        assert str(warning).startswith(
            "crm.xlsx › customer › row 2 › Format: 'dd/MM/yy' reads 2-digit"
            " years"
        )


CUSTOMER = mapping_specs.mapping(
    field("customer_key", "CUST_NO", primary_key=True),
    key_hashing=contract.KeyHashing.UUID5,
    key_case=contract.KeyCase.UPPER,
)
ORDER = mapping_specs.mapping(
    field("order_key", "ORD_NO", primary_key=True),
    field("customer_key", "CUST_NO", references="customer"),
    name="order",
    source="legacy.ORD",
    target="crm.order",
    key_hashing=contract.KeyHashing.UUID5,
)
LINE = mapping_specs.mapping(
    field("line_key", "LINE_NO", primary_key=True),
    field("order_key", "ORD_NO", references="order"),
    name="line",
    source="legacy.LINE",
    target="crm.line",
    key_hashing=contract.KeyHashing.UUID5,
)
DRAFT = contract.MappingStatus.DRAFT


class TestDrafts:

    def test_skip_the_drafts_and_what_references_them(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        catalog = mapping_specs.catalog(
            CUSTOMER, dataclasses.replace(ORDER, status=DRAFT), LINE
        )

        with caplog.at_level(logging.WARNING):
            compiled = mapping.compile_catalog(catalog, mapping_specs.registry)

        assert list(compiled) == ["customer"]
        assert mapping.skipped_mappings(catalog) == {
            "order": "it is a draft",
            "line": "it references 'order' (skipped)",
        }
        assert "Mapping 'line' is skipped" in caplog.text
        assert list(
            mapping.compile_catalog(
                catalog, mapping_specs.registry, allow_drafts=True
            )
        ) == ["customer", "order", "line"]

    def test_the_pipelines_run_the_others(self) -> None:
        catalog = mapping_specs.catalog(
            dataclasses.replace(CUSTOMER, status=DRAFT), ORDER
        )
        catalog = dataclasses.replace(
            catalog,
            mappings=(
                *catalog.mappings,
                dataclasses.replace(CUSTOMER, name="contact"),
            ),
        )

        mappings = mapping_pipelines.Mappings(
            crm_settings.CrmSettings.model_validate({}),
            catalog,
            mapping.compile_catalog(catalog, mapping_specs.registry),
        )

        assert mappings.dependencies == {"contact": frozenset()}

    def test_check_warns_about_them(self, tmp_path: pathlib.Path) -> None:
        path = tmp_path / "crm.xlsx"
        assert mappings_cli.main(["template", str(path)], io.StringIO()) == 0
        assert mappings_cli.main(["compile", str(tmp_path)], io.StringIO()) == 0
        out = io.StringIO()

        code = mappings_cli.main(["check", str(tmp_path)], out)

        assert code == 0
        assert (
            "WARN   crm.xlsx › Mappings › row 2 › Status: mapping 'customer'"
            " is skipped where drafts do not run: it is a draft"
        ) in out.getvalue()


class TestSourceKeys:

    def test_corrections_keep_source_columns_named_like_the_export(
        self, tmp_path: pathlib.Path
    ) -> None:
        path = tmp_path / "fixes.csv"
        path.write_text(
            "_id,_status,mapping,source_key,_issues,status,id,key,record\n"
            "7,open,customer,1003,bad,A,42,K,R\n",
            encoding="utf-8-sig",
        )

        corrections = operators.Corrections.from_csv(path)

        assert corrections.apply("customer", "1003", {}) == {
            "status": "A",
            "id": "42",
            "key": "K",
            "record": "R",
        }

    @pytest.mark.parametrize(
        "value",
        [1003, 1003.0, decimal.Decimal("1003.0"), "1003 ", " 1003"],
    )
    def test_are_canonical_whatever_the_driver_returns(
        self, value: object
    ) -> None:
        catalog = mapping_specs.catalog(CUSTOMER)
        mappings = mapping_pipelines.Mappings(
            crm_settings.CrmSettings.model_validate({}),
            catalog,
            mapping.compile_catalog(catalog, mapping_specs.registry),
        )

        lineage = mappings.lineage("customer")

        assert lineage({"CUST_NO": value}) == "1003"
        # In the key case of the mapping, as its keys are.
        assert lineage({"CUST_NO": "c001"}) == "C001"
        assert lineage({"CUST_NO": None}) == ""
        assert (
            lineage(
                contract.ChangeEvent(
                    "legacy.CUST",
                    contract.ChangeOperation.DELETE,
                    {"CUST_NO": decimal.Decimal("7.00")},
                    (decimal.Decimal("7.00"),),
                )
            )
            == "7"
        )
