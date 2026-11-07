"""The example mappings of a new workbook target its own module's entities."""

from __future__ import annotations

import io
import pathlib

import openpyxl

from cli import mappings as mappings_cli
from cli import scaffold


def _targets(workbook: pathlib.Path) -> list[str]:
    sheet = openpyxl.load_workbook(workbook)["Mappings"]
    column = next(c.column for c in sheet[1] if c.value == "Target table")
    return [
        str(row[0])
        for row in sheet.iter_rows(
            min_row=2, min_col=column, max_col=column, values_only=True
        )
        if row[0]
    ]


def test_a_new_module_does_not_share_the_crm_entities(
    tmp_path: pathlib.Path,
) -> None:
    scaffold.scaffold("billing", tmp_path / "src", tmp_path / "mappings")

    assert _targets(tmp_path / "mappings" / "billing.xlsx") == [
        "billing.customer",
        "billing.order",
    ]


def test_the_module_is_the_workbook_name_unless_given(
    tmp_path: pathlib.Path,
) -> None:
    out = io.StringIO()

    assert mappings_cli.main(["template", str(tmp_path / "hr.xlsx")], out) == 0
    assert (
        mappings_cli.main(
            ["template", str(tmp_path / "x.xlsx"), "--module", "payroll"], out
        )
        == 0
    )

    assert _targets(tmp_path / "hr.xlsx") == ["hr.customer", "hr.order"]
    assert _targets(tmp_path / "x.xlsx") == [
        "payroll.customer",
        "payroll.order",
    ]
