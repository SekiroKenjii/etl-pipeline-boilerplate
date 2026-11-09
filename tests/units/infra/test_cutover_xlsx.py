"""The cutover report as a workbook: what the data holds stays text."""

from __future__ import annotations

import pathlib
from typing import Any

import openpyxl
import pytest

from infra.reports import xlsx

# Values Excel or LibreOffice would run, or take for something else.
PAYLOADS = [
    '=HYPERLINK("http://evil.example","click")',
    "+cmd|' /C calc'!A0",
    "-2+3",
    "@SUM(A1:A2)",
    "\t=1+1",
    "\r=1+1",
    "#N/A",
]


def _report(value: str) -> dict[str, Any]:
    return {
        "module": "crm",
        "generated_at": "2026-11-03T10:00:00+00:00",
        "verdict": "FAILED",
        "passed": False,
        "provenance": {"env": "uat", "module": {"mappings": value}},
        "runs": [{
            "pipeline": "crm.full_load",
            "status": "success",
            "started_at": "2026-11-03T09:00:00+00:00",
            "completed_at": None,
            "stats": {},
            "message": value,
            "run_id": "r1",
        }],
        "entities": [{
            "entity": value,
            "status": "mismatch",
            "rejections": {value: 1},
            "samples": [{
                "kind": "different",
                "key": [value],
                "columns": {value: {"expected": value, "actual": value}},
            }],
        }],
    }


@pytest.mark.parametrize("value", PAYLOADS)
def test_data_is_never_a_formula(tmp_path: pathlib.Path, value: str) -> None:
    path = xlsx.write_cutover_report(_report(value), tmp_path / "r.xlsx")

    workbook = openpyxl.load_workbook(path)
    cells = [
        cell
        for sheet in workbook.worksheets
        for row in sheet.iter_rows()
        for cell in row
        if cell.value is not None
    ]

    # XML reads a carriage return back as a line feed.
    shown = value.replace("\r", "\n")
    assert {cell.data_type for cell in cells} <= {"s", "n"}
    found = [cell for cell in cells if cell.value == shown]
    # Entity, reason, column, expected, found, mappings, run message.
    assert len(found) >= 7
    differences = list(workbook["Differences"].iter_rows(values_only=True))
    assert differences[1][3:] == (shown, shown, shown)


def test_the_summary_tells_the_verdict(tmp_path: pathlib.Path) -> None:
    report = _report("x")
    report["verdict"] = "PASSED-WITH-REJECTS"

    path = xlsx.write_cutover_report(report, tmp_path / "r.xlsx")

    workbook = openpyxl.load_workbook(path)
    assert workbook.sheetnames == [
        "Summary",
        "Differences",
        "Rejected",
        "Provenance",
        "Runs",
    ]
    assert workbook["Summary"]["B2"].value == "Verdict: PASSED-WITH-REJECTS"
    assert list(workbook["Provenance"].iter_rows(values_only=True)) == [
        ("Entry", "Value"),
        ("env", "uat"),
        ("module.mappings", "x"),
    ]


@pytest.mark.parametrize(
    "value",
    ["bad\n# PASSED", "colour \x1b[31mred\x1b[0m", "nul \x00 byte", "\x0bx"],
    ids=["line feed", "escape", "nul", "vertical tab"],
)
def test_any_text_is_written(tmp_path: pathlib.Path, value: str) -> None:
    path = xlsx.write_cutover_report(_report(value), tmp_path / "r.xlsx")

    rejected = list(
        openpyxl.load_workbook(path)["Rejected"].iter_rows(values_only=True)
    )

    (reason,) = [row[1] for row in rejected[1:]]
    assert isinstance(reason, str)
    if value == "bad\n# PASSED":
        # The Rejected sheet is the acceptance CSV's: a reason stays as is.
        assert reason == value
    else:
        assert "\x1b" not in reason
        assert "\x00" not in reason
