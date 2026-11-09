"""The cutover report as a workbook, for BAs to review and sign off.

Takes `CutoverReport.as_dict()`: sheets for the summary, the differences,
the rejected rows, the last runs and the provenance. Every text is written
as text: a value starting with `=` would otherwise run as a formula when the
workbook is opened.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import json
import pathlib

import openpyxl
from openpyxl import styles
from openpyxl.cell import cell as openpyxl_cell
from openpyxl.worksheet import worksheet as openpyxl_worksheet

from shared import utils

type _Value = str | float | None

_HEADER_FONT = styles.Font(bold=True, color="FFFFFF")
_HEADER_FILL = styles.PatternFill("solid", fgColor="305496")
_STATUS_FILLS = {
    "match": styles.PatternFill("solid", fgColor="C6EFCE"),
    "mismatch": styles.PatternFill("solid", fgColor="FFC7CE"),
    "empty": styles.PatternFill("solid", fgColor="FFC7CE"),
    "counted": styles.PatternFill("solid", fgColor="FFEB9C"),
    "match-with-rejects": styles.PatternFill("solid", fgColor="FFEB9C"),
}
_SUMMARY = (
    ("Entity", "entity"),
    ("Status", "status"),
    ("Source rows", "source_rows"),
    ("Rejected", "rejected"),
    ("Accepted", "accepted"),
    ("Expected", "expected_rows"),
    ("Target rows", "target_rows"),
    ("Matched", "matched"),
    ("Missing", "missing"),
    ("Parked queue", "parked"),
    ("Parked missing", "parked_missing"),
    ("Unexplained missing", "unexplained_missing"),
    ("Extra", "extra"),
    ("Different", "different"),
    ("Duplicates", "duplicates"),
    ("Seconds", "seconds"),
)


def write_cutover_report(
    report: Mapping[str, object], path: pathlib.Path | str
) -> pathlib.Path:
    """Writes the report to an xlsx workbook; returns its path."""
    entities = _records(report["entities"])
    workbook = openpyxl.Workbook()
    summary = workbook.active
    if summary is None:  # A new workbook has one sheet.
        summary = workbook.create_sheet()
    summary.title = "Summary"
    verdict = report.get("verdict") or (
        "PASSED" if report["passed"] else "FAILED"
    )
    _put(summary, 1, 1, f"Cutover report: {report['module']}")
    summary["A1"].font = styles.Font(bold=True, size=14)
    _put(summary, 2, 1, f"Generated {report['generated_at']}")
    _put(summary, 2, 2, f"Verdict: {verdict}")
    _table(
        summary,
        [label for label, _ in _SUMMARY],
        [
            [_cell(entity.get(field)) for _, field in _SUMMARY]
            for entity in entities
        ],
        first_row=4,
    )
    for row, entity in enumerate(entities, start=5):
        fill = _STATUS_FILLS.get(str(entity["status"]))
        if fill is not None:
            summary.cell(row=row, column=2).fill = fill

    _table(
        workbook.create_sheet("Differences"),
        ["Entity", "Kind", "Key", "Column", "Expected", "Found"],
        [
            [
                _cell(entity["entity"]),
                _cell(sample["kind"]),
                json.dumps(sample["key"], ensure_ascii=False),
                column,
                _cell(values.get("expected")),
                _cell(values.get("actual")),
            ]
            for entity in entities
            for sample in _records(entity["samples"])
            for column, values in _columns(sample["columns"]).items()
        ],
    )
    _table(
        workbook.create_sheet("Rejected"),
        ["Entity", "Reason", "Rows"],
        [
            [_cell(entity["entity"]), reason, _cell(count)]
            for entity in entities
            for reason, count in _mapping(entity["rejections"]).items()
        ],
    )
    _table(
        workbook.create_sheet("Provenance"),
        ["Entry", "Value"],
        [
            [entry, _text(value)]
            for entry, value in _flat(
                _mapping(report.get("provenance") or {})
            ).items()
        ],
    )
    _table(
        workbook.create_sheet("Runs"),
        ["Pipeline", "Status", "Started", "Completed", "Stats", "Message"],
        [
            [
                _cell(run["pipeline"]),
                _cell(run["status"]),
                _cell(run["started_at"]),
                _cell(run["completed_at"]),
                json.dumps(run["stats"]),
                _cell(run["message"]),
            ]
            for run in _records(report["runs"])
        ],
    )
    target = pathlib.Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    workbook.save(target)
    return target


def _table(
    sheet: openpyxl_worksheet.Worksheet,
    headers: Sequence[str],
    rows: Sequence[Sequence[_Value]],
    first_row: int = 1,
) -> None:
    """Writes a header row and rows, the header styled and frozen."""
    for column, label in enumerate(headers, start=1):
        cell = sheet.cell(row=first_row, column=column, value=label)
        cell.font = _HEADER_FONT
        cell.fill = _HEADER_FILL
        letter = cell.column_letter
        sheet.column_dimensions[letter].width = max(14, len(label) + 4)
    for offset, values in enumerate(rows, start=1):
        for column, value in enumerate(values, start=1):
            _put(sheet, first_row + offset, column, value)
    sheet.freeze_panes = f"A{first_row + 1}"


def _put(
    sheet: openpyxl_worksheet.Worksheet,
    row: int,
    column: int,
    value: _Value,
) -> None:
    """Writes a value into a cell; text always as text, never a formula.

    The control characters a workbook cannot hold are escaped (`\\x1b`);
    line breaks and tabs are kept: the Rejected sheet's reasons must stay
    those an acceptance CSV names.
    """
    if isinstance(value, str):
        value = openpyxl_cell.ILLEGAL_CHARACTERS_RE.sub(
            lambda found: utils.one_line(found.group()), value
        )
    cell = sheet.cell(row=row, column=column, value=value)
    if isinstance(value, str):
        # Never a formula (`=...`) or an error (`#N/A`), whatever the text.
        cell.data_type = "s"


def _cell(value: object) -> _Value:
    """A canonical value, as a cell shows it: text keeps its quotes off."""
    return (
        value
        if value is None or isinstance(value, (int, float, str))
        else str(value)
    )


def _text(value: object) -> _Value:
    """A provenance value, as a cell shows it."""
    if value is None or isinstance(value, (int, str)):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _flat(data: Mapping[str, object], prefix: str = "") -> dict[str, object]:
    """Nested mappings as one, by dotted entry."""
    found: dict[str, object] = {}
    for name, value in data.items():
        if isinstance(value, Mapping) and value:
            found |= _flat(value, f"{prefix}{name}.")
        else:
            found[f"{prefix}{name}"] = value
    return found


def _mapping(value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise TypeError(f"not a mapping: {type(value).__name__}")
    return value


def _records(value: object) -> list[Mapping[str, object]]:
    if not isinstance(value, list):
        raise TypeError(f"not a list: {type(value).__name__}")
    return [_mapping(item) for item in value]


def _columns(value: object) -> dict[str, Mapping[str, object]]:
    """The expected and actual values of a sample, by column; one blank
    column for a sample without columns."""
    columns = _mapping(value or {"": {}})
    return {name: _mapping(values) for name, values in columns.items()}
