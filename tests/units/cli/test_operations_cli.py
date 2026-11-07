"""The operations commands' pure parts: the CSV export, the exit codes."""

from __future__ import annotations

import csv
import datetime
import io
import json
import pathlib

import pytest

from cli import exit_codes
from cli import mappings
from cli import operations
from core import contract
from core import runtime
from core.mapping import catalog as mapping_catalog
from core.operators import corrections

# What spreadsheets run as formulas, and a quote that must stay a quote.
NASTY = [
    '=HYPERLINK("http://evil","click")',
    "+1+2",
    "-3",
    "@SUM(A1)",
    "\tTAB",
    "\rCR",
    "'quoted",
    "''twice",
    "plain",
    "",
]


def _entry(key: str, value: str) -> contract.QuarantineEntry:
    return contract.QuarantineEntry(
        id="1",
        pipeline="crm.full_load",
        run_id="r1",
        checkpoint="after_transform",
        record={
            "mapping": "customer",
            "source_key": key,
            "entity": "crm.customer",
            "key": None,
            "data": {"CRT_DT": value},
        },
        issues=({"field": "CRT_DT", "message": value},),
        rejected_at=datetime.datetime(2026, 11, 3, tzinfo=datetime.UTC),
    )


@pytest.mark.parametrize("value", NASTY)
def test_a_cell_is_escaped_and_read_back(value: str) -> None:
    escaped = corrections.escape_cell(value)

    assert not escaped.startswith(("=", "+", "-", "@", "\t", "\r"))
    assert corrections.unescape_cell(escaped) == value


def test_an_export_runs_no_formula_and_corrects_the_same_rows(
    tmp_path: pathlib.Path,
) -> None:
    exported = tmp_path / "rejected.csv"
    entries = [_entry(value or "k", value) for value in NASTY[:-1]]

    # pylint: disable-next=protected-access
    assert operations._export(iter(entries), exported) == len(entries)

    with exported.open(encoding="utf-8-sig", newline="") as file:
        rows = list(csv.reader(file))
    cells = [cell for row in rows[1:] for cell in row]
    assert not [c for c in cells if c.startswith(("=", "+", "-", "@", "\t"))]
    # The BAs add a column with the corrected value: the rows read back are
    # those of the source keys as they were.
    fixes = tmp_path / "fixes.csv"
    with fixes.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([*rows[0], "CRT_DT"])
        for row in rows[1:]:
            writer.writerow([*row, "'=28/02/2020"])
    read = corrections.Corrections.from_csv(fixes)
    assert sorted(read.fixes["customer"]) == sorted(
        value or "k" for value in NASTY[:-1]
    )
    assert {row["CRT_DT"] for row in read.fixes["customer"].values()} == {
        "=28/02/2020"
    }
    record = json.loads(corrections.unescape_cell(rows[1][-1]))
    assert record["source_key"] == NASTY[0]


def test_every_command_line_shares_one_table_of_exit_codes() -> None:
    for code in runtime.ExitCode:
        assert exit_codes.of_run(code) == exit_codes.ExitCode[code.name]
    values = [code.value for code in exit_codes.ExitCode]
    assert len(values) == len(set(values))


def test_a_diff_with_changes_is_not_an_invalid_file(
    tmp_path: pathlib.Path,
) -> None:
    root = pathlib.Path(__file__).resolve().parents[3]
    catalog = mapping_catalog.load_catalog(
        root / "mappings" / "compiled" / "crm.json"
    )
    old = tmp_path / "old.json"
    mapping_catalog.save_catalog(old, catalog)
    new = tmp_path / "new.json"
    data = json.loads(old.read_text(encoding="utf-8"))
    data["mappings"][0]["version"] += 1
    new.write_text(json.dumps(data), encoding="utf-8")

    def diff(*paths: pathlib.Path) -> int:
        return mappings.main(["diff", *map(str, paths)], out=io.StringIO())

    assert diff(old, old) == exit_codes.ExitCode.SUCCESS
    assert diff(old, new) == exit_codes.ExitCode.CHANGED
    assert diff(old, tmp_path / "nope.json") == exit_codes.ExitCode.INVALID
