from __future__ import annotations

import io
import json
import pathlib

import openpyxl

from cli import exit_codes
from cli import mappings

REGISTRY = "tests.factories.mapping_specs:registry"


def _run(*args: str) -> tuple[int, str]:
    out = io.StringIO()
    code = mappings.main(list(args), out=out)
    return code, out.getvalue()


def _template(directory: pathlib.Path, name: str = "crm") -> pathlib.Path:
    path = directory / f"{name}.xlsx"
    code, _ = _run("template", str(path))
    assert code == 0
    return path


def test_the_ci_workflow(tmp_path: pathlib.Path) -> None:
    workbook = _template(tmp_path)
    compiled = tmp_path / "compiled" / "crm.json"

    assert _run("validate", str(tmp_path))[0] == 0

    code, output = _run("check", str(tmp_path))
    assert code == exit_codes.ExitCode.CHANGED
    assert "is missing: run `etl-mappings compile`" in output

    assert _run("compile", str(tmp_path))[0] == 0
    assert (
        json.loads(compiled.read_text(encoding="utf-8"))["mappings"][0]["name"]
        == "customer"
    )
    assert _run("check", str(tmp_path))[0] == 0

    # a BA edits the workbook but forgets to compile
    book = openpyxl.load_workbook(workbook)
    book["customer"]["H4"] = "inactive"
    book.save(workbook)

    code, output = _run("check", str(tmp_path))
    assert code == exit_codes.ExitCode.CHANGED
    assert "is out of date" in output


def test_validate_reports_problems_for_the_ba(tmp_path: pathlib.Path) -> None:
    workbook = _template(tmp_path)
    book = openpyxl.load_workbook(workbook)
    book["customer"]["E4"] = "valuemap"
    book.save(workbook)

    code, output = _run("validate", str(workbook))

    assert code == exit_codes.ExitCode.INVALID
    assert (
        "crm.xlsx › customer › row 4 › Transform: unknown transform 'valuemap'."
        " Did you mean 'value_map'?"
    ) in output


def test_compile_writes_nothing_when_a_workbook_is_invalid(
    tmp_path: pathlib.Path,
) -> None:
    workbook = _template(tmp_path)
    book = openpyxl.load_workbook(workbook)
    book["customer"]["E2"] = "nope"
    book.save(workbook)

    assert _run("compile", str(tmp_path))[0] == exit_codes.ExitCode.INVALID
    assert not (tmp_path / "compiled").exists()


def test_custom_transforms_come_from_the_project_registry(
    tmp_path: pathlib.Path,
) -> None:
    workbook = _template(tmp_path)
    book = openpyxl.load_workbook(workbook)
    sheet = book["customer"]
    sheet.append([
        "address",
        "VARCHAR(500)",
        "N",
        "STREET, WARD, DISTRICT",
        "customer.full_address",
    ])
    book.save(workbook)

    assert (
        _run("validate", str(workbook))[0] == exit_codes.ExitCode.INVALID
    )  # unknown without the registry
    assert _run("--registry", REGISTRY, "validate", str(workbook))[0] == 0


def test_check_finds_orphaned_compiled_files(tmp_path: pathlib.Path) -> None:
    _template(tmp_path)
    _run("compile", str(tmp_path))
    (tmp_path / "compiled" / "billing.json").write_text("{}", encoding="utf-8")

    code, output = _run("check", str(tmp_path))

    assert code == exit_codes.ExitCode.CHANGED
    assert "billing.json has no workbook anymore" in output


def test_excel_lock_files_are_skipped(tmp_path: pathlib.Path) -> None:
    _template(tmp_path)
    (tmp_path / "~$crm.xlsx").write_bytes(b"lock")

    code, output = _run("validate", str(tmp_path))

    assert code == 0
    assert "~$" not in output


def test_template_does_not_overwrite(tmp_path: pathlib.Path) -> None:
    workbook = _template(tmp_path)

    assert _run("template", str(workbook))[0] == exit_codes.ExitCode.INVALID
    assert _run("template", str(workbook), "--force")[0] == 0
    assert "Mappings" in openpyxl.load_workbook(workbook).sheetnames


def test_no_workbook_found(tmp_path: pathlib.Path) -> None:
    code, output = _run("validate", str(tmp_path))

    assert code == exit_codes.ExitCode.INVALID
    assert "No workbook found" in output
