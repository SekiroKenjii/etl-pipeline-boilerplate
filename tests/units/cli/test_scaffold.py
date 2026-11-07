"""`etl new-module`: a mapping-driven module that runs and lints as written."""

from __future__ import annotations

from collections.abc import Generator
import importlib
import io
import pathlib
import subprocess
import sys
import uuid

import openpyxl
import pytest
import sqlalchemy as sa

from app import bootstrap
from cli import mappings as mappings_cli
from cli import run
from cli import scaffold
from core import runtime
import modules
from modules.crm import demo

ROOT = pathlib.Path(__file__).resolve().parents[3]


@pytest.fixture
def root(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> Generator[pathlib.Path]:
    """A project root whose `modules` the real `modules` package sees; the
    workbooks go to its `mappings`."""
    monkeypatch.chdir(tmp_path)
    extra = str(tmp_path / "modules")
    modules.__path__.append(extra)
    try:
        yield tmp_path
    finally:
        modules.__path__.remove(extra)


def _new_module(root: pathlib.Path) -> str:
    """Writes a module whose name is as long as names go."""
    name = f"m{uuid.uuid4().hex}"[: scaffold.MAX_NAME_LENGTH]
    out = io.StringIO()

    assert run.main(["new-module", name, "--root", str(root)], out) == 0
    assert f'module = "modules.{name}:create"' in out.getvalue()
    return name


def _fill(workbook: pathlib.Path) -> None:
    """Fills the workbook as the BAs would for the demo tables: the example
    mappings approved, the watermark column not mapped."""
    book = openpyxl.load_workbook(workbook)
    sheet = book["Mappings"]
    column = {cell.value: int(cell.column or 0) for cell in sheet[1]}
    for row in range(2, sheet.max_row + 1):
        if sheet.cell(row, 1).value:
            ignored = column["Ignored source fields"]
            value = sheet.cell(row, ignored).value
            sheet.cell(row, column["Status"], value="approved")
            sheet.cell(
                row,
                ignored,
                value=", ".join(filter(None, [str(value or ""), "UPDATED_AT"])),
            )
    book.save(workbook)


def test_a_new_module_loads_its_workbook_with_gate_and_keys(
    root: pathlib.Path,
) -> None:
    name = _new_module(root)
    _fill(root / "mappings" / f"{name}.xlsx")
    out = io.StringIO()
    # Its own transforms are known to the tooling.
    assert (
        mappings_cli.main(
            [
                "--registry",
                f"modules.{name}:registry",
                "compile",
                f"mappings/{name}.xlsx",
            ],
            out,
        )
        == 0
    ), out.getvalue()
    # The tables of the template's example mappings.
    legacy = sa.create_engine(f"sqlite:///{root / 'legacy.db'}")
    target = sa.create_engine(f"sqlite:///{root / 'target.db'}")
    demo.seed(legacy, target)
    settings = {
        "databases": {
            "legacy": {"url": str(legacy.url)},
            name: {"url": str(target.url)},
        },
        "bookkeeping": {"database": name, "create": True},
        "key_secret": "a secret",
        "modules": {
            name: {
                "module": f"modules.{name}:create",
                "settings": {
                    "source_tables": {
                        "customer": "CUST_MASTER",
                        "order": "ORD_MASTER",
                    },
                    "target_tables": {"customer": "customer", "order": "order"},
                    "watermark": "UPDATED_AT",
                    "max_reject_rate": 0.6,
                },
            }
        },
    }

    with bootstrap.bootstrap(
        "test", directory=root / "none", overrides=settings, logs=False
    ) as application:
        plan = application.kernel.plan(f"{name}.full_load")
        outcome = application.kernel.run(f"{name}.full_load")
        fingerprint = application.kernel.modules[name].fingerprint()

    # The dirty rows of the demo are rejected by the gate: a bad date and an
    # unknown country, and the order whose customer is missing.
    assert outcome.exit_code is runtime.ExitCode.PARTIAL_SUCCESS
    assert [step.name for step in plan] == ["customer", "order"]
    with target.connect() as connection:
        loaded = connection.exec_driver_sql(
            'SELECT o.total FROM "order" o JOIN customer c'
            " ON c.customer_key = o.customer_key ORDER BY 1"
        ).all()
        quarantined = connection.exec_driver_sql(
            "SELECT count(*) FROM etl_quarantine"
        ).scalar()
        registered = connection.exec_driver_sql(
            "SELECT count(*) FROM etl_key_registry"
        ).scalar()
    assert [float(total) for (total,) in loaded] == [75.0, 120.5]
    assert quarantined == 3
    assert registered == 4
    assert runtime.KEYS in fingerprint
    legacy.dispose()
    target.dispose()


def test_a_new_module_passes_the_linters(root: pathlib.Path) -> None:
    name = _new_module(root)
    module = root / "modules" / name / "__init__.py"
    config = str(ROOT / "pyproject.toml")

    for command in [
        ["isort", "--check-only", "--settings-path", config],
        ["pyink", "--check", "--config", config],
        ["pylint", "--rcfile", config, "--score=n"],
        ["mypy", "--config-file", config, "--no-error-summary"],
    ]:
        done = subprocess.run(
            [sys.executable, "-m", *command, str(module)],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        assert done.returncode == 0, (command[0], done.stdout, done.stderr)
    assert len(module.read_text(encoding="utf-8").splitlines()) <= 80


def test_names_and_existing_modules_are_checked(root: pathlib.Path) -> None:
    out = io.StringIO()

    assert run.main(["new-module", "Bad-Name", "--root", str(root)], out) == 2
    assert run.main(["new-module", "class", "--root", str(root)], out) == 2
    assert run.main(["new-module", "x" * 21, "--root", str(root)], out) == 2
    assert run.main(["new-module", "dup", "--root", str(root)], out) == 0
    assert run.main(["new-module", "dup", "--root", str(root)], out) == 2
    assert "exists already" in out.getvalue()
    # A workbook of that name is never overwritten.
    (root / "modules" / "dup" / "__init__.py").unlink()
    (root / "modules" / "dup").rmdir()
    assert run.main(["new-module", "dup", "--root", str(root)], out) == 2
    assert not (root / "modules" / "dup").exists()


def test_the_module_imports_as_the_application_loads_it(
    root: pathlib.Path,
) -> None:
    name = _new_module(root)

    module = importlib.import_module(f"modules.{name}")

    assert module.NAME == name
    assert module.MAPPINGS == pathlib.Path(f"mappings/compiled/{name}.json")
