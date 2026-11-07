"""The cutover report an auditor can trust: what produced it, what it
accepts, and that the report signed is the report saved."""

from __future__ import annotations

import json
import pathlib
from typing import Any

import sqlalchemy as sa

from app import bootstrap
from cli import cutover as cli_cutover
from core import cutover
from core import runtime
from tests.integrations import test_crm_module as crm_test

# The fixture of the CRM module's tests: the demo databases on SQLite; and
# how they run `etl`.
sqlite = crm_test.sqlite
_etl = crm_test._etl  # pylint: disable=protected-access


def _save(
    settings: dict[str, Any], tmp_path: pathlib.Path, *arguments: str
) -> tuple[int, pathlib.Path]:
    """Runs a saved report: its exit code and the report saved."""
    saved = tmp_path / "rehearsals"
    code, text = _etl(
        settings, tmp_path, "cutover", "report", "crm", "--save", str(saved),
        *arguments,
    )  # fmt: skip
    assert text.startswith("Saved"), text
    (path,) = (saved / "crm").glob("*.json")
    return code, path


def test_a_report_edited_and_sealed_again_does_not_verify(
    sqlite: tuple[sa.Engine, sa.Engine, dict[str, Any]], tmp_path: pathlib.Path
) -> None:
    _, _, settings = sqlite
    _etl(settings, tmp_path, "run", "crm.full_load")
    code, path = _save(settings, tmp_path)
    report = json.loads(path.read_text(encoding="utf-8"))
    forged = tmp_path / "forged.json"
    forged.write_text(
        json.dumps(
            cutover.seal({**report, "verdict": "PASSED", "passed": True})
        ),
        encoding="utf-8",
    )

    verified = _etl(
        settings, tmp_path, "cutover", "verify", str(path), str(forged)
    )
    record = _etl(settings, tmp_path, "runs", "etl.cutover")[1]

    assert code == runtime.ExitCode.FAILED
    assert cutover.verify(json.loads(forged.read_text(encoding="utf-8")))
    assert verified[0] == runtime.ExitCode.FAILED
    ok, unrecorded = verified[1].splitlines()
    assert ok.startswith(f"OK         {path}: FAILED crm,")
    assert unrecorded.startswith(f"UNRECORDED {forged}")
    assert record.count("etl.cutover success") == 1
    assert report["digest"] in record


def test_the_report_tells_what_produced_it(
    sqlite: tuple[sa.Engine, sa.Engine, dict[str, Any]], tmp_path: pathlib.Path
) -> None:
    _, _, settings = sqlite
    _etl(settings, tmp_path, "run", "crm.full_load")
    accepted = tmp_path / "accepted.csv"
    accepted.write_text("entity,reason,rows\ncustomer,*,2\n", encoding="utf-8")

    code, path = _save(
        settings, tmp_path, "--accept-rejections", str(accepted),
        "--time-resolution", "300",
    )  # fmt: skip
    report = json.loads(path.read_text(encoding="utf-8"))
    provenance = report["provenance"]

    assert code == runtime.ExitCode.PARTIAL_SUCCESS
    assert report["verdict"] == "PASSED-WITH-REJECTS"
    assert provenance["env"] == "test"
    assert set(provenance["module"]) >= {"mappings", "keys"}
    source, target = (
        provenance["databases"]["source"],
        provenance["databases"]["target"],
    )
    assert [(d["name"], d["dialect"]) for d in source + target] == [
        ("legacy", "sqlite"),
        ("crm", "sqlite"),
    ]
    assert target[0]["database"].endswith("crm.db")
    assert provenance["options"]["time_resolution"] == 300
    assert provenance["accepted_rejections"]["rows"] == {"customer": {"*": 2}}
    assert len(provenance["accepted_rejections"]["sha256"]) == 64


def test_no_credential_in_the_provenance(
    sqlite: tuple[sa.Engine, sa.Engine, dict[str, Any]], tmp_path: pathlib.Path
) -> None:
    _, _, settings = sqlite
    # A server's URL, as a target: only where it is shows.
    settings["databases"]["remote"] = {
        "url": (
            "Server=db.example,1433;Database=crm;User Id=etl;"
            "Password=s3cr3t-Pa55;"
        )
    }

    with bootstrap.bootstrap(
        "test", directory=tmp_path / "none", overrides=settings, logs=False
    ) as application:
        found = cli_cutover._databases(  # pylint: disable=protected-access
            application.settings, ["remote", "unknown"]
        )

    assert found == [
        {
            "name": "remote",
            "dialect": "mssql",
            "host": "db.example",
            "port": 1433,
            "database": "crm",
        },
        {"name": "unknown", "error": "not in the settings"},
    ]
    assert "s3cr3t" not in json.dumps(found)
    assert "etl" not in json.dumps(found)
