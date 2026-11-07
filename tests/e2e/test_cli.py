"""The `etl` command, installed, as an operator and a scheduler run it."""

from __future__ import annotations

import json
import pathlib

import pytest

from tests.e2e import harness

pytestmark = pytest.mark.slow

# The demo's two bad customers, which the BAs accept to leave behind.
ACCEPTED = str(pathlib.Path(__file__).with_name("accepted.csv"))


@pytest.fixture
def project(tmp_path: pathlib.Path) -> harness.Project:
    project = harness.Project(
        legacy=f"sqlite:///{tmp_path / 'legacy.db'}",
        crm=f"sqlite:///{tmp_path / 'crm.db'}",
        extra={"ETL__BOOKKEEPING__DIRECTORY": str(tmp_path / "var")},
    )
    project.seed()
    return project


def test_a_day_of_operations(
    project: harness.Project, tmp_path: pathlib.Path
) -> None:
    first = project.etl("run", "crm.full_load", "--format", "json")
    lenient = project.etl(
        "run", "crm.full_load", "--partial-ok", "--format", "json"
    )
    runs = project.runs("crm.full_load")
    rejected = project.etl("quarantine", "list")
    report = project.etl(
        "cutover", "report", "crm", "--format", "json",
        "--save", str(tmp_path / "rehearsals"), "--accept-rejections", ACCEPTED,
    )  # fmt: skip

    assert first.returncode == 3, first.stdout + first.stderr
    ended = harness.outcome(first.stdout)
    assert (ended["status"], ended["exit_code"]) == ("partial_success", 3)
    assert (ended["stats"]["total"], ended["stats"]["created"]) == (7, 4)
    # Nothing new loaded, the rejected rows rejected again: a partial
    # success, that `--partial-ok` exits 0 for.
    again = harness.outcome(lenient.stdout)
    assert lenient.returncode == 0
    assert (again["status"], again["exit_code"]) == ("partial_success", 0)
    assert again["stats"]["created"] == 0
    assert [run["run_id"] for run in runs] == [
        again["run_id"],
        ended["run_id"],
    ]
    assert len(rejected.stdout.splitlines()) == 2
    assert report.returncode == 3, report.stdout
    data = json.loads(report.stdout[report.stdout.index("{") :])
    assert (data["passed"], data["verdict"]) == (True, "PASSED-WITH-REJECTS")
    (saved,) = (tmp_path / "rehearsals" / "crm").glob("*.json")
    verified = project.etl("cutover", "verify", str(saved))
    assert verified.returncode == 0


def test_mistakes_exit_with_their_codes(project: harness.Project) -> None:
    unknown = project.etl("run", "crm.nope", "--format", "json")
    invalid = project.etl(
        "run", "crm.full_load", "--format", "json",
        ETL__MODULES__CRM__SETTINGS__BATCH_SIZE="0",
    )  # fmt: skip

    assert unknown.returncode == 2
    assert harness.outcome(unknown.stdout)["exit_code"] == 2
    assert "crm.nope" in harness.outcome(unknown.stdout)["message"]
    assert invalid.returncode == 2
    assert "batch_size: Input should be greater than or equal to 1" in (
        harness.outcome(invalid.stdout)["message"]
    )


def test_logs_are_json_lines_for_shippers(project: harness.Project) -> None:
    run = project.etl("run", "crm.full_load", ETL__LOGGING__FORMAT="json")

    events = [
        json.loads(line)
        for line in run.stdout.splitlines()
        if line.startswith("{")
    ]
    ends = [e for e in events if e.get("etl.event") == "pipeline.end"]

    assert len(ends) == 1
    assert ends[0]["etl.status"] == "partial_success"
    assert ends[0]["etl.run_id"]
    assert all(e["service.name"] == "etl" for e in events)
