"""Resuming a run that died, sharing a pipeline's steps between processes
(`--step`), and a pipeline that cannot be built, with the installed `etl`
command."""

from __future__ import annotations

import os
import pathlib
import signal
import uuid

import pytest
import sqlalchemy as sa

from tests.e2e import harness

pytestmark = pytest.mark.slow

# The demo's two bad customers, which the BAs accept to leave behind.
ACCEPTED = str(pathlib.Path(__file__).with_name("accepted.csv"))

CUSTOMERS = 100_000
SQL_LOADED = "SELECT count(*) FROM customer"
PIPELINE = "crm.full_load"


def test_a_run_killed_half_way_is_resumed(tmp_path: pathlib.Path) -> None:
    crm = f"sqlite:///{tmp_path / 'crm.db'}"
    project = harness.Project(
        legacy=f"sqlite:///{tmp_path / 'legacy.db'}",
        crm=crm,
        extra={
            "ETL__MODULES__CRM__SETTINGS__BATCH_SIZE": "500",
            "ETL__MODULES__CRM__SETTINGS__PARTITIONS": "2",
            "ETL__RUNS__LEASE_SECONDS": "2",
        },
    )
    project.seed()
    harness.add_customers(project.legacy, CUSTOMERS)

    process = project.start("run", PIPELINE, "--partial-ok")
    harness.wait_for(
        lambda: harness.count(crm, SQL_LOADED) >= 2_000, what="rows loaded"
    )
    assert harness.leases(crm, PIPELINE), "the run holds no lock"
    os.kill(process.pid, signal.SIGKILL)
    process.communicate()
    assert harness.count(crm, SQL_LOADED) < CUSTOMERS
    harness.wait_for(
        lambda: harness.expired(crm, PIPELINE),
        timeout=10,
        what="the leases of the dead run to expire",
    )
    resumed = project.etl(
        "run", PIPELINE, "--partial-ok", "--resume", "--format", "json"
    )
    diff = project.etl(
        "cutover", "report", "crm", "--accept-rejections", ACCEPTED
    )

    assert resumed.returncode == 0, resumed.stdout
    ended = harness.outcome(resumed.stdout)
    (dead,) = [
        run for run in project.runs(PIPELINE) if run["status"] == "in_progress"
    ]
    assert ended["resumed_from"] == dead["run_id"]
    # The dead run had split the tables: the resumed one did not again.
    assert sorted(project.run_of(dead["run_id"])["steps"]) == [
        "customer:ranges",
        "order:ranges",
    ]
    run = project.run_of(ended["run_id"])
    assert run["meta"]["resumed_from"] == dead["run_id"]
    assert sorted(run["steps"]) == [
        "customer:0",
        "customer:1",
        "order:0",
        "order:1",
    ]
    assert harness.count(crm, SQL_LOADED) == CUSTOMERS + 2
    assert diff.returncode == 3, diff.stdout  # PASSED-WITH-REJECTS.


def test_processes_of_other_steps_run_at_once(tmp_path: pathlib.Path) -> None:
    url = os.environ.get("ETL_TEST_POSTGRES_URL")
    if not url:
        pytest.skip("set ETL_TEST_POSTGRES_URL to share the steps of a run")
    del tmp_path  # Unused.
    schema = f"steps_{uuid.uuid4().hex[:8]}"
    admin = sa.create_engine(url)
    with admin.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA "{schema}"')
    scoped = (
        sa.make_url(url)
        .update_query_dict({"options": f"-csearch_path={schema}"})
        .render_as_string(False)
    )
    project = harness.Project(
        legacy=scoped,
        crm=scoped,
        extra={
            "ETL__MODULES__CRM__SETTINGS__BATCH_SIZE": "1000",
            "ETL__MODULES__CRM__SETTINGS__PARTITIONS": "4",
        },
    )

    def run(label: str, *steps: str) -> list[str]:
        return [
            "run",
            PIPELINE,
            "--partial-ok",
            "--format",
            "json",
            "--label",
            label,
            *(f"--step={step}" for step in steps),
        ]

    try:
        project.seed()
        harness.add_customers(scoped, CUSTOMERS)
        # The tables are split first, once, as a scheduler would.
        split = project.etl(*run("split", "*:ranges"))
        # Two jobs, half of the customers each, at the same time.
        jobs = [
            project.start(*run("job-a", "customer:0", "customer:1")),
            project.start(*run("job-b", "customer:2", "customer:3")),
        ]
        harness.wait_for(
            lambda: harness.count(scoped, SQL_LOADED) >= 2_000,
            what="rows loaded",
        )
        # A third one, overlapping the first, while they run.
        overlapping = project.etl(*run("job-c", "customer:1"))
        outputs = [job.communicate(timeout=300)[0] for job in jobs]
        orders = project.etl(*run("orders", "order:*"))
        loaded = harness.count(scoped, SQL_LOADED)
        runs = project.runs(PIPELINE)
    finally:
        with admin.begin() as connection:
            connection.exec_driver_sql(f'DROP SCHEMA "{schema}" CASCADE')
        admin.dispose()

    assert split.returncode == 0, split.stdout
    for job, output in zip(jobs, outputs, strict=True):
        assert job.returncode == 0, output
    assert overlapping.returncode == 4, overlapping.stdout
    assert harness.outcome(overlapping.stdout)["message"] == "already running"
    assert orders.returncode == 0, orders.stdout
    # Every valid customer loaded once, by one job or the other.
    assert loaded == CUSTOMERS + 2
    # The run locked out was not recorded; the labels were.
    assert sorted(record["meta"]["label"] for record in runs) == [
        "job-a",
        "job-b",
        "orders",
        "split",
    ]


def test_a_pipeline_that_cannot_be_built_exits_2(
    tmp_path: pathlib.Path,
) -> None:
    project = harness.Project(
        legacy=f"sqlite:///{tmp_path / 'legacy.db'}",
        crm=f"sqlite:///{tmp_path / 'crm.db'}",
        # A release without its compiled mappings.
        extra={
            "ETL__MODULES__CRM__SETTINGS__MAPPINGS": str(tmp_path / "no.json")
        },
    )

    ran = project.etl("run", PIPELINE, "--format", "json")

    assert ran.returncode == 2, ran.stdout
    ended = harness.outcome(ran.stdout)
    assert (ended["exit_code"], ended["run_id"]) == (2, None)
    assert "No compiled mappings" in ended["message"]
    # No failed run recorded: nothing ran.
    assert project.runs(PIPELINE) == []
