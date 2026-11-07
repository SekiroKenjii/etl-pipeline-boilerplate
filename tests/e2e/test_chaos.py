"""Failures in the middle of a run: the process killed, the database lost.

At-least-once delivery and upserts make a run that died restartable: the
next one goes on from the last loaded batch, and the target ends as if
nothing happened. These tests break real runs to check it.
"""

from __future__ import annotations

import json
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

# Enough rows for a run to be killed half-way.
CUSTOMERS = 100_000
SQL_LOADED = "SELECT count(*) FROM customer"
PIPELINE = "crm.full_load"
# The lease of the killed run: it ends at most that long after its death.
LEASE_SECONDS = 2


def _statuses(report: str) -> dict[str, str]:
    """The status of each entity of `etl cutover report --format json`."""
    data = json.loads(report[report.index("{") :])
    return {entity["entity"]: entity["status"] for entity in data["entities"]}


def test_a_run_killed_half_way_is_finished_by_the_next(
    tmp_path: pathlib.Path,
) -> None:
    crm = f"sqlite:///{tmp_path / 'crm.db'}"
    project = harness.Project(
        legacy=f"sqlite:///{tmp_path / 'legacy.db'}",
        crm=crm,
        extra={
            "ETL__MODULES__CRM__SETTINGS__BATCH_SIZE": "500",
            # The killed run keeps its locks that long.
            "ETL__RUNS__LEASE_SECONDS": str(LEASE_SECONDS),
        },
    )
    project.seed()
    harness.add_customers(project.legacy, CUSTOMERS)

    process = project.start("run", PIPELINE, "--partial-ok")
    harness.wait_for(
        lambda: harness.count(crm, SQL_LOADED) >= 2_000, what="rows loaded"
    )
    os.kill(process.pid, signal.SIGKILL)  # No cleanup, no lock release.
    process.communicate()
    loaded = harness.count(crm, SQL_LOADED)
    assert loaded < CUSTOMERS, "the run ended before it was killed"

    # The next run, while the leases of the dead one last, is locked out:
    # they are kept while it starts, however long that takes.
    with harness.leases_kept(crm, PIPELINE):
        locked = project.etl(
            "run", PIPELINE, "--partial-ok", "--format", "json"
        )
    harness.wait_for(
        lambda: harness.expired(crm, PIPELINE),
        timeout=30,
        what="the leases of the dead run to expire",
    )
    resumed = project.etl("run", PIPELINE, "--partial-ok", "--format", "json")
    report = project.etl("cutover", "report", "crm", "--counts-only")
    diff = project.etl(
        "cutover", "report", "crm", "--format", "json",
        "--accept-rejections", ACCEPTED,
    )  # fmt: skip

    assert locked.returncode == 4, locked.stdout
    assert harness.outcome(locked.stdout) == {
        "pipeline": PIPELINE,
        "run_id": None,
        "status": None,
        "exit_code": 4,
        "resumed_from": None,
        "stats": {},
        "message": "already running",
    }
    assert resumed.returncode == 0, resumed.stdout
    ended = harness.outcome(resumed.stdout)
    assert (ended["status"], ended["exit_code"]) == ("partial_success", 0)
    # Every valid customer, once: the demo's two, and the ones added.
    assert harness.count(crm, SQL_LOADED) == CUSTOMERS + 2
    assert report.returncode == 0
    assert diff.returncode == 3, diff.stdout  # PASSED-WITH-REJECTS.
    assert _statuses(diff.stdout)["customer"] == "match-with-rejects"
    # The ledger shows the dead run as it was left, in progress, and not
    # the run locked out.
    runs = project.runs(PIPELINE)
    assert [(run["run_id"], run["status"]) for run in runs] == [
        (ended["run_id"], "partial_success"),
        (runs[1]["run_id"], "in_progress"),
    ]


def test_a_lost_connection_is_retried(tmp_path: pathlib.Path) -> None:
    url = os.environ.get("ETL_TEST_POSTGRES_URL")
    if not url:
        pytest.skip("set ETL_TEST_POSTGRES_URL to run the Postgres chaos test")
    del tmp_path  # Unused.
    schema = f"chaos_{uuid.uuid4().hex[:8]}"
    app = f"etl-{schema}"
    admin = sa.create_engine(url)
    with admin.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA "{schema}"')
    scoped = (
        sa.make_url(url)
        .update_query_dict({
            "options": f"-csearch_path={schema}",
            "application_name": app,
        })
        .render_as_string(False)
    )
    project = harness.Project(
        legacy=scoped,
        crm=scoped,
        extra={
            "ETL__MODULES__CRM__SETTINGS__BATCH_SIZE": "500",
            "ETL__MODULES__CRM__SETTINGS__RETRY_BACKOFF_SECONDS": "0.5",
        },
    )
    try:
        project.seed()
        harness.add_customers(scoped, CUSTOMERS // 2)
        process = project.start(
            "run", PIPELINE, "--partial-ok", "--format", "json"
        )
        harness.wait_for(
            lambda: harness.count(scoped, SQL_LOADED) >= 2_000,
            what="rows loaded",
        )
        with admin.begin() as connection:
            killed = connection.exec_driver_sql(
                "SELECT count(pg_terminate_backend(pid))"
                " FROM pg_stat_activity WHERE application_name = %(app)s",
                {"app": app},
            ).scalar_one()
        output, _ = process.communicate(timeout=300)
        ended = harness.outcome(output)
        run = project.run_of(ended["run_id"])
        loaded = harness.count(scoped, SQL_LOADED)
        diff = project.etl(
            "cutover", "report", "crm", "--accept-rejections", ACCEPTED
        )
    finally:
        with admin.begin() as connection:
            connection.exec_driver_sql(f'DROP SCHEMA "{schema}" CASCADE')
        admin.dispose()

    assert killed >= 1
    assert process.returncode == 0, output
    assert ended["exit_code"] == 0
    # A step lost its connection and ran again...
    assert max(step["attempts"] for step in run["steps"].values()) >= 2
    assert loaded == CUSTOMERS // 2 + 2  # ...and lost nothing.
    assert diff.returncode == 3, diff.stdout  # PASSED-WITH-REJECTS.
