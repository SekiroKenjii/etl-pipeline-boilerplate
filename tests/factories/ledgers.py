"""What every run ledger must do: one check for all."""

from __future__ import annotations

import datetime

from core import contract
from tests.factories import fakes


def check_steps(ledger: contract.IRunLedger) -> None:
    """Records steps as they end, and joins a run started elsewhere."""
    ctx = fakes.make_context("crm.load")
    ledger.start(ctx.run, "host-1", {"user": "an"})
    # Another worker of the same run: the first record stays.
    ledger.start(ctx.run, "host-2", {"user": "other"})
    failed = contract.ETLResult(stats=contract.ETLStats(total=4))
    failed.complete(contract.ETLStatus.FAILED, "boom")
    ledger.record_step(ctx.run.run_id, "customer:0", failed, "host-1")
    loaded = contract.ETLResult(
        stats=contract.ETLStats(total=7, rejected=1),
        data={"attempts": 2},
    )
    loaded.complete(contract.ETLStatus.PARTIAL_SUCCESS, "1 rejected")
    # Run again by another worker: replaces the failure.
    ledger.record_step(ctx.run.run_id, "customer:0", loaded, "host-2")
    ledger.record_step(ctx.run.run_id, "order", failed, "host-1")

    record = ledger.get(ctx.run.run_id)
    (listed,) = ledger.recent("crm.load")

    assert record is not None
    assert (record.host, record.meta) == ("host-1", {"user": "an"})
    assert record.status is contract.ETLStatus.IN_PROGRESS
    assert sorted(record.steps) == ["customer:0", "order"]
    step = record.steps["customer:0"]
    assert (step.status, step.message, step.host, step.attempts) == (
        contract.ETLStatus.PARTIAL_SUCCESS,
        "1 rejected",
        "host-2",
        2,
    )
    assert (step.stats["total"], step.stats["rejected"]) == (7, 1)
    assert step.completed_at.tzinfo is not None
    assert record.steps["order"].status is contract.ETLStatus.FAILED
    assert listed.steps == {}
    assert ledger.get("no such run") is None


def check_latest(ledger: contract.IRunLedger) -> None:
    """Finds the newest run that loaded, of a module, among every run."""
    t0 = datetime.datetime(2026, 11, 1, tzinfo=datetime.UTC)
    done = (contract.ETLStatus.SUCCESS, contract.ETLStatus.PARTIAL_SUCCESS)

    def run(
        pipeline: str,
        minute: int,
        status: contract.ETLStatus,
        meta: dict[str, str],
        dry_run: bool = False,
    ) -> str:
        info = contract.ETLRunInfo(
            pipeline,
            dry_run=dry_run,
            triggered_at=t0 + datetime.timedelta(minutes=minute),
        )
        ledger.start(info, "h", meta)
        if status is not contract.ETLStatus.IN_PROGRESS:  # Else it died.
            result = contract.PipelineResult(
                pipeline_name=pipeline, run_id=info.run_id
            )
            result.complete(status)
            ledger.finish(result)
        return info.run_id

    assert ledger.latest("crm.", done, "keys") is None
    loaded = run("crm.load", 0, contract.ETLStatus.SUCCESS, {"keys": "k1"})
    # Newer, but none of them counts.
    run("crm.sync", 1, contract.ETLStatus.FAILED, {"keys": "k2"})
    run("crm.sync", 2, contract.ETLStatus.SUCCESS, {"keys": "k3"}, True)
    run("crm.sync", 3, contract.ETLStatus.SUCCESS, {"mappings": '"keys":'})
    run("crmx.load", 4, contract.ETLStatus.SUCCESS, {"keys": "k4"})
    run("crm%.load", 5, contract.ETLStatus.SUCCESS, {"keys": "k5"})
    for minute in range(6, 30):
        run("crm.sync", minute, contract.ETLStatus.IN_PROGRESS, {"keys": "k"})

    found = ledger.latest("crm.", done, "keys")

    assert found is not None
    assert (found.run_id, found.pipeline, found.meta["keys"]) == (
        loaded,
        "crm.load",
        "k1",
    )
    partial = run(
        "crm.sync", 40, contract.ETLStatus.PARTIAL_SUCCESS, {"keys": "k6"}
    )
    newest = ledger.latest("crm.", done, "keys")
    assert newest is not None and newest.run_id == partial
    found_odd = ledger.latest("crm%.", done, "keys")
    assert found_odd is not None and found_odd.meta["keys"] == "k5"


def check_last_run(ledger: contract.IRunLedger) -> None:
    """Finds the newest run of a pipeline, of a label, among every run."""
    t0 = datetime.datetime(2026, 11, 1, tzinfo=datetime.UTC)

    def run(
        pipeline: str,
        minute: int,
        meta: dict[str, str],
        status: contract.ETLStatus = contract.ETLStatus.FAILED,
        dry_run: bool = False,
    ) -> str:
        info = contract.ETLRunInfo(
            pipeline,
            dry_run=dry_run,
            triggered_at=t0 + datetime.timedelta(minutes=minute),
        )
        ledger.start(info, "h", meta)
        if status is not contract.ETLStatus.IN_PROGRESS:  # Else it died.
            result = contract.PipelineResult(
                pipeline_name=pipeline, run_id=info.run_id
            )
            result.complete(status)
            ledger.finish(result)
        return info.run_id

    odd = 'j%_\\"1'
    assert ledger.last_run("crm.load") is None
    assert ledger.last_run("crm.load", "job-1") is None
    succeeded = run(
        "crm.load", 0, {"label": "job-1"}, contract.ETLStatus.SUCCESS
    )
    run("crm.load", 1, {"user": "an"})
    labelled_odd = run("crm.load", 2, {"label": odd})
    # Newer, but none of them is a run of job-1 of crm.load.
    run("crm.load", 3, {"label": "job-1"}, dry_run=True)
    run("crm.loader", 4, {"label": "job-1"})
    run("crm.load", 5, {"label": "job-10"})
    run("crm.load", 6, {"label": "job"})
    run("crm.load", 7, {"steps": '"label": "job-1"'})
    run("crm.load", 8, {"label": "jobx1"})
    for minute in range(10, 260):
        run("crm.load", minute, {"label": "job-2"})

    found = ledger.last_run("crm.load", "job-1")
    newest = ledger.last_run("crm.load")

    assert found is not None
    assert (found.run_id, found.status) == (
        succeeded,
        contract.ETLStatus.SUCCESS,
    )
    assert found.steps == {}
    assert newest is not None and newest.meta == {"label": "job-2"}
    found_odd = ledger.last_run("crm.load", odd)
    assert found_odd is not None and found_odd.run_id == labelled_odd
    assert ledger.last_run("crm.load", "j%") is None
    died = run(
        "crm.load", 300, {"label": "job-1"}, contract.ETLStatus.IN_PROGRESS
    )
    found_died = ledger.last_run("crm.load", "job-1")
    assert found_died is not None and found_died.run_id == died
