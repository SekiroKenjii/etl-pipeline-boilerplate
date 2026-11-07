"""What every run ledger must do with the steps of a run: one check for all."""

from __future__ import annotations

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
