"""Resuming a run, running some steps again, the locks of the steps, and
the label of a scheduler's job (`etl run --resume`, `--step`,
`--label`)."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import threading
import time

import pytest

from core import contract
from core import pipeline
from core import runtime
from core.runtime import names


class _Steps(pipeline.BaseOrchestrator):
    """`extract` → `load` → `report`, plus `parts` independent steps."""

    def __init__(self, module: _Module) -> None:
        super().__init__(max_workers=module.max_workers)
        self.module = module

    def steps(self) -> Sequence[contract.PipelineStep]:
        handler = self.module.handler
        return [
            contract.PipelineStep("extract", handler("extract")),
            contract.PipelineStep(
                "load", handler("load"), dependencies=("extract",)
            ),
            contract.PipelineStep(
                "report", handler("report"), dependencies=("load",)
            ),
            *(
                contract.PipelineStep(f"part:{i}", handler(f"part:{i}"))
                for i in range(self.module.parts)
            ),
        ]


class _Module(runtime.Module):
    """Records the steps it runs; fails those in `failing`."""

    def __init__(self, parts: int = 0, max_workers: int = 1) -> None:
        self.parts = parts
        self.max_workers = max_workers
        self.failing: set[str] = set()
        self.calls: list[str] = []
        self.delay = 0.0
        self._lock = threading.Lock()

    @property
    def name(self) -> str:
        return "crm"

    def pipelines(self) -> dict[str, runtime.PipelineFactory]:
        return {"load": lambda resources: _Steps(self)}

    def handler(self, step: str) -> contract.StepHandler:
        def run(
            ctx: contract.ETLContext,
            upstream: Mapping[str, contract.ETLResult],
        ) -> contract.ETLResult:
            del upstream  # Unused.
            with self._lock:
                self.calls.append(step)
            time.sleep(self.delay)
            if step in self.failing:
                raise contract.FatalError(f"{step} broke")
            return contract.ETLResult(stats=contract.ETLStats(total=1))

        return run


def _kernel(
    module: _Module,
    *,
    lock: contract.IRunLock | None = None,
    ledger: contract.IRunLedger | None = None,
    host: str = "host-1",
    lease_seconds: float = 60.0,
) -> runtime.Kernel:
    specs: list[runtime.ResourceSpec[object]] = [
        runtime.ResourceSpec(
            names.LEDGER, lambda r: ledger or runtime.InMemoryRunLedger()
        )
    ]
    if lock is not None:
        specs.append(runtime.ResourceSpec(names.LOCK, lambda r: lock))
    return runtime.Kernel(
        runtime.Resources(specs),
        [module],
        host=host,
        lease_seconds=lease_seconds,
    )


def _steps(outcome: runtime.RunOutcome) -> dict[str, contract.ETLStatus]:
    if outcome.result is None:
        raise AssertionError(outcome.message)
    return {
        name: result.status
        for name, result in outcome.result.step_results.items()
    }


class TestResume:

    def test_a_resumed_run_runs_only_the_steps_left(self) -> None:
        module = _Module()
        kernel = _kernel(module)
        module.failing = {"load"}

        first = kernel.run("crm.load")
        module.failing = set()
        module.calls.clear()
        second = kernel.run("crm.load", resume=True)

        assert first.exit_code is runtime.ExitCode.FAILED
        assert first.result is not None
        assert module.calls == ["load", "report"]
        assert second.exit_code is runtime.ExitCode.SUCCESS
        assert second.resumed_from == first.result.run_id
        assert second.result is not None
        extract = second.result.step_results["extract"]
        assert extract.message == f"done in run {first.result.run_id}"
        ledger = kernel.resources.get(names.LEDGER, contract.IRunLedger)
        record = ledger.get(second.result.run_id)
        assert record is not None
        assert record.meta["resumed_from"] == first.result.run_id
        assert sorted(record.steps) == ["load", "report"]

    def test_resuming_a_resumed_run_carries_the_whole_chain(self) -> None:
        module = _Module()
        kernel = _kernel(module)
        module.failing = {"load"}
        kernel.run("crm.load")
        module.failing = {"report"}
        kernel.run("crm.load", resume=True)
        module.failing = set()
        module.calls.clear()

        third = kernel.run("crm.load", resume=True)

        assert module.calls == ["report"]
        assert third.exit_code is runtime.ExitCode.SUCCESS

    def test_after_a_success_every_step_runs(self) -> None:
        module = _Module()
        kernel = _kernel(module)
        kernel.run("crm.load")
        module.calls.clear()

        outcome = kernel.run("crm.load", resume=True)

        assert len(module.calls) == 3
        assert outcome.resumed_from is None

    def test_a_run_that_died_tells_which_steps_it_finished(self) -> None:
        ledger = runtime.InMemoryRunLedger()
        dead = contract.ETLRunInfo("crm.load")
        ledger.start(dead, "host-0")
        done = contract.ETLResult()
        done.complete(contract.ETLStatus.SUCCESS)
        ledger.record_step(dead.run_id, "extract", done, "host-0")
        module = _Module()

        outcome = _kernel(module, ledger=ledger).run(
            "crm.load", resume=dead.run_id
        )

        assert module.calls == ["load", "report"]
        assert outcome.resumed_from == dead.run_id

    def test_what_cannot_be_resumed(self) -> None:
        module = _Module()
        kernel = _kernel(module)
        dry = kernel.run("crm.load", dry_run=True)
        assert dry.result is not None

        with pytest.raises(contract.PipelineConfigError, match="No run 'x'"):
            kernel.run("crm.load", resume="x")
        with pytest.raises(contract.PipelineConfigError, match="dry run"):
            kernel.run("crm.load", resume=dry.result.run_id)
        no_ledger = runtime.Kernel(runtime.Resources([]), [module])
        with pytest.raises(contract.PipelineConfigError, match="ledger"):
            no_ledger.run("crm.load", resume=True)
        with pytest.raises(contract.PipelineConfigError, match="label"):
            kernel.run("crm.load", label="x" * 191)

    def test_a_label_resumes_the_last_run_of_its_job_only(self) -> None:
        module = _Module(parts=2)
        kernel = _kernel(module)
        module.failing = {"part:0"}
        first = kernel.run("crm.load", label="job-1")
        # Another job of the scheduler ran since, and failed too.
        module.failing = {"part:1"}
        kernel.run("crm.load", label="job-2")
        module.failing = set()
        module.calls.clear()

        # The scheduler retries job-1: it resumes the run of job-1.
        retry = kernel.run("crm.load", label="job-1", resume=True)
        # The next night's job is a new label: every step runs.
        module.calls.clear()
        night = kernel.run("crm.load", label="job-3", resume=True)

        assert first.result is not None
        assert retry.resumed_from == first.result.run_id
        assert retry.exit_code is runtime.ExitCode.SUCCESS
        assert night.resumed_from is None
        assert len(module.calls) == 5
        assert retry.result is not None
        ledger = kernel.resources.get(names.LEDGER, contract.IRunLedger)
        record = ledger.get(retry.result.run_id)
        assert record is not None
        assert record.meta["label"] == "job-1"


class TestSelectedSteps:

    def test_one_entity_runs_again(self) -> None:
        module = _Module(parts=3)
        kernel = _kernel(module)

        outcome = kernel.run("crm.load", steps=["part:*", "report"])

        assert sorted(module.calls) == [
            "part:0",
            "part:1",
            "part:2",
            "report",
        ]
        assert outcome.exit_code is runtime.ExitCode.SUCCESS
        ledger = kernel.resources.get(names.LEDGER, contract.IRunLedger)
        assert outcome.result is not None
        record = ledger.get(outcome.result.run_id)
        assert record is not None
        assert record.meta["steps"] == "part:0,part:1,part:2,report"
        with pytest.raises(contract.PipelineConfigError, match="'nope'"):
            kernel.run("crm.load", steps=["nope"])


class TestStepLocks:

    def test_runs_of_other_steps_run_at_the_same_time(self) -> None:
        module = _Module(parts=4)
        lock, ledger = runtime.InMemoryRunLock(), runtime.InMemoryRunLedger()
        # Each run's steps wait for the other's: they must run at once.
        meet = threading.Barrier(2, timeout=5)
        handler = module.handler

        def met(step: str) -> contract.StepHandler:
            inner = handler(step)

            def run(
                ctx: contract.ETLContext,
                upstream: Mapping[str, contract.ETLResult],
            ) -> contract.ETLResult:
                if step in ("part:0", "part:2"):
                    meet.wait()
                return inner(ctx, upstream)

            return run

        module.handler = met  # type: ignore[method-assign]
        outcomes: dict[str, runtime.RunOutcome] = {}

        def job(host: str, steps: list[str]) -> None:
            kernel = _kernel(module, lock=lock, ledger=ledger, host=host)
            outcomes[host] = kernel.run("crm.load", steps=steps)

        threads = [
            threading.Thread(target=job, args=("j1", ["part:0", "part:1"])),
            threading.Thread(target=job, args=("j2", ["part:2", "part:3"])),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert {
            host: outcome.exit_code for host, outcome in outcomes.items()
        } == {"j1": runtime.ExitCode.SUCCESS, "j2": runtime.ExitCode.SUCCESS}
        assert sorted(module.calls) == [f"part:{i}" for i in range(4)]
        # Every lock is released.
        for step in ("extract", "part:0", "part:3"):
            assert lock.acquire(f"crm.load/{step}", "next run", 60)

    def test_a_run_of_a_step_running_is_locked_out_and_holds_nothing(
        self,
    ) -> None:
        lock = runtime.InMemoryRunLock()
        assert lock.acquire("crm.load/load", "another run", 60)
        module = _Module(parts=1)
        kernel = _kernel(module, lock=lock)

        whole = kernel.run("crm.load")
        overlapping = kernel.run("crm.load", steps=["load", "part:0"])
        other = kernel.run("crm.load", steps=["part:0"])

        assert whole.exit_code is runtime.ExitCode.LOCKED
        assert overlapping.exit_code is runtime.ExitCode.LOCKED
        assert other.exit_code is runtime.ExitCode.SUCCESS
        assert module.calls == ["part:0"]
        # What the locked-out runs took, they gave back.
        assert lock.acquire("crm.load/extract", "a third run", 60)
        assert lock.acquire("crm.load/part:0", "a third run", 60)
        # Nothing recorded for the runs that never started.
        ledger = kernel.resources.get(names.LEDGER, contract.IRunLedger)
        assert len(ledger.recent("crm.load")) == 1

    def test_a_retry_of_a_job_takes_over_the_locks_its_run_left(
        self,
    ) -> None:
        lock, ledger = runtime.InMemoryRunLock(), runtime.InMemoryRunLedger()
        module = _Module()
        kernel = _kernel(module, lock=lock, ledger=ledger)
        module.failing = {"load"}
        kernel.run("crm.load", label="crm-full-load-29012345")
        # The pod running it then died: its lease outlives it.
        assert lock.acquire("crm.load/load", "label:crm-full-load-29012345", 60)
        module.failing = set()
        module.calls.clear()

        other = kernel.run("crm.load", label="another-job", resume=True)
        retry = kernel.run(
            "crm.load", label="crm-full-load-29012345", resume=True
        )

        assert other.exit_code is runtime.ExitCode.LOCKED
        assert retry.exit_code is runtime.ExitCode.SUCCESS
        assert module.calls == ["load", "report"]
        assert lock.acquire("crm.load/load", "next run", 60)

    def test_the_steps_carried_over_are_not_locked(self) -> None:
        lock, ledger = runtime.InMemoryRunLock(), runtime.InMemoryRunLedger()
        module = _Module()
        kernel = _kernel(module, lock=lock, ledger=ledger)
        module.failing = {"report"}
        kernel.run("crm.load")
        module.failing = set()
        # Another run runs `extract` again meanwhile.
        assert lock.acquire("crm.load/extract", "another run", 60)

        outcome = kernel.run("crm.load", resume=True)

        assert outcome.exit_code is runtime.ExitCode.SUCCESS


class _FailingLock(runtime.InMemoryRunLock):
    """Its database goes away after `taken` locks, or for releases."""

    def __init__(self, taken: int, releases: bool = True) -> None:
        super().__init__(clock=lambda: self.now)
        self.now = 1000.0
        self.taken = taken
        self.releases = releases

    def acquire(self, name: str, owner: str, lease_seconds: float) -> bool:
        if self.taken <= 0:
            raise contract.RetryableError("server closed the connection")
        self.taken -= 1
        return super().acquire(name, owner, lease_seconds)

    def release(self, name: str, owner: str) -> None:
        if not self.releases:
            raise contract.RetryableError("server closed the connection")
        super().release(name, owner)


class TestStepLocksFailing:

    def test_a_lock_failing_while_taken_gives_back_what_was_taken(
        self,
    ) -> None:
        lock, ledger = _FailingLock(taken=2), runtime.InMemoryRunLedger()
        module = _Module(parts=1)
        kernel = _kernel(module, lock=lock, ledger=ledger)

        # The third lock of four (in name order) cannot be taken.
        with pytest.raises(contract.RetryableError, match="closed"):
            kernel.run("crm.load")

        assert module.calls == []
        assert ledger.recent("crm.load") == []
        lock.taken = 10
        for step in ("extract", "load", "part:0", "report"):
            assert lock.acquire(f"crm.load/{step}", "next run", 60), step

    def test_a_lock_that_cannot_be_released_expires(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        lock = _FailingLock(taken=10, releases=False)
        module = _Module()
        kernel = _kernel(module, lock=lock, lease_seconds=60)

        outcome = kernel.run("crm.load")

        assert outcome.exit_code is runtime.ExitCode.SUCCESS
        assert "could not release crm.load/load, it expires" in caplog.text
        assert not lock.acquire("crm.load/load", "next run", 60)
        lock.now += 61
        assert lock.acquire("crm.load/load", "next run", 60)


def test_done_steps_follow_the_chain_of_resumed_runs() -> None:
    ledger = runtime.InMemoryRunLedger()

    def run(
        run_id: str, steps: Mapping[str, contract.ETLStatus], **meta: str
    ) -> None:
        ledger.start(contract.ETLRunInfo("p", run_id=run_id), "h", meta)
        for step, status in steps.items():
            result = contract.ETLResult()
            result.complete(status)
            ledger.record_step(run_id, step, result, "h")

    ok, failed = contract.ETLStatus.SUCCESS, contract.ETLStatus.FAILED
    run("r1", {"a": ok, "b": failed, "c": ok})
    run("r2", {"b": ok, "c": failed}, resumed_from="r1")
    run("r3", {"d": contract.ETLStatus.PARTIAL_SUCCESS}, resumed_from="r2")

    done = runtime.done_steps(ledger, "r3")

    assert {name: step.run_id for name, step in done.items()} == {
        "a": "r1",
        "b": "r2",
        "d": "r3",
    }
    assert runtime.done_steps(ledger, "missing") == {}


def test_the_steps_a_process_runs_at_once_can_be_capped() -> None:
    module = _Module(parts=4, max_workers=4)
    module.delay = 0.05
    running, most = [0], [0]
    lock = threading.Lock()
    handler = module.handler

    def counted(step: str) -> contract.StepHandler:
        inner = handler(step)

        def run(
            ctx: contract.ETLContext,
            upstream: Mapping[str, contract.ETLResult],
        ) -> contract.ETLResult:
            with lock:
                running[0] += 1
                most[0] = max(most[0], running[0])
            try:
                return inner(ctx, upstream)
            finally:
                with lock:
                    running[0] -= 1

        return run

    module.handler = counted  # type: ignore[method-assign]
    kernel = _kernel(module)
    kernel.max_parallel_steps = 2

    assert kernel.run("crm.load").exit_code is runtime.ExitCode.SUCCESS
    assert most[0] == 2
    orchestrator = _Steps(module)
    with pytest.raises(ValueError, match="count"):
        orchestrator.cap_workers(0)
