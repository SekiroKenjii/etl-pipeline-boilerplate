"""A retry of a label that waited for the label's locks reads the run it
resumes again: a step carried before the wait but not after (a try of the
label failed it meanwhile) runs, under the retry's own lock.

The module, the fake clock and the kernels of the pods are those of
`tests.factories.label_runs`.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
import uuid

from core import contract
from core import runtime
from tests.factories import label_runs

OK, FAILED = contract.ETLStatus.SUCCESS, contract.ETLStatus.FAILED


def _scripted(*actions: Callable[[], None]) -> Callable[[], None]:
    """What happens at each pause of a retry, one action a pause, in turn;
    nothing once they ran."""
    left = list(actions)

    def then() -> None:
        if left:
            left.pop(0)()

    return then


def _try_of_the_label(
    ledger: contract.IRunLedger,
    steps: Mapping[str, contract.ETLStatus],
    status: contract.ETLStatus = FAILED,
) -> str:
    """Records a try of `job-1`, the label's last run then, that ran
    `steps` and ended `status`; its run id."""
    run = contract.ETLRunInfo("crm.load")
    ledger.start(run, "pod-1", {"label": "job-1"})
    for step, outcome in steps.items():
        result = contract.ETLResult()
        result.complete(outcome)
        ledger.record_step(run.run_id, step, result, "pod-1")
    ended = contract.PipelineResult(pipeline_name="crm.load", run_id=run.run_id)
    ended.complete(status, None if status is OK else "broke")
    ledger.finish(ended)
    return run.run_id


def _holders(
    module: label_runs.RecordingModule, lock: contract.IRunLock
) -> dict[str, str | None]:
    """Who holds the lock of each step while the step runs, by step."""
    seen: dict[str, str | None] = {}
    inner = module.handler

    def handler(step: str) -> contract.StepHandler:
        run = inner(step)

        def observed(
            ctx: contract.ETLContext,
            upstream: Mapping[str, contract.ETLResult],
        ) -> contract.ETLResult:
            seen[step] = lock.holder(f"crm.load/{step}")
            return run(ctx, upstream)

        return observed

    module.handler = handler  # type: ignore[method-assign]
    return seen


def _owner_of_the_label() -> str:
    return f"label:job-1@{uuid.uuid4().hex}"


class TestStepsCarriedBeforeTheWaitOnly:

    def _first_try(
        self, lock: contract.IRunLock, ledger: contract.IRunLedger
    ) -> tuple[label_runs.RecordingModule, dict[str, str | None]]:
        """The first try of `job-1`: `extract` and `load` done, `report`
        failed. The module is then ready for the retry, its steps telling
        who holds their locks."""
        module = label_runs.RecordingModule()
        module.failing = {"report"}
        first = label_runs.kernel_of(
            module, lock=lock, ledger=ledger, host="pod-1"
        ).run("crm.load", label="job-1")
        assert first.exit_code is runtime.ExitCode.FAILED
        module.failing = set()
        module.calls.clear()
        return module, _holders(module, lock)

    def test_a_step_carried_before_the_wait_but_not_after_runs_locked(
        self,
    ) -> None:
        time_ = label_runs.FakeTime()
        lock = runtime.InMemoryRunLock(clock=time_.now)
        ledger = runtime.InMemoryRunLedger()
        module, holders = self._first_try(lock, ledger)
        # A try of the label runs every step again (no --resume): it holds
        # `load` and `report`, and ends during the wait having failed `load`.
        alive, second = _owner_of_the_label(), list[str]()
        for step in ("load", "report"):
            assert lock.acquire(f"crm.load/{step}", alive, label_runs.LEASE)

        def ended() -> None:
            second.append(
                _try_of_the_label(ledger, {"extract": OK, "load": FAILED})
            )
            for step in ("load", "report"):
                lock.release(f"crm.load/{step}", alive)

        time_.then = ended

        outcome = label_runs.retry_kernel(module, lock, ledger, time_).run(
            "crm.load", label="job-1", resume=True
        )

        assert outcome.exit_code is runtime.ExitCode.SUCCESS
        assert outcome.resumed_from == second[0]
        assert time_.pauses == [label_runs.LEASE / 10]
        assert module.calls == ["load", "report"]
        assert outcome.result is not None
        owner = f"label:job-1@{outcome.result.run_id}"
        assert holders == {"load": owner, "report": owner}
        for step in ("extract", "load", "report"):
            assert lock.acquire(
                f"crm.load/{step}", "a third run", label_runs.LEASE
            ), step

    def test_a_step_no_longer_carried_held_by_another_label_exits_4(
        self,
    ) -> None:
        time_ = label_runs.FakeTime()
        lock = runtime.InMemoryRunLock(clock=time_.now)
        ledger = runtime.InMemoryRunLedger()
        module, holders = self._first_try(lock, ledger)
        alive = _owner_of_the_label()
        other = f"label:job-2@{uuid.uuid4().hex}"
        assert lock.acquire("crm.load/report", alive, label_runs.LEASE)

        def ended() -> None:
            _try_of_the_label(ledger, {"extract": OK, "load": FAILED})
            lock.release("crm.load/report", alive)
            # job-2 loads `load` again meanwhile.
            assert lock.acquire("crm.load/load", other, label_runs.LEASE)

        time_.then = ended

        outcome = label_runs.retry_kernel(module, lock, ledger, time_).run(
            "crm.load", label="job-1", resume=True
        )

        assert outcome.exit_code is runtime.ExitCode.LOCKED
        assert time_.pauses == [label_runs.LEASE / 10]
        assert module.calls == []
        assert not holders
        # It gave `report` back; job-2 keeps `load`; nothing recorded.
        assert lock.holder("crm.load/report") is None
        assert lock.holder("crm.load/load") == other
        assert len(ledger.recent("crm.load")) == 2

    def test_a_step_no_longer_carried_held_by_its_label_is_waited_for(
        self,
    ) -> None:
        time_ = label_runs.FakeTime()
        lock = runtime.InMemoryRunLock(clock=time_.now)
        ledger = runtime.InMemoryRunLedger()
        module, holders = self._first_try(lock, ledger)
        alive, zombie = _owner_of_the_label(), _owner_of_the_label()
        assert lock.acquire("crm.load/report", alive, label_runs.LEASE)

        def ended() -> None:
            _try_of_the_label(ledger, {"extract": OK, "load": FAILED})
            lock.release("crm.load/report", alive)
            # A third try of the label, alive, loads `load` again; it keeps
            # renewing its lease.
            assert lock.acquire("crm.load/load", zombie, 10 * label_runs.LEASE)

        time_.then = _scripted(ended)

        outcome = label_runs.retry_kernel(module, lock, ledger, time_).run(
            "crm.load", label="job-1", resume=True
        )

        assert outcome.exit_code is runtime.ExitCode.LOCKED
        # Waited once for `report`, then the lease for `load`.
        assert time_.pauses == [
            label_runs.LEASE / 10,
            *label_runs.WAITED_THE_LEASE,
        ]
        assert module.calls == []
        assert not holders
        assert lock.holder("crm.load/report") is None
        assert lock.holder("crm.load/load") == zombie

    def test_the_run_resumed_is_read_again_twice_at_most(self) -> None:
        # Each wait ends with a try of the label that failed a step carried
        # until then, and a next try holding that step: the third wait is
        # the last, the retry runs what it read before it, every step
        # locked, even those the last try finished.
        time_ = label_runs.FakeTime()
        lock = runtime.InMemoryRunLock(clock=time_.now)
        ledger = runtime.InMemoryRunLedger()
        module, holders = self._first_try(lock, ledger)
        tries = [_owner_of_the_label() for _ in range(3)]
        runs: list[str] = []
        assert lock.acquire("crm.load/report", tries[0], label_runs.LEASE)

        def first_ended() -> None:
            runs.append(
                _try_of_the_label(ledger, {"extract": OK, "load": FAILED})
            )
            lock.release("crm.load/report", tries[0])
            assert lock.acquire("crm.load/load", tries[1], label_runs.LEASE)

        def second_ended() -> None:
            runs.append(_try_of_the_label(ledger, {"extract": FAILED}))
            lock.release("crm.load/load", tries[1])
            assert lock.acquire("crm.load/extract", tries[2], label_runs.LEASE)

        def third_ended() -> None:
            runs.append(
                _try_of_the_label(
                    ledger,
                    {"extract": OK, "load": OK, "report": OK},
                    status=OK,
                )
            )
            lock.release("crm.load/extract", tries[2])

        time_.then = _scripted(first_ended, second_ended, third_ended)

        outcome = label_runs.retry_kernel(module, lock, ledger, time_).run(
            "crm.load", label="job-1", resume=True
        )

        assert outcome.exit_code is runtime.ExitCode.SUCCESS
        assert time_.pauses == [label_runs.LEASE / 10] * 3
        assert outcome.resumed_from == runs[1]
        assert module.calls == ["extract", "load", "report"]
        assert outcome.result is not None
        owner = f"label:job-1@{outcome.result.run_id}"
        assert holders == {step: owner for step in module.calls}
