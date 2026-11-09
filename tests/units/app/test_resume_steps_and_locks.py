"""Resuming a run, running some steps again, the locks of the steps, and
the label of a scheduler's job (`etl run --resume`, `--step`,
`--label`)."""

from __future__ import annotations

from collections.abc import Callable, Mapping
import datetime
import signal
import threading
import uuid

import pytest

from core import contract
from core import runtime
from core.runtime import names
from tests.factories import label_runs


def _steps(outcome: runtime.RunOutcome) -> dict[str, contract.ETLStatus]:
    if outcome.result is None:
        raise AssertionError(outcome.message)
    return {
        name: result.status
        for name, result in outcome.result.step_results.items()
    }


class TestResume:

    def test_a_resumed_run_runs_only_the_steps_left(self) -> None:
        module = label_runs.RecordingModule()
        kernel = label_runs.kernel_of(module)
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
        module = label_runs.RecordingModule()
        kernel = label_runs.kernel_of(module)
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
        module = label_runs.RecordingModule()
        kernel = label_runs.kernel_of(module)
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
        module = label_runs.RecordingModule()

        outcome = label_runs.kernel_of(module, ledger=ledger).run(
            "crm.load", resume=dead.run_id
        )

        assert module.calls == ["load", "report"]
        assert outcome.resumed_from == dead.run_id

    def test_what_cannot_be_resumed(self) -> None:
        module = label_runs.RecordingModule()
        kernel = label_runs.kernel_of(module)
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
            kernel.run("crm.load", label="x" * 162)
        # The longest label, in the owner of a lock: 200 characters.
        assert kernel.run("crm.load", label="x" * 161).exit_code is (
            runtime.ExitCode.SUCCESS
        )

    def test_a_label_resumes_the_last_run_of_its_job_only(self) -> None:
        module = label_runs.RecordingModule(parts=2)
        kernel = label_runs.kernel_of(module)
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
        module = label_runs.RecordingModule(parts=3)
        kernel = label_runs.kernel_of(module)

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
        module = label_runs.RecordingModule(parts=4)
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
            kernel = label_runs.kernel_of(
                module, lock=lock, ledger=ledger, host=host
            )
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
        module = label_runs.RecordingModule(parts=1)
        kernel = label_runs.kernel_of(module, lock=lock)

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

    def test_the_steps_carried_over_are_not_locked(self) -> None:
        lock, ledger = runtime.InMemoryRunLock(), runtime.InMemoryRunLedger()
        module = label_runs.RecordingModule()
        kernel = label_runs.kernel_of(module, lock=lock, ledger=ledger)
        module.failing = {"report"}
        kernel.run("crm.load")
        module.failing = set()
        # Another run runs `extract` again meanwhile.
        assert lock.acquire("crm.load/extract", "another run", 60)

        outcome = kernel.run("crm.load", resume=True)

        assert outcome.exit_code is runtime.ExitCode.SUCCESS


class _Blocking:
    """Holds the first run of `extract` until `go` is set."""

    def __init__(self, module: label_runs.RecordingModule) -> None:
        self.started = threading.Event()
        self.go = threading.Event()
        inner = module.handler
        first = [True]

        def handler(step: str) -> contract.StepHandler:
            run = inner(step)

            def blocked(
                ctx: contract.ETLContext,
                upstream: Mapping[str, contract.ETLResult],
            ) -> contract.ETLResult:
                if step == "extract" and first[0]:
                    first[0] = False
                    self.started.set()
                    self.go.wait(timeout=30)
                return run(ctx, upstream)

            return blocked

        module.handler = handler  # type: ignore[method-assign]


class _Try(threading.Thread):
    """A run of `crm.load` in its own thread: a pod of the job."""

    def __init__(self, kernel: runtime.Kernel, **options: str | bool) -> None:
        super().__init__(daemon=True)
        self.kernel = kernel
        self.options = options
        self.outcome: runtime.RunOutcome | None = None

    def run(self) -> None:
        self.outcome = self.kernel.run(
            "crm.load", **self.options  # type: ignore[arg-type]
        )

    def finish(self, blocking: _Blocking) -> Callable[[], None]:
        """What lets this try end, and waits for it."""

        def finished() -> None:
            blocking.go.set()
            self.join(timeout=10)

        return finished


class TestRetriesOfALabel:
    """A retry of a label never runs while another live process holds the
    label's locks: it waits for them, up to the lease."""

    def _alive(
        self,
        lock: contract.IRunLock,
        ledger: contract.IRunLedger,
        lease: float = label_runs.LEASE,
    ) -> tuple[label_runs.RecordingModule, _Blocking, _Try]:
        """A first try of `job-1`, running, holding its locks."""
        module = label_runs.RecordingModule()
        blocking = _Blocking(module)
        first = _Try(
            label_runs.kernel_of(
                module,
                lock=lock,
                ledger=ledger,
                host="pod-1",
                lease_seconds=lease,
            ),
            label="job-1",
        )
        first.start()
        assert blocking.started.wait(timeout=10)
        return module, blocking, first

    def test_a_retry_exits_4_while_its_first_try_is_alive(self) -> None:
        lock, ledger = runtime.InMemoryRunLock(), runtime.InMemoryRunLedger()
        module, blocking, first = self._alive(lock, ledger)
        time_ = label_runs.FakeTime()

        outcome = label_runs.retry_kernel(module, lock, ledger, time_).run(
            "crm.load", label="job-1", resume=True
        )
        first.finish(blocking)()

        assert outcome.exit_code is runtime.ExitCode.LOCKED
        # It waited as long as a dead try's lease would have lasted.
        assert time_.pauses == label_runs.WAITED_THE_LEASE
        assert first.outcome is not None
        assert first.outcome.exit_code is runtime.ExitCode.SUCCESS
        assert module.calls == ["extract", "load", "report"]

    def test_a_retry_runs_nothing_its_first_try_finished_meanwhile(
        self,
    ) -> None:
        lock, ledger = runtime.InMemoryRunLock(), runtime.InMemoryRunLedger()
        module, blocking, first = self._alive(lock, ledger)
        time_ = label_runs.FakeTime(then=first.finish(blocking))

        outcome = label_runs.retry_kernel(module, lock, ledger, time_).run(
            "crm.load", label="job-1", resume=True
        )

        assert first.outcome is not None
        assert first.outcome.result is not None
        assert outcome.exit_code is runtime.ExitCode.SUCCESS
        assert time_.pauses == [label_runs.LEASE / 10]
        # The first try did it all while the retry waited: carried over.
        assert module.calls == ["extract", "load", "report"]
        assert outcome.resumed_from == first.outcome.result.run_id

    def test_a_run_of_the_label_not_resuming_runs_every_step_after(
        self,
    ) -> None:
        lock, ledger = runtime.InMemoryRunLock(), runtime.InMemoryRunLedger()
        module, blocking, first = self._alive(lock, ledger)
        time_ = label_runs.FakeTime(then=first.finish(blocking))

        outcome = label_runs.retry_kernel(module, lock, ledger, time_).run(
            "crm.load", label="job-1"
        )

        assert outcome.exit_code is runtime.ExitCode.SUCCESS
        assert outcome.resumed_from is None
        assert time_.pauses == [label_runs.LEASE / 10]
        # One after the other, never at once.
        assert module.calls == ["extract", "load", "report"] * 2

    def test_a_retry_carries_what_its_first_try_finished_meanwhile(
        self,
    ) -> None:
        lock, ledger = runtime.InMemoryRunLock(), runtime.InMemoryRunLedger()
        module = label_runs.RecordingModule()
        blocking = _Blocking(module)
        inner = module.handler
        reported: list[str] = []
        free: list[str | None] = []

        def handler(step: str) -> contract.StepHandler:
            run = inner(step)

            def once(
                ctx: contract.ETLContext,
                upstream: Mapping[str, contract.ETLResult],
            ) -> contract.ETLResult:
                if step == "report":
                    reported.append(step)
                    # The retry holds no lock of the steps it carries.
                    free.append(lock.holder("crm.load/extract"))
                    if len(reported) == 1:
                        raise contract.FatalError("report broke")
                return run(ctx, upstream)

            return once

        module.handler = handler  # type: ignore[method-assign]
        first = _Try(
            label_runs.kernel_of(
                module, lock=lock, ledger=ledger, host="pod-1"
            ),
            label="job-1",
        )
        first.start()
        assert blocking.started.wait(timeout=10)
        time_ = label_runs.FakeTime(then=first.finish(blocking))

        outcome = label_runs.retry_kernel(module, lock, ledger, time_).run(
            "crm.load", label="job-1", resume=True
        )

        assert first.outcome is not None
        assert first.outcome.exit_code is runtime.ExitCode.FAILED
        assert outcome.exit_code is runtime.ExitCode.SUCCESS
        # extract and load, finished by the first try during the wait.
        assert module.calls == ["extract", "load", "report"]
        assert reported == ["report", "report"]
        assert free[-1] is None
        assert outcome.result is not None
        record = ledger.get(outcome.result.run_id)
        assert record is not None
        assert sorted(record.steps) == ["report"]

    def test_a_retry_resumes_a_try_recorded_while_it_waited(self) -> None:
        # The first try held its locks, not yet recorded in the ledger.
        lock, ledger = runtime.InMemoryRunLock(), runtime.InMemoryRunLedger()
        owner = f"label:job-1@{uuid.uuid4().hex}"
        assert lock.acquire("crm.load/extract", owner, label_runs.LEASE)
        first = contract.ETLRunInfo("crm.load")

        def recorded() -> None:
            ledger.start(first, "pod-1", {"label": "job-1"})
            done = contract.ETLResult()
            done.complete(contract.ETLStatus.SUCCESS)
            ledger.record_step(first.run_id, "extract", done, "pod-1")
            lock.release("crm.load/extract", owner)

        module, time_ = label_runs.RecordingModule(), label_runs.FakeTime(
            then=recorded
        )

        outcome = label_runs.retry_kernel(module, lock, ledger, time_).run(
            "crm.load", label="job-1", resume=True
        )

        assert outcome.exit_code is runtime.ExitCode.SUCCESS
        assert outcome.resumed_from == first.run_id
        assert module.calls == ["load", "report"]

    @pytest.mark.parametrize(
        ("dead_lease", "exit_code", "pauses"),
        [
            # Its leases expire after 10 polls: taken over.
            (
                label_runs.LEASE,
                runtime.ExitCode.SUCCESS,
                [label_runs.LEASE / 10] * 10,
            ),
            # Longer than the retry waits: never taken over.
            (
                2 * label_runs.LEASE,
                runtime.ExitCode.LOCKED,
                label_runs.WAITED_THE_LEASE,
            ),
        ],
    )
    def test_a_retry_takes_over_once_a_dead_try_s_leases_expire(
        self,
        dead_lease: float,
        exit_code: runtime.ExitCode,
        pauses: list[float],
    ) -> None:
        time_ = label_runs.FakeTime()
        lock = runtime.InMemoryRunLock(clock=time_.now)
        ledger = runtime.InMemoryRunLedger()
        module = label_runs.RecordingModule()
        # The pod of the first try died (OOM) just after it renewed them.
        dead = f"label:job-1@{uuid.uuid4().hex}"
        for step in ("extract", "load", "report"):
            assert lock.acquire(f"crm.load/{step}", dead, dead_lease)

        outcome = label_runs.retry_kernel(module, lock, ledger, time_).run(
            "crm.load", label="job-1", resume=True
        )

        assert outcome.exit_code is exit_code
        assert time_.pauses == pauses
        ran = exit_code is runtime.ExitCode.SUCCESS
        assert module.calls == (["extract", "load", "report"] if ran else [])

    def test_a_try_cut_off_from_the_lock_frees_none_of_the_retry_s(
        self,
    ) -> None:
        # The first try's node lost the lock's database, not its own: its
        # leases expire, the retry takes them over, then the first try ends.
        # Its heartbeat runs on real time: a short lease.
        lease = 0.6
        lock = _PartitionedLock()
        ledger = runtime.InMemoryRunLedger()
        module, blocking, first = self._alive(lock, ledger, lease)
        retry = _Try(
            label_runs.kernel_of(
                module,
                lock=lock,
                ledger=ledger,
                host="pod-2",
                lease_seconds=lease,
            ),
            label="job-1",
            resume=True,
        )
        second = _Blocking(module)
        retry.start()
        assert second.started.wait(timeout=10)
        blocking.go.set()
        first.join(timeout=10)

        # The first try saw its lease lost, and stopped; the retry holds on.
        assert first.outcome is not None
        assert first.outcome.exit_code is runtime.ExitCode.CANCELLED
        assert not lock.acquire("crm.load/extract", "a third run", lease)
        second.go.set()
        retry.join(timeout=10)
        assert retry.outcome is not None
        assert retry.outcome.exit_code is runtime.ExitCode.SUCCESS

    def test_a_run_of_another_label_exits_4_at_once(self) -> None:
        lock, ledger = runtime.InMemoryRunLock(), runtime.InMemoryRunLedger()
        module, blocking, first = self._alive(lock, ledger)
        time_ = label_runs.FakeTime()
        other = label_runs.retry_kernel(module, lock, ledger, time_)

        labelled = other.run("crm.load", label="job-2")
        unlabelled = other.run("crm.load")
        first.finish(blocking)()

        assert labelled.exit_code is runtime.ExitCode.LOCKED
        assert unlabelled.exit_code is runtime.ExitCode.LOCKED
        assert not time_.pauses

    def test_a_retry_waiting_is_cancelled_by_sigterm(self) -> None:
        if threading.current_thread() is not threading.main_thread():
            pytest.skip("signals are handled in the main thread only")
        lock, ledger = runtime.InMemoryRunLock(), runtime.InMemoryRunLedger()
        module, blocking, first = self._alive(lock, ledger)
        time_ = label_runs.FakeTime(
            then=lambda: signal.raise_signal(signal.SIGTERM)
        )

        outcome = label_runs.retry_kernel(module, lock, ledger, time_).run(
            "crm.load", label="job-1", resume=True
        )
        first.finish(blocking)()

        assert outcome.exit_code is runtime.ExitCode.CANCELLED
        assert time_.pauses == [label_runs.LEASE / 10]
        assert module.calls == ["extract", "load", "report"]


class TestOwnersOfTheLocks:
    """Who holds a lock decides whether a run of a label waits for it."""

    @pytest.mark.parametrize(
        ("owner", "label", "pauses"),
        [
            # Written by 0.1.0, a label with '@': the whole label.
            ("label:a@b", "a@b", label_runs.WAITED_THE_LEASE),
            ("label:a@b", "a", []),
            # `label:<label>@<run id>`.
            (f"label:a@b@{'0f' * 16}", "a@b", label_runs.WAITED_THE_LEASE),
            (f"label:a@b@{'0f' * 16}", "a", []),
            ("label:a@b@not-a-run-id", "a@b", []),
            ("host-1:0f0f", "a", []),
        ],
    )
    def test_a_run_waits_for_its_own_label_only(
        self, owner: str, label: str, pauses: list[float]
    ) -> None:
        lock, ledger = runtime.InMemoryRunLock(), runtime.InMemoryRunLedger()
        assert lock.acquire("crm.load/load", owner, label_runs.LEASE)
        module, time_ = label_runs.RecordingModule(), label_runs.FakeTime()

        outcome = label_runs.retry_kernel(module, lock, ledger, time_).run(
            "crm.load", label=label
        )

        assert outcome.exit_code is runtime.ExitCode.LOCKED
        assert time_.pauses == pauses
        assert not module.calls

    def test_a_lock_must_tell_who_holds_it(self) -> None:
        class Mute(contract.IRunLock):  # pylint: disable=abstract-method
            """Cannot tell who holds a lock."""

            def acquire(
                self, name: str, owner: str, lease_seconds: float
            ) -> bool:
                return False

            def renew(
                self, name: str, owner: str, lease_seconds: float
            ) -> bool:
                return False

            def release(self, name: str, owner: str) -> None:
                del name, owner  # Unused.

        with pytest.raises(TypeError, match="holder"):
            # pylint: disable-next=abstract-class-instantiated
            Mute()  # type: ignore[abstract]

    def test_a_lock_freed_before_its_holder_is_read_is_taken(self) -> None:
        class Freed(runtime.InMemoryRunLock):
            """Taken by another run, which ends just after."""

            def __init__(self) -> None:
                super().__init__()
                self.refused = 1

            def acquire(
                self, name: str, owner: str, lease_seconds: float
            ) -> bool:
                if self.refused:
                    self.refused -= 1
                    return False
                return super().acquire(name, owner, lease_seconds)

        module, time_ = label_runs.RecordingModule(), label_runs.FakeTime()
        kernel = label_runs.retry_kernel(
            module, Freed(), runtime.InMemoryRunLedger(), time_
        )

        outcome = kernel.run("crm.load", label="job-1")

        assert outcome.exit_code is runtime.ExitCode.SUCCESS
        assert time_.pauses == [label_runs.LEASE / 10]


class TestResumingALabel:
    """`--label --resume` resumes the last run of the label, however many
    runs of the pipeline came after it, and whatever its outcome."""

    def test_a_retry_after_its_first_try_succeeded_runs_nothing(
        self,
    ) -> None:
        module = label_runs.RecordingModule()
        kernel = label_runs.kernel_of(module)
        first = kernel.run("crm.load", label="job-1")
        # A dry run of the job does not count: it loaded nothing.
        kernel.run("crm.load", label="job-1", dry_run=True)
        module.calls.clear()

        retry = kernel.run("crm.load", label="job-1", resume=True)

        assert first.result is not None
        assert retry.exit_code is runtime.ExitCode.SUCCESS
        assert module.calls == []
        assert retry.resumed_from == first.result.run_id

    def test_a_retry_after_300_runs_of_others_resumes_its_label(self) -> None:
        ledger = runtime.InMemoryRunLedger()
        module = label_runs.RecordingModule()
        kernel = label_runs.kernel_of(module, ledger=ledger)
        module.failing = {"report"}
        first = kernel.run("crm.load", label="job-1")
        module.failing = set()
        _others(ledger, 300, label="job-2")
        module.calls.clear()

        retry = kernel.run("crm.load", label="job-1", resume=True)

        assert first.result is not None
        assert retry.resumed_from == first.result.run_id
        assert module.calls == ["report"]

    def test_a_resume_after_300_dry_runs_resumes_the_last_that_loaded(
        self,
    ) -> None:
        ledger = runtime.InMemoryRunLedger()
        module = label_runs.RecordingModule()
        kernel = label_runs.kernel_of(module, ledger=ledger)
        module.failing = {"report"}
        first = kernel.run("crm.load")
        module.failing = set()
        _others(ledger, 300, dry_run=True)
        module.calls.clear()

        retry = kernel.run("crm.load", resume=True)

        assert first.result is not None
        assert retry.resumed_from == first.result.run_id
        assert module.calls == ["report"]


def _others(
    ledger: contract.IRunLedger,
    count: int,
    *,
    label: str | None = None,
    dry_run: bool = False,
) -> None:
    """Records `count` runs of `crm.load`, newer than any before, that
    failed having done nothing."""
    later = datetime.datetime.now(datetime.UTC) + datetime.timedelta(hours=1)
    for i in range(count):
        run = contract.ETLRunInfo(
            "crm.load",
            dry_run=dry_run,
            triggered_at=later + datetime.timedelta(seconds=i),
        )
        ledger.start(run, "host-9", {} if label is None else {"label": label})
        result = contract.PipelineResult(
            pipeline_name="crm.load", run_id=run.run_id
        )
        result.complete(contract.ETLStatus.FAILED, "broke")
        ledger.finish(result)


class _PartitionedLock(runtime.InMemoryRunLock):
    """The first owner's renewals never reach the lock's database."""

    def __init__(self) -> None:
        super().__init__()
        self.cut_off: str | None = None

    def acquire(self, name: str, owner: str, lease_seconds: float) -> bool:
        self.cut_off = self.cut_off or owner
        return super().acquire(name, owner, lease_seconds)

    def renew(self, name: str, owner: str, lease_seconds: float) -> bool:
        if owner == self.cut_off:
            raise contract.RetryableError("could not reach the lock")
        return super().renew(name, owner, lease_seconds)


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
        module = label_runs.RecordingModule(parts=1)
        kernel = label_runs.kernel_of(module, lock=lock, ledger=ledger)

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
        module = label_runs.RecordingModule()
        kernel = label_runs.kernel_of(module, lock=lock, lease_seconds=60)

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
    module = label_runs.RecordingModule(parts=4, max_workers=4)
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
    kernel = label_runs.kernel_of(module)
    kernel.max_parallel_steps = 2

    assert kernel.run("crm.load").exit_code is runtime.ExitCode.SUCCESS
    assert most[0] == 2
    orchestrator = label_runs.Steps(module)
    with pytest.raises(ValueError, match="count"):
        orchestrator.cap_workers(0)
