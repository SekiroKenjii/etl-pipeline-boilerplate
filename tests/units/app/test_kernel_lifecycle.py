"""The kernel around a run: its locks and their leases, its ledger, the
signals that cancel it, and what goes wrong while it starts or ends.

The commands, the keys of a module and what a run records are in
`test_kernel.py`.
"""

from __future__ import annotations

from collections.abc import Sequence
import getpass
import os
import pathlib
import signal
import threading
import time
from typing import Any

import pytest

from core import contract
from core import pipeline
from core import runtime
from core.observability import metrics
from core.runtime import names
from tests.factories import fakes
from tests.factories import ledgers


class _Nothing(pipeline.BaseOrchestrator):

    def steps(self) -> Sequence[contract.PipelineStep]:
        def nothing(
            ctx: contract.ETLContext, upstream: object
        ) -> contract.ETLResult:
            return contract.ETLResult()

        return [contract.PipelineStep("nothing", nothing)]


class _Slow(pipeline.BaseOrchestrator):

    def __init__(self, started: threading.Event) -> None:
        super().__init__()
        self._started = started

    def steps(self) -> Sequence[contract.PipelineStep]:
        def wait(ctx: contract.ETLContext, upstream: Any) -> contract.ETLResult:
            self._started.set()
            for _ in range(200):
                if ctx.is_cancelled:
                    break
                threading.Event().wait(0.01)
            result = contract.ETLResult()
            result.complete(
                contract.ETLStatus.CANCELLED if ctx.is_cancelled else None
            )
            return result

        return [contract.PipelineStep("wait", wait)]


class _Module(runtime.Module):

    def __init__(self) -> None:
        self.started = threading.Event()

    @property
    def name(self) -> str:
        return "slow"

    def pipelines(self) -> dict[str, runtime.PipelineFactory]:
        return {"run": lambda resources: _Slow(self.started)}


class _ForgetfulLock(runtime.InMemoryRunLock):

    def renew(self, name: str, owner: str, lease_seconds: float) -> bool:
        return False


class _UnreachableLock(runtime.InMemoryRunLock):
    """Its database is unreachable for the `failures` first renewals."""

    def __init__(self, failures: int) -> None:
        super().__init__()
        self.failures = failures
        self.renewals = 0

    def renew(self, name: str, owner: str, lease_seconds: float) -> bool:
        self.renewals += 1
        if self.renewals <= self.failures:
            raise contract.RetryableError("server closed the connection")
        return super().renew(name, owner, lease_seconds)


def _slow_kernel(lock: contract.IRunLock, lease: float) -> runtime.Kernel:
    resources = runtime.Resources([
        runtime.ResourceSpec(names.LOCK, lambda r: lock),
        runtime.ResourceSpec(
            names.LEDGER, lambda r: runtime.InMemoryRunLedger()
        ),
    ])
    return runtime.Kernel(resources, [_Module()], lease_seconds=lease)


class TestHeartbeat:

    def test_a_renewal_that_raises_is_tried_again(self) -> None:
        lock = _UnreachableLock(failures=1)

        outcome = _slow_kernel(lock, 0.3).run("slow.run")

        assert outcome.exit_code is runtime.ExitCode.SUCCESS
        # It went on renewing after the failure.
        assert lock.renewals >= 3

    def test_a_lease_that_cannot_be_renewed_cancels_the_run(self) -> None:
        lock = _UnreachableLock(failures=1_000)
        meter = metrics.InMemoryMeter()
        previous = metrics.meter()
        metrics.set_meter(meter)
        try:
            kernel = _slow_kernel(lock, 0.3)
            outcome = kernel.run("slow.run")
        finally:
            metrics.set_meter(previous)

        assert outcome.exit_code is runtime.ExitCode.CANCELLED
        # Tried again a few times, while the lease lasted.
        assert lock.renewals >= 2
        assert meter.total("etl.lock.lost", **{"etl.pipeline": "slow.run"})
        ledger = kernel.resources.get(names.LEDGER, contract.IRunLedger)
        (record,) = ledger.recent("slow.run")
        assert record.status is contract.ETLStatus.CANCELLED


def test_a_lost_lease_cancels_the_run() -> None:
    resources = runtime.Resources([
        runtime.ResourceSpec(names.LOCK, lambda r: _ForgetfulLock()),
        runtime.ResourceSpec(
            names.LEDGER, lambda r: runtime.InMemoryRunLedger()
        ),
    ])
    kernel = runtime.Kernel(resources, [_Module()], lease_seconds=0.06)

    outcome = kernel.run("slow.run")

    assert outcome.exit_code is runtime.ExitCode.CANCELLED
    ledger = resources.get(names.LEDGER, contract.IRunLedger)
    (record,) = ledger.recent("slow.run")
    assert record.status is contract.ETLStatus.CANCELLED


def test_file_and_memory_ledgers_and_locks(tmp_path: pathlib.Path) -> None:
    clock = [1000.0]
    for lock in (
        runtime.InMemoryRunLock(clock=lambda: clock[0]),
        runtime.FileRunLock(tmp_path / str(len(clock)), clock=lambda: clock[0]),
    ):
        clock.append(0)
        assert lock.holder("p") is None
        assert lock.acquire("p", "a", 10)
        assert not lock.acquire("p", "b", 10)
        assert lock.holder("p") == "a"
        assert lock.renew("p", "a", 10)
        assert not lock.renew("p", "b", 10)
        clock[0] += 20  # Expired: taken over.
        assert lock.holder("p") is None
        assert lock.acquire("p", "b", 10)
        lock.release("p", "a")  # Not its owner: kept.
        assert not lock.renew("p", "a", 10)
        assert lock.holder("p") == "b"
        assert not lock.acquire("p", "c", 10)
        lock.release("p", "b")
        assert lock.holder("p") is None
        assert lock.acquire("p", "c", 10)

    ctx = fakes.make_context("crm.load")
    result = contract.PipelineResult(
        pipeline_name="crm.load", run_id=ctx.run.run_id
    )
    result.stats.total = 3
    result.complete(contract.ETLStatus.SUCCESS)
    for ledger in (
        runtime.InMemoryRunLedger(),
        runtime.JsonLinesRunLedger(tmp_path),
    ):
        ledger.start(ctx.run, "host-1", {"user": "an", "mappings": "abc"})
        ledger.finish(result)
        (record,) = ledger.recent("crm.load")
        assert (record.status, record.stats["total"], record.host) == (
            contract.ETLStatus.SUCCESS,
            3,
            "host-1",
        )
        assert record.meta == {"user": "an", "mappings": "abc"}
        assert ledger.recent("other") == []
    ledgers.check_steps(runtime.InMemoryRunLedger())
    ledgers.check_steps(runtime.JsonLinesRunLedger(tmp_path / "steps"))
    ledgers.check_latest(runtime.InMemoryRunLedger())
    ledgers.check_latest(runtime.JsonLinesRunLedger(tmp_path / "latest"))
    ledgers.check_last_run(runtime.InMemoryRunLedger())
    ledgers.check_last_run(runtime.JsonLinesRunLedger(tmp_path / "last"))


class _Plain(runtime.Module):
    """One pipeline, `plain.load`, built by `build`."""

    def __init__(
        self,
        build: runtime.PipelineFactory = lambda resources: _Nothing(),
        name: str = "plain",
    ) -> None:
        self._build = build
        self._name = name
        self.broken_fingerprint = False

    @property
    def name(self) -> str:
        return self._name

    def pipelines(self) -> dict[str, runtime.PipelineFactory]:
        return {"load": self._build}

    def fingerprint(self) -> dict[str, str]:
        if self.broken_fingerprint:
            raise OSError("mappings/compiled/plain.json: permission denied")
        return {}


class _CannotStart(_Nothing):

    def setup(self, ctx: contract.ETLContext) -> None:
        raise contract.FatalError("no table customer")


class _UnrecordedSteps(runtime.InMemoryRunLedger):
    """Its database goes away for steps."""

    def record_step(
        self, run_id: str, step: str, result: contract.ETLResult, host: str
    ) -> None:
        raise contract.RetryableError("server closed the connection")


def _plain_kernel(
    module: runtime.Module, ledger: contract.IRunLedger | None = None
) -> runtime.Kernel:
    specs: list[runtime.ResourceSpec[object]] = []
    if ledger is not None:
        specs.append(runtime.ResourceSpec(names.LEDGER, lambda r: ledger))
    return runtime.Kernel(runtime.Resources(specs), [module])


class _NothingToDo(pipeline.BaseOrchestrator):

    def steps(self) -> Sequence[contract.PipelineStep]:
        def nothing(
            ctx: contract.ETLContext, upstream: Any
        ) -> contract.ETLResult:
            return contract.ETLResult.skipped("no file landed")

        return [contract.PipelineStep("nothing", nothing)]


def test_a_run_with_nothing_to_do_is_skipped_and_exits_zero() -> None:
    ledger = runtime.InMemoryRunLedger()
    kernel = _plain_kernel(_Plain(lambda resources: _NothingToDo()), ledger)

    outcome = kernel.run("plain.load")

    assert outcome.exit_code is runtime.ExitCode.SUCCESS
    assert outcome.result is not None
    record = ledger.get(outcome.result.run_id)
    assert record is not None
    assert record.status is contract.ETLStatus.SKIPPED


class TestWhatGoesWrongAroundARun:

    def test_a_step_that_cannot_be_recorded_does_not_fail_the_run(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        ledger = _UnrecordedSteps()

        outcome = _plain_kernel(_Plain(), ledger).run("plain.load")

        assert outcome.exit_code is runtime.ExitCode.SUCCESS
        assert outcome.result is not None
        record = ledger.get(outcome.result.run_id)
        assert record is not None
        assert record.status is contract.ETLStatus.SUCCESS
        # Resuming it would run the step again.
        assert record.steps == {}
        assert "could not record the step nothing" in caplog.text

    def test_a_pipeline_that_cannot_start_is_a_failed_run(self) -> None:
        ledger = runtime.InMemoryRunLedger()
        module = _Plain(lambda resources: _CannotStart())

        outcome = _plain_kernel(module, ledger).run("plain.load")

        assert outcome.exit_code is runtime.ExitCode.FAILED
        (record,) = ledger.recent("plain.load")
        assert (record.status, record.message) == (
            contract.ETLStatus.FAILED,
            "no table customer",
        )

    @pytest.mark.parametrize("recorded", [True, False])
    def test_a_source_down_while_building_is_a_failed_run_not_invalid(
        self, recorded: bool
    ) -> None:
        def build(resources: runtime.Resources) -> pipeline.BaseOrchestrator:
            raise ConnectionRefusedError("legacy:1521")

        ledger = runtime.InMemoryRunLedger() if recorded else None

        outcome = _plain_kernel(_Plain(build), ledger).run("plain.load")

        # Retried by the scheduler (1), unlike a configuration error (2).
        assert outcome.exit_code is runtime.ExitCode.FAILED
        assert outcome.result is not None
        assert outcome.result.message == "legacy:1521"
        if ledger is not None:
            (record,) = ledger.recent("plain.load")
            assert record.status is contract.ETLStatus.FAILED

    def test_a_fingerprint_that_fails_is_recorded_not_fatal(self) -> None:
        ledger = runtime.InMemoryRunLedger()
        module = _Plain()
        module.broken_fingerprint = True

        outcome = _plain_kernel(module, ledger).run("plain.load")

        assert outcome.exit_code is runtime.ExitCode.SUCCESS
        (record,) = ledger.recent("plain.load")
        assert "permission denied" in record.meta["fingerprint_error"]

    def test_who_runs_without_a_login(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def no_login() -> str:
            raise OSError("no login name")

        monkeypatch.setattr(getpass, "getuser", no_login)
        ledger = runtime.InMemoryRunLedger()

        _plain_kernel(_Plain(), ledger).run("plain.load")

        (record,) = ledger.recent("plain.load")
        assert record.meta["user"] == str(os.getuid())

    def test_two_modules_of_one_name_are_refused(self) -> None:
        resources = runtime.Resources([])
        modules = [_Plain(), _Plain()]

        with pytest.raises(contract.PipelineConfigError, match="Two modules"):
            runtime.Kernel(resources, modules)


class _Terminated(pipeline.BaseOrchestrator):
    """Its step gets SIGTERM, as from Kubernetes, then ends its batch."""

    def steps(self) -> Sequence[contract.PipelineStep]:
        def step(ctx: contract.ETLContext, upstream: Any) -> contract.ETLResult:
            del upstream  # Unused.
            os.kill(os.getpid(), signal.SIGTERM)
            deadline = time.monotonic() + 10
            while not ctx.is_cancelled and time.monotonic() < deadline:
                time.sleep(0.01)
            result = contract.ETLResult()
            result.complete(
                contract.ETLStatus.CANCELLED if ctx.is_cancelled else None
            )
            return result

        return [contract.PipelineStep("step", step)]


@pytest.mark.skipif(
    threading.current_thread() is not threading.main_thread(),
    reason="signals reach the main thread only",
)
def test_sigterm_cancels_the_run_then_gives_back_the_handler() -> None:
    received: list[int] = []
    # Ours while no run is going: the test process is never terminated.
    previous = signal.signal(
        signal.SIGTERM, lambda signum, frame: received.append(signum)
    )
    try:
        ledger = runtime.InMemoryRunLedger()
        module = _Plain(lambda resources: _Terminated())

        outcome = _plain_kernel(module, ledger).run("plain.load")
        restored = signal.getsignal(signal.SIGTERM)
    finally:
        mine = signal.signal(signal.SIGTERM, previous)

    assert outcome.exit_code is runtime.ExitCode.CANCELLED
    assert received == []
    assert restored is mine
    (record,) = ledger.recent("plain.load")
    assert record.status is contract.ETLStatus.CANCELLED


def test_a_run_with_a_lock_and_no_ledger() -> None:
    lock = runtime.InMemoryRunLock()
    kernel = runtime.Kernel(
        runtime.Resources([runtime.ResourceSpec(names.LOCK, lambda r: lock)]),
        [_Plain()],
    )

    outcome = kernel.run("plain.load")

    assert outcome.exit_code is runtime.ExitCode.SUCCESS
    assert lock.acquire("plain.load/nothing", "next run", 60)


class TestFileRunLock:

    def test_a_lock_file_half_written_is_taken_over_once_expired(
        self, tmp_path: pathlib.Path
    ) -> None:
        lock = runtime.FileRunLock(tmp_path, clock=lambda: 1000.0)
        # A process died writing it: no owner, no lease.
        (tmp_path / "p.lock").write_text('{"owner": "a", "exp', "utf-8")

        assert lock.acquire("p", "b", 10)
        assert not lock.acquire("p", "c", 10)

    def test_a_lock_file_created_meanwhile_is_not_taken(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        lock = runtime.FileRunLock(tmp_path)
        create = os.open

        def raced(path: str, flags: int, *args: Any) -> int:
            # Another process creates it between the read and the create.
            create(path, flags, *args)
            raise FileExistsError(path)

        monkeypatch.setattr(os, "open", raced)

        assert not lock.acquire("p", "b", 10)
