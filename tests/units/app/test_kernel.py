from __future__ import annotations

from collections.abc import Sequence
import getpass
import io
import json
import os
import pathlib
import signal
import threading
import time
from typing import Any

import pytest
import sqlalchemy as sa

from app import bootstrap
from cli import run
from core import contract
from core import pipeline
from core import runtime
from core.observability import metrics
from core.runtime import names
from tests.factories import fakes
from tests.factories import ledgers


@pytest.fixture
def project(tmp_path: pathlib.Path) -> pathlib.Path:
    """A config directory, a SQLite database with a source table."""
    database = tmp_path / "src.db"
    engine = sa.create_engine(f"sqlite:///{database}")
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "CREATE TABLE source (id INTEGER PRIMARY KEY, name TEXT)"
        )
        connection.exec_driver_sql(
            "CREATE TABLE target (id INTEGER PRIMARY KEY, name TEXT)"
        )
        connection.exec_driver_sql(
            "INSERT INTO source VALUES (1, 'a'), (2, 'b')"
        )
    engine.dispose()
    directory = tmp_path / "config"
    directory.mkdir()
    (directory / "base.toml").write_text(
        f"""
[databases.src]
url = "sqlite:///{database}"
[bookkeeping]
directory = "{tmp_path / 'var'}"
[modules.demo]
module = "tests.factories.demo_module:create"
""",
        encoding="utf-8",
    )
    return directory


def _etl(project: pathlib.Path, *argv: str) -> tuple[int, str]:
    out = io.StringIO()
    code = run.main(["--env", "test", "--config", str(project), *argv], out)
    return code, out.getvalue()


def _targets(project: pathlib.Path) -> list[tuple[Any, ...]]:
    engine = sa.create_engine(f"sqlite:///{project.parent / 'src.db'}")
    with engine.connect() as connection:
        rows = connection.exec_driver_sql(
            "SELECT id, name FROM target ORDER BY id"
        ).all()
    engine.dispose()
    return [tuple(row) for row in rows]


class TestCommands:

    def test_list_plan_check(self, project: pathlib.Path) -> None:
        assert _etl(project, "list") == (0, "demo.broken\ndemo.copy\n")
        assert _etl(project, "plan", "demo.copy") == (0, "copy\n")
        code, out = _etl(project, "check")
        assert (code, out) == (0, "ok   database.src\n")

    def test_runs_are_incremental_and_recorded(
        self, project: pathlib.Path
    ) -> None:
        code, out = _etl(project, "run", "demo.copy", "--format", "json")
        again = _etl(project, "run", "demo.copy", "--format", "json")

        first = json.loads(out)
        assert (code, first["status"], first["exit_code"]) == (0, "success", 0)
        assert (first["stats"]["total"], first["stats"]["created"]) == (2, 2)
        assert json.loads(again[1])["stats"]["total"] == 0
        assert _targets(project) == [(1, "a"), (2, "b")]
        code, runs = _etl(project, "runs", "demo.copy", "--format", "json")
        assert code == 0
        assert [run["status"] for run in json.loads(runs)] == ["success"] * 2
        assert json.loads(runs)[1]["run_id"] == first["run_id"]

    def test_dry_runs_load_nothing(self, project: pathlib.Path) -> None:
        code, out = _etl(project, "run", "demo.copy", "--dry-run")

        assert (code, _targets(project)) == (0, [])
        assert "2 read, 0 created" in out

    def test_failures_and_mistakes_have_their_exit_codes(
        self, project: pathlib.Path
    ) -> None:
        code, out = _etl(project, "run", "demo.broken", "--format", "json")
        assert (code, json.loads(out)["status"]) == (1, "failed")
        assert _etl(project, "run", "demo.nope")[0] == 2
        assert "No pipeline 'demo.nope'" in _etl(project, "run", "demo.nope")[1]
        assert (
            _etl(project, "--config", str(project / "missing"), "list")[0] == 0
        )

    def test_a_running_pipeline_is_not_run_twice(
        self, project: pathlib.Path
    ) -> None:
        lock = runtime.FileRunLock(project.parent / "var" / "locks")
        assert lock.acquire("demo.copy/copy", "another host", 60)

        code, out = _etl(project, "run", "demo.copy")

        assert (code, out) == (4, "demo.copy: already running\n")
        lock.release("demo.copy/copy", "another host")
        assert _etl(project, "run", "demo.copy")[0] == 0

    def test_runs_and_their_steps_as_json(self, project: pathlib.Path) -> None:
        lock = runtime.FileRunLock(project.parent / "var" / "locks")
        assert lock.acquire("demo.copy/copy", "another host", 60)
        locked = _etl(project, "run", "demo.copy", "--format", "json")
        lock.release("demo.copy/copy", "another host")
        code, out = _etl(
            project,
            "run",
            "demo.copy",
            "--label",
            "nightly",
            "--format",
            "json",
        )
        run_id = json.loads(out)["run_id"]

        shown = _etl(project, "runs", "--run", run_id, "--format", "json")
        missing = _etl(project, "runs", "--run", "nope", "--format", "json")
        invalid = _etl(project, "run", "demo.nope", "--format", "json")

        assert locked[0] == 4
        assert json.loads(locked[1]) == {
            "pipeline": "demo.copy",
            "run_id": None,
            "status": None,
            "exit_code": 4,
            "resumed_from": None,
            "stats": {},
            "message": "already running",
        }
        assert code == 0
        assert shown[0] == 0
        run = json.loads(shown[1])
        assert (run["run_id"], run["status"], run["meta"]["label"]) == (
            run_id,
            "success",
            "nightly",
        )
        ((step),) = run["steps"]
        assert (step["step"], step["status"], step["attempts"]) == (
            "copy",
            "success",
            1,
        )
        assert step["stats"]["created"] == 2
        assert missing == (7, "null\n")
        assert invalid[0] == 2
        assert json.loads(invalid[1])["exit_code"] == 2
        assert "No pipeline 'demo.nope'" in json.loads(invalid[1])["message"]

    def test_a_pipeline_that_cannot_be_built_is_invalid_not_failed(
        self, project: pathlib.Path
    ) -> None:
        with (project / "base.toml").open("a", encoding="utf-8") as file:
            file.write(
                "[modules.unbuilt]\n"
                'module = "tests.units.app.test_kernel:unbuilt"\n'
            )

        for pipeline_name in ("unbuilt.settings", "unbuilt.mappings"):
            code, out = _etl(project, "run", pipeline_name)
            assert code == 2, out
            assert "FAILED" in out
            assert _etl(project, "plan", pipeline_name)[0] == 2
        # Nothing recorded: no failed run, for a run that never started.
        assert _etl(project, "runs") == (0, "")


def test_logs_carry_the_run(project: pathlib.Path) -> None:
    stream = io.StringIO()
    with bootstrap.bootstrap(
        "test",
        directory=project,
        overrides={"logging": {"format": "json"}},
        log_stream=stream,
    ) as application:
        outcome = application.kernel.run("demo.copy")
    lines = [json.loads(line) for line in stream.getvalue().splitlines()]

    assert outcome.result is not None
    run_id = outcome.result.run_id
    events = {
        line.get("etl.event"): line
        for line in lines
        if line.get("etl.run_id") == run_id
    }
    assert {"pipeline.start", "step.start", "step.end", "pipeline.end"} <= set(
        events
    )
    assert events["step.end"]["etl.step"] == "copy"
    assert events["step.end"]["etl.stats.created"] == 2


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


class _Unbuilt(runtime.Module):
    """Pipelines that cannot be built: bad settings, missing mappings."""

    @property
    def name(self) -> str:
        return "unbuilt"

    def pipelines(self) -> dict[str, runtime.PipelineFactory]:
        def settings(resources: runtime.Resources) -> pipeline.BaseOrchestrator:
            raise contract.PipelineConfigError("No compiled mappings at x")

        def mappings(resources: runtime.Resources) -> pipeline.BaseOrchestrator:
            raise contract.FatalError("No change log on sqlite")

        return {"settings": settings, "mappings": mappings}


def unbuilt(settings: object) -> runtime.Module:
    """The entry point of `_Unbuilt`, as the settings name it."""
    del settings  # Unused.
    return _Unbuilt()


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


class _Keyed(runtime.Module):
    """A module whose keys are made with `keys`."""

    def __init__(self) -> None:
        self.keys = "k1"

    @property
    def name(self) -> str:
        return "keyed"

    def pipelines(self) -> dict[str, runtime.PipelineFactory]:
        return {"load": lambda resources: _Nothing()}

    def fingerprint(self) -> dict[str, str]:
        return {runtime.KEYS: self.keys, "mappings": "abc"}


class _Nothing(pipeline.BaseOrchestrator):

    def steps(self) -> Sequence[contract.PipelineStep]:
        def nothing(
            ctx: contract.ETLContext, upstream: Any
        ) -> contract.ETLResult:
            return contract.ETLResult()

        return [contract.PipelineStep("nothing", nothing)]


class TestKeys:

    def test_a_run_whose_keys_changed_is_refused_unless_rekeying(
        self,
    ) -> None:
        module = _Keyed()
        ledger = runtime.InMemoryRunLedger()
        kernel = runtime.Kernel(
            runtime.Resources(
                [runtime.ResourceSpec(names.LEDGER, lambda r: ledger)]
            ),
            [module],
        )
        first = kernel.run("keyed.load")
        module.keys = "k2"  # The key secret was rotated.

        with pytest.raises(
            contract.PipelineConfigError, match="keys of keyed.load changed"
        ):
            kernel.run("keyed.load")
        rekeyed = kernel.run("keyed.load", rekey=True)
        after = kernel.run("keyed.load")

        assert first.exit_code is runtime.ExitCode.SUCCESS
        assert rekeyed.exit_code is runtime.ExitCode.SUCCESS
        assert after.exit_code is runtime.ExitCode.SUCCESS
        assert rekeyed.result is not None
        record = ledger.get(rekeyed.result.run_id)
        assert record is not None
        assert record.meta[runtime.KEYS] == "k2"
        assert record.meta["rekeyed_from"] == "k1"
        # The refused run was not recorded.
        assert len(ledger.recent("keyed.load")) == 3

    def test_keys_are_compared_one_by_one(self) -> None:
        module = _Keyed()
        module.keys = "customer:aaa,order:bbb"
        ledger = runtime.InMemoryRunLedger()
        kernel = runtime.Kernel(
            runtime.Resources(
                [runtime.ResourceSpec(names.LEDGER, lambda r: ledger)]
            ),
            [module],
        )
        kernel.run("keyed.load")

        # A mapping added, one removed: no key changes.
        module.keys = "customer:aaa,invoice:ccc"
        added = kernel.run("keyed.load")
        # The customer table renamed: its keys change.
        module.keys = "customer:ddd,invoice:ccc"
        with pytest.raises(
            contract.PipelineConfigError, match="customer: aaa then, ddd now"
        ):
            kernel.run("keyed.load")

        assert added.exit_code is runtime.ExitCode.SUCCESS

    def test_only_successful_runs_that_loaded_count(self) -> None:
        module = _Keyed()
        ledger = runtime.InMemoryRunLedger()
        kernel = runtime.Kernel(
            runtime.Resources(
                [runtime.ResourceSpec(names.LEDGER, lambda r: ledger)]
            ),
            [module],
        )
        kernel.run("keyed.load", dry_run=True)
        module.keys = "k2"

        # A dry run loaded nothing: no keys to keep.
        assert kernel.run("keyed.load").exit_code is runtime.ExitCode.SUCCESS


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


def test_modules_must_be_named_as_in_the_settings(
    tmp_path: pathlib.Path,
) -> None:
    with pytest.raises(contract.PipelineConfigError, match="named 'demo'"):
        bootstrap.bootstrap(
            "test",
            directory=tmp_path,
            overrides={
                "modules": {
                    "other": {"module": "tests.factories.demo_module:create"}
                }
            },
            logs=False,
        )
    with pytest.raises(contract.PipelineConfigError, match="Cannot import"):
        bootstrap.bootstrap(
            "test",
            directory=tmp_path,
            overrides={"modules": {"x": {"module": "no.such.module:create"}}},
            logs=False,
        )
    with pytest.raises(contract.PipelineConfigError, match="entry point"):
        bootstrap.bootstrap(
            "test",
            directory=tmp_path,
            overrides={"modules": {"x": {}}},
            logs=False,
        )


def test_file_and_memory_ledgers_and_locks(tmp_path: pathlib.Path) -> None:
    clock = [1000.0]
    for lock in (
        runtime.InMemoryRunLock(clock=lambda: clock[0]),
        runtime.FileRunLock(tmp_path / str(len(clock)), clock=lambda: clock[0]),
    ):
        clock.append(0)
        assert lock.acquire("p", "a", 10)
        assert not lock.acquire("p", "b", 10)
        assert lock.renew("p", "a", 10)
        assert not lock.renew("p", "b", 10)
        clock[0] += 20  # Expired: taken over.
        assert lock.acquire("p", "b", 10)
        lock.release("p", "a")  # Not its owner: kept.
        assert not lock.acquire("p", "c", 10)
        lock.release("p", "b")
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


def test_run_parameters_reach_the_run() -> None:
    parser = run._parser()  # pylint: disable=protected-access
    args = parser.parse_args(
        ["run", "crm.replay", "--param", "a=b", "--param", "url=x=y"]
    )

    assert dict(args.param) == {"a": "b", "url": "x=y"}
    with pytest.raises(SystemExit):
        parser.parse_args(["run", "crm.replay", "--param", "nothing"])
    assert contract.ETLRunInfo("p", params={"a": "b"}).params == {"a": "b"}


def test_what_runs_is_recorded(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import config  # pylint: disable=import-outside-toplevel

    def meta(password: str) -> dict[str, str]:
        settings = config.load_settings(
            "test",
            directory=tmp_path / "none",
            overrides={
                "databases": {
                    "crm": {"url": f"postgresql://u:{password}@h/crm"}
                }
            },
        )
        return bootstrap.run_meta(settings)

    monkeypatch.setenv(bootstrap.BUILD_VARIABLE, "v1.2.3")
    first = meta("one")

    assert first["build"] == "v1.2.3"
    assert first["env"] == "test"
    assert first["version"]
    # Secrets are hidden: rotating one does not change the settings' hash.
    assert meta("two")["settings"] == first["settings"]


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
