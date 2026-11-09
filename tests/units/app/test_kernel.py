"""The kernel through its commands: `etl list`, `plan`, `run`, `runs`; the
keys of a module; what a run records. Its locks, ledger and signals are in
`test_kernel_lifecycle.py`.
"""

from __future__ import annotations

from collections.abc import Sequence
import io
import json
import pathlib
import time
from typing import Any

import pytest
import sqlalchemy as sa

from app import bootstrap
from cli import run
from core import contract
from core import pipeline
from core import runtime
from core.runtime import names


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


class TestBookkeepingDown:
    """No traceback, whichever command, when the bookkeeping is down."""

    @pytest.fixture(name="down")
    def _down(self, project: pathlib.Path) -> pathlib.Path:
        base = project / "base.toml"
        base.write_text(
            base.read_text(encoding="utf-8") + """
[databases.book]
url = "sqlite:////no/such/dir/S3cr3tPW/book.db"
""" + "\n",
            encoding="utf-8",
        )
        text = base.read_text(encoding="utf-8").replace(
            "[bookkeeping]\n", '[bookkeeping]\ndatabase = "book"\n'
        )
        base.write_text(text, encoding="utf-8")
        return project

    @pytest.mark.parametrize(
        "command", [["runs"], ["runs", "demo.copy"], ["run", "demo.copy"]]
    )
    def test_a_command_fails_in_one_line(
        self, down: pathlib.Path, command: list[str]
    ) -> None:
        code, out = _etl(down, *command)

        assert code == 1
        (line,) = out.splitlines()
        assert line.startswith("FAILED "), out
        assert "Traceback" not in out

    def test_a_json_run_ends_with_its_summary(self, down: pathlib.Path) -> None:
        code, out = _etl(down, "run", "demo.copy", "--format", "json")

        assert code == 1
        summary = json.loads(out.splitlines()[-1])
        assert (summary["exit_code"], summary["run_id"]) == (1, None)
        assert summary["message"]

    def test_a_url_s_password_is_not_shown(self, project: pathlib.Path) -> None:
        base = project / "base.toml"
        text = base.read_text(encoding="utf-8").replace(
            "[bookkeeping]\n", '[bookkeeping]\ndatabase = "book"\n'
        )
        base.write_text(
            text
            + "[databases.book]\n"
            + 'url = "postgresql+psycopg://etl:S3cr3tPW@127.0.0.1:9/crm"\n',
            encoding="utf-8",
        )

        started = time.monotonic()
        code, out = _etl(project, "run", "demo.copy", "--format", "json")

        assert code == 1
        assert "S3cr3tPW" not in out
        assert json.loads(out.splitlines()[-1])["exit_code"] == 1
        assert time.monotonic() - started < 30


class TestCommands:

    def test_list_plan_check(self, project: pathlib.Path) -> None:
        assert _etl(project, "list") == (0, "demo.broken\ndemo.copy\n")
        assert _etl(project, "plan", "demo.copy") == (0, "copy\n")
        code, out = _etl(project, "check")
        tuning, resources = out.split("\n", 1)
        assert (code, resources) == (0, "ok   database.src\n")
        assert tuning.startswith("tuning memory_budget=")
        assert tuning.endswith("(* derived; etl config tuning)")

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


class _Keyed(runtime.Module):
    """A module whose keys are made with `keys`."""

    def __init__(self, name: str = "keyed") -> None:
        self.keys = "k1"
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    def pipelines(self) -> dict[str, runtime.PipelineFactory]:
        return {
            "load": lambda resources: _Nothing(),
            "sync": lambda resources: _Nothing(),
        }

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


class TestKeysOfTheModule:
    """The keys are the module's: every pipeline of it loads the same rows."""

    def _kernel(
        self, *modules: runtime.Module
    ) -> tuple[runtime.Kernel, runtime.InMemoryRunLedger]:
        ledger = runtime.InMemoryRunLedger()
        kernel = runtime.Kernel(
            runtime.Resources(
                [runtime.ResourceSpec(names.LEDGER, lambda r: ledger)]
            ),
            modules,
        )
        return kernel, ledger

    def test_a_sibling_pipeline_is_refused_after_a_rotation(self) -> None:
        module = _Keyed()
        kernel, ledger = self._kernel(module)
        kernel.run("keyed.load")
        module.keys = "k2"

        with pytest.raises(
            contract.PipelineConfigError, match="keys of keyed.sync changed"
        ):
            kernel.run("keyed.sync")
        rekeyed = kernel.run("keyed.sync", rekey=True)

        assert rekeyed.result is not None
        record = ledger.get(rekeyed.result.run_id)
        assert record is not None
        assert record.meta["rekeyed_from"] == "k1"
        # Re-keyed by its sibling: the other pipeline runs on.
        assert kernel.run("keyed.load").exit_code is runtime.ExitCode.SUCCESS

    def test_runs_that_did_not_load_never_hide_the_last_one(self) -> None:
        module = _Keyed()
        kernel, ledger = self._kernel(module)
        kernel.run("keyed.load")
        for _ in range(250):
            failed = contract.ETLRunInfo("keyed.load")
            ledger.start(failed, "h", {runtime.KEYS: "k2"})
            result = contract.PipelineResult(
                pipeline_name="keyed.load", run_id=failed.run_id
            )
            result.complete(contract.ETLStatus.FAILED, "source down")
            ledger.finish(result)
        for _ in range(250):
            kernel.run("keyed.sync", dry_run=True)
        module.keys = "k2"

        with pytest.raises(contract.PipelineConfigError, match="k1 then"):
            kernel.run("keyed.load")

    def test_another_module_s_runs_do_not_count(self) -> None:
        # `keyedx.load` starts like `keyed.`'s pipelines, but is not one.
        module, other = _Keyed(), _Keyed("keyedx")
        other.keys = "x1"
        kernel, _ = self._kernel(module, other)
        kernel.run("keyedx.load")

        assert kernel.run("keyed.load").exit_code is runtime.ExitCode.SUCCESS
        other.keys = "x2"
        with pytest.raises(contract.PipelineConfigError, match="x1 then"):
            kernel.run("keyedx.sync")


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
