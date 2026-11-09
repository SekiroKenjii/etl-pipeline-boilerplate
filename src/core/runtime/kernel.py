"""The kernel: runs the pipelines of the modules.

For every run it:

- builds the pipeline in a run scope; a pipeline that cannot be built is a
  configuration error, not a failed run;
- takes a lease lock per step it runs (`<pipeline>/<step>`): the same step
  never runs twice at once, other steps (`--step`) may;
- records the run in the ledger and binds the log context;
- turns SIGTERM and SIGINT into a cancellation: running steps end their
  batch, no new step starts;
- turns the outcome into an exit code.

A run may select steps (`steps`), resume an earlier run (`resume`: finished
steps are carried over) and carry a scheduler's job name (`label`: resuming
without a run id resumes the last run of that label only).
"""

from __future__ import annotations

from collections.abc import Collection, Generator, Mapping, Sequence
import contextlib
import dataclasses
import datetime
import enum
import fnmatch
import functools
import getpass
import logging
import os
import signal
import socket
import threading
import time
import types

from core import contract
from core import pipeline
from core.observability import context as observability_context
from core.observability import metrics
from core.observability import redaction
from core.runtime import locks
from core.runtime import module as runtime_module
from core.runtime import names
from core.runtime import resources as runtime_resources
from core.runtime import steps as runtime_steps
from core.runtime import types as runtime_types

_LOG = logging.getLogger("etl.kernel")


class ExitCode(enum.IntEnum):
    """What a run tells its scheduler."""

    SUCCESS = 0
    FAILED = 1
    INVALID = 2
    PARTIAL_SUCCESS = 3
    LOCKED = 4
    CANCELLED = 130


_REFUSED = {
    locks.Refusal.LOCKED: ExitCode.LOCKED,
    locks.Refusal.CANCELLED: ExitCode.CANCELLED,
}

_EXIT_CODES = {
    contract.ETLStatus.SUCCESS: ExitCode.SUCCESS,
    contract.ETLStatus.PARTIAL_SUCCESS: ExitCode.PARTIAL_SUCCESS,
    contract.ETLStatus.CANCELLED: ExitCode.CANCELLED,
    contract.ETLStatus.SKIPPED: ExitCode.SUCCESS,
}


@dataclasses.dataclass(frozen=True, slots=True)
class RunOutcome:
    """How a run went.

    Attributes:
        exit_code: For the scheduler.
        result: The result of the pipeline; None if it never started.
        message: Why, when it never started.
        resumed_from: The run it resumed, if any.
    """

    exit_code: ExitCode
    result: contract.PipelineResult | None = None
    message: str | None = None
    resumed_from: str | None = None


class Kernel:
    """Runs the pipelines of the modules.

    Attributes:
        resources: The resources of the process.
        modules: The modules, by name.
        lease_seconds: The lease of the run locks; renewed every third of
            it.
        max_parallel_steps: The most steps a run runs at the same time here,
            whatever its module asks; None lets the module decide.
        redact: More secret names hidden in what the ledger keeps, the
            outcome tells and `check` reports of an error.
    """

    def __init__(
        self,
        resources: runtime_resources.Resources,
        modules: Sequence[runtime_module.Module],
        *,
        listeners: Sequence[pipeline.PipelineListener] = (),
        lease_seconds: float = 300.0,
        max_parallel_steps: int | None = None,
        host: str | None = None,
        meta: Mapping[str, str] | None = None,
        redact: Collection[str] = (),
        lock_wait: locks.LockWait | None = None,
    ) -> None:
        """Initializes the kernel.

        Args:
            resources: The resources of the process: the ledger and the
                lock are `names.LEDGER` and `names.LOCK`, if declared.
            modules: The modules.
            listeners: Added to every pipeline, e.g. run logging.
            lease_seconds: The lease of the run locks.
            max_parallel_steps: The most steps a run runs at the same time
                here; None lets the module decide.
            host: Where runs happen, for the ledger; the host name by
                default.
            meta: What runs, for the ledger: the version, the build, the
                hash of the settings. The user and the fingerprint of the
                module are added at each run.
            redact: More secret names hidden in what the ledger keeps and
                the outcome tells of an error, on top of
                `redaction.SECRET_NAMES`: the logs' `redact`.
            lock_wait: How a run of a label waits for the locks of its
                label; `LockWait()` by default.

        Raises:
            PipelineConfigError: If two modules have the same name.
        """
        by_name: dict[str, runtime_module.Module] = {}
        for module in modules:
            if module.name in by_name:
                raise contract.PipelineConfigError(
                    f"Two modules are named {module.name!r}"
                )
            by_name[module.name] = module
        self.resources = resources
        self.modules: Mapping[str, runtime_module.Module] = by_name
        self.lease_seconds = lease_seconds
        self.max_parallel_steps = max_parallel_steps
        self._listeners = tuple(listeners)
        self._host = host or socket.gethostname()
        self._meta = dict(meta or {})
        self.redact = tuple(redact)
        self._lock_wait = lock_wait or locks.LockWait()

    def pipelines(self) -> list[str]:
        """Returns every pipeline, `module.pipeline`, sorted."""
        return sorted(
            f"{module.name}.{name}"
            for module in self.modules.values()
            for name in module.pipelines()
        )

    def factory(self, name: str) -> runtime_types.PipelineFactory:
        """Returns how to build pipeline `module.pipeline`.

        Raises:
            PipelineConfigError: If there is no such pipeline.
        """
        module_name, _, pipeline_name = name.partition(".")
        module = self.modules.get(module_name)
        found = module.pipelines().get(pipeline_name) if module else None
        if found is None:
            raise contract.PipelineConfigError(
                f"No pipeline {name!r}; known: {self.pipelines()}"
            )
        return found

    def plan(self, name: str) -> list[contract.PipelineStep]:
        """Returns the steps of a pipeline, in the order they may run.

        Raises:
            PipelineConfigError: If there is no such pipeline, it cannot be
                built, or its steps cannot be ordered.
        """
        factory = self.factory(name)
        with self.resources.run_scope() as run:
            return _build(name, factory, run)[1]

    # Ten options; each phase is already one call (_build, _execute...).
    def run(  # pylint: disable=too-many-locals
        self,
        name: str,
        *,
        since: datetime.datetime | None = None,
        until: datetime.datetime | None = None,
        dry_run: bool = False,
        params: Mapping[str, str] | None = None,
        steps: Sequence[str] = (),
        resume: bool | str = False,
        label: str | None = None,
        rekey: bool = False,
    ) -> RunOutcome:
        """Runs a pipeline once.

        Args:
            name: The pipeline, `module.pipeline`.
            since: The start of the extraction window.
            until: Its end.
            dry_run: Whether to load nothing.
            params: The parameters of the run.
            steps: The steps to run, as names or patterns (`customer*`);
                every step when empty. Their dependencies are taken as done.
            resume: Resumes a run, by id, or True for the last run of the
                `label`, whatever its outcome, or without a label the last
                run of the pipeline unless it succeeded: the steps it
                finished are not run again.
            label: The scheduler's name of the job, recorded with the run:
                a retry of the job resumes its run (`resume=True`). A run
                of a label never runs while another holds the label's locks:
                it waits for them up to the lease (a try that died frees
                them when its lease expires), then gives up (`LOCKED`).
            rekey: Runs even if the module's keys changed since its last run
                that loaded, of any of its pipelines (`Module.fingerprint`):
                every surrogate key changes.

        Returns:
            How it went; never raises for the pipeline's own failures.

        Raises:
            PipelineConfigError: If there is no such pipeline, step or run to
                resume; the pipeline cannot be built (its settings, its
                mappings...); its keys changed and `rekey` is not set; or
                the ledger needed is missing. Nothing is recorded then.
        """
        factory = self.factory(name)
        locks.check_label(label)
        ledger = self._optional(names.LEDGER, contract.IRunLedger)
        resumed = self._to_resume(name, resume, label, ledger)
        fingerprint = self._fingerprint(name)
        meta = functools.partial(
            _run_meta,
            {**self._meta, **fingerprint},
            label=label,
            rekeyed=_check_keys(name, fingerprint, ledger, rekey=rekey),
        )

        with self.resources.run_scope() as scope:
            try:
                orchestrator, plan = _build(name, factory, scope)
            except contract.PipelineConfigError:
                raise
            # Anything else may pass (a source down): a failed run, retried.
            except Exception as exc:  # pylint: disable=broad-exception-caught
                unbuilt = contract.ETLRunInfo(name, dry_run=dry_run)
                return self._unbuilt(
                    unbuilt, exc, ledger, meta(unbuilt, resumed=resumed)
                )
            ctx = contract.ETLContext(
                contract.ETLRunInfo(
                    pipeline_name=name,
                    since=since,
                    until=until,
                    dry_run=dry_run,
                    params=dict(params or {}),
                    steps=_select(name, plan, steps) if steps else frozenset(),
                )
            )
            with _running(ctx):
                taken = self._lock_steps(
                    plan,
                    ctx,
                    label=label,
                    ledger=ledger,
                    resume=resume,
                    resumed=resumed,
                )
                if isinstance(taken, locks.Refusal):
                    return RunOutcome(_REFUSED[taken], message=taken.value)
                leases, resumed, carried = taken
                result = self._run_locked(
                    orchestrator,
                    ctx,
                    leases,
                    ledger=ledger,
                    carried=carried,
                    meta=meta(ctx.run, resumed=resumed),
                )
        return RunOutcome(_exit_code(result), result, resumed_from=resumed)

    def _lock_steps(
        self,
        plan: Sequence[contract.PipelineStep],
        ctx: contract.ETLContext,
        *,
        label: str | None,
        ledger: contract.IRunLedger | None,
        resume: bool | str,
        resumed: str | None,
    ) -> (
        locks.Refusal
        | tuple[locks.Leases, str | None, dict[str, runtime_steps.DoneStep]]
    ):
        """Takes the locks of the steps a run runs, but those it carries
        over from the run it resumes (`locks.Locking.lock_steps`)."""
        locking = locks.Locking(
            self._optional(names.LOCK, contract.IRunLock),
            self.lease_seconds,
            self._lock_wait,
        )
        return locking.lock_steps(
            plan,
            ctx,
            locks.owner_of(self._host, label, ctx.run.run_id),
            label=label,
            ledger=ledger,
            resumed=resumed,
            resume_again=(
                functools.partial(
                    self._to_resume,
                    ctx.run.pipeline_name,
                    resume,
                    label,
                    ledger,
                )
                if resume
                else None
            ),
        )

    def _run_locked(
        self,
        orchestrator: pipeline.BaseOrchestrator,
        ctx: contract.ETLContext,
        leases: locks.Leases,
        *,
        ledger: contract.IRunLedger | None,
        carried: Mapping[str, runtime_steps.DoneStep],
        meta: Mapping[str, str],
    ) -> contract.PipelineResult:
        """Runs the pipeline built, its locks taken, recording it in the
        ledger; frees the locks."""
        try:
            with self._heartbeat(leases, ctx):
                if ledger is not None:
                    ledger.start(ctx.run, self._host, meta)
                result = self._execute(
                    orchestrator,
                    ctx,
                    None
                    if ledger is None
                    else runtime_steps.LedgerSteps(
                        ledger,
                        ctx.run.run_id,
                        f"{self._host}:{os.getpid()}",
                        carried=carried,
                        redact=self.redact,
                    ),
                )
                runtime_steps.hide_secrets(result, self.redact)
                if ledger is not None:
                    ledger.finish(result)
        finally:
            leases.release()
        return result

    def _unbuilt(
        self,
        run: contract.ETLRunInfo,
        error: Exception,
        ledger: contract.IRunLedger | None,
        meta: Mapping[str, str],
    ) -> RunOutcome:
        """Records a run whose pipeline could not be built: FAILED."""
        ctx = contract.ETLContext(run)
        with observability_context.log_context(
            **observability_context.run_fields(ctx)
        ):
            _LOG.error(
                "%s could not be built: %s", ctx.run.pipeline_name, error
            )
            result = contract.PipelineResult(
                pipeline_name=ctx.run.pipeline_name, run_id=ctx.run.run_id
            )
            result.complete(contract.ETLStatus.FAILED, str(error))
            runtime_steps.hide_secrets(result, self.redact)
            if ledger is not None:
                ledger.start(ctx.run, self._host, meta)
                ledger.finish(result)
        return RunOutcome(ExitCode.FAILED, result)

    def _to_resume(
        self,
        name: str,
        resume: bool | str,
        label: str | None,
        ledger: contract.IRunLedger | None,
    ) -> str | None:
        """The run to resume: the one given; or the last one of the label,
        if any, whatever its outcome (a retry after a success runs nothing);
        or the last one of the pipeline unless it succeeded. None if there is
        none, or `resume` is False.

        Raises:
            PipelineConfigError: If there is no ledger; or the run given
                does not exist, is of another pipeline, or was a dry run.
        """
        if not resume:
            return None
        if ledger is None:
            raise contract.PipelineConfigError(
                f"Resuming a run needs a run ledger ({names.LEDGER})"
            )
        if isinstance(resume, str):
            record = ledger.get(resume)
            if record is None or record.pipeline != name:
                raise contract.PipelineConfigError(
                    f"No run {resume!r} of {name} to resume"
                )
            if record.dry_run:
                raise contract.PipelineConfigError(
                    f"The run {resume!r} was a dry run: it loaded nothing"
                )
            return record.run_id
        last = ledger.last_run(name, label)
        if last is None or (
            label is None and last.status is contract.ETLStatus.SUCCESS
        ):
            _LOG.info("%s: no run to resume, every step runs", name)
            return None
        return last.run_id

    def check(self) -> dict[str, runtime_resources.Health]:
        """Checks the resources that can be: `ok` and its warnings, or what
        is wrong, redacted."""
        return {
            name: runtime_resources.Health(
                redaction.redact_text(health.status, self.redact),
                tuple(
                    redaction.redact_text(warning, self.redact)
                    for warning in health.warnings
                ),
            )
            for name, health in self.resources.health().items()
        }

    def _execute(
        self,
        orchestrator: pipeline.BaseOrchestrator,
        ctx: contract.ETLContext,
        coordinator: pipeline.StepCoordinator | None,
    ) -> contract.PipelineResult:
        """Runs the pipeline built.

        A pipeline that fails to start (its `setup`: a missing table, a
        source down...) is a FAILED run.
        """
        try:
            if self.max_parallel_steps is not None:
                orchestrator.cap_workers(self.max_parallel_steps)
            for listener in self._listeners:
                orchestrator.add_listener(listener)
            return orchestrator.run(ctx, coordinator)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            _LOG.exception("%s could not run: %s", ctx.run.pipeline_name, exc)
            result = contract.PipelineResult(
                pipeline_name=ctx.run.pipeline_name, run_id=ctx.run.run_id
            )
            result.complete(contract.ETLStatus.FAILED, str(exc))
            return result

    def _fingerprint(self, name: str) -> dict[str, str]:
        """The fingerprint of the pipeline's module, or why there is none."""
        module = self.modules.get(name.partition(".")[0])
        if module is None:
            return {}
        try:
            return dict(module.fingerprint())
        # The ledger records the run whatever the fingerprint does.
        except Exception as exc:  # pylint: disable=broad-exception-caught
            return {
                "fingerprint_error": redaction.redact_text(
                    str(exc), self.redact
                )
            }

    def _optional[T](self, name: str, kind: type[T]) -> T | None:
        if name not in self.resources.names:
            return None
        return self.resources.get(name, kind)

    @contextlib.contextmanager
    def _heartbeat(
        self, leases: locks.Leases, ctx: contract.ETLContext
    ) -> Generator[None]:
        """Renews the leases of the run while it goes on; cancels the run
        if one is lost, or cannot be renewed before it expires.

        A renewal that fails (the lock's database is unreachable) is tried
        again every tenth of the lease, while the lease lasts.
        """
        if leases.lock is None or not leases.names:
            yield
            return
        stop = threading.Event()
        context = observability_context.current()
        lease = self.lease_seconds

        def renew() -> None:
            with observability_context.log_context(**context):
                renewed, wait = time.monotonic(), lease / 3
                while not stop.wait(wait):
                    started = time.monotonic()
                    try:
                        held = leases.renew(lease)
                    # Whatever the lock raises: never outlive the lease.
                    except Exception as exc:  # pylint: disable=broad-exception-caught
                        left = renewed + lease - time.monotonic()
                        if left > lease / 10:
                            _LOG.warning(
                                "could not renew the run lock of %s, %.0fs"
                                " before it expires: %s",
                                leases.pipeline,
                                left,
                                exc,
                            )
                            wait = lease / 10
                            continue
                        _LOG.exception(
                            "could not renew the run lock of %s: %s",
                            leases.pipeline,
                            exc,
                        )
                        held = False
                    if held:
                        renewed, wait = started, lease / 3
                        continue
                    ctx.cancel()
                    _lost(leases.pipeline)
                    return

        thread = threading.Thread(
            target=renew, name=f"lease:{leases.pipeline}", daemon=True
        )
        thread.start()
        try:
            yield
        finally:
            stop.set()
            thread.join()


def _build(
    name: str,
    factory: runtime_types.PipelineFactory,
    resources: runtime_resources.Resources,
) -> tuple[pipeline.BaseOrchestrator, list[contract.PipelineStep]]:
    """Builds a pipeline, and orders its steps.

    Raises:
        PipelineConfigError: If it cannot be built: its settings, mappings
            or steps are wrong (a `PipelineConfigError` or a `FatalError` of
            the module), so running it again would not help.
    """
    try:
        orchestrator = factory(resources)
        return orchestrator, pipeline.resolve_execution_order(
            orchestrator.steps()
        )
    except contract.PipelineConfigError as exc:
        _LOG.error("%s cannot be built: %s", name, exc, exc_info=False)
        raise
    except contract.FatalError as exc:
        _LOG.error("%s cannot be built: %s", name, exc, exc_info=False)
        raise contract.PipelineConfigError(
            f"{name} cannot be built: {exc}"
        ) from exc


def _select(
    name: str, plan: Sequence[contract.PipelineStep], patterns: Sequence[str]
) -> frozenset[str]:
    """The steps of a pipeline matching any of `patterns`.

    Raises:
        PipelineConfigError: If a pattern matches no step.
    """
    known = [step.name for step in plan]
    selected: set[str] = set()
    for pattern in patterns:
        found = fnmatch.filter(known, pattern)
        if not found:
            raise contract.PipelineConfigError(
                f"No step of {name} matches {pattern!r}; known: {known}"
            )
        selected.update(found)
    return frozenset(selected)


def _check_keys(
    name: str,
    fingerprint: Mapping[str, str],
    ledger: contract.IRunLedger | None,
    *,
    rekey: bool,
) -> str | None:
    """Checks that the module makes its keys as in its last run that loaded
    (the `runtime.KEYS` entry of its fingerprint), of any of its pipelines:
    they load the same tables.

    Returns:
        The keys of that run when they changed and `rekey` lets it run.

    Raises:
        PipelineConfigError: If they changed, and `rekey` is not set.
    """
    keys = fingerprint.get(runtime_module.KEYS)
    if keys is None or ledger is None:
        return None
    module = name.partition(".")[0]
    last = ledger.latest(f"{module}.", runtime_steps.DONE, runtime_module.KEYS)
    if last is None or last.meta[runtime_module.KEYS] == keys:
        return None
    before = last.meta[runtime_module.KEYS]
    then, now = _key_entries(before), _key_entries(keys)
    if added := sorted(now.keys() - then.keys()):
        _LOG.info("%s: new keys since the run %s: %s", name, last.run_id, added)
    if gone := sorted(then.keys() - now.keys()):
        _LOG.info("%s: keys gone since the run %s: %s", name, last.run_id, gone)
    changed = sorted(
        entry for entry in then.keys() & now.keys() if then[entry] != now[entry]
    )
    if not changed:
        return None
    shown = ", ".join(
        f"{entry or '(keys)'}: {then[entry]} then, {now[entry]} now"
        for entry in changed
    )
    if not rekey:
        raise contract.PipelineConfigError(
            f"The keys of {name} changed since the run {last.run_id} of"
            f" {last.pipeline} ({shown}):"
            " every surrogate key of these would change, and the rows loaded"
            " before would be loaded again under new keys. Undo the change,"
            " or run again with --rekey once the target is ready for new keys"
        )
    _LOG.warning(
        "%s: the keys changed since the run %s (%s): re-keying",
        name,
        last.run_id,
        shown,
    )
    return before


def _key_entries(keys: str) -> dict[str, str]:
    """The entries of a `runtime.KEYS` fingerprint, `name:value` pairs
    separated by commas, by name; a value without a name is named ''."""
    found: dict[str, str] = {}
    for part in keys.split(","):
        entry, colon, value = part.strip().rpartition(":")
        found[entry if colon else ""] = value
    return found


def _run_meta(
    meta: Mapping[str, str],
    run: contract.ETLRunInfo,
    *,
    resumed: str | None,
    label: str | None,
    rekeyed: str | None,
) -> dict[str, str]:
    """What runs: the kernel's meta and the module's fingerprint, the user,
    the steps selected, the run resumed, the label, the keys replaced."""
    found = {**meta, "user": _user()}
    if run.steps:
        found["steps"] = ",".join(sorted(run.steps))
    if resumed:
        found["resumed_from"] = resumed
    if label is not None:
        found["label"] = label
    if rekeyed is not None:
        found["rekeyed_from"] = rekeyed
    return found


def _exit_code(result: contract.PipelineResult) -> ExitCode:
    """The exit code of a run that ran.

    A step that refused for its configuration or its parameters (a
    `PipelineConfigError`: deletes to confirm) fails the run INVALID: a
    retry would refuse again, an operator must act.
    """
    if result.status is contract.ETLStatus.FAILED and any(
        isinstance(error.exception, contract.PipelineConfigError)
        for step in result.step_results.values()
        for error in step.errors
    ):
        return ExitCode.INVALID
    return _EXIT_CODES.get(result.status, ExitCode.FAILED)


def _lost(pipeline_name: str) -> None:
    """Reports a run cancelled because it lost its lock."""
    _LOG.error("%s lost its run lock: cancelled", pipeline_name)
    metrics.meter().add(
        name="etl.lock.lost",
        value=1,
        attributes={"etl.pipeline": pipeline_name},
        description="Runs cancelled, their lease lost",
    )


def _user() -> str:
    """Who runs: the login, or the process's user id."""
    try:
        return getpass.getuser()
    except (KeyError, OSError):
        return str(os.getuid()) if hasattr(os, "getuid") else "unknown"


@contextlib.contextmanager
def _running(ctx: contract.ETLContext) -> Generator[None]:
    """Binds the log context of a run, and cancels it on SIGTERM or
    SIGINT."""
    with (
        observability_context.log_context(
            **observability_context.run_fields(ctx)
        ),
        _cancel_on_signals(ctx),
    ):
        yield


@contextlib.contextmanager
def _cancel_on_signals(ctx: contract.ETLContext) -> Generator[None]:
    """Cancels the run on SIGTERM or SIGINT, in the main thread only."""
    if threading.current_thread() is not threading.main_thread():
        yield
        return

    def cancel(signum: int, frame: types.FrameType | None) -> None:
        del frame  # Unused.
        _LOG.warning(
            "%s received: cancelling, running steps end their batch",
            signal.Signals(signum).name,
        )
        ctx.cancel()

    previous = {
        sig: signal.signal(sig, cancel)
        for sig in (signal.SIGTERM, signal.SIGINT)
    }
    try:
        yield
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
