"""Runs the steps of a pipeline in dependency order, possibly in parallel."""

from __future__ import annotations

import abc
from collections.abc import Callable, Collection, Mapping, Sequence
from concurrent import futures
import contextvars
import logging
import time
from typing import NamedTuple

from core import contract
from core.observability import context as observability_context
from core.observability import tracing

_LOG = logging.getLogger("etl.pipeline")

_BLOCKING_STATUSES = frozenset({
    contract.ETLStatus.FAILED,
    contract.ETLStatus.SKIPPED,
    contract.ETLStatus.CANCELLED,
})


class StepCoordinator:
    """Which steps of a run already ended, and what to do once one ends.

    The default runs every step. The kernel's (`runtime.LedgerSteps`) records
    each step in the run ledger and carries over those the resumed run finished.
    """

    def settled(
        self, ctx: contract.ETLContext
    ) -> Mapping[str, contract.ETLResult]:
        """The steps that ended before this run: in the run it resumes.

        Read once, when the run starts.
        """
        del ctx  # Unused.
        return {}

    def finished(
        self, step: str, result: contract.ETLResult, ctx: contract.ETLContext
    ) -> None:
        """Called once a step ended, from its thread; even if a listener
        failed."""


class BaseOrchestrator(abc.ABC):
    """Runs the steps of a module in dependency order.

    - A step whose dependency failed or was skipped is SKIPPED, unless the
      failed dependency uses `StepFailurePolicy.CONTINUE`.
    - A failed step with `StepFailurePolicy.STOP` halts the pipeline: the
      remaining steps are SKIPPED.
    - Exceptions matching the step's `RetryPolicy` are retried with backoff.

    Up to `max_workers` steps run at once, in threads (1: one after the other,
    in declaration order); steps sharing resources must then be thread-safe.
    After a STOP failure or a cancellation no new step starts. A
    `StepCoordinator` may settle steps without running them (done in the run
    being resumed).

    The run fails if any step failed, whatever its policy; PARTIAL_SUCCESS is
    for rows rejected or skipped, never a failed step.
    """

    def __init__(
        self,
        listeners: Sequence[contract.PipelineListener] = (),
        sleep: Callable[[float], None] = time.sleep,
        max_workers: int = 1,
    ) -> None:
        """Initializes the orchestrator.

        Args:
            listeners: The observability hooks.
            sleep: Waits between retries; replaceable in tests.
            max_workers: The most steps running at the same time.

        Raises:
            ValueError: If `max_workers` is below 1.
        """
        if max_workers < 1:
            raise ValueError("max_workers must be >= 1")

        self._listeners = tuple(listeners)
        self._sleep = sleep
        self._max_workers = max_workers

    @property
    def name(self) -> str:
        return type(self).__name__

    def cap_workers(self, count: int) -> None:
        """Runs at most `count` steps at the same time, whatever the module
        asks (`runs.max_parallel_steps`): to bound the connections and the
        memory of a process.

        Raises:
            ValueError: If `count` is below 1.
        """
        if count < 1:
            raise ValueError("count must be >= 1")
        self._max_workers = min(self._max_workers, count)

    def add_listener(self, listener: contract.PipelineListener) -> None:
        """Adds an observability hook, e.g. the kernel's run logging."""
        self._listeners = (*self._listeners, listener)

    @abc.abstractmethod
    def steps(self) -> Sequence[contract.PipelineStep]:
        """Returns the steps of the pipeline, in any order."""

    def setup(self, ctx: contract.ETLContext) -> None:
        """Acquires module-wide resources; does nothing by default."""

    def teardown(self, ctx: contract.ETLContext) -> None:
        """Releases module-wide resources; does nothing by default."""

    def run(
        self,
        ctx: contract.ETLContext,
        coordinator: StepCoordinator | None = None,
    ) -> contract.PipelineResult:
        """Runs every step once, or the selected ones (`ETLRunInfo.steps`).

        Args:
            ctx: The run context.
            coordinator: Which steps already ended, and what to do once one
                ends; every step runs by default.

        Returns:
            The result of the run and of each step.

        Raises:
            PipelineConfigError: If the steps cannot be ordered, or a
                selected step does not exist.
        """
        plan = resolve_execution_order(self.steps())
        unknown = sorted(ctx.run.steps - {step.name for step in plan})
        if unknown:
            raise contract.PipelineConfigError(f"No step(s) {unknown}")
        coordinator = coordinator or StepCoordinator()
        result = contract.PipelineResult(
            pipeline_name=ctx.run.pipeline_name, run_id=ctx.run.run_id
        )

        with tracing.span(
            "etl.pipeline",
            {
                observability_context.PIPELINE: ctx.run.pipeline_name,
                observability_context.RUN_ID: ctx.run.run_id,
            },
        ) as span:
            self._notify("on_pipeline_start", ctx)
            self.setup(ctx)
            try:
                halted_by = self._schedule(plan, ctx, result, coordinator)
            finally:
                self.teardown(ctx)

            result.complete(
                _resolve_status(result, ctx, halted_by, _selected(plan, ctx))
            )
            _close(span, result.status, result.message, result.stats)
            self._notify("on_pipeline_end", result, ctx)
        return result

    def _schedule(
        self,
        plan: list[contract.PipelineStep],
        ctx: contract.ETLContext,
        result: contract.PipelineResult,
        coordinator: StepCoordinator,
    ) -> str | None:
        """Runs the plan, starting each step once its dependencies are done.

        Returns:
            The step whose failure halted the pipeline, if any.
        """
        by_name = {step.name: step for step in plan}
        selected = _selected(plan, ctx)
        pending = list(plan)
        running: dict[
            futures.Future[contract.ETLResult], contract.PipelineStep
        ] = {}
        halted_by = _adopt(pending, result, coordinator.settled(ctx))
        for step in list(pending):
            if step.name not in selected:
                pending.remove(step)
                result.add_step_result(step.name, _not_selected())

        with futures.ThreadPoolExecutor(
            max_workers=self._max_workers, thread_name_prefix=self.name
        ) as pool:
            while pending or running:
                self._start_ready(
                    pending,
                    running,
                    pool,
                    _Run(ctx, result, by_name, selected, coordinator),
                    halted_by,
                )
                if not running:
                    if pending:
                        # Topological order: with none running, one is ready.
                        raise RuntimeError(
                            f"Scheduler stalled on {pending[0].name!r}"
                        )
                    continue
                done, _ = futures.wait(
                    running, return_when=futures.FIRST_COMPLETED
                )
                for future in done:
                    step = running.pop(future)
                    step_result = future.result()
                    result.add_step_result(step.name, step_result)
                    if halted_by is None and _halts(step, step_result):
                        halted_by = step.name

        return halted_by

    def _start_ready(
        self,
        pending: list[contract.PipelineStep],
        running: dict[
            futures.Future[contract.ETLResult], contract.PipelineStep
        ],
        pool: futures.ThreadPoolExecutor,
        run: _Run,
        halted_by: str | None,
    ) -> None:
        """Settles every pending step that can be settled now.

        In declaration order, a step is skipped, cancelled, or started if a
        worker is free. Settling a step may make another one ready, so this
        loops until nothing changes.
        """
        ctx, result = run.ctx, run.result
        changed = True
        while changed:
            changed = False
            for step in list(pending):
                if halted_by is not None:
                    outcome = contract.ETLResult.skipped(
                        f"pipeline halted by step '{halted_by}'"
                    )
                elif ctx.is_cancelled:
                    outcome = contract.ETLResult()
                    outcome.complete(
                        contract.ETLStatus.CANCELLED, "pipeline cancelled"
                    )
                elif not all(
                    dep in result.step_results for dep in step.dependencies
                ):
                    continue
                elif blocker := _find_blocker(step, run, result):
                    outcome = contract.ETLResult.skipped(
                        f"dependency '{blocker}' did not succeed"
                    )
                elif len(running) < self._max_workers:
                    upstream = {
                        name: result.step_results[name]
                        for name in step.dependencies
                    }
                    pending.remove(step)
                    # A copy of this context: the run's log fields follow.
                    running[
                        pool.submit(
                            contextvars.copy_context().run,
                            self._run_step,
                            step,
                            ctx,
                            upstream,
                            run.coordinator,
                        )
                    ] = step
                    continue
                else:
                    continue

                pending.remove(step)
                result.add_step_result(step.name, outcome)
                changed = True

    def _run_step(
        self,
        step: contract.PipelineStep,
        ctx: contract.ETLContext,
        upstream: dict[str, contract.ETLResult],
        coordinator: StepCoordinator,
    ) -> contract.ETLResult:
        """Runs a step, with its retries; a final failure is a FAILED result."""
        attempt = 1
        while True:
            self._notify("on_step_start", step, ctx, attempt)
            try:
                with (
                    observability_context.log_context(**{
                        observability_context.STEP: step.name,
                        observability_context.ATTEMPT: attempt,
                    }),
                    tracing.span(
                        "etl.step",
                        {
                            observability_context.STEP: step.name,
                            observability_context.ATTEMPT: attempt,
                        },
                    ) as span,
                ):
                    step_result = step.handler(ctx, upstream)
                    if step_result.is_done:
                        _close(
                            span,
                            step_result.status,
                            step_result.message,
                            step_result.stats,
                        )
                break
            # Recorded in the step's result; its policy decides what next.
            except Exception as exc:  # pylint: disable=broad-exception-caught
                self._notify("on_step_error", step, exc, ctx, attempt)

                if step.retry.should_retry(exc, attempt):
                    self._sleep(step.retry.delay_for(attempt))
                    attempt += 1
                    continue

                step_result = contract.ETLResult()
                step_result.add_error(_error_of(exc, step))
                step_result.complete(
                    contract.ETLStatus.FAILED,
                    f"failed after {attempt} attempt(s)",
                )
                break

        if not step_result.is_done:
            step_result.complete()

        step_result.data.setdefault("attempts", attempt)
        try:
            self._notify("on_step_end", step, step_result, ctx)
        finally:
            # Recorded whatever the listeners do: resuming relies on it.
            coordinator.finished(step.name, step_result, ctx)
        return step_result

    def _notify(self, hook: str, *args: object) -> None:
        """Calls `hook` of every listener; one that raises is logged, and
        the others are still called."""
        for listener in self._listeners:
            try:
                getattr(listener, hook)(*args)
            # Observability never fails a run.
            except Exception:  # pylint: disable=broad-exception-caught
                _LOG.exception(
                    "the listener %s failed in %s",
                    type(listener).__name__,
                    hook,
                )


def resolve_execution_order(
    steps: Sequence[contract.PipelineStep],
) -> list[contract.PipelineStep]:
    """Sorts steps so that each comes after its dependencies.

    Independent steps keep their declaration order.

    Args:
        steps: The steps, in declaration order.

    Returns:
        The steps, in an order that satisfies every dependency.

    Raises:
        PipelineConfigError: On a duplicate step name, an unknown dependency,
            or a dependency cycle.
    """
    by_name: dict[str, contract.PipelineStep] = {}
    for step in steps:
        if step.name in by_name:
            raise contract.PipelineConfigError(
                f"Duplicate step name '{step.name}'"
            )

        by_name[step.name] = step

    for step in steps:
        unknown = [name for name in step.dependencies if name not in by_name]

        if unknown:
            raise contract.PipelineConfigError(
                f"Step '{step.name}' depends on unknown step(s) {unknown}"
            )

    ordered: list[contract.PipelineStep] = []
    done: set[str] = set()
    pending = list(steps)

    while pending:
        ready = next(
            (
                step
                for step in pending
                if all(dep in done for dep in step.dependencies)
            ),
            None,
        )

        if ready is None:
            names = ", ".join(step.name for step in pending)
            raise contract.PipelineConfigError(
                f"Dependency cycle between steps: {names}"
            )

        ordered.append(ready)
        done.add(ready.name)
        pending.remove(ready)

    return ordered


class _Run(NamedTuple):
    """What settling the steps of a run needs."""

    ctx: contract.ETLContext
    result: contract.PipelineResult
    by_name: Mapping[str, contract.PipelineStep]
    selected: Collection[str]
    coordinator: StepCoordinator


def _selected(
    plan: Sequence[contract.PipelineStep], ctx: contract.ETLContext
) -> frozenset[str]:
    """The steps the run runs: those it selects, or every one."""
    return ctx.run.steps or frozenset(step.name for step in plan)


def _not_selected() -> contract.ETLResult:
    outcome = contract.ETLResult.skipped("not selected")
    outcome.data["selected"] = False
    return outcome


def _halts(step: contract.PipelineStep, outcome: contract.ETLResult) -> bool:
    """Whether the outcome of `step` halts the pipeline."""
    return (
        outcome.status is contract.ETLStatus.FAILED
        and step.on_failure is contract.StepFailurePolicy.STOP
    )


def _adopt(
    pending: list[contract.PipelineStep],
    result: contract.PipelineResult,
    settled: Mapping[str, contract.ETLResult],
) -> str | None:
    """Takes the pending steps settled before as they ended then.

    Returns:
        The first of them whose failure halts the pipeline, if any.
    """
    halted_by = None
    for step in list(pending):
        outcome = settled.get(step.name)
        if outcome is None:
            continue
        pending.remove(step)
        result.add_step_result(step.name, outcome)
        if halted_by is None and _halts(step, outcome):
            halted_by = step.name
    return halted_by


def _find_blocker(
    step: contract.PipelineStep,
    run: _Run,
    result: contract.PipelineResult,
) -> str | None:
    """Returns a dependency of `step` that prevents it from running, if any.

    A dependency the run did not select never does: it is taken as done.
    """
    for name in step.dependencies:
        if name not in run.selected:
            continue
        dep_result = result.step_results[name]

        if dep_result.status not in _BLOCKING_STATUSES:
            continue

        if (
            dep_result.status is contract.ETLStatus.FAILED
            and run.by_name[name].on_failure
            is contract.StepFailurePolicy.CONTINUE
        ):
            continue

        return name

    return None


def _resolve_status(
    result: contract.PipelineResult,
    ctx: contract.ETLContext,
    halted_by: str | None,
    selected: Collection[str],
) -> contract.ETLStatus:
    """The status of a run, from the statuses of its selected steps.

    A failed step fails the run, whatever its policy: a partial success only
    ever means rows rejected. A run whose steps all skipped themselves (nothing
    to do) is SKIPPED.
    """
    statuses = [
        step_result.status
        for name, step_result in result.step_results.items()
        if name in selected
    ]

    if ctx.is_cancelled:
        return contract.ETLStatus.CANCELLED
    if halted_by is not None or contract.ETLStatus.FAILED in statuses:
        return contract.ETLStatus.FAILED
    if all(status is contract.ETLStatus.SUCCESS for status in statuses):
        return contract.ETLStatus.SUCCESS
    if any(
        status
        in (contract.ETLStatus.SUCCESS, contract.ETLStatus.PARTIAL_SUCCESS)
        for status in statuses
    ):
        return contract.ETLStatus.PARTIAL_SUCCESS
    if contract.ETLStatus.CANCELLED in statuses:
        return contract.ETLStatus.CANCELLED
    return contract.ETLStatus.SKIPPED


def _error_of(exc: Exception, step: contract.PipelineStep) -> contract.ETLError:
    """Records the exception that failed `step`."""
    if isinstance(exc, contract.PipelineError):
        return contract.ETLError.from_exception(
            exc, phase=exc.phase or step.phase, record_id=exc.record_id
        )
    return contract.ETLError.from_exception(exc, phase=step.phase)


def _close(
    span: tracing.Span,
    status: contract.ETLStatus,
    message: str | None,
    stats: contract.ETLStats,
) -> None:
    """Gives a span the outcome of its run or step."""
    span.set_attributes({
        observability_context.STATUS: status.value,
        **{f"etl.stats.{k}": v for k, v in stats.as_dict().items() if v},
    })
    if status is contract.ETLStatus.FAILED:
        span.fail(message or "failed")
