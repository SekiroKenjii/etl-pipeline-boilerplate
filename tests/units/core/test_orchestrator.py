from __future__ import annotations

from collections.abc import Mapping, Sequence
import threading
import time

import pytest

from core import contract
from core import pipeline
from tests.factories import fakes


def ok(
    ctx: contract.ETLContext, upstream: Mapping[str, contract.ETLResult]
) -> contract.ETLResult:
    return contract.ETLResult(stats=contract.ETLStats(total=1, successful=1))


def boom(
    ctx: contract.ETLContext, upstream: Mapping[str, contract.ETLResult]
) -> contract.ETLResult:
    raise contract.FatalError("boom")


class Flaky:

    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.calls = 0

    def __call__(
        self,
        ctx: contract.ETLContext,
        upstream: Mapping[str, contract.ETLResult],
    ) -> contract.ETLResult:
        self.calls += 1
        if self.calls <= self.failures:
            raise contract.RetryableError("timeout")
        return ok(ctx, upstream)


class Recorder(pipeline.PipelineListener):

    def __init__(self) -> None:
        self.events: list[str] = []

    def on_pipeline_start(self, ctx: contract.ETLContext) -> None:
        self.events.append("pipeline_start")

    def on_step_start(
        self,
        step: contract.PipelineStep,
        ctx: contract.ETLContext,
        attempt: int,
    ) -> None:
        self.events.append(f"start:{step.name}#{attempt}")

    def on_step_error(
        self,
        step: contract.PipelineStep,
        error: Exception,
        ctx: contract.ETLContext,
        attempt: int,
    ) -> None:
        self.events.append(f"error:{step.name}#{attempt}")

    def on_step_end(
        self,
        step: contract.PipelineStep,
        result: contract.ETLResult,
        ctx: contract.ETLContext,
    ) -> None:
        self.events.append(f"end:{step.name}:{result.status}")

    def on_pipeline_end(
        self, result: contract.PipelineResult, ctx: contract.ETLContext
    ) -> None:
        self.events.append(f"pipeline_end:{result.status}")


class StaticOrchestrator(pipeline.BaseOrchestrator):

    def __init__(
        self,
        steps: Sequence[contract.PipelineStep],
        *,
        listeners: Sequence[pipeline.PipelineListener] = (),
        max_workers: int = 1,
    ) -> None:
        self._steps = steps
        self.slept: list[float] = []
        super().__init__(
            listeners, sleep=self.slept.append, max_workers=max_workers
        )

    def steps(self) -> Sequence[contract.PipelineStep]:
        return self._steps


def _statuses(result: contract.PipelineResult) -> dict[str, contract.ETLStatus]:
    return {name: r.status for name, r in result.step_results.items()}


class TestExecutionOrder:

    def test_dependencies_run_first_and_declaration_order_is_kept(self) -> None:
        steps = [
            contract.PipelineStep("load", ok, dependencies=("transform",)),
            contract.PipelineStep("extract", ok),
            contract.PipelineStep("transform", ok, dependencies=("extract",)),
            contract.PipelineStep("audit", ok),
        ]

        assert [s.name for s in pipeline.resolve_execution_order(steps)] == [
            "extract",
            "transform",
            "load",
            "audit",
        ]

    @pytest.mark.parametrize(
        ("steps", "message"),
        [
            (
                [
                    contract.PipelineStep("a", ok),
                    contract.PipelineStep("a", ok),
                ],
                "Duplicate",
            ),
            ([contract.PipelineStep("a", ok, dependencies=("x",))], "unknown"),
            (
                [
                    contract.PipelineStep("a", ok, dependencies=("b",)),
                    contract.PipelineStep("b", ok, dependencies=("a",)),
                ],
                "cycle",
            ),
        ],
    )
    def test_invalid_definitions(
        self, steps: list[contract.PipelineStep], message: str
    ) -> None:
        with pytest.raises(contract.PipelineConfigError, match=message):
            pipeline.resolve_execution_order(steps)


def test_successful_pipeline_aggregates_stats_and_passes_upstream_results() -> (
    None
):
    seen: dict[str, contract.ETLResult] = {}

    def capture(
        ctx: contract.ETLContext, upstream: Mapping[str, contract.ETLResult]
    ) -> contract.ETLResult:
        seen.update(upstream)
        return ok(ctx, upstream)

    orchestrator = StaticOrchestrator([
        contract.PipelineStep("a", ok),
        contract.PipelineStep("b", capture, dependencies=("a",)),
    ])

    result = orchestrator.run(fakes.make_context("users"))

    assert result.status is contract.ETLStatus.SUCCESS
    assert result.pipeline_name == "users"
    assert result.stats.successful == 2
    assert list(seen) == ["a"]


def test_retryable_errors_are_retried_with_backoff() -> None:
    flaky = Flaky(failures=2)
    recorder = Recorder()
    orchestrator = StaticOrchestrator(
        [
            contract.PipelineStep(
                "a",
                flaky,
                retry=contract.RetryPolicy(max_attempts=3, backoff_seconds=1.0),
            )
        ],
        listeners=[recorder],
    )

    result = orchestrator.run(fakes.make_context())

    assert result.status is contract.ETLStatus.SUCCESS
    assert result.step_results["a"].data["attempts"] == 3
    assert orchestrator.slept == [1.0, 2.0]
    assert recorder.events == [
        "pipeline_start",
        "start:a#1",
        "error:a#1",
        "start:a#2",
        "error:a#2",
        "start:a#3",
        "end:a:success",
        "pipeline_end:success",
    ]


def test_fatal_errors_are_not_retried_and_stop_the_pipeline() -> None:
    orchestrator = StaticOrchestrator([
        contract.PipelineStep(
            "a", boom, retry=contract.RetryPolicy(max_attempts=3)
        ),
        contract.PipelineStep("b", ok),
    ])

    result = orchestrator.run(fakes.make_context())

    assert result.status is contract.ETLStatus.FAILED
    assert _statuses(result) == {
        "a": contract.ETLStatus.FAILED,
        "b": contract.ETLStatus.SKIPPED,
    }
    assert result.errors[0].message == "boom"
    assert orchestrator.slept == []


def test_skip_dependents_keeps_independent_steps_running() -> None:
    orchestrator = StaticOrchestrator([
        contract.PipelineStep(
            "a", boom, on_failure=contract.StepFailurePolicy.SKIP_DEPENDENTS
        ),
        contract.PipelineStep("b", ok, dependencies=("a",)),
        contract.PipelineStep("c", ok, dependencies=("b",)),
        contract.PipelineStep("d", ok),
    ])

    result = orchestrator.run(fakes.make_context())

    assert _statuses(result) == {
        "a": contract.ETLStatus.FAILED,
        "b": contract.ETLStatus.SKIPPED,
        "c": contract.ETLStatus.SKIPPED,
        "d": contract.ETLStatus.SUCCESS,
    }
    # A failed step fails the run: a partial success means rows rejected.
    assert result.status is contract.ETLStatus.FAILED


def test_continue_lets_dependents_run_but_the_run_fails() -> None:
    orchestrator = StaticOrchestrator([
        contract.PipelineStep(
            "a", boom, on_failure=contract.StepFailurePolicy.CONTINUE
        ),
        contract.PipelineStep("b", ok, dependencies=("a",)),
    ])

    result = orchestrator.run(fakes.make_context())

    assert _statuses(result) == {
        "a": contract.ETLStatus.FAILED,
        "b": contract.ETLStatus.SUCCESS,
    }
    assert result.status is contract.ETLStatus.FAILED


def test_a_window_of_rejected_rows_only_is_a_partial_success() -> None:
    def rejected(
        ctx: contract.ETLContext, upstream: Mapping[str, contract.ETLResult]
    ) -> contract.ETLResult:
        # The only row that changed tonight is bad: quarantined.
        return contract.ETLResult(stats=contract.ETLStats(total=1, rejected=1))

    orchestrator = StaticOrchestrator([
        contract.PipelineStep("customer", rejected),
        contract.PipelineStep("order", ok, dependencies=("customer",)),
    ])

    result = orchestrator.run(fakes.make_context())

    assert _statuses(result) == {
        "customer": contract.ETLStatus.PARTIAL_SUCCESS,
        "order": contract.ETLStatus.SUCCESS,
    }
    assert result.status is contract.ETLStatus.PARTIAL_SUCCESS


class TestCancelledRetries:
    """A run cancelled is not retried: no backoff, no new attempt."""

    def test_a_step_failing_once_cancelled_is_not_retried(self) -> None:
        calls = []

        def cancelled_then_failed(
            ctx: contract.ETLContext,
            upstream: Mapping[str, contract.ETLResult],
        ) -> contract.ETLResult:
            calls.append(1)
            ctx.cancel()  # SIGTERM, while the batch fails.
            raise contract.RetryableError("timeout")

        orchestrator = StaticOrchestrator([
            contract.PipelineStep(
                "a",
                cancelled_then_failed,
                retry=contract.RetryPolicy(max_attempts=3),
            )
        ])

        result = orchestrator.run(fakes.make_context())

        assert (len(calls), orchestrator.slept) == (1, [])
        assert result.status is contract.ETLStatus.CANCELLED
        assert result.step_results["a"].errors

    def test_a_cancel_during_the_backoff_ends_it(self) -> None:
        flaky = Flaky(failures=1)

        class Waiting(pipeline.BaseOrchestrator):
            """Waits its backoff for real."""

            def steps(self) -> Sequence[contract.PipelineStep]:
                return [
                    contract.PipelineStep(
                        "a",
                        flaky,
                        retry=contract.RetryPolicy(
                            max_attempts=2, backoff_seconds=30.0
                        ),
                    )
                ]

        ctx = fakes.make_context()
        threading.Timer(0.1, ctx.cancel).start()
        started = time.monotonic()

        result = Waiting().run(ctx)

        assert time.monotonic() - started < 10
        assert flaky.calls == 1
        assert result.status is contract.ETLStatus.CANCELLED


def test_cancelled_run() -> None:
    def cancel(
        ctx: contract.ETLContext, upstream: Mapping[str, contract.ETLResult]
    ) -> contract.ETLResult:
        ctx.cancel()
        return ok(ctx, upstream)

    orchestrator = StaticOrchestrator(
        [contract.PipelineStep("a", cancel), contract.PipelineStep("b", ok)]
    )

    result = orchestrator.run(fakes.make_context())

    assert result.status is contract.ETLStatus.CANCELLED
    assert _statuses(result)["b"] is contract.ETLStatus.CANCELLED


def test_a_run_whose_steps_all_skip_themselves_is_skipped() -> None:
    def nothing_to_do(
        ctx: contract.ETLContext, upstream: Mapping[str, contract.ETLResult]
    ) -> contract.ETLResult:
        return contract.ETLResult.skipped("no file landed")

    orchestrator = StaticOrchestrator([
        contract.PipelineStep("customer", nothing_to_do),
        contract.PipelineStep("order", ok, dependencies=("customer",)),
    ])

    result = orchestrator.run(fakes.make_context())

    assert _statuses(result) == {
        "customer": contract.ETLStatus.SKIPPED,
        "order": contract.ETLStatus.SKIPPED,
    }
    assert result.status is contract.ETLStatus.SKIPPED


def test_a_step_cancelled_by_itself_cancels_a_run_that_did_nothing() -> None:
    def gave_up(
        ctx: contract.ETLContext, upstream: Mapping[str, contract.ETLResult]
    ) -> contract.ETLResult:
        result = contract.ETLResult()
        result.complete(contract.ETLStatus.CANCELLED, "source in maintenance")
        return result

    orchestrator = StaticOrchestrator([contract.PipelineStep("a", gave_up)])

    assert orchestrator.run(fakes.make_context()).status is (
        contract.ETLStatus.CANCELLED
    )


def _meeting_point(parties: int, timeout: float) -> contract.StepHandler:
    """Returns a step succeeding only if `parties` steps run it at once."""
    barrier = threading.Barrier(parties, timeout=timeout)

    def meet(
        ctx: contract.ETLContext, upstream: Mapping[str, contract.ETLResult]
    ) -> contract.ETLResult:
        barrier.wait()
        return ok(ctx, upstream)

    return meet


class TestParallelSteps:

    def test_independent_steps_run_at_the_same_time(self) -> None:
        meet = _meeting_point(3, timeout=2)
        orchestrator = StaticOrchestrator(
            [contract.PipelineStep(name, meet) for name in ("a", "b", "c")],
            max_workers=3,
        )

        result = orchestrator.run(fakes.make_context())

        assert result.status is contract.ETLStatus.SUCCESS
        assert result.stats.successful == 3

    def test_steps_run_one_by_one_by_default(self) -> None:
        meet = _meeting_point(2, timeout=0.2)
        orchestrator = StaticOrchestrator([
            contract.PipelineStep(
                "a", meet, on_failure=contract.StepFailurePolicy.CONTINUE
            ),
            contract.PipelineStep(
                "b", meet, on_failure=contract.StepFailurePolicy.CONTINUE
            ),
        ])

        result = orchestrator.run(fakes.make_context())

        assert set(_statuses(result).values()) == {contract.ETLStatus.FAILED}

    def test_a_step_starts_only_after_its_dependencies(self) -> None:
        timeline: list[str] = []
        lock = threading.Lock()

        def traced(name: str) -> contract.StepHandler:
            def handler(
                ctx: contract.ETLContext,
                upstream: Mapping[str, contract.ETLResult],
            ) -> contract.ETLResult:
                with lock:
                    timeline.append(f"start:{name}")
                time.sleep(0.02)
                with lock:
                    timeline.append(f"end:{name}")
                return ok(ctx, upstream)

            return handler

        orchestrator = StaticOrchestrator(
            [
                contract.PipelineStep("extract_a", traced("extract_a")),
                contract.PipelineStep("extract_b", traced("extract_b")),
                contract.PipelineStep(
                    "merge",
                    traced("merge"),
                    dependencies=("extract_a", "extract_b"),
                ),
            ],
            max_workers=4,
        )

        result = orchestrator.run(fakes.make_context())

        assert result.status is contract.ETLStatus.SUCCESS
        start_merge = timeline.index("start:merge")
        assert timeline.index("end:extract_a") < start_merge
        assert timeline.index("end:extract_b") < start_merge

    def test_stop_failure_lets_running_steps_finish_but_starts_no_new_one(
        self,
    ) -> None:
        def slow(
            ctx: contract.ETLContext, upstream: Mapping[str, contract.ETLResult]
        ) -> contract.ETLResult:
            time.sleep(0.05)
            return ok(ctx, upstream)

        orchestrator = StaticOrchestrator(
            [
                contract.PipelineStep("a", boom),
                contract.PipelineStep("b", slow),
                contract.PipelineStep("c", ok),
            ],
            max_workers=2,
        )

        result = orchestrator.run(fakes.make_context())

        assert _statuses(result) == {
            "a": contract.ETLStatus.FAILED,
            "b": contract.ETLStatus.SUCCESS,
            "c": contract.ETLStatus.SKIPPED,
        }
        assert result.status is contract.ETLStatus.FAILED
        assert result.step_results["c"].message == "pipeline halted by step 'a'"

    def test_rejects_invalid_worker_count(self) -> None:
        with pytest.raises(ValueError):
            StaticOrchestrator([], max_workers=0)


class _Coordinator(pipeline.StepCoordinator):
    """Settles `before` (an earlier run finished them)."""

    def __init__(self, before: Mapping[str, contract.ETLResult]) -> None:
        self.before = before
        self.looks = 0
        self.finished_steps: list[str] = []

    def settled(
        self, ctx: contract.ETLContext
    ) -> Mapping[str, contract.ETLResult]:
        self.looks += 1
        return self.before

    def finished(
        self, step: str, result: contract.ETLResult, ctx: contract.ETLContext
    ) -> None:
        self.finished_steps.append(step)


def _done(
    status: contract.ETLStatus = contract.ETLStatus.SUCCESS,
) -> contract.ETLResult:
    result = contract.ETLResult(stats=contract.ETLStats(total=5))
    result.complete(status, "done before")
    return result


class TestSelectedAndSettledSteps:

    def test_only_selected_steps_run_their_dependencies_taken_as_done(
        self,
    ) -> None:
        calls: list[str] = []

        def track(name: str) -> contract.StepHandler:
            def handler(
                ctx: contract.ETLContext,
                upstream: Mapping[str, contract.ETLResult],
            ) -> contract.ETLResult:
                calls.append(name)
                return ok(ctx, upstream)

            return handler

        orchestrator = StaticOrchestrator([
            contract.PipelineStep("customer", track("customer")),
            contract.PipelineStep(
                "order", track("order"), dependencies=("customer",)
            ),
            contract.PipelineStep(
                "invoice", track("invoice"), dependencies=("order",)
            ),
        ])
        ctx = contract.ETLContext(
            contract.ETLRunInfo("p", steps=frozenset({"order"}))
        )

        result = orchestrator.run(ctx)

        assert calls == ["order"]
        assert result.status is contract.ETLStatus.SUCCESS
        assert result.step_results["customer"].data == {"selected": False}
        assert result.step_results["invoice"].message == "not selected"
        unknown = contract.ETLContext(
            contract.ETLRunInfo("p", steps=frozenset({"nope"}))
        )
        with pytest.raises(contract.PipelineConfigError, match="'nope'"):
            orchestrator.run(unknown)

    def test_steps_settled_before_are_not_run_again(self) -> None:
        flaky = Flaky(failures=0)
        orchestrator = StaticOrchestrator([
            contract.PipelineStep("a", boom),
            contract.PipelineStep("b", flaky, dependencies=("a",)),
        ])
        coordinator = _Coordinator({"a": _done()})

        result = orchestrator.run(fakes.make_context(), coordinator)

        assert _statuses(result) == {
            "a": contract.ETLStatus.SUCCESS,
            "b": contract.ETLStatus.SUCCESS,
        }
        assert result.stats.total == 6
        assert flaky.calls == 1
        assert coordinator.finished_steps == ["b"]
        # Read once, when the run starts: nobody else settles its steps.
        assert coordinator.looks == 1
        assert orchestrator.slept == []

    def test_a_step_settled_failed_halts_the_run(self) -> None:
        flaky = Flaky(failures=0)
        orchestrator = StaticOrchestrator([
            contract.PipelineStep("a", ok),
            contract.PipelineStep("b", flaky),
            contract.PipelineStep("c", flaky, dependencies=("a",)),
        ])
        # A coordinator may settle a step as it failed (STOP, the default).
        coordinator = _Coordinator({"a": _done(contract.ETLStatus.FAILED)})

        result = orchestrator.run(fakes.make_context(), coordinator)

        assert result.status is contract.ETLStatus.FAILED
        assert _statuses(result) == {
            "a": contract.ETLStatus.FAILED,
            "b": contract.ETLStatus.SKIPPED,
            "c": contract.ETLStatus.SKIPPED,
        }
        assert result.step_results["b"].message == "pipeline halted by step 'a'"
        assert flaky.calls == 0
        assert coordinator.finished_steps == []


class _Broken(pipeline.PipelineListener):
    """Raises in every hook."""

    def __init__(self) -> None:
        self.calls = 0

    def on_pipeline_start(self, ctx: contract.ETLContext) -> None:
        self.calls += 1
        raise ValueError("badly formed hexadecimal UUID string")

    def on_step_start(
        self,
        step: contract.PipelineStep,
        ctx: contract.ETLContext,
        attempt: int,
    ) -> None:
        self.calls += 1
        raise ValueError("the collector is down")

    def on_step_end(
        self,
        step: contract.PipelineStep,
        result: contract.ETLResult,
        ctx: contract.ETLContext,
    ) -> None:
        self.calls += 1
        raise ValueError("the collector is down")

    def on_pipeline_end(
        self, result: contract.PipelineResult, ctx: contract.ETLContext
    ) -> None:
        self.calls += 1
        raise ValueError("the collector is down")


def test_a_failing_listener_neither_fails_the_run_nor_loses_its_steps(
    caplog: pytest.LogCaptureFixture,
) -> None:
    broken, recorder = _Broken(), Recorder()
    orchestrator = StaticOrchestrator(
        [contract.PipelineStep("a", ok), contract.PipelineStep("b", ok)],
        listeners=[broken, recorder],
    )
    coordinator = _Coordinator({})

    result = orchestrator.run(fakes.make_context(), coordinator)

    assert result.status is contract.ETLStatus.SUCCESS
    # The steps are recorded, and the listeners after it still called.
    assert coordinator.finished_steps == ["a", "b"]
    assert recorder.events[0] == "pipeline_start"
    assert recorder.events[-1] == "pipeline_end:success"
    assert broken.calls == 6
    assert "the listener _Broken failed in on_step_end" in caplog.text


class _Stuck(StaticOrchestrator):
    """Never starts a step: its scheduling is broken."""

    def __init__(self, steps: Sequence[contract.PipelineStep]) -> None:
        super().__init__(steps)
        self.torn_down = False

    def _start_ready(self, *args: object, **kwargs: object) -> None:
        del args, kwargs  # Unused.

    def teardown(self, ctx: contract.ETLContext) -> None:
        self.torn_down = True


def test_a_scheduler_that_cannot_start_a_step_raises_not_hangs() -> None:
    orchestrator = _Stuck([contract.PipelineStep("a", ok)])
    raised: list[BaseException] = []

    def run() -> None:
        try:
            orchestrator.run(fakes.make_context())
        except RuntimeError as exc:
            raised.append(exc)

    # In a thread: without the guard, the run would spin for ever.
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    thread.join(timeout=10)

    assert not thread.is_alive(), "the run hangs"
    assert [str(exc) for exc in raised] == ["Scheduler stalled on 'a'"]
    assert orchestrator.torn_down
