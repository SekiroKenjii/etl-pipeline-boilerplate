"""Pipeline steps, their retry and failure policies, and pipeline results."""

from __future__ import annotations

from collections.abc import Callable, Mapping
import dataclasses
import datetime
import enum

from core.contract import etl
from core.contract import exceptions
from core.contract import results
from shared import utils

type StepHandler = Callable[
    [etl.ETLContext, Mapping[str, results.ETLResult]], results.ETLResult
]


class StepFailurePolicy(enum.StrEnum):
    """What the pipeline does when a step fails."""

    STOP = "stop"
    SKIP_DEPENDENTS = "skip_dependents"
    CONTINUE = "continue"


@dataclasses.dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Retries a step when it raises one of `retry_on`.

    Attributes:
        max_attempts: The most attempts; 1 means no retry.
        backoff_seconds: The delay before the second attempt.
        backoff_multiplier: How much the delay grows after each attempt.
        retry_on: The exceptions worth a retry.
    """

    max_attempts: int = 1
    backoff_seconds: float = 0.0
    backoff_multiplier: float = 2.0
    retry_on: tuple[type[BaseException], ...] = (exceptions.RetryableError,)

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")

    def should_retry(self, exception: BaseException, attempt: int) -> bool:
        return attempt < self.max_attempts and isinstance(
            exception, self.retry_on
        )

    def delay_for(self, attempt: int) -> float:
        """Returns the delay before the attempt after `attempt` (from 1)."""
        return self.backoff_seconds * (self.backoff_multiplier ** (attempt - 1))


@dataclasses.dataclass(frozen=True, slots=True)
class Dataset:
    """A dataset a step reads or writes, for lineage (OpenLineage).

    Attributes:
        namespace: Where it lives: the database, as the settings name it.
        name: The table, collection, topic or path.
    """

    namespace: str
    name: str


@dataclasses.dataclass(frozen=True, slots=True)
class PipelineStep:
    """A single step of a pipeline.

    Attributes:
        name: The unique name of the step in the pipeline.
        handler: The function the step runs.
        dependencies: The steps that must finish before this one.
        phase: The ETL phase of the step, used to tag its errors.
        on_failure: What the pipeline does when this step fails.
        retry: How the step is retried on transient failures.
        inputs: The datasets it reads, for lineage.
        outputs: The datasets it writes, for lineage.
    """

    name: str
    handler: StepHandler
    dependencies: tuple[str, ...] = ()
    phase: etl.ETLPhase | None = None
    on_failure: StepFailurePolicy = StepFailurePolicy.STOP
    retry: RetryPolicy = dataclasses.field(default_factory=RetryPolicy)
    inputs: tuple[Dataset, ...] = ()
    outputs: tuple[Dataset, ...] = ()


@dataclasses.dataclass(slots=True)
class PipelineResult:
    """The result of a whole pipeline run.

    Attributes:
        pipeline_name: The pipeline.
        run_id: The run.
        status: The outcome of the run.
        started_at: When the run started.
        completed_at: When it completed, or None while it runs.
        step_results: The result of each step, by step name.
        stats: The counters of every step, added up.
        message: A human-readable summary.
    """

    pipeline_name: str
    run_id: str
    status: etl.ETLStatus = etl.ETLStatus.IN_PROGRESS
    started_at: datetime.datetime = dataclasses.field(
        default_factory=utils.utc_now
    )
    completed_at: datetime.datetime | None = None
    step_results: dict[str, results.ETLResult] = dataclasses.field(
        default_factory=dict[str, results.ETLResult]
    )
    stats: etl.ETLStats = dataclasses.field(default_factory=etl.ETLStats)
    message: str | None = None

    @property
    def errors(self) -> list[etl.ETLError]:
        """The errors of every step."""
        errors: list[etl.ETLError] = []
        for result in self.step_results.values():
            errors.extend(result.errors)
        return errors

    @property
    def succeeded(self) -> bool:
        return self.status is etl.ETLStatus.SUCCESS

    def add_step_result(self, name: str, result: results.ETLResult) -> None:
        self.step_results[name] = result
        self.stats += result.stats

    def duration(self) -> float | None:
        """Returns the duration in seconds, or None while it runs."""
        if self.completed_at is None:
            return None

        return (self.completed_at - self.started_at).total_seconds()

    def complete(
        self, status: etl.ETLStatus, message: str | None = None
    ) -> None:
        self.completed_at = utils.utc_now()
        self.status = status

        if message is not None:
            self.message = message


class PipelineListener:
    """Observability hooks of a pipeline run; each does nothing by default.

    With `max_workers > 1`, step hooks are called from worker threads, possibly
    at the same time: implementations must be thread-safe. A hook that raises is
    logged and ignored.
    """

    def on_pipeline_start(self, ctx: etl.ETLContext) -> None:
        """Called before the first step starts."""

    def on_step_start(
        self,
        step: PipelineStep,
        ctx: etl.ETLContext,
        attempt: int,
    ) -> None:
        """Called before each attempt of a step (from 1)."""

    def on_step_error(
        self,
        step: PipelineStep,
        error: Exception,
        ctx: etl.ETLContext,
        attempt: int,
    ) -> None:
        """Called when an attempt of a step raised `error`."""

    def on_step_end(
        self,
        step: PipelineStep,
        result: results.ETLResult,
        ctx: etl.ETLContext,
    ) -> None:
        """Called once a step finished, after its last attempt."""

    def on_pipeline_end(
        self, result: PipelineResult, ctx: etl.ETLContext
    ) -> None:
        """Called once the run is complete."""
