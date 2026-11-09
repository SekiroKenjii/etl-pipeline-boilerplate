"""Logging and measuring the life of pipeline runs: starts, retries,
results."""

from __future__ import annotations

import logging

from core import contract
from core.observability import context as fields
from core.observability import metrics

_LOG = logging.getLogger("etl.run")
_LEVEL_BY_STATUS = {
    contract.ETLStatus.FAILED: logging.ERROR,
    contract.ETLStatus.PARTIAL_SUCCESS: logging.WARNING,
}


def _stats(stats: contract.ETLStats) -> dict[str, object]:
    """The counters as flat fields: every backend indexes those."""
    return {
        f"etl.stats.{name}": value for name, value in stats.as_dict().items()
    }


class LoggingListener(contract.PipelineListener):
    """Logs every pipeline and step event, with structured fields.

    Fields are flat and dotted (`etl.step`, `etl.stats.created`), as
    Elasticsearch, Loki, Seq and OpenTelemetry attributes all take them. The
    run's fields (`etl.run_id`...) come from the log context.
    """

    def __init__(self, logger: logging.Logger = _LOG) -> None:
        self._log = logger

    def on_pipeline_start(self, ctx: contract.ETLContext) -> None:
        self._log.info(
            "pipeline %s started",
            ctx.run.pipeline_name,
            extra={fields.EVENT: "pipeline.start"},
        )

    def on_step_start(
        self,
        step: contract.PipelineStep,
        ctx: contract.ETLContext,
        attempt: int,
    ) -> None:
        self._log.info(
            "step %s started (attempt %d)",
            step.name,
            attempt,
            extra={
                fields.EVENT: "step.start",
                fields.STEP: step.name,
                fields.ATTEMPT: attempt,
            },
        )

    def on_step_error(
        self,
        step: contract.PipelineStep,
        error: Exception,
        ctx: contract.ETLContext,
        attempt: int,
    ) -> None:
        # A retried attempt is one line: only the last one logs the traceback.
        # A refusal (a configuration error) is deliberate: its message says
        # what to do, a traceback would not.
        retried = step.retry.should_retry(error, attempt)
        refused = isinstance(error, contract.PipelineConfigError)
        self._log.warning(
            "step %s failed (attempt %d%s): %s",
            step.name,
            attempt,
            ", will retry" if retried else "",
            error,
            exc_info=None if retried or refused else error,
            extra={
                fields.EVENT: "step.error",
                fields.STEP: step.name,
                fields.ATTEMPT: attempt,
                "etl.retryable": isinstance(error, contract.RetryableError),
                "etl.will_retry": retried,
            },
        )

    def on_step_end(
        self,
        step: contract.PipelineStep,
        result: contract.ETLResult,
        ctx: contract.ETLContext,
    ) -> None:
        level = _LEVEL_BY_STATUS.get(result.status, logging.INFO)
        rejected = sum(r.rejected for r in result.quality_reports.values())
        self._log.log(
            level,
            "step %s %s: %d read, %d loaded, %d rejected",
            step.name,
            result.status.value,
            result.stats.total,
            result.stats.successful,
            rejected,
            extra={
                fields.EVENT: "step.end",
                fields.STEP: step.name,
                fields.STATUS: result.status.value,
                "etl.duration_ms": _milliseconds(result.duration()),
                "etl.errors": len(result.errors),
                **_stats(result.stats),
            },
        )

    def on_pipeline_end(
        self, result: contract.PipelineResult, ctx: contract.ETLContext
    ) -> None:
        level = (
            logging.INFO
            if result.status is contract.ETLStatus.SUCCESS
            else _LEVEL_BY_STATUS.get(result.status, logging.WARNING)
        )
        self._log.log(
            level,
            "pipeline %s %s",
            ctx.run.pipeline_name,
            result.status.value,
            extra={
                fields.EVENT: "pipeline.end",
                fields.STATUS: result.status.value,
                "etl.duration_ms": _milliseconds(result.duration()),
                **_stats(result.stats),
            },
        )


def _milliseconds(seconds: float | None) -> int | None:
    return None if seconds is None else round(seconds * 1000)


# Dashboards and alerts follow these names.
_ROWS = {
    "total": "etl.rows.read",
    "successful": "etl.rows.loaded",
    "rejected": "etl.rows.rejected",
    "filtered": "etl.rows.filtered",
    "created": "etl.rows.created",
    "updated": "etl.rows.updated",
    "deleted": "etl.rows.deleted",
    "corrected": "etl.rows.corrected",
}


class MetricsListener(contract.PipelineListener):
    """Measures every run: rows by outcome, durations, runs by status.

    Measurements go to the process's meter (`metrics.meter()`), with the
    pipeline (and step) as attributes; run ids are not attributes, they
    would make a series of each run.
    """

    def on_step_error(
        self,
        step: contract.PipelineStep,
        error: Exception,
        ctx: contract.ETLContext,
        attempt: int,
    ) -> None:
        metrics.meter().add(
            "etl.step.errors",
            1,
            {
                fields.PIPELINE: ctx.run.pipeline_name,
                fields.STEP: step.name,
                "etl.retryable": isinstance(error, contract.RetryableError),
            },
            description="Failed attempts of steps",
        )

    def on_step_end(
        self,
        step: contract.PipelineStep,
        result: contract.ETLResult,
        ctx: contract.ETLContext,
    ) -> None:
        meter = metrics.meter()
        labels = {
            fields.PIPELINE: ctx.run.pipeline_name,
            fields.STEP: step.name,
        }
        for stat, name in _ROWS.items():
            value = getattr(result.stats, stat)
            if value:
                meter.add(name, value, labels, unit="{row}")
        duration = result.duration()
        if duration is not None:
            meter.record(
                "etl.step.duration",
                duration,
                {**labels, fields.STATUS: result.status.value},
                unit="s",
                description="Duration of steps",
            )

    def on_pipeline_end(
        self, result: contract.PipelineResult, ctx: contract.ETLContext
    ) -> None:
        meter = metrics.meter()
        labels = {
            fields.PIPELINE: ctx.run.pipeline_name,
            fields.STATUS: result.status.value,
        }
        meter.add("etl.runs", 1, labels, description="Runs, by status")
        duration = result.duration()
        if duration is not None:
            meter.record(
                "etl.pipeline.duration",
                duration,
                labels,
                unit="s",
                description="Duration of runs",
            )
        if result.completed_at is not None:
            meter.gauge(
                "etl.run.last_completed",
                result.completed_at.timestamp(),
                labels,
                unit="s",
                description="When a run of this status last completed",
            )
