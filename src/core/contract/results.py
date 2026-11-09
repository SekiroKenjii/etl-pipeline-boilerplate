"""The results of ETL operations: counters, errors and quality reports."""

from __future__ import annotations

import dataclasses
import datetime

from core.contract import etl
from core.contract import quality
from shared import utils


@dataclasses.dataclass(slots=True)
class ETLResult:
    """The result of an ETL operation, usually one pipeline step.

    Attributes:
        status: The outcome.
        stats: The record counters.
        errors: The errors met.
        quality_reports: One report per quality checkpoint, merged over the
            batches.
        started_at: When the operation started.
        completed_at: When it completed, or None while it runs.
        message: A human-readable summary.
        data: Free-form values, e.g. the number of attempts of a step.
    """

    status: etl.ETLStatus = etl.ETLStatus.IN_PROGRESS
    stats: etl.ETLStats = dataclasses.field(default_factory=etl.ETLStats)
    errors: list[etl.ETLError] = dataclasses.field(
        default_factory=list[etl.ETLError]
    )
    quality_reports: dict[quality.Checkpoint, quality.QualityReport] = (
        dataclasses.field(
            default_factory=dict[quality.Checkpoint, quality.QualityReport]
        )
    )
    started_at: datetime.datetime = dataclasses.field(
        default_factory=utils.utc_now
    )
    completed_at: datetime.datetime | None = None
    message: str | None = None
    data: dict[str, object] = dataclasses.field(
        default_factory=dict[str, object]
    )

    @classmethod
    def skipped(cls, message: str) -> ETLResult:
        """Returns a completed result with the SKIPPED status."""
        result = cls()
        result.complete(etl.ETLStatus.SKIPPED, message)
        return result

    def add_error(self, error: etl.ETLError) -> None:
        self.errors.append(error)

    def add_quality_report(self, report: quality.QualityReport) -> None:
        """Merges the report of a batch into the one kept per checkpoint."""
        self.stats.rejected += report.rejected

        existing = self.quality_reports.get(report.checkpoint)
        if existing is None:
            self.quality_reports[report.checkpoint] = report
        else:
            existing.merge(report)

    @property
    def is_done(self) -> bool:
        return self.completed_at is not None

    @property
    def has_blocking_issues(self) -> bool:
        """Whether errors, rejects or a failed gate prevent a full success.

        Quality warnings are not blocking.
        """
        return (
            bool(self.errors)
            or self.stats.failed > 0
            or self.stats.rejected > 0
            or self.quality_failed
        )

    @property
    def quality_failed(self) -> bool:
        return any(report.failed for report in self.quality_reports.values())

    def duration(self) -> float | None:
        """Returns the duration in seconds, or None while it runs."""
        if self.completed_at is None:
            return None

        return (self.completed_at - self.started_at).total_seconds()

    def mark(
        self, status: etl.ETLStatus, with_message: str | None = None
    ) -> None:
        self.status = status

        if with_message is not None:
            self.message = with_message

    def complete(
        self, status: etl.ETLStatus | None = None, message: str | None = None
    ) -> None:
        """Marks the operation as completed.

        Args:
            status: The final status. When None and the operation is still
                pending or in progress, it is derived from the errors,
                counters and quality reports.
            message: A summary, replacing the current one.
        """
        self.completed_at = utils.utc_now()

        if status is not None:
            self.status = status
        elif self.status in (etl.ETLStatus.PENDING, etl.ETLStatus.IN_PROGRESS):
            self.status = self._resolve_status()

        if message is not None:
            self.message = message

    def _resolve_status(self) -> etl.ETLStatus:
        """SUCCESS without blocking issues; PARTIAL_SUCCESS when rows were
        rejected and nothing else went wrong; FAILED otherwise: a failed
        quality gate, an error, a record failed (`stats.failed`).

        Rows rejected are bad data, quarantined, not a failure: a window
        holding only rejected rows is a partial success, so the steps
        depending on this one still run. A record failed is a system error,
        whatever else was loaded: `--partial-ok` must not let it pass.
        """
        if not self.has_blocking_issues:
            return etl.ETLStatus.SUCCESS
        if self.quality_failed or self.errors or self.stats.failed > 0:
            return etl.ETLStatus.FAILED
        return etl.ETLStatus.PARTIAL_SUCCESS
