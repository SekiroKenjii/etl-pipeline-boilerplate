"""The exceptions of the ETL core.

Retrying is decided by type: `RetryableError` for transient failures,
`FatalError` for the ones retrying cannot fix.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from core.contract import etl

if TYPE_CHECKING:
    from core.contract import quality


class PipelineError(Exception):
    """The base class of every exception raised by the ETL core.

    Attributes:
        phase: The ETL phase the error is about, if known.
        record_id: The record the error is about, if any.
    """

    def __init__(
        self,
        message: str,
        phase: etl.ETLPhase | None = None,
        record_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.phase = phase
        self.record_id = record_id


class RetryableError(PipelineError):
    """A transient failure: timeout, connection reset, rate limit..."""


class FatalError(PipelineError):
    """A failure retrying will not fix: bad configuration, bad data..."""


class QualityGateError(FatalError):
    """The quality gate decided that a batch must not continue.

    Attributes:
        report: The report of the failing checkpoint; `phase` is the ETL
            phase that checkpoint audits.
    """

    def __init__(self, report: quality.QualityReport) -> None:
        super().__init__(
            f"Quality gate failed at {report.checkpoint}: {report.reason}",
            phase=report.checkpoint.phase,
        )
        self.report = report


class PipelineConfigError(FatalError):
    """The definition of a pipeline is invalid.

    For example: a duplicate step, an unknown dependency, a cycle, or a quality
    gate configured with an action its checkpoint cannot apply.
    """
