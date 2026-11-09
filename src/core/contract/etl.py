"""The run context, counters and errors of ETL operations."""

from __future__ import annotations

from collections.abc import Mapping
import dataclasses
import datetime
import enum
import threading
import uuid

from shared import utils


class ETLPhase(enum.StrEnum):
    """The data-moving phases of ETL.

    Data quality is not a phase: it audits these phases from the outside, see
    `quality.Checkpoint`.
    """

    EXTRACT = "extract"
    TRANSFORM = "transform"
    LOAD = "load"


class ETLStatus(enum.StrEnum):
    """The outcome of a step or of a whole pipeline run."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    PARTIAL_SUCCESS = "partial_success"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


@dataclasses.dataclass(frozen=True, slots=True)
class ETLRunInfo:
    """Immutable metadata of a single pipeline run.

    Attributes:
        pipeline_name: The pipeline; also the namespace of its saved state.
        run_id: A unique identifier of the run.
        triggered_at: When the run started.
        dry_run: Whether to run without loading anything.
        since: The start of the extraction window, for incremental loads.
        until: The end of the extraction window.
        params: Parameters of this run only, e.g. the corrections file of a
            replay (`etl run --param name=value`).
        steps: The steps to run, by name; every step when empty. The others
            are taken as done: they neither run nor hold back the steps
            that depend on them (`etl run --step`).
    """

    pipeline_name: str
    run_id: str = dataclasses.field(default_factory=lambda: uuid.uuid4().hex)
    triggered_at: datetime.datetime = dataclasses.field(
        default_factory=utils.utc_now
    )
    dry_run: bool = False
    since: datetime.datetime | None = None
    until: datetime.datetime | None = None
    params: Mapping[str, str] = dataclasses.field(
        default_factory=dict[str, str]
    )
    steps: frozenset[str] = frozenset()


class ETLContext:
    """What the steps of one pipeline run share: its metadata, and whether
    it is cancelled.

    Create a new context per run; never share one between runs.

    Attributes:
        run: The metadata of the run.
    """

    def __init__(self, run: ETLRunInfo) -> None:
        self.run = run
        self._cancelled = threading.Event()

    def cancel(self) -> None:
        """Asks the run to stop: no new batch or step starts."""
        self._cancelled.set()

    @property
    def is_cancelled(self) -> bool:
        return self._cancelled.is_set()

    def wait(self, seconds: float) -> bool:
        """Waits up to `seconds` for the run to be cancelled.

        Returns:
            Whether it was.
        """
        return self._cancelled.wait(seconds)


@dataclasses.dataclass(slots=True)
class ETLStats:
    """Record counters of an ETL operation.

    None of `filtered` and `skipped` is an error; `rejected` is bad data and
    `failed` a system error.

    Attributes:
        total: Records read from the source.
        successful: Records loaded.
        failed: Records that could not be processed (system errors).
        skipped: Records not loaded because of a dry run.
        filtered: Records dropped on purpose by a transformer (a business
            filter, duplicates).
        rejected: Records dropped by a quality gate (bad data).
        created: Records created in the target.
        updated: Records updated in the target.
        deleted: Records deleted in the target.
        corrected: Records corrected by hand before their mapping
            (`operators.Corrections`).
    """

    total: int = 0
    successful: int = 0
    failed: int = 0
    skipped: int = 0
    filtered: int = 0
    rejected: int = 0
    created: int = 0
    updated: int = 0
    deleted: int = 0
    corrected: int = 0

    def __add__(self, other: ETLStats) -> ETLStats:
        return ETLStats(
            total=self.total + other.total,
            successful=self.successful + other.successful,
            failed=self.failed + other.failed,
            skipped=self.skipped + other.skipped,
            filtered=self.filtered + other.filtered,
            rejected=self.rejected + other.rejected,
            created=self.created + other.created,
            updated=self.updated + other.updated,
            deleted=self.deleted + other.deleted,
            corrected=self.corrected + other.corrected,
        )

    def as_dict(self) -> dict[str, int]:
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True, slots=True)
class ETLError:
    """The record of an error that occurred during an ETL operation.

    Attributes:
        message: What went wrong.
        phase: The ETL phase the error is about, if known.
        record_id: The record the error is about, if any.
        exception: The exception raised, if any.
        timestamp: When the error was recorded.
    """

    message: str
    phase: ETLPhase | None = None
    record_id: str | None = None
    exception: BaseException | None = None
    timestamp: datetime.datetime = dataclasses.field(
        default_factory=utils.utc_now
    )

    @classmethod
    def from_exception(
        cls,
        exception: BaseException,
        phase: ETLPhase | None = None,
        record_id: str | None = None,
    ) -> ETLError:
        """Records `exception`; its text, or its type, is the message."""
        return cls(
            message=str(exception) or type(exception).__name__,
            phase=phase,
            record_id=record_id,
            exception=exception,
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "message": self.message,
            "phase": self.phase.value if self.phase else None,
            "record_id": self.record_id,
            "exception": repr(self.exception) if self.exception else None,
            "timestamp": self.timestamp.isoformat(),
        }
