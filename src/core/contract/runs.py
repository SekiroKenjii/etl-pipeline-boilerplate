"""The ports of the runs: their history (ledger), and who may run (lock)."""

from __future__ import annotations

import abc
from collections.abc import Collection, Mapping
import dataclasses
import datetime

from core.contract import etl
from core.contract import pipeline
from core.contract import results
from shared import utils


@dataclasses.dataclass(frozen=True, slots=True)
class StepRecord:
    """How a step of a run ended, as the ledger keeps it.

    Attributes:
        step: The step.
        status: Its outcome.
        completed_at: When it ended.
        stats: Its counters.
        message: A summary.
        host: Where it ran, and the process.
        attempts: How many times it was tried.
    """

    step: str
    status: etl.ETLStatus
    completed_at: datetime.datetime
    stats: dict[str, int] = dataclasses.field(default_factory=dict[str, int])
    message: str | None = None
    host: str = ""
    attempts: int = 1

    @classmethod
    def of(cls, step: str, result: results.ETLResult, host: str) -> StepRecord:
        """How a step ended, from its result."""
        attempts = result.data.get("attempts", 1)
        return cls(
            step=step,
            status=result.status,
            completed_at=result.completed_at or utils.utc_now(),
            stats=result.stats.as_dict(),
            message=result.message,
            host=host,
            attempts=attempts if isinstance(attempts, int) else 1,
        )


@dataclasses.dataclass(frozen=True, slots=True)
class RunRecord:
    """One run of a pipeline, as the ledger keeps it.

    Attributes:
        run_id: The run.
        pipeline: The pipeline, `module.pipeline`.
        status: Its outcome; IN_PROGRESS while it runs, or if it died.
        started_at: When it started.
        completed_at: When it ended, if it did.
        stats: The counters of every step, added up.
        message: A summary.
        host: Where it ran.
        dry_run: Whether nothing was loaded.
        meta: What ran, for audits: the user, the versions of the code and
            the mappings, the hash of the settings, the run it resumed...
        steps: How each step ended, by step; only `IRunLedger.get` reads
            them.
    """

    run_id: str
    pipeline: str
    status: etl.ETLStatus
    started_at: datetime.datetime
    completed_at: datetime.datetime | None = None
    stats: dict[str, int] = dataclasses.field(default_factory=dict[str, int])
    message: str | None = None
    host: str = ""
    dry_run: bool = False
    meta: dict[str, str] = dataclasses.field(default_factory=dict[str, str])
    steps: dict[str, StepRecord] = dataclasses.field(
        default_factory=dict[str, StepRecord]
    )


class IRunLedger(abc.ABC):
    """The history of the runs: what ran, when, how it went.

    Each step is recorded as soon as it ends, so a run that died still tells
    which steps it finished. A run still IN_PROGRESS long after it started died
    with its process.
    """

    @abc.abstractmethod
    def start(
        self,
        run: etl.ETLRunInfo,
        host: str,
        meta: Mapping[str, str] | None = None,
    ) -> None:
        """Records that a run started, and what ran (`RunRecord.meta`).

        A run already recorded is left as it is.
        """

    @abc.abstractmethod
    def record_step(
        self, run_id: str, step: str, result: results.ETLResult, host: str
    ) -> None:
        """Records how a step of a run ended; replaces an earlier record of
        that step."""

    @abc.abstractmethod
    def finish(self, result: pipeline.PipelineResult) -> None:
        """Records how a run ended."""

    @abc.abstractmethod
    def recent(
        self, pipeline_name: str | None, limit: int = 20
    ) -> list[RunRecord]:
        """Returns the latest runs, of one pipeline or of all, newest first.

        Their steps are not read: `get` reads them.
        """

    @abc.abstractmethod
    def get(self, run_id: str) -> RunRecord | None:
        """Returns a run and its steps; None if there is no such run."""

    @abc.abstractmethod
    def latest(
        self,
        pipeline_prefix: str,
        statuses: Collection[etl.ETLStatus],
        meta_key: str,
    ) -> RunRecord | None:
        """Returns the newest run, not a dry run, of a pipeline whose name
        starts with `pipeline_prefix`, that ended in one of `statuses` and
        has `meta_key` in its meta; None if there is none.

        Every run is searched, not only the recent ones; its steps are not
        read.
        """

    @abc.abstractmethod
    def last_run(
        self, pipeline_name: str, label: str | None = None
    ) -> RunRecord | None:
        """Returns the newest run, not a dry run, of a pipeline, whatever
        its status; of that label (`meta["label"]`) when given. None if
        there is none.

        Every run is searched, not only the recent ones; its steps are not
        read.
        """


class IRunLock(abc.ABC):
    """Only one run of a step at a time, across hosts.

    The kernel takes `<pipeline>/<step>` for every step a run runs. A lock is
    a lease: its owner renews it while it runs; a lease not renewed
    (the process died) expires, and another run may take it. An owner is
    one process: only it renews or frees its leases.
    """

    @abc.abstractmethod
    def acquire(self, name: str, owner: str, lease_seconds: float) -> bool:
        """Takes the lock if it is free or expired; returns whether it did."""

    @abc.abstractmethod
    def renew(self, name: str, owner: str, lease_seconds: float) -> bool:
        """Extends the lease; returns False if `owner` lost the lock."""

    @abc.abstractmethod
    def release(self, name: str, owner: str) -> None:
        """Frees the lock, if `owner` holds it."""

    @abc.abstractmethod
    def holder(self, name: str) -> str | None:
        """Returns the owner of the lease on the lock; None when the lock
        is free or its lease expired.

        A run of a label waits for a lock held by its own label only.
        """
