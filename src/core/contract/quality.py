"""The data quality contract.

A gate audits the data at the checkpoints of a step. A rule detects
(`Violation`), its `on_violation` decides (`QualityAction`), the gate
enforces and reports (`QualityReport`): it lets a record through (WARN),
rejects it into a quarantine (REJECT) or stops the batch (FAIL). It never
modifies records. Reports keep counters and a few samples; every issue goes
to an `IIssueSink`, every rejected record to an `IQuarantine`.
"""

from __future__ import annotations

import abc
from collections.abc import Collection, Iterable, Iterator, Mapping, Sequence
import dataclasses
import datetime
import enum
import hashlib
import json
import random
import typing
from typing import Any, Protocol

from core.contract import etl
from shared import utils


class Checkpoint(enum.StrEnum):
    """A boundary of an ETL phase where the quality gate inspects the data."""

    BEFORE_EXTRACT = "before_extract"
    AFTER_EXTRACT = "after_extract"
    AFTER_TRANSFORM = "after_transform"
    AFTER_LOAD = "after_load"

    @property
    def phase(self) -> etl.ETLPhase:
        """The ETL phase this checkpoint audits."""
        return _AUDITED_PHASE[self]

    @property
    def has_records(self) -> bool:
        """Whether records flow through this checkpoint."""
        return self is not Checkpoint.BEFORE_EXTRACT

    @property
    def allows_reject(self) -> bool:
        """Whether records can still be dropped: not once they are loaded."""
        return self in (Checkpoint.AFTER_EXTRACT, Checkpoint.AFTER_TRANSFORM)


_AUDITED_PHASE = {
    Checkpoint.BEFORE_EXTRACT: etl.ETLPhase.EXTRACT,
    Checkpoint.AFTER_EXTRACT: etl.ETLPhase.EXTRACT,
    Checkpoint.AFTER_TRANSFORM: etl.ETLPhase.TRANSFORM,
    Checkpoint.AFTER_LOAD: etl.ETLPhase.LOAD,
}


class QualityAction(enum.StrEnum):
    """What the gate does when a rule is violated."""

    WARN = "warn"
    REJECT = "reject"
    FAIL = "fail"

    @property
    def severity(self) -> QualitySeverity:
        if self is QualityAction.WARN:
            return QualitySeverity.WARNING
        return QualitySeverity.ERROR


class QualitySeverity(enum.StrEnum):
    """The severity of an issue, derived from its action, for reporting."""

    WARNING = "warning"
    ERROR = "error"


@dataclasses.dataclass(frozen=True, slots=True)
class Violation:
    """What a rule found. It carries no decision: the rule's action does.

    Attributes:
        message: What is wrong.
        field: The field at fault, if any.
        value: The value at fault, if any.
        record_id: The record at fault, if the rule knows it.
    """

    message: str
    field: str | None = None
    value: object = None
    record_id: str | None = None


class RecordRule[T](Protocol):
    """A rule checked against every record: schema, types, business rules."""

    @property
    def name(self) -> str:
        """The unique name of the rule, used in reports and overrides."""
        ...

    @property
    def on_violation(self) -> QualityAction:
        """What the gate does when the rule is violated."""
        ...

    def check_record(
        self, record: T, ctx: etl.ETLContext
    ) -> Iterable[Violation]:
        """Returns the violations of `record`; none when it is valid."""
        ...


class BatchRule[T](Protocol):
    """A rule checked against a whole batch: uniqueness, row count, freshness.

    It runs per batch, as the source is streamed: it cannot see the whole
    dataset. At `BEFORE_EXTRACT` the batch is empty: inspect `ctx` instead.
    """

    @property
    def name(self) -> str:
        """The unique name of the rule, used in reports and overrides."""
        ...

    @property
    def on_violation(self) -> QualityAction:
        """What the gate does when the rule is violated."""
        ...

    def check_batch(
        self, records: Sequence[T], ctx: etl.ETLContext
    ) -> Iterable[Violation]:
        """Returns the violations of the batch; none when it is valid."""
        ...


@dataclasses.dataclass(frozen=True, slots=True)
class QualityIssue:
    """A violation once the gate applied the rule's action to it.

    Attributes:
        rule: The rule violated.
        checkpoint: Where the violation was found.
        action: What the gate did about it.
        message: What is wrong.
        field: The field at fault, if any.
        value: The value at fault, if any.
        record_id: The record at fault, if known.
    """

    rule: str
    checkpoint: Checkpoint
    action: QualityAction
    message: str
    field: str | None = None
    value: object = None
    record_id: str | None = None

    @property
    def severity(self) -> QualitySeverity:
        return self.action.severity

    def as_dict(self) -> dict[str, object]:
        return {
            "rule": self.rule,
            "checkpoint": self.checkpoint.value,
            "action": self.action.value,
            "severity": self.severity.value,
            "message": self.message,
            "field": self.field,
            "value": self.value,
            "record_id": self.record_id,
        }


@dataclasses.dataclass(frozen=True, slots=True)
class RejectedRecord[T]:
    """A record the gate dropped into the quarantine.

    Attributes:
        record: The record, as it was at the checkpoint.
        checkpoint: Where it was rejected.
        issues: Why it was rejected.
    """

    record: T
    checkpoint: Checkpoint
    issues: tuple[QualityIssue, ...]


DEFAULT_MAX_SAMPLES_PER_RULE = 20


class SamplingStrategy(enum.StrEnum):
    """How a report picks the issues it keeps as samples of each rule."""

    FIRST = "first"
    RESERVOIR = "reservoir"


@dataclasses.dataclass(slots=True)
class RuleStats:
    """The counters of one rule, always complete whatever the data volume.

    Attributes:
        action: What the gate does when the rule is violated.
        violations: The violations found.
        records_affected: The records with at least one violation (record
            rules only: one record may yield several violations).
    """

    action: QualityAction
    violations: int = 0
    records_affected: int = 0


@dataclasses.dataclass(slots=True)
class QualityReport:
    """What the gate found at one checkpoint, bounded whatever the volume.

    The gate produces one report per batch; `ETLResult` merges them into one
    report per checkpoint. The full detail goes out of the process: every issue
    to the `IIssueSink`, rejected records to the `IQuarantine`.

    Attributes:
        checkpoint: Where the data was inspected.
        checked: The records inspected.
        rejected: The records dropped into the quarantine.
        rules: The counters of each rule, by rule name.
        samples_by_rule: At most `max_samples_per_rule` issues per rule, picked
            according to `sampling`.
        failed: Whether the batch must not continue.
        reason: Why it failed.
        max_reject_rate: The effective circuit breaker, for auditing.
        max_samples_per_rule: The effective sample quota.
        sampling: The effective sampling strategy.
        rng: The random generator of reservoir sampling.
    """

    checkpoint: Checkpoint
    checked: int = 0
    rejected: int = 0
    rules: dict[str, RuleStats] = dataclasses.field(
        default_factory=dict[str, RuleStats]
    )
    samples_by_rule: dict[str, list[QualityIssue]] = dataclasses.field(
        default_factory=dict[str, list[QualityIssue]]
    )
    failed: bool = False
    reason: str | None = None
    max_reject_rate: float | None = None
    max_samples_per_rule: int = DEFAULT_MAX_SAMPLES_PER_RULE
    sampling: SamplingStrategy = SamplingStrategy.FIRST
    rng: random.Random = dataclasses.field(
        default_factory=random.Random, repr=False, compare=False
    )

    @property
    def samples(self) -> list[QualityIssue]:
        """The samples of every rule, rule after rule."""
        samples: list[QualityIssue] = []
        for kept in self.samples_by_rule.values():
            samples.extend(kept)
        return samples

    @property
    def actions(self) -> dict[str, QualityAction]:
        """The effective action of each rule, for auditing."""
        return {name: stats.action for name, stats in self.rules.items()}

    @property
    def violations(self) -> int:
        return sum(stats.violations for stats in self.rules.values())

    @property
    def has_warnings(self) -> bool:
        return any(
            stats.action is QualityAction.WARN and stats.violations > 0
            for stats in self.rules.values()
        )

    def reject_rate(self) -> float:
        """Returns the share of the checked records that were rejected."""
        if self.checked == 0:
            return 0.0

        return self.rejected / self.checked

    def fail(self, reason: str) -> None:
        """Marks the batch as failed; the first reason given is kept."""
        self.failed = True
        self.reason = self.reason or reason

    def record(
        self, issues: Sequence[QualityIssue], per_record: bool = True
    ) -> None:
        """Counts issues found together, keeping samples within the quota.

        Args:
            issues: The issues of one record, or of one batch rule.
            per_record: Whether `issues` belong to one record, to count the
                records affected.
        """
        for issue in issues:
            stats = self._stats(issue)
            stats.violations += 1
            self._offer(issue, seen=stats.violations)

        if per_record:
            for rule in {issue.rule for issue in issues}:
                self.rules[rule].records_affected += 1

    def merge(self, other: QualityReport) -> None:
        """Folds the report of another batch at the same checkpoint into this.

        Args:
            other: The report to fold in.

        Raises:
            ValueError: If `other` is about another checkpoint.
        """
        if other.checkpoint is not self.checkpoint:
            raise ValueError(
                f"Cannot merge a {other.checkpoint} report into a"
                f" {self.checkpoint} one"
            )

        self.checked += other.checked
        self.rejected += other.rejected

        for name, stats in other.rules.items():
            mine = self.rules.setdefault(name, RuleStats(stats.action))
            self._merge_samples(name, mine.violations, other, stats.violations)
            mine.violations += stats.violations
            mine.records_affected += stats.records_affected

        if other.failed:
            self.fail(other.reason or "failed")

    def _stats(self, issue: QualityIssue) -> RuleStats:
        return self.rules.setdefault(issue.rule, RuleStats(issue.action))

    def _offer(self, issue: QualityIssue, seen: int) -> None:
        kept = self.samples_by_rule.setdefault(issue.rule, [])

        if self.sampling is SamplingStrategy.RESERVOIR:
            utils.reservoir_offer(
                kept, issue, seen, self.max_samples_per_rule, self.rng
            )
        elif len(kept) < self.max_samples_per_rule:
            kept.append(issue)

    def _merge_samples(
        self, rule: str, seen: int, other: QualityReport, other_seen: int
    ) -> None:
        """Merges the samples of `rule` in `other` into this report's.

        Args:
            rule: The rule whose samples to merge.
            seen: The violations of `rule` this report counted so far.
            other: The report to take samples from.
            other_seen: The violations of `rule` `other` counted.
        """
        theirs = other.samples_by_rule.get(rule, [])
        if not theirs:
            return

        mine = self.samples_by_rule.get(rule, [])
        cap = self.max_samples_per_rule

        if self.sampling is SamplingStrategy.RESERVOIR:
            merged = utils.merge_reservoirs(
                mine, seen, theirs, other_seen, cap, self.rng
            )
        else:
            merged = (mine + theirs)[:cap]

        self.samples_by_rule[rule] = merged


@dataclasses.dataclass(frozen=True, slots=True)
class GateOutcome[T]:
    """What the gate hands back to the ETL job.

    Attributes:
        records: The records allowed to continue.
        report: What the gate found, or None when no rule ran.
    """

    records: list[T]
    report: QualityReport | None = None


class QuarantineStatus(enum.StrEnum):
    """Where a quarantined record stands."""

    OPEN = "open"
    REPLAYED = "replayed"
    CLOSED = "closed"


@dataclasses.dataclass(frozen=True, slots=True)
class QuarantineEntry:
    """A record of the quarantine, as stored.

    Attributes:
        id: Identifies it in its quarantine.
        pipeline: The pipeline that rejected it.
        run_id: The run.
        checkpoint: Where it was rejected.
        record: The record, as JSON-compatible data.
        issues: Why, as `QualityIssue.as_dict()` data.
        rejected_at: When.
        status: Where it stands.
        resolved_at: When it was replayed or closed.
    """

    id: str
    pipeline: str
    run_id: str
    checkpoint: str
    record: Any
    issues: tuple[Mapping[str, Any], ...]
    rejected_at: datetime.datetime
    status: QuarantineStatus = QuarantineStatus.OPEN
    resolved_at: datetime.datetime | None = None

    @property
    def key(self) -> str:
        """Identifies the record of the entry: see `key_of`."""
        return self.key_of(self.pipeline, self.checkpoint, self.record)

    @staticmethod
    def key_of(pipeline: str, checkpoint: str, record: object) -> str:
        """Identifies a quarantined record, whatever its run and issues.

        A record rejected again (read again at a watermark boundary, a run
        resumed) has the same key, so the quarantine refreshes its open entry. A
        mapped record is identified by its mapping, entity, source key, part and
        deletion flag; any other record by all of its data.

        Args:
            pipeline: The pipeline that rejected it.
            checkpoint: Where.
            record: The record, as stored or as rejected: it is turned into
                JSON-compatible data first, as quarantines store it.

        Returns:
            A SHA-256 digest, as 64 hexadecimal characters.
        """
        # Identified as stored: tuples become lists, dataclasses dicts.
        data = json.loads(json.dumps(record, default=utils.to_jsonable))
        identity: object = data
        if isinstance(data, dict):
            fields = typing.cast(dict[str, object], data)
            if fields.get("source_key") is not None:
                identity = {
                    name: fields.get(name) for name in _MAPPED_RECORD_IDENTITY
                }
        text = json.dumps(
            [pipeline, checkpoint, identity],
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
            default=utils.to_jsonable,
        )
        return hashlib.sha256(text.encode("utf-8")).hexdigest()


_MAPPED_RECORD_IDENTITY = ("mapping", "entity", "source_key", "part", "deleted")


class IQuarantine(abc.ABC):
    """Where rejected records go: a dead-letter table, a file, a queue...

    A gate with any REJECT rule requires one, so that dropped data is never
    silent. The records wait there until replayed (`operators.QuarantineReplay`)
    or closed, and are purged after their retention.
    """

    @abc.abstractmethod
    def put(
        self, records: Sequence[RejectedRecord[Any]], ctx: etl.ETLContext
    ) -> None:
        """Stores the records rejected in one batch.

        Idempotent for open entries: a record with the key
        (`QuarantineEntry.key_of`) of an OPEN entry refreshes that entry (its
        record, issues, run and time) instead of adding another; a record
        whose entries were all replayed or closed gets a new entry.
        """

    @abc.abstractmethod
    def entries(
        self,
        pipeline: str | None = None,
        status: QuarantineStatus | None = QuarantineStatus.OPEN,
    ) -> Iterator[QuarantineEntry]:
        """Yields the records of `pipeline` (all when None) in `status` (any
        when None), oldest first."""

    @abc.abstractmethod
    def resolve(self, ids: Collection[str], status: QuarantineStatus) -> int:
        """Marks records replayed or closed; returns how many changed."""

    @abc.abstractmethod
    def erase(self, ids: Collection[str]) -> int:
        """Deletes records whatever their age, e.g. for a right to erasure;
        returns how many."""

    @abc.abstractmethod
    def purge(self, before: datetime.datetime) -> int:
        """Deletes the records rejected before `before`, whatever their
        status; returns how many."""


class IIssueSink(abc.ABC):
    """Where every issue goes, in full: an audit table, a log file...

    The equivalent of dbt's `store_failures`: a report only keeps counters and
    samples, the sink keeps everything. It receives the issues of one batch at
    a time, whatever their action, so it must not buffer a whole run.
    """

    @abc.abstractmethod
    def write(
        self, issues: Sequence[QualityIssue], ctx: etl.ETLContext
    ) -> None:
        """Stores the issues found in one batch."""

    @abc.abstractmethod
    def erase(self, record_ids: Collection[str]) -> int:
        """Deletes the issues of some records (`QualityIssue.record_id`),
        whatever their age, e.g. for a right to erasure; returns how many."""

    @abc.abstractmethod
    def purge(self, before: datetime.datetime) -> int:
        """Deletes the issues found before `before`; returns how many."""


class IQualityGate(abc.ABC):
    """The only thing the ETL job knows about data quality.

    What a gate stores (rejected records, issues) waits for `flush`, which the
    job calls once the batch is handled, right before acknowledging it: a
    batch that fails and is read again stores it once, not once per attempt.
    """

    @abc.abstractmethod
    def inspect[T](
        self, checkpoint: Checkpoint, records: Sequence[T], ctx: etl.ETLContext
    ) -> GateOutcome[T]:
        """Inspects a batch at a checkpoint.

        Args:
            checkpoint: Where the batch is.
            records: The batch.
            ctx: The run context.

        Returns:
            The records allowed to continue, and what the gate found.
        """

    def flush(self, ctx: etl.ETLContext) -> None:
        """Stores what the inspections since the last flush set aside.

        Called by the job once a batch is handled, before acknowledging it,
        in the thread that inspected it. Nothing to store by default.
        """

    def discard(self) -> None:
        """Drops what the inspections since the last flush set aside.

        Called by the job when a batch fails: it is read again, and inspected
        again. Nothing to drop by default.
        """
