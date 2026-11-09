"""The quality gate: applies the rules of each checkpoint and their actions."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
import dataclasses
import random
import threading
import typing
from typing import NamedTuple, Never

from core import contract

# Contravariant, so that the check sets of every checkpoint go in one
# mapping (`CheckSet[Never]`). Declared, not PEP 695: pyright infers the
# parameters of a dataclass invariant on Python 3.13+, from the
# `__replace__` it synthesizes.
TRecord_contra = typing.TypeVar("TRecord_contra", contravariant=True)


class _SetAside(NamedTuple):
    rejected: Sequence[contract.RejectedRecord[object]]
    issues: Sequence[contract.QualityIssue]


_STRENGTH = {
    contract.QualityAction.WARN: 0,
    contract.QualityAction.REJECT: 1,
    contract.QualityAction.FAIL: 2,
}


@dataclasses.dataclass(frozen=True, slots=True)
class CheckSet(typing.Generic[TRecord_contra]):
    """The rules run at one checkpoint.

    Attributes:
        records: The rules checked against every record.
        batch: The rules checked against every batch.
        max_reject_rate: A circuit breaker, from 0.0 to 1.0: when the share of
            rejected records in a batch goes above it, the whole batch fails.
        record_id: Gets the identifier of a record, for the issues.
    """

    records: Sequence[contract.RecordRule[TRecord_contra]] = ()
    batch: Sequence[contract.BatchRule[TRecord_contra]] = ()
    max_reject_rate: float | None = None
    record_id: Callable[[TRecord_contra], str | None] | None = None

    def __post_init__(self) -> None:
        if (
            self.max_reject_rate is not None
            and not 0.0 <= self.max_reject_rate <= 1.0
        ):
            raise ValueError("max_reject_rate must be between 0.0 and 1.0")


class QualityGate(contract.IQualityGate):
    """Applies the rules declared per checkpoint, and enforces their actions.

    - WARN: the record goes through, the issue is reported.
    - REJECT: the record is dropped from the batch, into the quarantine.
    - FAIL: the batch fails; the job turns it into a FAILED step.

    The configuration is checked at construction, so that a pipeline that
    cannot honor its policy never starts:

    - record rules cannot run at BEFORE_EXTRACT, where there is no record;
    - REJECT is only allowed for record rules at AFTER_EXTRACT and
      AFTER_TRANSFORM;
    - any REJECT requires a quarantine, so that dropped data is never silent.

    Rejected records and issues are set aside by `inspect`, and stored by
    `flush` once the batch is handled (`discard` drops them when it fails):
    a retried batch is quarantined, parked and reported once. What is set
    aside belongs to the thread that inspected it, so jobs running at once
    can share a gate.
    """

    def __init__(
        self,
        checks: Mapping[contract.Checkpoint, CheckSet[Never]],
        quarantine: contract.IQuarantine | None = None,
        *,
        issue_sink: contract.IIssueSink | None = None,
        overrides: Mapping[str, contract.QualityAction] | None = None,
        max_samples_per_rule: int = contract.DEFAULT_MAX_SAMPLES_PER_RULE,
        sampling: contract.SamplingStrategy = contract.SamplingStrategy.FIRST,
        sampling_seed: int | None = None,
    ) -> None:
        """Initializes and validates the gate.

        Args:
            checks: The rules of each checkpoint.
            quarantine: Where rejected records go; required by REJECT.
            issue_sink: Where every issue goes, batch by batch.
            overrides: New actions for rules, by name (e.g. stricter in
                production).
            max_samples_per_rule: The issues a report keeps per rule; 0
                keeps counters only.
            sampling: How reports pick their samples.
            sampling_seed: Makes reservoir sampling reproducible.

        Raises:
            ValueError: If `max_samples_per_rule` is negative.
            PipelineConfigError: If the configuration cannot be honored.
        """
        if max_samples_per_rule < 0:
            raise ValueError("max_samples_per_rule must be >= 0")

        self._checks = dict(checks)
        self._quarantine = quarantine
        self._issue_sink = issue_sink
        self._max_samples_per_rule = max_samples_per_rule
        self._sampling = sampling
        # One per gate, so a seed reproduces a whole run; not for security.
        self._rng = random.Random(sampling_seed)  # nosec B311
        self._actions = self._resolve_actions(overrides or {})
        self._validate()
        self._local = threading.local()

    @property
    def actions(self) -> Mapping[str, contract.QualityAction]:
        """The effective action of every rule, after overrides."""
        return dict(self._actions)

    def inspect[T](
        self,
        checkpoint: contract.Checkpoint,
        records: Sequence[T],
        ctx: contract.ETLContext,
    ) -> contract.GateOutcome[T]:
        """See `contract.IQualityGate.inspect`."""
        # The rules of a checkpoint take the records of that checkpoint.
        check_set = typing.cast(
            CheckSet[T] | None, self._checks.get(checkpoint)
        )
        if check_set is None:
            return contract.GateOutcome(list(records))

        batch = list(records)
        report = self._report(checkpoint, check_set, len(batch))
        found: list[contract.QualityIssue] = []

        passed, rejected = self._check_records(
            checkpoint, check_set, batch, report, found, ctx
        )
        self._check_batch(checkpoint, check_set, batch, report, found, ctx)

        report.rejected = len(rejected)
        limit = check_set.max_reject_rate
        if limit is not None and report.reject_rate() > limit:
            report.fail(
                f"reject rate {report.reject_rate():.2%} exceeds {limit:.2%}"
            )

        if rejected or found:
            self._pending().append(_SetAside(rejected, found))

        return contract.GateOutcome(passed, report)

    def flush(self, ctx: contract.ETLContext) -> None:
        """Stores what was set aside since the last flush.

        The rejected records go to the quarantine, the issues to the sink.
        What was set aside is dropped even when they raise.
        """
        pending, self._local.pending = self._pending(), []
        for rejected, found in pending:
            if rejected:
                if self._quarantine is None:
                    raise RuntimeError(
                        "REJECT without a quarantine passed _validate"
                    )
                self._quarantine.put(rejected, ctx)
            if found and self._issue_sink is not None:
                self._issue_sink.write(found, ctx)

    def discard(self) -> None:
        """Drops what was set aside since the last flush: the batch failed."""
        self._local.pending = []

    def _pending(self) -> list[_SetAside]:
        """What this thread set aside, by inspection."""
        pending: list[_SetAside] = getattr(self._local, "pending", None) or []
        self._local.pending = pending
        return pending

    def _report[T](
        self,
        checkpoint: contract.Checkpoint,
        check_set: CheckSet[T],
        checked: int,
    ) -> contract.QualityReport:
        return contract.QualityReport(
            checkpoint=checkpoint,
            checked=checked,
            rules={
                rule.name: contract.RuleStats(self._actions[rule.name])
                for rule in _rules(check_set)
            },
            max_reject_rate=check_set.max_reject_rate,
            max_samples_per_rule=self._max_samples_per_rule,
            sampling=self._sampling,
            rng=self._rng,
        )

    def _check_records[T](
        self,
        checkpoint: contract.Checkpoint,
        check_set: CheckSet[T],
        batch: list[T],
        report: contract.QualityReport,
        found: list[contract.QualityIssue],
        ctx: contract.ETLContext,
    ) -> tuple[list[T], list[contract.RejectedRecord[T]]]:
        """Applies the record rules, recording their issues in `report`.

        Returns:
            The records that pass, and the records rejected.
        """
        passed: list[T] = []
        rejected: list[contract.RejectedRecord[T]] = []

        # Optional hook: a rule may look the whole batch up at once.
        for rule in check_set.records:
            prepare = getattr(rule, "prepare_batch", None)
            if callable(prepare):
                prepare(batch, ctx)

        for record in batch:
            issues = self._record_issues(checkpoint, check_set, record, ctx)
            report.record(issues)
            found.extend(issues)

            match _strongest(issues):
                case contract.QualityAction.FAIL:
                    failing = next(
                        i
                        for i in issues
                        if i.action is contract.QualityAction.FAIL
                    )
                    report.fail(f"rule '{failing.rule}' failed")
                case contract.QualityAction.REJECT:
                    rejected.append(
                        contract.RejectedRecord(
                            record, checkpoint, tuple(issues)
                        )
                    )
                case _:
                    passed.append(record)

        return passed, rejected

    def _check_batch[T](
        self,
        checkpoint: contract.Checkpoint,
        check_set: CheckSet[T],
        batch: list[T],
        report: contract.QualityReport,
        found: list[contract.QualityIssue],
        ctx: contract.ETLContext,
    ) -> None:
        """Applies the batch rules, recording their issues in `report`."""
        for batch_rule in check_set.batch:
            action = self._actions[batch_rule.name]
            issues = [
                _issue(batch_rule.name, checkpoint, action, violation)
                for violation in batch_rule.check_batch(batch, ctx)
            ]
            report.record(issues, per_record=False)
            found.extend(issues)
            if issues and action is contract.QualityAction.FAIL:
                report.fail(f"batch rule '{batch_rule.name}' failed")

    def _stores(self) -> list[contract.IQuarantine | contract.IIssueSink]:
        return [
            s for s in (self._quarantine, self._issue_sink) if s is not None
        ]

    def _record_issues[T](
        self,
        checkpoint: contract.Checkpoint,
        check_set: CheckSet[T],
        record: T,
        ctx: contract.ETLContext,
    ) -> list[contract.QualityIssue]:
        """Returns the issues the record rules find in `record`."""
        record_id = check_set.record_id(record) if check_set.record_id else None
        issues: list[contract.QualityIssue] = []

        for rule in check_set.records:
            action = self._actions[rule.name]
            for violation in rule.check_record(record, ctx):
                issues.append(
                    _issue(rule.name, checkpoint, action, violation, record_id)
                )

        return issues

    def _resolve_actions(
        self, overrides: Mapping[str, contract.QualityAction]
    ) -> dict[str, contract.QualityAction]:
        """Returns the action of every rule, after overrides.

        Raises:
            PipelineConfigError: On a duplicate rule name, or an override of
                an unknown rule.
        """
        actions: dict[str, contract.QualityAction] = {}

        for check_set in self._checks.values():
            for rule in _rules(check_set):
                if rule.name in actions:
                    raise contract.PipelineConfigError(
                        f"Duplicate quality rule name '{rule.name}'"
                    )
                actions[rule.name] = rule.on_violation

        unknown = sorted(set(overrides) - set(actions))
        if unknown:
            raise contract.PipelineConfigError(
                f"Overrides target unknown quality rule(s) {unknown}"
            )

        return actions | dict(overrides)

    def _validate(self) -> None:
        """Checks that every action can be applied where it is declared.

        Raises:
            PipelineConfigError: If an action cannot be applied.
        """
        for checkpoint, check_set in self._checks.items():
            if check_set.records and not checkpoint.has_records:
                raise contract.PipelineConfigError(
                    f"Record rules cannot run at {checkpoint}: no record"
                    " exists yet"
                )

            for record_rule in check_set.records:
                if (
                    self._actions[record_rule.name]
                    is contract.QualityAction.REJECT
                    and not checkpoint.allows_reject
                ):
                    raise contract.PipelineConfigError(
                        f"Rule '{record_rule.name}' cannot REJECT at"
                        f" {checkpoint}: records can only be dropped before"
                        " they are loaded"
                    )

            for batch_rule in check_set.batch:
                if (
                    self._actions[batch_rule.name]
                    is contract.QualityAction.REJECT
                ):
                    raise contract.PipelineConfigError(
                        f"Batch rule '{batch_rule.name}' cannot REJECT:"
                        " it cannot tell which record to drop"
                    )

        uses_reject = contract.QualityAction.REJECT in self._actions.values()
        if uses_reject and self._quarantine is None:
            raise contract.PipelineConfigError(
                "Quality rules with REJECT require a quarantine"
                " (use InMemoryQuarantine for tests and local development)"
            )


def _rules[T](
    check_set: CheckSet[T],
) -> list[contract.RecordRule[T] | contract.BatchRule[T]]:
    return [*check_set.records, *check_set.batch]


def _strongest(
    issues: Sequence[contract.QualityIssue],
) -> contract.QualityAction | None:
    if not issues:
        return None

    return max((issue.action for issue in issues), key=_STRENGTH.__getitem__)


def _issue(
    rule: str,
    checkpoint: contract.Checkpoint,
    action: contract.QualityAction,
    violation: contract.Violation,
    record_id: str | None = None,
) -> contract.QualityIssue:
    return contract.QualityIssue(
        rule=rule,
        checkpoint=checkpoint,
        action=action,
        message=violation.message,
        field=violation.field,
        value=violation.value,
        record_id=violation.record_id or record_id,
    )
