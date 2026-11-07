from __future__ import annotations

import pytest

from core import contract
from tests.factories import fakes


def _report(
    *, rejected: int = 0, warnings: int = 0, failed: bool = False
) -> contract.QualityReport:
    report = contract.QualityReport(
        checkpoint=contract.Checkpoint.AFTER_EXTRACT,
        checked=10,
        rejected=rejected,
        failed=failed,
    )
    report.record([_issue(contract.QualityAction.WARN)] * warnings)
    return report


def _issue(
    action: contract.QualityAction, rule: str = "rule"
) -> contract.QualityIssue:
    return contract.QualityIssue(
        rule, contract.Checkpoint.AFTER_EXTRACT, action, "looks odd"
    )


class TestETLContext:

    def test_each_run_gets_its_own_id(self) -> None:
        first, second = fakes.make_context(), fakes.make_context()

        assert first.run.run_id != second.run.run_id

    def test_cancel(self) -> None:
        ctx = fakes.make_context()
        assert ctx.is_cancelled is False

        ctx.cancel()

        assert ctx.is_cancelled is True


def test_errors_get_their_own_timestamp() -> None:
    first = contract.ETLError("a", contract.ETLPhase.EXTRACT)
    second = contract.ETLError("b", contract.ETLPhase.EXTRACT)

    assert first.timestamp <= second.timestamp
    assert first.timestamp is not second.timestamp


def test_error_from_exception() -> None:
    error = contract.ETLError.from_exception(
        ValueError("boom"), phase=contract.ETLPhase.LOAD, record_id="42"
    )

    assert (error.message, error.phase, error.record_id) == (
        "boom",
        contract.ETLPhase.LOAD,
        "42",
    )
    assert error.as_dict()["phase"] == "load"


def test_stats_can_be_added() -> None:
    total = contract.ETLStats(total=2, successful=1) + contract.ETLStats(
        total=3, rejected=2
    )

    assert total == contract.ETLStats(total=5, successful=1, rejected=2)


class TestETLResultComplete:

    def test_success_when_nothing_went_wrong(self) -> None:
        result = contract.ETLResult(
            stats=contract.ETLStats(total=1, successful=1)
        )
        result.complete()

        assert result.status is contract.ETLStatus.SUCCESS
        assert result.duration() is not None

    def test_warnings_do_not_downgrade_the_status(self) -> None:
        result = contract.ETLResult(stats=contract.ETLStats(successful=1))
        result.add_quality_report(_report(warnings=3))
        result.complete()

        assert result.status is contract.ETLStatus.SUCCESS

    def test_rejected_records_mean_partial_success(self) -> None:
        result = contract.ETLResult(stats=contract.ETLStats(successful=8))
        result.add_quality_report(_report(rejected=2))
        result.complete()

        assert result.stats.rejected == 2
        assert result.status is contract.ETLStatus.PARTIAL_SUCCESS

    def test_failed_quality_gate_means_failure(self) -> None:
        result = contract.ETLResult(stats=contract.ETLStats(successful=8))
        result.add_quality_report(_report(failed=True))
        result.complete()

        assert result.status is contract.ETLStatus.FAILED

    def test_errors_without_successful_records_mean_failure(self) -> None:
        result = contract.ETLResult()
        result.add_error(contract.ETLError("boom"))
        result.complete()

        assert result.status is contract.ETLStatus.FAILED

    def test_explicit_status_wins(self) -> None:
        result = contract.ETLResult()
        result.complete(contract.ETLStatus.SKIPPED, "nothing to do")

        assert (result.status, result.message) == (
            contract.ETLStatus.SKIPPED,
            "nothing to do",
        )


class TestCheckpoint:

    def test_each_checkpoint_audits_an_etl_phase(self) -> None:
        assert [c.phase for c in contract.Checkpoint] == [
            contract.ETLPhase.EXTRACT,
            contract.ETLPhase.EXTRACT,
            contract.ETLPhase.TRANSFORM,
            contract.ETLPhase.LOAD,
        ]

    def test_reject_is_only_possible_before_load(self) -> None:
        assert [c for c in contract.Checkpoint if c.allows_reject] == [
            contract.Checkpoint.AFTER_EXTRACT,
            contract.Checkpoint.AFTER_TRANSFORM,
        ]


def test_quality_is_not_an_etl_phase() -> None:
    assert list(contract.ETLPhase) == [
        contract.ETLPhase.EXTRACT,
        contract.ETLPhase.TRANSFORM,
        contract.ETLPhase.LOAD,
    ]


def test_severity_is_derived_from_the_action() -> None:
    assert (
        contract.QualityAction.WARN.severity is contract.QualitySeverity.WARNING
    )
    assert (
        contract.QualityAction.REJECT.severity is contract.QualitySeverity.ERROR
    )
    assert (
        contract.QualityAction.FAIL.severity is contract.QualitySeverity.ERROR
    )


class TestQualityReport:

    def test_counts_violations_and_affected_records_per_rule(self) -> None:
        report = contract.QualityReport(contract.Checkpoint.AFTER_EXTRACT)

        report.record([
            _issue(contract.QualityAction.WARN, "a"),
            _issue(contract.QualityAction.WARN, "a"),
        ])
        report.record([_issue(contract.QualityAction.WARN, "a")])

        assert report.rules["a"].violations == 3
        assert report.rules["a"].records_affected == 2
        assert report.has_warnings

    def test_samples_are_capped_per_rule(self) -> None:
        report = contract.QualityReport(
            contract.Checkpoint.AFTER_EXTRACT, max_samples_per_rule=2
        )

        for _ in range(5):
            report.record([_issue(contract.QualityAction.WARN, "noisy")])
        report.record([_issue(contract.QualityAction.REJECT, "rare")])

        assert report.violations == 6
        assert [s.rule for s in report.samples] == ["noisy", "noisy", "rare"]

    def test_zero_samples_keeps_counters_only(self) -> None:
        report = contract.QualityReport(
            contract.Checkpoint.AFTER_EXTRACT, max_samples_per_rule=0
        )

        report.record([_issue(contract.QualityAction.WARN)])

        assert (report.violations, report.samples) == (1, [])

    def test_merge_adds_batches_up_and_keeps_the_sample_cap(self) -> None:
        first = contract.QualityReport(
            contract.Checkpoint.AFTER_EXTRACT,
            checked=3,
            rejected=1,
            max_samples_per_rule=2,
        )
        first.record([_issue(contract.QualityAction.REJECT)])
        second = contract.QualityReport(
            contract.Checkpoint.AFTER_EXTRACT, checked=4, rejected=2
        )
        second.record([_issue(contract.QualityAction.REJECT)])
        second.record([_issue(contract.QualityAction.REJECT)])
        second.fail("too many rejects")

        first.merge(second)

        assert (first.checked, first.rejected) == (7, 3)
        assert first.rules["rule"].violations == 3
        assert len(first.samples) == 2
        assert (first.failed, first.reason) == (True, "too many rejects")

    def test_merge_refuses_another_checkpoint(self) -> None:
        extract = contract.QualityReport(contract.Checkpoint.AFTER_EXTRACT)
        load = contract.QualityReport(contract.Checkpoint.AFTER_LOAD)

        with pytest.raises(ValueError):
            extract.merge(load)


def test_result_keeps_one_report_per_checkpoint() -> None:
    result = contract.ETLResult()

    for _ in range(3):
        result.add_quality_report(_report(rejected=1))

    assert list(result.quality_reports) == [contract.Checkpoint.AFTER_EXTRACT]
    assert (
        result.quality_reports[contract.Checkpoint.AFTER_EXTRACT].checked == 30
    )
    assert result.stats.rejected == 3


def test_the_values_of_an_in_condition_are_a_tuple() -> None:
    condition = contract.Condition("id", contract.Operator.IN, [3, 1])

    assert condition.values == (3, 1)
    with pytest.raises(ValueError, match="takes no collection"):
        _ = contract.Condition("id", contract.Operator.EQ, 3).values
