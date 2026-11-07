from __future__ import annotations

from collections.abc import Iterator, Sequence
import json
import pathlib
import threading
from typing import Any

import pytest

from core import contract
from core import quality
from tests.factories import fakes

GOOD = fakes.RawUser("1", "a@x.io", 30)
NO_EMAIL = fakes.RawUser("2", "", 30)
OLD = fakes.RawUser("3", "c@x.io", 150)

HAS_EMAIL = quality.RecordCheck[fakes.RawUser](
    "has_email",
    lambda u: bool(u.email),
    "email is required",
    on_violation=contract.QualityAction.REJECT,
    field="email",
)
REALISTIC_AGE = quality.RecordCheck[fakes.RawUser](
    "realistic_age",
    lambda u: u.age < 120,
    on_violation=contract.QualityAction.WARN,
)
UNIQUE_ID = quality.BatchCheck[fakes.RawUser](
    "unique_id", quality.unique_by(lambda u: u.id)
)

AFTER_EXTRACT = contract.Checkpoint.AFTER_EXTRACT


def _gate(
    check_set: quality.CheckSet[fakes.RawUser],
    checkpoint: contract.Checkpoint = AFTER_EXTRACT,
    **kwargs: Any,
) -> quality.QualityGate:
    kwargs.setdefault("quarantine", quality.InMemoryQuarantine())
    return quality.QualityGate({checkpoint: check_set}, **kwargs)


class TestInspect:

    def test_reject_goes_to_quarantine_and_warn_passes_through(self) -> None:
        quarantine = quality.InMemoryQuarantine()
        gate = _gate(
            quality.CheckSet(
                [HAS_EMAIL, REALISTIC_AGE], record_id=lambda u: u.id
            ),
            quarantine=quarantine,
        )

        ctx = fakes.make_context()
        outcome = gate.inspect(AFTER_EXTRACT, [GOOD, NO_EMAIL, OLD], ctx)
        before = list(quarantine.records)
        gate.flush(ctx)

        assert outcome.records == [GOOD, OLD]
        assert before == []  # Until the batch is handled.
        assert [r.record for r in quarantine.records] == [NO_EMAIL]
        report = outcome.report
        assert report is not None
        assert (report.checked, report.rejected, report.failed) == (3, 1, False)
        assert report.has_warnings
        reject = quarantine.records[0].issues[0]
        assert (reject.record_id, reject.field, reject.action) == (
            "2",
            "email",
            contract.QualityAction.REJECT,
        )

    def test_fail_is_the_default_action(self) -> None:
        gate = quality.QualityGate({
            AFTER_EXTRACT: quality.CheckSet([
                quality.RecordCheck[fakes.RawUser](
                    "has_email", lambda u: bool(u.email)
                )
            ])
        })

        report = gate.inspect(
            AFTER_EXTRACT, [GOOD, NO_EMAIL], fakes.make_context()
        ).report

        assert report is not None
        assert report.failed
        assert report.reason == "rule 'has_email' failed"

    def test_reject_rate_above_threshold_fails_the_batch(self) -> None:
        gate = _gate(quality.CheckSet([HAS_EMAIL], max_reject_rate=0.25))

        report = gate.inspect(
            AFTER_EXTRACT, [GOOD, GOOD, NO_EMAIL], fakes.make_context()
        ).report

        assert report is not None
        assert report.failed
        assert report.reason == "reject rate 33.33% exceeds 25.00%"

    def test_batch_rule_failure_fails_the_batch(self) -> None:
        gate = _gate(quality.CheckSet(batch=[UNIQUE_ID]))

        report = gate.inspect(
            AFTER_EXTRACT, [GOOD, GOOD], fakes.make_context()
        ).report

        assert report is not None
        assert report.failed
        assert report.reason == "batch rule 'unique_id' failed"

    def test_checkpoint_without_rules_lets_everything_through(self) -> None:
        gate = _gate(quality.CheckSet([HAS_EMAIL]))

        outcome = gate.inspect(
            contract.Checkpoint.AFTER_TRANSFORM,
            [NO_EMAIL],
            fakes.make_context(),
        )

        assert (outcome.records, outcome.report) == ([NO_EMAIL], None)

    def test_before_extract_inspects_the_context(self) -> None:
        class SourceConfigured:
            name = "source_configured"
            on_violation = contract.QualityAction.FAIL

            def check_batch(
                self, records: Sequence[fakes.RawUser], ctx: contract.ETLContext
            ) -> Iterator[contract.Violation]:
                if "source_url" not in ctx.run.params:
                    yield contract.Violation("source_url is not configured")

        gate = _gate(
            quality.CheckSet(batch=[SourceConfigured()]),
            contract.Checkpoint.BEFORE_EXTRACT,
        )

        report = gate.inspect(
            contract.Checkpoint.BEFORE_EXTRACT, [], fakes.make_context()
        ).report

        assert report is not None
        assert report.failed
        assert report.samples[0].message == "source_url is not configured"

    def test_overrides_change_actions_and_are_reported(self) -> None:
        gate = _gate(
            quality.CheckSet([HAS_EMAIL]),
            overrides={"has_email": contract.QualityAction.FAIL},
        )

        report = gate.inspect(
            AFTER_EXTRACT, [NO_EMAIL], fakes.make_context()
        ).report

        assert report is not None
        assert report.failed
        assert report.actions == {"has_email": contract.QualityAction.FAIL}


class TestSetAside:

    def test_a_discarded_batch_stores_nothing(self) -> None:
        quarantine = quality.InMemoryQuarantine()
        sink = quality.InMemoryIssueSink()
        gate = _gate(
            quality.CheckSet([HAS_EMAIL]),
            quarantine=quarantine,
            issue_sink=sink,
        )
        ctx = fakes.make_context()

        gate.inspect(AFTER_EXTRACT, [NO_EMAIL], ctx)
        gate.discard()
        gate.inspect(AFTER_EXTRACT, [NO_EMAIL], ctx)
        gate.flush(ctx)
        gate.flush(ctx)

        assert [r.record for r in quarantine.records] == [NO_EMAIL]
        assert len(sink.issues) == 1

    def test_each_thread_flushes_its_own_batch(self) -> None:
        quarantine = quality.InMemoryQuarantine()
        gate = _gate(quality.CheckSet([HAS_EMAIL]), quarantine=quarantine)
        ctx = fakes.make_context()
        other = fakes.RawUser("9", "", 1)
        gate.inspect(AFTER_EXTRACT, [NO_EMAIL], ctx)

        def elsewhere() -> None:
            gate.inspect(AFTER_EXTRACT, [other], ctx)
            gate.discard()

        thread = threading.Thread(target=elsewhere)
        thread.start()
        thread.join()
        gate.flush(ctx)

        assert [r.record for r in quarantine.records] == [NO_EMAIL]


class TestConfiguration:

    def test_reject_requires_a_quarantine(self) -> None:
        checks = quality.CheckSet([HAS_EMAIL])

        with pytest.raises(
            contract.PipelineConfigError, match="require a quarantine"
        ):
            quality.QualityGate({AFTER_EXTRACT: checks})

    def test_override_to_reject_also_requires_a_quarantine(self) -> None:
        checks = quality.CheckSet([REALISTIC_AGE])

        with pytest.raises(
            contract.PipelineConfigError, match="require a quarantine"
        ):
            quality.QualityGate(
                {AFTER_EXTRACT: checks},
                overrides={"realistic_age": contract.QualityAction.REJECT},
            )

    def test_cannot_reject_after_load(self) -> None:
        checks = quality.CheckSet([HAS_EMAIL])

        with pytest.raises(
            contract.PipelineConfigError, match="cannot REJECT at after_load"
        ):
            _gate(checks, contract.Checkpoint.AFTER_LOAD)

    def test_batch_rules_cannot_reject(self) -> None:
        unique = quality.BatchCheck[fakes.RawUser](
            "unique_id",
            quality.unique_by(lambda u: u.id),
            on_violation=contract.QualityAction.REJECT,
        )
        checks = quality.CheckSet(batch=[unique])

        with pytest.raises(
            contract.PipelineConfigError, match="cannot tell which record"
        ):
            _gate(checks)

    def test_record_rules_cannot_run_before_extract(self) -> None:
        checks = quality.CheckSet([REALISTIC_AGE])

        with pytest.raises(
            contract.PipelineConfigError, match="no record exists yet"
        ):
            _gate(checks, contract.Checkpoint.BEFORE_EXTRACT)

    def test_rule_names_must_be_unique(self) -> None:
        checks = {
            AFTER_EXTRACT: quality.CheckSet([REALISTIC_AGE]),
            contract.Checkpoint.AFTER_TRANSFORM: quality.CheckSet(
                [REALISTIC_AGE]
            ),
        }

        with pytest.raises(contract.PipelineConfigError, match="Duplicate"):
            quality.QualityGate(checks)

    def test_overrides_must_target_known_rules(self) -> None:
        checks = quality.CheckSet([HAS_EMAIL])

        with pytest.raises(
            contract.PipelineConfigError, match="unknown quality rule"
        ):
            _gate(checks, overrides={"typo": contract.QualityAction.WARN})

    def test_max_reject_rate_must_be_a_ratio(self) -> None:
        with pytest.raises(ValueError):
            quality.CheckSet[fakes.RawUser](max_reject_rate=1.5)


class TestIssueSink:

    def test_sink_gets_every_issue_while_the_report_stays_bounded(self) -> None:
        sink = quality.InMemoryIssueSink()
        gate = _gate(
            quality.CheckSet([HAS_EMAIL, REALISTIC_AGE]),
            issue_sink=sink,
            max_samples_per_rule=1,
        )

        ctx = fakes.make_context()
        report = gate.inspect(
            AFTER_EXTRACT, [NO_EMAIL, NO_EMAIL, OLD, OLD, OLD], ctx
        ).report
        gate.flush(ctx)

        assert report is not None
        assert [i.rule for i in sink.issues] == ["has_email"] * 2 + [
            "realistic_age"
        ] * 3
        assert report.rules["realistic_age"].violations == 3
        assert [s.rule for s in report.samples] == [
            "has_email",
            "realistic_age",
        ]

    def test_sink_is_not_called_for_a_clean_batch(self) -> None:
        sink = quality.InMemoryIssueSink()
        gate = _gate(quality.CheckSet([HAS_EMAIL]), issue_sink=sink)

        ctx = fakes.make_context()
        gate.inspect(AFTER_EXTRACT, [GOOD], ctx)
        gate.flush(ctx)

        assert sink.issues == []

    def test_gate_opens_and_closes_its_stores(
        self, tmp_path: pathlib.Path
    ) -> None:
        sink = quality.JsonLinesIssueSink(tmp_path)
        gate = _gate(
            quality.CheckSet([HAS_EMAIL], record_id=lambda u: u.id),
            issue_sink=sink,
        )
        ctx = fakes.make_context("users")

        gate.inspect(AFTER_EXTRACT, [NO_EMAIL], ctx)
        gate.inspect(AFTER_EXTRACT, [NO_EMAIL], ctx)
        gate.flush(ctx)

        lines = sink.path_for(ctx).read_text(encoding="utf-8").splitlines()
        assert (
            sink.path_for(ctx)
            == tmp_path / "users" / f"{ctx.run.run_id}.issues.jsonl"
        )
        assert len(lines) == 2
        first = json.loads(lines[0])
        assert first | {"written_at": None} == {
            "pipeline": "users",
            "run_id": ctx.run.run_id,
            "written_at": None,
            "rule": "has_email",
            "checkpoint": "after_extract",
            "action": "reject",
            "severity": "error",
            "message": "email is required",
            "field": "email",
            "value": "",
            "record_id": "2",
        }

    def test_jsonl_sink_writes_each_run_to_its_file_without_setup(
        self, tmp_path: pathlib.Path
    ) -> None:
        sink = quality.JsonLinesIssueSink(tmp_path)
        first, second = fakes.make_context(), fakes.make_context()
        issue = contract.QualityIssue(
            "has_email",
            AFTER_EXTRACT,
            contract.QualityAction.REJECT,
            "no email",
        )

        sink.write([issue], first)
        sink.write([issue, issue], second)

        assert len(sink.path_for(first).read_text().splitlines()) == 1
        assert len(sink.path_for(second).read_text().splitlines()) == 2

    def test_negative_sample_cap_is_rejected(self) -> None:
        checks = quality.CheckSet([HAS_EMAIL])

        with pytest.raises(ValueError):
            _gate(checks, max_samples_per_rule=-1)


OLD_USERS = [fakes.RawUser(str(i), "x@x.io", 150) for i in range(1, 11)]


def _sampled_ids(
    sampling: contract.SamplingStrategy, seed: int | None = None
) -> tuple[contract.SamplingStrategy, list[str | None]]:
    """Runs 2 batches of 5 warnings through a gate keeping 2 samples a rule."""
    gate = quality.QualityGate(
        {
            AFTER_EXTRACT: quality.CheckSet(
                [REALISTIC_AGE], record_id=lambda u: u.id
            )
        },
        max_samples_per_rule=2,
        sampling=sampling,
        sampling_seed=seed,
    )
    result = contract.ETLResult()
    ctx = fakes.make_context()

    for batch in (OLD_USERS[:5], OLD_USERS[5:]):
        report = gate.inspect(AFTER_EXTRACT, batch, ctx).report
        assert report is not None
        result.add_quality_report(report)

    merged = result.quality_reports[AFTER_EXTRACT]
    return merged.sampling, [issue.record_id for issue in merged.samples]


class TestSampling:

    def test_first_keeps_the_earliest_issues_across_batches(self) -> None:
        assert _sampled_ids(contract.SamplingStrategy.FIRST) == (
            contract.SamplingStrategy.FIRST,
            ["1", "2"],
        )

    def test_reservoir_is_reproducible_with_a_seed(self) -> None:
        strategy, first_run = _sampled_ids(
            contract.SamplingStrategy.RESERVOIR, seed=3
        )

        assert strategy is contract.SamplingStrategy.RESERVOIR
        assert len(first_run) == 2
        assert (
            _sampled_ids(contract.SamplingStrategy.RESERVOIR, seed=3)[1]
            == first_run
        )

    def test_reservoir_samples_from_every_batch(self) -> None:
        sampled = {
            record_id
            for seed in range(50)
            for record_id in _sampled_ids(
                contract.SamplingStrategy.RESERVOIR, seed
            )[1]
        }

        assert sampled == {issue.id for issue in OLD_USERS}


class TestJsonLinesQuarantine:

    def test_writes_rejected_records_with_their_issues(
        self, tmp_path: pathlib.Path
    ) -> None:
        quarantine = quality.JsonLinesQuarantine(tmp_path)
        gate = _gate(quality.CheckSet([HAS_EMAIL]), quarantine=quarantine)
        ctx = fakes.make_context("users")

        gate.inspect(AFTER_EXTRACT, [GOOD, NO_EMAIL], ctx)
        gate.flush(ctx)

        path = quarantine.path_for(ctx)
        assert path == tmp_path / "users" / f"{ctx.run.run_id}.quarantine.jsonl"
        (line,) = path.read_text(encoding="utf-8").splitlines()
        row = json.loads(line)
        assert (row["pipeline"], row["run_id"], row["checkpoint"]) == (
            "users",
            ctx.run.run_id,
            "after_extract",
        )
        assert row["record"] == {"id": "2", "email": "", "age": 30}
        assert [issue["rule"] for issue in row["issues"]] == ["has_email"]

    def test_custom_serializer_controls_what_is_written(
        self, tmp_path: pathlib.Path
    ) -> None:
        quarantine = quality.JsonLinesQuarantine(
            tmp_path, serialize=lambda record: {"id": getattr(record, "id")}
        )
        gate = _gate(quality.CheckSet([HAS_EMAIL]), quarantine=quarantine)
        ctx = fakes.make_context()

        gate.inspect(AFTER_EXTRACT, [NO_EMAIL], ctx)
        gate.flush(ctx)

        row = json.loads(quarantine.path_for(ctx).read_text(encoding="utf-8"))
        assert row["record"] == {"id": "2"}

    def test_put_needs_no_setup(self, tmp_path: pathlib.Path) -> None:
        quarantine = quality.JsonLinesQuarantine(tmp_path)
        gate = _gate(quality.CheckSet([HAS_EMAIL]), quarantine=quarantine)
        ctx = fakes.make_context()

        gate.inspect(AFTER_EXTRACT, [NO_EMAIL], ctx)
        gate.flush(ctx)

        assert len(list(quarantine.entries())) == 1
