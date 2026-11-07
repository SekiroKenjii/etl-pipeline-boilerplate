from __future__ import annotations

from collections.abc import Sequence

import pytest

from core import contract
from core import pipeline
from core import quality
from tests.factories import fakes

RAW = [
    fakes.RawUser("1", "A@X.IO", 30),
    fakes.RawUser("2", "", 40),
    fakes.RawUser("3", "c@x.io", 12),
]

HAS_EMAIL = quality.RecordCheck[fakes.RawUser](
    "has_email",
    lambda u: bool(u.email),
    on_violation=contract.QualityAction.REJECT,
)
IS_ADULT = quality.RecordCheck[fakes.User](
    "is_adult", lambda u: u.is_adult, on_violation=contract.QualityAction.REJECT
)


def _job(
    loader: fakes.InMemoryLoader,
    gate: quality.QualityGate | None = None,
    batch_size: int = 2,
) -> pipeline.ETLJob[fakes.RawUser, fakes.User]:
    return pipeline.ETLJob(
        fakes.InMemoryExtractor(RAW),
        fakes.UserTransformer(),
        loader,
        gate,
        batch_size=batch_size,
    )


def _extractor(
    job: pipeline.ETLJob[fakes.RawUser, fakes.User],
) -> fakes.InMemoryExtractor:
    assert isinstance(job.extractor, fakes.InMemoryExtractor)
    return job.extractor


def test_runs_without_any_quality_gate() -> None:
    loader = fakes.InMemoryLoader()

    result = _job(loader)(fakes.make_context(), {})

    assert sorted(loader.rows) == ["1", "2", "3"]
    assert result.quality_reports == {}
    assert result.status is contract.ETLStatus.SUCCESS


def test_gate_audits_each_phase_boundary_and_quarantines_rejects() -> None:
    quarantine = quality.InMemoryQuarantine()
    gate = quality.QualityGate(
        {
            contract.Checkpoint.AFTER_EXTRACT: quality.CheckSet([HAS_EMAIL]),
            contract.Checkpoint.AFTER_TRANSFORM: quality.CheckSet([IS_ADULT]),
        },
        quarantine=quarantine,
    )
    loader = fakes.InMemoryLoader()

    result = _job(loader, gate)(fakes.make_context(), {})

    assert list(loader.rows) == ["1"]
    assert loader.rows["1"].email == "a@x.io"
    assert result.stats.total == 3
    assert result.stats.created == result.stats.successful == 1
    assert result.stats.rejected == 2
    assert [(r.checkpoint, r.issues[0].rule) for r in quarantine.records] == [
        (contract.Checkpoint.AFTER_EXTRACT, "has_email"),
        (contract.Checkpoint.AFTER_TRANSFORM, "is_adult"),
    ]
    # 2 batches, but one merged report per checkpoint
    assert list(result.quality_reports) == [
        contract.Checkpoint.AFTER_EXTRACT,
        contract.Checkpoint.AFTER_TRANSFORM,
    ]
    after_extract = result.quality_reports[contract.Checkpoint.AFTER_EXTRACT]
    assert (after_extract.checked, after_extract.rejected) == (3, 1)
    assert result.status is contract.ETLStatus.PARTIAL_SUCCESS


def test_streams_the_source_in_batches() -> None:
    loader = fakes.InMemoryLoader()

    _job(loader, batch_size=2)(fakes.make_context(), {})

    assert loader.batches == [2, 1]


def test_dry_run_does_not_load() -> None:
    loader = fakes.InMemoryLoader()

    result = _job(loader)(fakes.make_context(dry_run=True), {})

    assert loader.rows == {}
    assert result.stats.skipped == 3
    assert result.status is contract.ETLStatus.SUCCESS


def test_failed_gate_stops_the_job_and_keeps_the_report() -> None:
    gate = quality.QualityGate({
        contract.Checkpoint.AFTER_EXTRACT: quality.CheckSet([
            quality.RecordCheck[fakes.RawUser](
                "has_email", lambda u: bool(u.email)
            )
        ])  # default FAIL
    })
    loader = fakes.InMemoryLoader()
    job = _job(loader, gate)

    result = job(fakes.make_context(), {})

    assert result.status is contract.ETLStatus.FAILED
    assert loader.rows == {}
    assert (
        result.quality_reports[contract.Checkpoint.AFTER_EXTRACT].failed is True
    )
    assert (
        result.errors[0].phase is contract.ETLPhase.EXTRACT
    )  # the audited phase
    assert "has_email" in (result.message or "")
    assert _extractor(job).events == ["setup", "teardown"]
    assert loader.events == ["setup", "teardown"]


class TestAcknowledge:

    def test_each_batch_is_acknowledged_after_it_is_loaded(self) -> None:
        job = _job(fakes.InMemoryLoader(), batch_size=2)

        job(fakes.make_context(), {})

        assert _extractor(job).acked == [["1", "2"], ["3"]]

    def test_a_fully_rejected_batch_is_still_acknowledged(self) -> None:
        reject_all = quality.RecordCheck[fakes.RawUser](
            "never", lambda u: False, on_violation=contract.QualityAction.REJECT
        )
        gate = quality.QualityGate(
            {contract.Checkpoint.AFTER_EXTRACT: quality.CheckSet([reject_all])},
            quarantine=quality.InMemoryQuarantine(),
        )
        loader = fakes.InMemoryLoader()
        job = _job(loader, gate, batch_size=3)

        job(fakes.make_context(), {})

        assert loader.batches == []
        assert _extractor(job).acked == [["1", "2", "3"]]

    def test_dry_run_is_never_acknowledged(self) -> None:
        job = _job(fakes.InMemoryLoader())

        job(fakes.make_context(dry_run=True), {})

        assert _extractor(job).acked == []

    def test_a_failed_batch_is_not_acknowledged(self) -> None:
        gate = quality.QualityGate({
            contract.Checkpoint.AFTER_EXTRACT: quality.CheckSet([
                quality.RecordCheck[fakes.RawUser](
                    "has_email", lambda u: bool(u.email)
                )
            ])
        })
        job = _job(fakes.InMemoryLoader(), gate, batch_size=1)

        result = job(fakes.make_context(), {})

        assert result.status is contract.ETLStatus.FAILED
        assert _extractor(job).acked == [["1"]]

    def test_a_loader_error_is_not_acknowledged(self) -> None:
        class BrokenLoader(fakes.InMemoryLoader):

            def load(
                self, records: Sequence[fakes.User], ctx: contract.ETLContext
            ) -> contract.ETLStats:
                raise contract.RetryableError("database unavailable")

        job = _job(BrokenLoader())
        ctx = fakes.make_context()

        with pytest.raises(contract.RetryableError):
            job(ctx, {})

        assert _extractor(job).acked == []


class _FlakyLoader(fakes.InMemoryLoader):
    """Fails its first batch, as a database blip would."""

    def __init__(self) -> None:
        super().__init__()
        self.calls = 0

    def load(
        self, records: Sequence[fakes.User], ctx: contract.ETLContext
    ) -> contract.ETLStats:
        self.calls += 1
        if self.calls == 1:
            raise contract.RetryableError("database unavailable")
        return super().load(records, ctx)


def _retried(
    job: pipeline.ETLJob[fakes.RawUser, fakes.User],
) -> contract.ETLResult:
    """Runs the job as a step retried once."""

    class Steps(pipeline.BaseOrchestrator):

        def steps(self) -> Sequence[contract.PipelineStep]:
            return [
                contract.PipelineStep(
                    "users", job, retry=contract.RetryPolicy(max_attempts=2)
                )
            ]

    result = Steps(sleep=lambda seconds: None).run(fakes.make_context())
    return result.step_results["users"]


class TestRetries:

    def test_a_retried_batch_is_quarantined_and_reported_once(self) -> None:
        quarantine = quality.InMemoryQuarantine()
        sink = quality.InMemoryIssueSink()
        gate = quality.QualityGate(
            {contract.Checkpoint.AFTER_EXTRACT: quality.CheckSet([HAS_EMAIL])},
            quarantine=quarantine,
            issue_sink=sink,
        )
        loader = _FlakyLoader()

        step = _retried(_job(loader, gate, batch_size=3))

        assert loader.calls == 2
        assert [r.record.id for r in quarantine.records] == ["2"]
        assert [issue.rule for issue in sink.issues] == ["has_email"]
        assert step.stats.rejected == 1

    def test_a_retried_batch_is_parked_once(self) -> None:
        lot = quality.InMemoryParkingLot()
        parking = quality.ParkingQuarantine(
            lot,
            "users",
            rules={"has_email"},
            encode=lambda user: {"id": user.id},
            identity=lambda user: user.id,
            fallback=quality.InMemoryQuarantine(),
        )
        gate = quality.QualityGate(
            {contract.Checkpoint.AFTER_EXTRACT: quality.CheckSet([HAS_EMAIL])},
            quarantine=parking,
        )

        _retried(_job(_FlakyLoader(), gate, batch_size=3))

        assert [(e.identity, e.attempts) for e in lot.entries("users")] == [
            ("2", 1)
        ]

    def test_a_failed_checkpoint_keeps_its_issues(self) -> None:
        sink = quality.InMemoryIssueSink()
        gate = quality.QualityGate(
            {
                contract.Checkpoint.AFTER_EXTRACT: quality.CheckSet([
                    quality.RecordCheck[fakes.RawUser](
                        "has_email", lambda u: bool(u.email)
                    )
                ])
            },
            issue_sink=sink,
        )

        result = _job(fakes.InMemoryLoader(), gate)(fakes.make_context(), {})

        assert result.status is contract.ETLStatus.FAILED
        assert [issue.rule for issue in sink.issues] == ["has_email"]

    def test_a_dry_run_still_quarantines(self) -> None:
        quarantine = quality.InMemoryQuarantine()
        gate = quality.QualityGate(
            {contract.Checkpoint.AFTER_EXTRACT: quality.CheckSet([HAS_EMAIL])},
            quarantine=quarantine,
        )

        _job(fakes.InMemoryLoader(), gate)(fakes.make_context(dry_run=True), {})

        assert [r.record.id for r in quarantine.records] == ["2"]
