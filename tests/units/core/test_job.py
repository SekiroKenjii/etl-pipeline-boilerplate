from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence

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


def _ids(quarantine: quality.InMemoryQuarantine) -> list[str]:
    """The ids of the users rejected into the quarantine."""
    users = [r.record for r in quarantine.records]
    return [user.id for user in users if isinstance(user, fakes.RawUser)]


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


class _FlakyAcknowledge(fakes.InMemoryExtractor):
    """Its first acknowledgement fails: the state store blipped."""

    def __init__(self, records: Sequence[fakes.RawUser]) -> None:
        super().__init__(records)
        self.attempts = 0

    def acknowledge(
        self, records: Sequence[fakes.RawUser], ctx: contract.ETLContext
    ) -> None:
        self.attempts += 1
        if self.attempts == 1:
            raise contract.RetryableError("state store unavailable")
        super().acknowledge(records, ctx)


class _Cancelling(fakes.InMemoryLoader):
    """Cancels the run while loading its first batch: SIGTERM."""

    def load(
        self, records: Sequence[fakes.User], ctx: contract.ETLContext
    ) -> contract.ETLStats:
        ctx.cancel()
        return super().load(records, ctx)


class _Counting(fakes.InMemoryExtractor):
    """Counts the records the source gave, and whether it was read."""

    def __init__(self, records: Sequence[fakes.RawUser]) -> None:
        super().__init__(records)
        self.given = 0
        self.extracted = False

    def extract(self, ctx: contract.ETLContext) -> Iterator[fakes.RawUser]:
        self.extracted = True
        for record in self.records:
            self.given += 1
            yield record


class TestCancelled:

    def test_a_cancelled_run_reads_no_source(self) -> None:
        extractor = _Counting(RAW)
        job = pipeline.ETLJob(
            extractor, fakes.UserTransformer(), fakes.InMemoryLoader()
        )
        ctx = fakes.make_context()
        ctx.cancel()

        result = job(ctx, {})

        assert result.status is contract.ETLStatus.CANCELLED
        assert not extractor.extracted

    def test_a_run_cancelled_mid_way_reads_no_next_batch(self) -> None:
        extractor = _Counting(RAW * 3)
        job = pipeline.ETLJob(
            extractor, fakes.UserTransformer(), _Cancelling(), batch_size=2
        )

        result = job(fakes.make_context(), {})

        assert result.status is contract.ETLStatus.CANCELLED
        assert extractor.given == 2
        assert extractor.acked == [["1", "2"]]


class TestRetries:

    def test_a_batch_whose_acknowledgement_failed_is_stored_once(
        self,
    ) -> None:
        quarantine = quality.InMemoryQuarantine()
        sink = quality.InMemoryIssueSink()
        gate = quality.QualityGate(
            {contract.Checkpoint.AFTER_EXTRACT: quality.CheckSet([HAS_EMAIL])},
            quarantine=quarantine,
            issue_sink=sink,
        )
        extractor = _FlakyAcknowledge(RAW)
        job = pipeline.ETLJob(
            extractor, fakes.UserTransformer(), fakes.InMemoryLoader(), gate,
            batch_size=2,
        )  # fmt: skip

        step = _retried(job)

        assert step.status is contract.ETLStatus.PARTIAL_SUCCESS
        assert extractor.acked == [["1", "2"], ["3"]]
        assert [issue.rule for issue in sink.issues] == ["has_email"]
        assert _ids(quarantine) == ["2"]

    def test_a_batch_read_differently_on_retry_is_stored_anew(self) -> None:
        # The retry's first batch is not the one stored: never skip it.
        sink = quality.InMemoryIssueSink()
        gate = quality.QualityGate(
            {contract.Checkpoint.AFTER_EXTRACT: quality.CheckSet([HAS_EMAIL])},
            quarantine=quality.InMemoryQuarantine(),
            issue_sink=sink,
        )
        extractor = _FlakyAcknowledge(RAW)
        job = pipeline.ETLJob(
            extractor, fakes.UserTransformer(), fakes.InMemoryLoader(), gate,
            batch_size=2,
        )  # fmt: skip

        def changed(
            ctx: contract.ETLContext,
            upstream: Mapping[str, contract.ETLResult],
        ) -> contract.ETLResult:
            if extractor.attempts:
                extractor.records = [fakes.RawUser("0", "", 1), *RAW]
            return job(ctx, upstream)

        class Steps(pipeline.BaseOrchestrator):

            def steps(self) -> Sequence[contract.PipelineStep]:
                return [
                    contract.PipelineStep(
                        "users",
                        changed,
                        retry=contract.RetryPolicy(max_attempts=2),
                    )
                ]

        Steps(sleep=lambda seconds: None).run(fakes.make_context())

        # User 2's twice: in the batch stored, and in the retry's second.
        assert len(sink.issues) == 3

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
        assert _ids(quarantine) == ["2"]
        assert [issue.rule for issue in sink.issues] == ["has_email"]
        assert step.stats.rejected == 1

    def test_a_retried_batch_is_parked_once(self) -> None:
        lot = quality.InMemoryParkingLot()
        parking = quality.ParkingQuarantine[fakes.RawUser](
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

        assert _ids(quarantine) == ["2"]


FOUR = [
    fakes.RawUser("1", "a@x.io", 30),
    fakes.RawUser("2", "b@x.io", 40),
    fakes.RawUser("3", "c@x.io", 50),
    fakes.RawUser("4", "", 60),
]


class _AckFails(fakes.InMemoryExtractor):
    """Acknowledging a batch of `failing` fails, as many times as given.

    A stateful source reads again from the batch after the last one
    acknowledged; a stateless one from the start.
    """

    def __init__(self, failing: dict[str, int], *, stateful: bool) -> None:
        super().__init__(FOUR)
        self.failing = failing
        self.stateful = stateful
        self.done = 0

    def extract(self, ctx: contract.ETLContext) -> Iterator[fakes.RawUser]:
        yield from self.records[self.done if self.stateful else 0 :]

    def acknowledge(
        self, records: Sequence[fakes.RawUser], ctx: contract.ETLContext
    ) -> None:
        first = records[0].id
        if self.failing.get(first, 0) > 0:
            self.failing[first] -= 1
            raise contract.RetryableError("state store unavailable")
        super().acknowledge(records, ctx)
        self.done = int(records[-1].id)


def _stored(extractor: _AckFails, attempts: int) -> quality.InMemoryIssueSink:
    """Runs a job over `FOUR` as a step tried up to `attempts` times, with
    batches of 2 (user 4 has no email); returns its issue sink."""
    sink = quality.InMemoryIssueSink()
    gate = quality.QualityGate(
        {contract.Checkpoint.AFTER_EXTRACT: quality.CheckSet([HAS_EMAIL])},
        quarantine=quality.InMemoryQuarantine(),
        issue_sink=sink,
    )
    job = pipeline.ETLJob(
        extractor, fakes.UserTransformer(), fakes.InMemoryLoader(), gate,
        batch_size=2,
    )  # fmt: skip

    class Steps(pipeline.BaseOrchestrator):

        def steps(self) -> Sequence[contract.PipelineStep]:
            return [
                contract.PipelineStep(
                    "users",
                    job,
                    retry=contract.RetryPolicy(max_attempts=attempts),
                )
            ]

    Steps(sleep=lambda seconds: None).run(fakes.make_context())
    return sink


class TestAcknowledgementFailed:
    """A batch whose acknowledgement failed is stored once, however the
    source reads again."""

    def test_a_stateless_source_reading_from_the_start(self) -> None:
        extractor = _AckFails({"3": 1}, stateful=False)

        sink = _stored(extractor, attempts=2)

        assert extractor.acked == [["1", "2"], ["1", "2"], ["3", "4"]]
        assert len(sink.issues) == 1

    def test_a_stateful_source_reading_from_the_failed_batch(self) -> None:
        extractor = _AckFails({"3": 1}, stateful=True)

        sink = _stored(extractor, attempts=2)

        assert extractor.acked == [["1", "2"], ["3", "4"]]
        assert len(sink.issues) == 1

    def test_an_acknowledgement_failing_every_attempt(self) -> None:
        extractor = _AckFails({"3": 3}, stateful=False)

        sink = _stored(extractor, attempts=3)

        assert extractor.acked == [["1", "2"]] * 3
        assert len(sink.issues) == 1

    def test_two_batches_failing_one_after_the_other(self) -> None:
        # The second try fails on the batch the first acknowledged.
        extractor = _AckFails({"3": 1}, stateful=False)
        acknowledge = extractor.acknowledge
        tries = [0]

        def flaky(
            records: Sequence[fakes.RawUser], ctx: contract.ETLContext
        ) -> None:
            if records[0].id == "1":
                tries[0] += 1
                if tries[0] == 2:
                    raise contract.RetryableError("state store unavailable")
            acknowledge(records, ctx)

        extractor.acknowledge = flaky  # type: ignore[method-assign]

        sink = _stored(extractor, attempts=3)

        assert extractor.acked == [["1", "2"], ["1", "2"], ["3", "4"]]
        assert len(sink.issues) == 1

    def test_a_batch_read_changed_is_stored_again(self) -> None:
        # User 4 changed between the tries: what the gate found may differ.
        extractor = _AckFails({"3": 1}, stateful=True)
        acknowledge = extractor.acknowledge

        def changing(
            records: Sequence[fakes.RawUser], ctx: contract.ETLContext
        ) -> None:
            try:
                acknowledge(records, ctx)
            except contract.RetryableError:
                extractor.records[3] = fakes.RawUser("4", "", 61)
                raise

        extractor.acknowledge = changing  # type: ignore[method-assign]

        sink = _stored(extractor, attempts=2)

        assert extractor.acked == [["1", "2"], ["3", "4"]]
        assert len(sink.issues) == 2
