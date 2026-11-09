"""Failed spans carry the redacted error, as the logs do, never the raw one."""

from __future__ import annotations

from collections.abc import Callable, Generator, Sequence
import json

from opentelemetry import trace as otel_trace
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import in_memory_span_exporter
import pytest
from sqlalchemy import exc as sa_exc
import sqlalchemy as sa

from core import contract
from core import pipeline
from core.observability import tracing
from infra.observability import telemetry
from tests.factories import fakes

_EMAIL = "jane.doe@bank.example"
_SSN = "123-45-6789"
_PASSWORD = "S3cr3tPw"
_IBAN = "DE89370400440532013000"
_LEAKS = (_EMAIL, _SSN, _PASSWORD, _IBAN)


def _insert_duplicates() -> None:
    """Raises SQLAlchemy's IntegrityError, the rows in its parameters."""
    engine = sa.create_engine("sqlite://", poolclass=sa.pool.StaticPool)
    rows = [
        {"id": 1, "email": _EMAIL, "ssn": _SSN},
        {"id": 1, "email": _EMAIL, "ssn": _SSN},
    ]
    try:
        with engine.begin() as connection:
            connection.execute(
                sa.text("CREATE TABLE c (id INTEGER PRIMARY KEY, email, ssn)")
            )
            connection.execute(
                sa.text("INSERT INTO c VALUES (:id, :email, :ssn)"), rows
            )
    finally:
        engine.dispose()


def _duplicate_key() -> None:
    """Raises what Postgres says of a duplicate: the key's value quoted."""
    detail = f"DETAIL:  Key (email)=({_EMAIL}) already exists."
    raise RuntimeError(
        f'duplicate key value violates unique constraint "u"\n{detail}'
    )


def _connect() -> None:
    """Raises an error quoting a URL, chained to one quoting a password."""
    url = f"postgresql://etl:{_PASSWORD}@db/crm"
    try:
        raise ConnectionError(f"Password={_PASSWORD}; refused")
    except ConnectionError as exc:
        raise RuntimeError(f"cannot connect to {url}") from exc


def _noted() -> None:
    """Raises an error whose note quotes a URL."""
    error = RuntimeError("cannot connect")
    error.add_note(f"to postgresql://etl:{_PASSWORD}@db/crm")
    raise error


def _configured_name() -> None:
    """Raises an error quoting a field the settings name secret."""
    raise ValueError(f"bad row: iban={_IBAN}")


_FAILURES = pytest.mark.parametrize(
    "fail",
    [_insert_duplicates, _duplicate_key, _connect, _noted, _configured_name],
    ids=["dbapi", "detail-key", "url", "notes", "configured-name"],
)
_RAISED: dict[Callable[[], None], type[Exception]] = {
    _insert_duplicates: sa_exc.IntegrityError,
    _duplicate_key: RuntimeError,
    _connect: RuntimeError,
    _noted: RuntimeError,
    _configured_name: ValueError,
}


class _Unprintable(Exception):

    def __str__(self) -> str:
        raise RuntimeError(f"cannot print {_EMAIL}")


@pytest.fixture
def spans() -> Generator[in_memory_span_exporter.InMemorySpanExporter]:
    exporter = in_memory_span_exporter.InMemorySpanExporter()
    installed = telemetry.configure_telemetry(
        service="etl",
        environment="test",
        signals=("traces",),
        span_exporter=exporter,
        redact=("iban",),
    )
    yield exporter
    installed.stop()


def _finished(
    exporter: in_memory_span_exporter.InMemorySpanExporter,
) -> Sequence[ReadableSpan]:
    installed = tracing.tracer()
    assert isinstance(installed, telemetry.OtelTracer)
    installed.provider.force_flush()
    return exporter.get_finished_spans()


def _exported(span: ReadableSpan) -> str:
    """Everything a span sends to the backend, as text."""
    return json.dumps({
        "attributes": dict(span.attributes or {}),
        "status": span.status.description,
        "events": [
            {"name": event.name, "attributes": dict(event.attributes or {})}
            for event in span.events
        ],
    })


def _leaks(span: ReadableSpan) -> list[str]:
    exported = _exported(span)
    return [leak for leak in _LEAKS if leak in exported]


@_FAILURES
def test_a_failed_span_exports_the_redacted_error(
    spans: in_memory_span_exporter.InMemorySpanExporter,
    fail: Callable[[], None],
) -> None:
    with pytest.raises(_RAISED[fail]), tracing.span("etl.step"):
        fail()

    (span,) = _finished(spans)
    (event,) = span.events
    assert span.status.status_code is otel_trace.StatusCode.ERROR
    assert event.name == "exception"
    assert "Traceback" in str((event.attributes or {})["exception.stacktrace"])
    assert str((event.attributes or {})["exception.type"]).endswith("Error")
    assert not _leaks(span)


@_FAILURES
def test_every_span_an_error_crosses_is_redacted(
    spans: in_memory_span_exporter.InMemorySpanExporter,
    fail: Callable[[], None],
) -> None:
    with (
        pytest.raises(_RAISED[fail]),
        tracing.span("etl.step"),
        tracing.span("etl.batch"),
    ):
        fail()

    finished = _finished(spans)
    assert [span.name for span in finished] == ["etl.batch", "etl.step"]
    assert all(span.events for span in finished)
    assert not [leak for span in finished for leak in _leaks(span)]


@_FAILURES
def test_a_failed_run_is_redacted_in_its_step_and_pipeline_spans(
    spans: in_memory_span_exporter.InMemorySpanExporter,
    fail: Callable[[], None],
) -> None:
    def broken(
        ctx: contract.ETLContext, upstream: object
    ) -> contract.ETLResult:
        del ctx, upstream  # Unused.
        fail()
        return contract.ETLResult()

    class _Broken(pipeline.BaseOrchestrator):

        def steps(self) -> Sequence[contract.PipelineStep]:
            return [contract.PipelineStep("broken", broken)]

    result = _Broken().run(fakes.make_context("crm.load"))

    finished = _finished(spans)
    assert result.status is contract.ETLStatus.FAILED
    assert {span.name for span in finished} == {"etl.step", "etl.pipeline"}
    assert all(
        span.status.status_code is otel_trace.StatusCode.ERROR
        for span in finished
    )
    assert not [leak for span in finished for leak in _leaks(span)]


def test_a_failure_given_as_text_is_redacted(
    spans: in_memory_span_exporter.InMemorySpanExporter,
) -> None:
    with tracing.span("etl.step") as span:
        span.fail(f"cannot connect to postgresql://etl:{_PASSWORD}@db/crm")

    (finished,) = _finished(spans)
    assert finished.status.description == (
        "cannot connect to postgresql://etl:***@db/crm"
    )


def test_an_error_that_cannot_be_printed_still_fails_its_span(
    spans: in_memory_span_exporter.InMemorySpanExporter,
) -> None:
    with pytest.raises(_Unprintable), tracing.span("etl.step"):
        raise _Unprintable()

    (span,) = _finished(spans)
    (event,) = span.events
    assert span.status.status_code is otel_trace.StatusCode.ERROR
    assert str((event.attributes or {})["exception.type"]).endswith(
        "_Unprintable"
    )
    assert not _leaks(span)
