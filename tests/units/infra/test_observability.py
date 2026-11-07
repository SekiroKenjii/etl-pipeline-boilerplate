from __future__ import annotations

from collections.abc import Generator, Sequence
import io
import json
import logging
import queue
from typing import Any

from opentelemetry.sdk import _logs as otel_logs
from opentelemetry.sdk._logs import export as otel_export
import pytest

from core import contract
from core import observability
from core import pipeline
from core.observability import listener as run_listener
from infra import observability as infra_observability
from infra.observability import otlp
from tests.factories import fakes

_LOG = logging.getLogger("tests.observability")


class Lines:

    def __init__(
        self, buffer: io.StringIO, configured: infra_observability.Logging
    ) -> None:
        self._buffer = buffer
        self._configured = configured

    def __call__(self) -> list[dict[str, Any]]:
        self._configured.stop()
        return [
            json.loads(line) for line in self._buffer.getvalue().splitlines()
        ]


@pytest.fixture
def lines() -> Generator[Lines]:
    """Logging configured as JSON into a buffer; yields a reader of lines."""
    buffer = io.StringIO()
    configured = infra_observability.configure_logging(
        service="etl-test", environment="test", format="json", stream=buffer
    )
    reader = Lines(buffer, configured)
    try:
        yield reader
    finally:
        configured.stop()


class TestContext:

    def test_fields_nest_and_are_restored(self) -> None:
        with observability.log_context(a=1):
            with observability.log_context(b=2, a=3):
                inner = dict(observability.current())
            outer = dict(observability.current())

        assert (inner, outer, dict(observability.current())) == (
            {"a": 3, "b": 2},
            {"a": 1},
            {},
        )

    def test_records_carry_their_context_into_json(self, lines: Lines) -> None:
        ctx = fakes.make_context("crm")
        with observability.log_context(**observability.run_fields(ctx)):
            _LOG.info("loaded %d rows", 3, extra={"etl.rows": 3})

        (record,) = [
            line for line in lines() if line["log.logger"] == _LOG.name
        ]
        assert record["message"] == "loaded 3 rows"
        assert record["etl.run_id"] == ctx.run.run_id
        assert record["etl.pipeline"] == "crm"
        assert record["etl.rows"] == 3
        assert (record["service.name"], record["log.level"]) == (
            "etl-test",
            "info",
        )

    def test_steps_in_parallel_threads_keep_the_run_context(
        self, lines: Lines
    ) -> None:
        class Two(pipeline.BaseOrchestrator):

            def steps(self) -> Sequence[contract.PipelineStep]:
                def step(
                    ctx: contract.ETLContext, upstream: Any
                ) -> contract.ETLResult:
                    _LOG.info("working")
                    return contract.ETLResult()

                return [
                    contract.PipelineStep("a", step),
                    contract.PipelineStep("b", step),
                ]

        ctx = fakes.make_context("crm")
        with observability.log_context(**observability.run_fields(ctx)):
            Two(max_workers=2).run(ctx)

        working = [line for line in lines() if line["message"] == "working"]
        assert sorted(line["etl.step"] for line in working) == ["a", "b"]
        assert {line["etl.run_id"] for line in working} == {ctx.run.run_id}


class TestRedaction:

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            (
                "postgresql+psycopg://etl:s3cret@db/crm",
                "postgresql+psycopg://etl:***@db/crm",
            ),
            (
                "Server=db;User Id=etl;Password=s3cret;Encrypt=yes",
                "Server=db;User Id=etl;Password=***;Encrypt=yes",
            ),
            ('{"token": "abc123"}', '{"token": "***"}'),
            ("nothing secret here", "nothing secret here"),
        ],
    )
    def test_text(self, text: str, expected: str) -> None:
        assert (
            observability.redact_text(text, ("password", "token")) == expected
        )

    def test_records_are_redacted_before_any_sink(self, lines: Lines) -> None:
        _LOG.warning(
            "connecting to %s",
            "mssql://sa:Pw0rd!@db/x",
            extra={
                "etl.password": "Pw0rd!",
                "etl.config": {"api_key": "k", "n": 1},
            },
        )

        (record,) = [
            line for line in lines() if line["log.logger"] == _LOG.name
        ]
        assert "Pw0rd!" not in json.dumps(record)
        assert record["etl.password"] == "***"
        assert record["etl.config"] == {"api_key": "***", "n": 1}


class TestFormats:

    def test_errors_have_their_type_message_and_stack(
        self, lines: Lines
    ) -> None:
        try:
            raise contract.RetryableError("db down")
        except contract.RetryableError:
            _LOG.exception("step failed")

        (record,) = [
            line for line in lines() if line["log.logger"] == _LOG.name
        ]
        assert record["error.type"] == "RetryableError"
        assert record["error.message"] == "db down"
        assert "Traceback" in record["error.stack_trace"]
        assert record["@timestamp"].endswith("Z")

    def test_console_lines_are_for_humans(self) -> None:
        buffer = io.StringIO()
        configured = infra_observability.configure_logging(
            service="s", environment="dev", stream=buffer
        )
        with observability.log_context(
            **{"etl.pipeline": "crm", "etl.step": "customer"}
        ):
            _LOG.info("loaded", extra={"etl.rows": 2})
        configured.stop()

        line = buffer.getvalue().strip().splitlines()[-1]
        assert "INFO    [crm/customer] loaded  etl.rows=2" in line

    def test_unknown_formats_are_refused(self) -> None:
        with pytest.raises(ValueError, match="xml"):
            infra_observability.configure_logging(
                service="s", environment="e", format="xml"
            )


def test_a_full_queue_drops_records_and_says_so() -> None:
    records: queue.Queue[logging.LogRecord] = queue.Queue(maxsize=2)
    handler = infra_observability.BoundedQueueHandler(records)

    for i in range(4):
        handler.emit(logging.makeLogRecord({"msg": f"m{i}"}))
    drained = [records.get_nowait().getMessage() for _ in range(2)]
    handler.emit(logging.makeLogRecord({"msg": "after"}))

    assert drained == ["m0", "m1"]
    assert records.get_nowait().getMessage() == "after"
    assert "2 log record(s) dropped" in records.get_nowait().getMessage()


def test_runs_are_logged_by_the_listener(lines: Lines) -> None:
    class Failing(pipeline.BaseOrchestrator):

        def steps(self) -> Sequence[contract.PipelineStep]:
            def boom(
                ctx: contract.ETLContext, upstream: Any
            ) -> contract.ETLResult:
                raise contract.FatalError("bad config")

            def ok(
                ctx: contract.ETLContext, upstream: Any
            ) -> contract.ETLResult:
                result = contract.ETLResult()
                result.stats.total = result.stats.successful = 4
                return result

            return [
                contract.PipelineStep("ok", ok),
                contract.PipelineStep("boom", boom),
            ]

    Failing(listeners=[run_listener.LoggingListener()]).run(
        fakes.make_context("crm")
    )

    events = {
        (line["etl.event"], line.get("etl.step")): line
        for line in lines()
        if "etl.event" in line
    }
    assert events[("step.end", "ok")]["etl.stats.successful"] == 4
    assert events[("step.error", "boom")]["log.level"] == "warning"
    assert events[("step.end", "boom")]["log.level"] == "error"
    assert events[("pipeline.end", None)]["etl.status"] == "failed"


def test_only_the_last_failed_attempt_carries_the_traceback(
    lines: Lines,
) -> None:
    class Flaky(pipeline.BaseOrchestrator):

        def steps(self) -> Sequence[contract.PipelineStep]:
            def down(
                ctx: contract.ETLContext, upstream: Any
            ) -> contract.ETLResult:
                raise contract.RetryableError("db down")

            return [
                contract.PipelineStep(
                    "flaky", down, retry=contract.RetryPolicy(max_attempts=3)
                )
            ]

    Flaky(listeners=[run_listener.LoggingListener()], sleep=lambda _: None).run(
        fakes.make_context("crm")
    )

    errors = [line for line in lines() if line.get("etl.event") == "step.error"]
    assert [
        (
            line["etl.attempt"],
            line["etl.will_retry"],
            "error.stack_trace" in line,
        )
        for line in errors
    ] == [(1, True, False), (2, True, False), (3, False, True)]
    assert errors[0]["message"] == (
        "step flaky failed (attempt 1, will retry): db down"
    )


def test_otlp_attributes_are_flat_and_primitive() -> None:
    # The OpenTelemetry SDK leaves this constructor untyped.
    exporter: Any = otel_export.InMemoryLogRecordExporter()  # type: ignore[no-untyped-call]
    provider = otel_logs.LoggerProvider()
    provider.add_log_record_processor(
        otel_export.SimpleLogRecordProcessor(exporter)
    )
    handler = otlp.OtlpHandler(provider)
    record = logging.makeLogRecord({
        "name": "x",
        "levelno": logging.INFO,
        "levelname": "INFO",
        "msg": "hello",
        observability.FIELDS_ATTRIBUTE: {
            "etl.run_id": "r1",
            "etl.dry_run": False,
        },
        "etl.stats": {"a": 1},
        "etl.keys": ["k1", "k2"],
    })

    handler.emit(record)
    handler.close()

    (exported,) = exporter.get_finished_logs()
    attributes: dict[str, Any] = dict(exported.log_record.attributes or {})
    assert attributes["etl.run_id"] == "r1"
    assert attributes["etl.dry_run"] is False
    assert attributes["etl.stats"] == "{'a': 1}"
    assert list(attributes["etl.keys"]) == ["k1", "k2"]
    assert exported.log_record.body == "hello"
