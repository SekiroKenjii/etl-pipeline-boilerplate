"""Traces and metrics of runs, through OpenTelemetry, in memory."""

from __future__ import annotations

from collections.abc import Generator, Sequence
import json
import logging
import pathlib
import re
from typing import Any

from opentelemetry import trace as otel_trace
from opentelemetry.sdk import _logs as otel_logs
from opentelemetry.sdk._logs import export as otel_export
from opentelemetry.sdk.metrics import export as metrics_export
from opentelemetry.sdk.trace.export import in_memory_span_exporter
import pytest

from core import contract
from core import observability
from core import pipeline
from core.observability import listener as run_listener
from core.observability import metrics
from core.observability import tracing
from infra.observability import otlp
from infra.observability import telemetry
from tests.factories import fakes

ROOT = pathlib.Path(__file__).resolve().parents[3]


class _Pipeline(pipeline.BaseOrchestrator):

    def __init__(self, fail: bool = False) -> None:
        super().__init__(listeners=[run_listener.MetricsListener()])
        self._fail = fail

    def steps(self) -> Sequence[contract.PipelineStep]:
        def broken(ctx: contract.ETLContext, upstream: Any) -> Any:
            raise contract.FatalError("source gone")

        load = pipeline.ETLJob(
            fakes.InMemoryExtractor(
                [fakes.RawUser(str(i), f"u{i}@x", 30) for i in range(5)]
            ),
            fakes.UserTransformer(),
            fakes.InMemoryLoader(),
            batch_size=2,
        )
        steps = [contract.PipelineStep("load", load)]
        if self._fail:
            steps.append(contract.PipelineStep("broken", broken))
        return steps


@pytest.fixture
def otel() -> Generator[
    tuple[
        in_memory_span_exporter.InMemorySpanExporter,
        metrics_export.InMemoryMetricReader,
    ]
]:
    spans = in_memory_span_exporter.InMemorySpanExporter()
    reader = metrics_export.InMemoryMetricReader()
    installed = telemetry.configure_telemetry(
        service="etl",
        environment="test",
        span_exporter=spans,
        metric_readers=[reader],
    )
    yield spans, reader
    installed.stop()


def _points(reader: metrics_export.InMemoryMetricReader) -> dict[str, Any]:
    data = reader.get_metrics_data()
    found: dict[str, Any] = {}
    assert data is not None
    for resource in data.resource_metrics:
        for scope in resource.scope_metrics:
            for metric in scope.metrics:
                found[metric.name] = list(metric.data.data_points)
    return found


def test_runs_are_traced_pipeline_step_batch(otel: Any) -> None:
    spans, _ = otel
    trace_ids: list[str] = []

    class _Capture(logging.Handler):

        def emit(self, record: logging.LogRecord) -> None:
            fields = getattr(record, observability.FIELDS_ATTRIBUTE, {})
            trace_ids.append(fields.get("trace.id", ""))

    handler = _Capture()
    handler.addFilter(observability.ContextFilter())
    logger = logging.getLogger("tests.telemetry")
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    try:
        result = _Pipeline(fail=True).run(fakes.make_context("crm.load"))
        with tracing.span("outside"):
            logger.info("in a span")
    finally:
        logger.removeHandler(handler)
    installed = tracing.tracer()
    assert isinstance(installed, telemetry.OtelTracer)
    installed.provider.force_flush()  # Spans are exported in batches.
    telemetry_spans = spans.get_finished_spans()
    by_name: dict[str, list[Any]] = {}
    for span in telemetry_spans:
        by_name.setdefault(span.name, []).append(span)

    assert result.status is contract.ETLStatus.FAILED
    (run,) = by_name["etl.pipeline"]
    steps = {s.attributes["etl.step"]: s for s in by_name["etl.step"]}
    assert run.attributes["etl.pipeline"] == "crm.load"
    assert run.status.status_code is otel_trace.StatusCode.ERROR
    assert all(s.parent.span_id == run.context.span_id for s in steps.values())
    assert steps["load"].attributes["etl.status"] == "success"
    assert steps["load"].attributes["etl.stats.successful"] == 5
    assert steps["broken"].status.status_code is otel_trace.StatusCode.ERROR
    assert steps["broken"].events[0].name == "exception"
    batches = by_name["etl.batch"]
    assert [b.attributes["etl.rows"] for b in batches] == [2, 2, 1]
    assert all(
        b.parent.span_id == steps["load"].context.span_id for b in batches
    )
    # Logs carry the span they were logged in.
    assert trace_ids == [
        otel_trace.format_trace_id(by_name["outside"][0].context.trace_id)
    ]


def test_runs_are_measured(otel: Any) -> None:
    _, reader = otel

    _Pipeline(fail=True).run(fakes.make_context("crm.load"))
    metrics.meter().gauge("etl.changes.lag", 12.5, {"etl.table": "t"}, unit="s")
    points = _points(reader)

    (loaded,) = points["etl.rows.loaded"]
    assert loaded.value == 5
    assert dict(loaded.attributes) == {
        "etl.pipeline": "crm.load",
        "etl.step": "load",
    }
    (run,) = points["etl.runs"]
    assert run.attributes["etl.status"] == "failed"
    assert {p.attributes["etl.step"] for p in points["etl.step.duration"]} == {
        "load",
        "broken",
    }
    (error,) = points["etl.step.errors"]
    assert error.attributes["etl.retryable"] is False
    assert points["etl.changes.lag"][0].value == 12.5


def test_without_telemetry_nothing_is_recorded() -> None:
    assert isinstance(tracing.tracer(), tracing.NoopTracer)
    assert isinstance(metrics.meter(), metrics.NoopMeter)
    with tracing.span("x") as span:
        assert span.trace_id == ""
        assert observability.current() == {}


def test_the_memory_meter() -> None:
    meter = metrics.InMemoryMeter()
    previous = metrics.meter()
    metrics.set_meter(meter)
    try:
        _Pipeline().run(fakes.make_context("crm.load"))
    finally:
        metrics.set_meter(previous)

    assert meter.total("etl.rows.loaded", **{"etl.step": "load"}) == 5
    assert meter.total("etl.runs", **{"etl.status": "success"}) == 1


def test_otlp_logs_are_linked_to_their_span() -> None:
    exporter: Any = otel_export.InMemoryLogRecordExporter()  # type: ignore[no-untyped-call]
    provider = otel_logs.LoggerProvider()
    provider.add_log_record_processor(
        otel_export.SimpleLogRecordProcessor(exporter)
    )
    handler = otlp.OtlpHandler(provider)
    trace_id, span_id = "0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331"
    handler.emit(
        logging.makeLogRecord({
            "name": "x",
            "levelno": logging.INFO,
            "levelname": "INFO",
            "msg": "hello",
            observability.FIELDS_ATTRIBUTE: {
                "trace.id": trace_id,
                "span.id": span_id,
            },
        })
    )
    handler.close()

    (exported,) = exporter.get_finished_logs()
    assert exported.log_record.trace_id == int(trace_id, 16)
    assert exported.log_record.span_id == int(span_id, 16)
    assert "trace.id" not in (exported.log_record.attributes or {})


# What the application emits, as Prometheus names it.
_COUNTERS = {
    *run_listener._ROWS.values(),  # pylint: disable=protected-access
    "etl.runs",
    "etl.step.errors",
    "etl.lock.lost",
}
_SECONDS = {
    "etl.step.duration",
    "etl.pipeline.duration",
    "etl.run.last_completed",
    "etl.changes.lag",
}
_PROMETHEUS = (
    {name.replace(".", "_") + "_total" for name in _COUNTERS}
    | {name.replace(".", "_") + "_seconds" for name in _SECONDS}
    | {"etl_messages_lag"}
)


@pytest.mark.parametrize(
    "path", ["deploy/grafana/etl-dashboard.json", "deploy/grafana/alerts.yaml"]
)
def test_dashboards_and_alerts_use_metrics_that_exist(path: str) -> None:
    text = (ROOT / path).read_text(encoding="utf-8")
    if path.endswith(".json"):
        json.loads(text)
    used = {
        re.sub(r"_bucket$", "", name)
        for name in re.findall(r"\b(etl_[a-z_]+)\b", text)
        if not name.startswith(("etl_pipeline", "etl_step", "etl_status"))
        and name not in {"etl_table", "etl_topic", "etl_group", "etl_retryable"}
    }

    assert used
    assert used <= _PROMETHEUS, used - _PROMETHEUS
