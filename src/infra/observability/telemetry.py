"""Traces and metrics exported by OpenTelemetry (OTLP), behind the core's
ports (`core.observability.tracing`, `core.observability.metrics`).

The collector routes them with the logs: traces to
Tempo, Elastic APM or Seq; metrics to Prometheus, Mimir or Elasticsearch
(`deploy/otel-collector/`).

    telemetry = configure_telemetry(service="etl", environment="prod",
                                    endpoint="http://otel-collector:4318")
    ...
    telemetry.stop()  # Flushes what is left.
"""

from __future__ import annotations

from collections.abc import Callable, Collection, Mapping, Sequence
import threading

from opentelemetry import context as otel_context
from opentelemetry import trace as otel_trace
from opentelemetry.exporter.otlp.proto.http import metric_exporter
from opentelemetry.exporter.otlp.proto.http import trace_exporter
from opentelemetry.sdk import metrics as otel_metrics
from opentelemetry.sdk import resources
from opentelemetry.sdk import trace as otel_sdk_trace
from opentelemetry.sdk.metrics import export as metrics_export
from opentelemetry.sdk.trace import export as trace_export

from core import contract
from core import observability
from core.observability import metrics
from core.observability import tracing


class OtelSpan(tracing.Span):
    """An OpenTelemetry span, whose errors are exported redacted.

    Attributes:
        span: The span.
        redact: More secret names, on top of `observability.SECRET_NAMES`.
    """

    def __init__(
        self, span: otel_trace.Span, redact: Collection[str] = ()
    ) -> None:
        self.span = span
        self.redact = tuple(redact)
        found = span.get_span_context()
        self._trace_id = otel_trace.format_trace_id(found.trace_id)
        self._span_id = otel_trace.format_span_id(found.span_id)

    @property
    def trace_id(self) -> str:
        return self._trace_id

    @property
    def span_id(self) -> str:
        return self._span_id

    def set_attributes(
        self, attributes: Mapping[str, contract.Attribute]
    ) -> None:
        self.span.set_attributes(dict(attributes))

    def fail(self, error: BaseException | str) -> None:
        # Never `span.record_exception`: it exports the raw text and stack,
        # which quote rows and passwords.
        if isinstance(error, BaseException):
            described = observability.describe_error(error, self.redact)
            self.span.add_event(
                "exception",
                {
                    "exception.type": described.qualified_type,
                    "exception.message": described.message,
                    "exception.stacktrace": described.stack_trace,
                },
            )
            description = described.message or described.type
        else:
            description = observability.redact_text(error, self.redact)
        self.span.set_status(
            otel_trace.Status(otel_trace.StatusCode.ERROR, description)
        )

    def end(self) -> None:
        self.span.end()


class OtelTracer(tracing.ITracer):
    """Starts OpenTelemetry spans.

    Attributes:
        provider: The provider, which batches and exports.
        redact: More secret names, on top of `observability.SECRET_NAMES`.
    """

    def __init__(
        self,
        provider: otel_sdk_trace.TracerProvider,
        redact: Collection[str] = (),
    ) -> None:
        self.provider = provider
        self.redact = tuple(redact)
        self._tracer = provider.get_tracer("etl")

    def start_span(
        self,
        name: str,
        attributes: Mapping[str, contract.Attribute],
        parent: tracing.Span | None,
    ) -> tracing.Span:
        # A root span: not a child of whatever span the thread holds.
        context = (
            otel_trace.set_span_in_context(parent.span)
            if isinstance(parent, OtelSpan)
            else otel_context.Context()
        )
        return OtelSpan(
            self._tracer.start_span(
                name, context=context, attributes=dict(attributes)
            ),
            self.redact,
        )


type _Record = Callable[[float, contract.Attributes], None]


class OtelMeter(metrics.IMeter):
    """Records into OpenTelemetry instruments, created on first use.

    Attributes:
        provider: The provider, which aggregates and exports.
    """

    def __init__(self, provider: otel_metrics.MeterProvider) -> None:
        self.provider = provider
        self._meter = provider.get_meter("etl")
        self._records: dict[tuple[str, str], _Record] = {}
        self._lock = threading.Lock()

    def add(
        self,
        name: str,
        value: float,
        attributes: contract.Attributes | None = None,
        *,
        unit: str = "1",
        description: str = "",
    ) -> None:
        record = self._record(
            "counter",
            name,
            lambda: self._meter.create_counter(name, unit, description).add,
        )
        record(value, dict(attributes or {}))

    def record(
        self,
        name: str,
        value: float,
        attributes: contract.Attributes | None = None,
        *,
        unit: str = "1",
        description: str = "",
    ) -> None:
        record = self._record(
            "histogram",
            name,
            lambda: self._meter.create_histogram(
                name, unit, description
            ).record,
        )
        record(value, dict(attributes or {}))

    def gauge(
        self,
        name: str,
        value: float,
        attributes: contract.Attributes | None = None,
        *,
        unit: str = "1",
        description: str = "",
    ) -> None:
        record = self._record(
            "gauge",
            name,
            lambda: self._meter.create_gauge(name, unit, description).set,
        )
        record(value, dict(attributes or {}))

    def _record(
        self, kind: str, name: str, create: Callable[[], _Record]
    ) -> _Record:
        with self._lock:
            found = self._records.get((kind, name))
            if found is None:
                found = self._records[(kind, name)] = create()
            return found


class Telemetry:
    """The tracer and meter of the process, installed in the core's ports.

    Attributes:
        tracer: The tracer, if traces are exported.
        meter: The meter, if metrics are exported.
    """

    def __init__(
        self, tracer: OtelTracer | None, meter: OtelMeter | None
    ) -> None:
        self.tracer = tracer
        self.meter = meter

    def stop(self) -> None:
        """Flushes and shuts the exports down, and uninstalls them."""
        tracing.set_tracer(None)
        metrics.set_meter(None)
        if self.tracer is not None:
            self.tracer.provider.shutdown()
        if self.meter is not None:
            self.meter.provider.shutdown()


def configure_telemetry(
    *,
    service: str,
    environment: str,
    endpoint: str | None = None,
    headers: Mapping[str, str] | None = None,
    timeout_seconds: float = 10.0,
    signals: Sequence[str] = ("traces", "metrics"),
    export_seconds: float = 15.0,
    span_exporter: trace_export.SpanExporter | None = None,
    metric_readers: Sequence[metrics_export.MetricReader] = (),
    redact: Collection[str] = (),
) -> Telemetry:
    """Exports traces and metrics, and installs them in the core's ports.

    Args:
        service: `service.name` of the resource.
        environment: `deployment.environment` of the resource.
        endpoint: The OTLP endpoint, e.g. `http://otel-collector:4318`.
        headers: Sent with every export.
        timeout_seconds: How long an export may take.
        signals: `traces`, `metrics`, or both.
        export_seconds: How often metrics are exported; the last ones at
            `stop`.
        span_exporter: Exports spans instead of OTLP (tests).
        metric_readers: Read metrics instead of OTLP (tests).
        redact: More secret names hidden in the errors of spans, on top of
            `observability.SECRET_NAMES`: the logs' `redact`.
    """
    resource = resources.Resource.create(
        {"service.name": service, "deployment.environment": environment}
    )
    base = (endpoint or "").rstrip("/")
    tracer = meter = None
    if "traces" in signals:
        provider = otel_sdk_trace.TracerProvider(resource=resource)
        provider.add_span_processor(
            trace_export.BatchSpanProcessor(
                span_exporter
                or trace_exporter.OTLPSpanExporter(
                    endpoint=f"{base}/v1/traces",
                    headers=dict(headers or {}),
                    timeout=timeout_seconds,
                )
            )
        )
        tracer = OtelTracer(provider, redact)
        tracing.set_tracer(tracer)
    if "metrics" in signals:
        readers = list(metric_readers) or [
            metrics_export.PeriodicExportingMetricReader(
                metric_exporter.OTLPMetricExporter(
                    endpoint=f"{base}/v1/metrics",
                    headers=dict(headers or {}),
                    timeout=timeout_seconds,
                ),
                export_interval_millis=export_seconds * 1000,
            )
        ]
        meter = OtelMeter(
            otel_metrics.MeterProvider(
                resource=resource, metric_readers=readers
            )
        )
        metrics.set_meter(meter)
    return Telemetry(tracer, meter)
