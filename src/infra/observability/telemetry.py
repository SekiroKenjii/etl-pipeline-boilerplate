"""Traces and metrics exported by OpenTelemetry (OTLP), behind the core's
ports (`core.observability.tracing`, `core.observability.metrics`).

Needs the `otel` extra. The collector routes them with the logs: traces to
Tempo, Elastic APM or Seq; metrics to Prometheus, Mimir or Elasticsearch
(`deploy/otel-collector/`).

    telemetry = configure_telemetry(service="etl", environment="prod",
                                    endpoint="http://otel-collector:4318")
    ...
    telemetry.stop()  # Flushes what is left.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import threading
from typing import Any

from opentelemetry import context as otel_context
from opentelemetry import trace as otel_trace
from opentelemetry.exporter.otlp.proto.http import metric_exporter
from opentelemetry.exporter.otlp.proto.http import trace_exporter
from opentelemetry.sdk import metrics as otel_metrics
from opentelemetry.sdk import resources
from opentelemetry.sdk import trace as otel_sdk_trace
from opentelemetry.sdk.metrics import export as metrics_export
from opentelemetry.sdk.trace import export as trace_export

from core.observability import metrics
from core.observability import tracing


class OtelSpan(tracing.Span):
    """An OpenTelemetry span."""

    def __init__(self, span: otel_trace.Span) -> None:
        self.span = span
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
        self, attributes: Mapping[str, tracing.Attribute]
    ) -> None:
        self.span.set_attributes(dict(attributes))

    def fail(self, error: BaseException | str) -> None:
        if isinstance(error, BaseException):
            self.span.record_exception(error)
            description = str(error) or type(error).__name__
        else:
            description = error
        self.span.set_status(
            otel_trace.Status(otel_trace.StatusCode.ERROR, description)
        )

    def end(self) -> None:
        self.span.end()


class OtelTracer(tracing.ITracer):
    """Starts OpenTelemetry spans.

    Attributes:
        provider: The provider, which batches and exports.
    """

    def __init__(self, provider: otel_sdk_trace.TracerProvider) -> None:
        self.provider = provider
        self._tracer = provider.get_tracer("etl")

    def start_span(
        self,
        name: str,
        attributes: Mapping[str, tracing.Attribute],
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
            )
        )


class OtelMeter(metrics.IMeter):
    """Records into OpenTelemetry instruments, created on first use.

    Attributes:
        provider: The provider, which aggregates and exports.
    """

    def __init__(self, provider: otel_metrics.MeterProvider) -> None:
        self.provider = provider
        self._meter = provider.get_meter("etl")
        self._instruments: dict[tuple[str, str], Any] = {}
        self._lock = threading.Lock()

    def add(
        self,
        name: str,
        value: float,
        attributes: metrics.Attributes | None = None,
        *,
        unit: str = "1",
        description: str = "",
    ) -> None:
        self._instrument("counter", name, unit, description).add(
            value, dict(attributes or {})
        )

    def record(
        self,
        name: str,
        value: float,
        attributes: metrics.Attributes | None = None,
        *,
        unit: str = "1",
        description: str = "",
    ) -> None:
        self._instrument("histogram", name, unit, description).record(
            value, dict(attributes or {})
        )

    def gauge(
        self,
        name: str,
        value: float,
        attributes: metrics.Attributes | None = None,
        *,
        unit: str = "1",
        description: str = "",
    ) -> None:
        self._instrument("gauge", name, unit, description).set(
            value, dict(attributes or {})
        )

    def _instrument(
        self, kind: str, name: str, unit: str, description: str
    ) -> Any:
        with self._lock:
            found = self._instruments.get((kind, name))
            if found is None:
                create = getattr(self._meter, f"create_{kind}")
                found = create(name, unit=unit, description=description)
                self._instruments[(kind, name)] = found
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
        tracer = OtelTracer(provider)
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
