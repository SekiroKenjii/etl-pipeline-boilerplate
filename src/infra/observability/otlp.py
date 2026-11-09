"""Logs exported by OpenTelemetry (OTLP): to a collector, or a backend.

One protocol reaches them all: an OpenTelemetry
Collector routes logs to Elasticsearch, Grafana Loki, Seq, Datadog...
(`deploy/otel-collector/`), and Seq, Elastic and Grafana Cloud take OTLP
directly too.
"""

from __future__ import annotations

from collections.abc import Mapping
import logging

from opentelemetry import _logs as otel_api_logs
from opentelemetry import context as otel_context
from opentelemetry import trace as otel_trace
from opentelemetry.exporter.otlp.proto.http import _log_exporter
from opentelemetry.sdk import _logs as otel_logs
from opentelemetry.sdk import resources
from opentelemetry.sdk._logs import export as otel_export
from opentelemetry.util import types as otel_types

from core import observability
from infra.observability import formatter

_LOGS_PATH = "/v1/logs"


class OtlpHandler(logging.Handler):
    """Turns log records into OpenTelemetry log records.

    OpenTelemetry attributes are strings, numbers, booleans or sequences of
    them: the context and extra fields become flat attributes, other values
    text. Exceptions become the `exception.*` attributes, from their
    redacted text (`observability.logged_error`): never the exception
    itself, whose traceback the SDK would render raw. The current
    trace context, if any, links the log to its span.

    Attributes:
        provider: The logger provider, which batches and exports.
    """

    def __init__(self, provider: otel_logs.LoggerProvider) -> None:
        super().__init__()
        self.provider = provider

    def emit(self, record: logging.LogRecord) -> None:
        try:
            attributes: dict[str, otel_types.AnyValue] = {
                key: _attribute(value)
                for key, value in formatter.fields(record)
            }
            span = _span_context(
                attributes.pop("trace.id", None),
                attributes.pop("span.id", None),
            )
            attributes["code.file.path"] = record.pathname
            attributes["code.line.number"] = record.lineno
            attributes["code.function.name"] = record.funcName
            attributes["thread.name"] = record.threadName or ""
            error = observability.logged_error(record)
            if error is not None:
                attributes["exception.type"] = error.qualified_type
                attributes["exception.message"] = error.message
                attributes["exception.stacktrace"] = error.stack_trace
            self.provider.get_logger(record.name).emit(
                timestamp=int(record.created * 1e9),
                severity_number=_severity(record.levelno),
                severity_text=record.levelname,
                body=record.getMessage(),
                attributes=attributes,
                context=span,
            )
        # A failing sink must never fail the pipeline: report and go on.
        except Exception:  # pylint: disable=broad-exception-caught
            self.handleError(record)

    def flush(self) -> None:
        self.provider.force_flush()

    def close(self) -> None:
        self.provider.shutdown()
        super().close()


def _severity(level: int) -> otel_api_logs.SeverityNumber:
    """The OpenTelemetry severity of a `logging` level."""
    if level >= logging.CRITICAL:
        return otel_api_logs.SeverityNumber.FATAL
    if level >= logging.ERROR:
        return otel_api_logs.SeverityNumber.ERROR
    if level >= logging.WARNING:
        return otel_api_logs.SeverityNumber.WARN
    if level >= logging.INFO:
        return otel_api_logs.SeverityNumber.INFO
    return otel_api_logs.SeverityNumber.DEBUG


def otlp_handler(
    endpoint: str,
    *,
    service: str,
    environment: str,
    headers: Mapping[str, str] | None = None,
    timeout_seconds: float = 10.0,
) -> OtlpHandler:
    """Returns a handler exporting logs by OTLP over HTTP, in batches.

    Args:
        endpoint: The OTLP endpoint, e.g. `http://otel-collector:4318`;
            `/v1/logs` is added if missing.
        service: `service.name` of the resource.
        environment: `deployment.environment` of the resource.
        headers: Sent with every export, e.g. an API key.
        timeout_seconds: How long an export may take.
    """
    url = endpoint.rstrip("/")
    if not url.endswith(_LOGS_PATH):
        url += _LOGS_PATH
    provider = otel_logs.LoggerProvider(
        resource=resources.Resource.create(
            {"service.name": service, "deployment.environment": environment}
        )
    )
    provider.add_log_record_processor(
        otel_export.BatchLogRecordProcessor(
            _log_exporter.OTLPLogExporter(
                endpoint=url,
                headers=dict(headers or {}),
                timeout=timeout_seconds,
            )
        )
    )
    return OtlpHandler(provider)


def _attribute(value: object) -> otel_types.AnyValue:
    if value is None or isinstance(value, (str, bool, int, float)):
        return value if value is not None else ""
    if isinstance(value, (list, tuple)) and all(
        isinstance(item, (str, bool, int, float)) for item in value
    ):
        return list(value)
    return str(value)


def _span_context(
    trace_id: object, span_id: object
) -> otel_context.Context | None:
    """A context holding the span of these ids; None without ids."""
    if not isinstance(trace_id, str) or not isinstance(span_id, str):
        return None
    try:
        found = otel_trace.SpanContext(
            trace_id=int(trace_id, 16),
            span_id=int(span_id, 16),
            is_remote=False,
            trace_flags=otel_trace.TraceFlags(otel_trace.TraceFlags.SAMPLED),
        )
    except ValueError:
        return None
    return otel_trace.set_span_in_context(otel_trace.NonRecordingSpan(found))
