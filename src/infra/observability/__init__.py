"""Shipping logs: JSON on stdout for log agents, OpenTelemetry for the rest.

`configure_logging` once at start-up; `stop()` the result at exit. The OTLP
sink needs the `otel` extra, imported only when configured.
"""

from infra.observability.formatter import ConsoleFormatter
from infra.observability.formatter import JsonFormatter
from infra.observability.handlers import BoundedQueueHandler
from infra.observability.setup import configure_logging
from infra.observability.setup import Logging
from infra.observability.setup import Otlp

__all__ = [
    "BoundedQueueHandler",
    "ConsoleFormatter",
    "JsonFormatter",
    "Logging",
    "Otlp",
    "configure_logging",
]
