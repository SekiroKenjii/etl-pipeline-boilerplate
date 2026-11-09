"""Setting up logging for the whole process, once, at start-up."""

from __future__ import annotations

from collections.abc import Collection, Mapping
import dataclasses
import logging
from logging import handlers
import queue
import sys
from typing import TextIO

from core import observability
from infra.observability import formatter
from infra.observability import handlers as bounded
from infra.observability import otlp as otlp_sink


@dataclasses.dataclass(frozen=True, slots=True)
class Otlp:
    """Where logs are exported by OpenTelemetry.

    Attributes:
        endpoint: The OTLP endpoint.
        headers: Sent with every export.
        timeout_seconds: How long an export may take.
    """

    endpoint: str
    headers: Mapping[str, str] = dataclasses.field(default_factory=dict)
    timeout_seconds: float = 10.0


class Logging:
    """The logging of the process: stop it to flush every sink.

    Attributes:
        handler: The queue every record goes through.
    """

    def __init__(
        self,
        handler: bounded.BoundedQueueHandler,
        listener: handlers.QueueListener,
        previous: list[logging.Handler],
    ) -> None:
        self.handler = handler
        self._listener = listener
        self._previous = previous

    def stop(self) -> None:
        """Flushes and closes the sinks, and restores the previous handlers."""
        root = logging.getLogger()
        root.removeHandler(self.handler)
        logging.captureWarnings(False)
        self._listener.stop()
        for sink in self._listener.handlers:
            sink.close()
        for previous in self._previous:
            root.addHandler(previous)


def _sinks(
    *,
    service: str,
    environment: str,
    format: str,  # pylint: disable=redefined-builtin
    otlp: Otlp | None,
    stream: TextIO | None,
) -> list[logging.Handler]:
    """The handlers the log lines go to: the console, and OTLP if set.

    Raises:
        ValueError: If the format is unknown.
    """
    if format not in ("json", "console"):
        raise ValueError(f"Unknown log format {format!r}")
    line = (
        formatter.JsonFormatter(service, environment)
        if format == "json"
        else formatter.ConsoleFormatter()
    )
    console = logging.StreamHandler(
        stream if stream is not None else sys.stdout
    )
    console.setFormatter(line)
    sinks: list[logging.Handler] = [console]
    if otlp is not None:
        sinks.append(
            otlp_sink.otlp_handler(
                otlp.endpoint,
                service=service,
                environment=environment,
                headers=otlp.headers,
                timeout_seconds=otlp.timeout_seconds,
            )
        )
    return sinks


def configure_logging(
    *,
    service: str,
    environment: str,
    level: str = "INFO",
    levels: Mapping[str, str] | None = None,
    format: str = "console",  # pylint: disable=redefined-builtin
    otlp: Otlp | None = None,
    redact: Collection[str] = (),
    queue_size: int = 10_000,
    stream: TextIO | None = None,
) -> Logging:
    """Routes every log of the process through a queue, to the sinks.

    The pipelines' threads only enqueue: a thread formats and writes, so
    that a slow sink never slows them down. Records carry their context
    (`etl.run_id`, `etl.step`...) and are redacted before they leave the
    pipeline's thread.

    Args:
        service: The name of the service, in every record.
        environment: The environment, in every record.
        level: The level of the root logger.
        levels: Levels by logger name.
        format: `json` (for log shippers) or `console` (for humans).
        otlp: Also export to OpenTelemetry, when set.
        redact: More names of secret fields, on top of the ones always
            hidden (`observability.SECRET_NAMES`).
        queue_size: The records buffered before some are dropped.
        stream: Where the lines go; stdout by default.

    Returns:
        The logging, to stop at exit.

    Raises:
        ValueError: If the format is unknown.
    """
    sinks = _sinks(
        service=service,
        environment=environment,
        format=format,
        otlp=otlp,
        stream=stream,
    )
    records: queue.Queue[logging.LogRecord] = queue.Queue(maxsize=queue_size)
    handler = bounded.BoundedQueueHandler(records)
    handler.addFilter(observability.ContextFilter())
    handler.addFilter(observability.RedactingFilter(redact))
    listener = handlers.QueueListener(
        records, *sinks, respect_handler_level=True
    )

    root = logging.getLogger()
    previous = list(root.handlers)
    for old in previous:
        root.removeHandler(old)
    root.addHandler(handler)
    root.setLevel(level.upper())
    for name, name_level in (levels or {}).items():
        logging.getLogger(name).setLevel(name_level.upper())
    logging.captureWarnings(True)
    listener.start()
    return Logging(handler, listener, previous)
