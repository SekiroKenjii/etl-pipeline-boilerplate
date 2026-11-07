"""Log formats: JSON lines for log shippers, and a console format."""

from __future__ import annotations

from collections.abc import Iterator
import datetime
import json
import logging
from typing import Any

from core import observability
from shared import utils

_STANDARD_RECORD_ATTRIBUTES = frozenset(vars(logging.makeLogRecord({}))) | {
    "message",
    "asctime",
    observability.FIELDS_ATTRIBUTE,
}


def fields(record: logging.LogRecord) -> Iterator[tuple[str, Any]]:
    """Yields the context fields of a record, then its extra fields."""
    context = getattr(record, observability.FIELDS_ATTRIBUTE, None) or {}
    yield from context.items()
    for key, value in vars(record).items():
        if key not in _STANDARD_RECORD_ATTRIBUTES and not key.startswith("_"):
            yield key, value


class JsonFormatter(logging.Formatter):
    """One JSON object per line, with Elastic Common Schema field names.

    `@timestamp`, `log.level`, `log.logger`, `message`, `service.name`,
    `error.*` (redacted, see `observability.logged_error`)... then the
    context and extra fields (`etl.run_id`,
    `etl.step`...), flat and dotted: Elasticsearch, Loki, Seq and the
    OpenTelemetry collector's filelog receiver all read them as they are.

    Attributes:
        service: The name of the service.
        environment: The environment: dev, prod...
    """

    def __init__(self, service: str, environment: str) -> None:
        super().__init__()
        self.service = service
        self.environment = environment

    def format(self, record: logging.LogRecord) -> str:
        document: dict[str, Any] = {
            "@timestamp": (
                datetime.datetime.fromtimestamp(record.created, datetime.UTC)
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z")
            ),
            "log.level": record.levelname.lower(),
            "log.logger": record.name,
            "message": record.getMessage(),
            "service.name": self.service,
            "service.environment": self.environment,
            "process.pid": record.process,
            "process.thread.name": record.threadName,
        }
        document.update(fields(record))
        error = observability.logged_error(record)
        if error is not None:
            document["error.type"] = error.type
            document["error.message"] = error.message
            document["error.stack_trace"] = error.stack_trace
        return json.dumps(
            document, ensure_ascii=False, default=utils.to_jsonable
        )


class ConsoleFormatter(logging.Formatter):
    """A line for humans: time, level, where, message, then the fields."""

    def format(self, record: logging.LogRecord) -> str:
        time = datetime.datetime.fromtimestamp(record.created).strftime(
            "%H:%M:%S"
        )
        context = getattr(record, observability.FIELDS_ATTRIBUTE, None) or {}
        where = "/".join(
            str(context[key])
            for key in ("etl.pipeline", "etl.step")
            if key in context
        )
        # Zeros, falses and blanks are noise to a human reader.
        extras = " ".join(
            f"{key}={value}"
            for key, value in fields(record)
            if key
            not in ("etl.pipeline", "etl.step", "etl.run_id", "etl.error")
            and value not in (None, 0, False, "")
        )
        line = (
            f"{time} {record.levelname:<7} "
            + (f"[{where}] " if where else "")
            + record.getMessage()
            + (f"  {extras}" if extras else "")
        )
        error = observability.logged_error(record)
        if error is not None:
            line += "\n" + error.stack_trace.rstrip("\n")
        return line
