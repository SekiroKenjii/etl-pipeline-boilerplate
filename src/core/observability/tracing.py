"""Traces of runs: a span per pipeline, step attempt and batch.

    with tracing.span("etl.batch", {"etl.rows": 1000}):
        load(batch)

The composition root installs a tracer exporting to OpenTelemetry, or none:
spans are then free. The current span is a `contextvars.ContextVar`: it
follows steps into the threads they are copied to. Its ids go into the log
context (`trace.id`, `span.id`).
"""

from __future__ import annotations

import abc
from collections.abc import Generator, Mapping
import contextlib
import contextvars

from core import contract
from core.observability import context as log_context


class Span(abc.ABC):
    """An operation of a trace, open until `end`."""

    @property
    @abc.abstractmethod
    def trace_id(self) -> str:
        """The trace, 32 hex digits; empty when not recorded."""

    @property
    @abc.abstractmethod
    def span_id(self) -> str:
        """The span, 16 hex digits; empty when not recorded."""

    @abc.abstractmethod
    def set_attributes(
        self, attributes: Mapping[str, contract.Attribute]
    ) -> None:
        """Adds attributes, e.g. the stats known at the end."""

    @abc.abstractmethod
    def fail(self, error: BaseException | str) -> None:
        """Marks the span failed, with its error."""

    @abc.abstractmethod
    def end(self) -> None:
        """Closes the span."""


class ITracer(abc.ABC):
    """Starts spans."""

    @abc.abstractmethod
    def start_span(
        self,
        name: str,
        attributes: Mapping[str, contract.Attribute],
        parent: Span | None,
    ) -> Span:
        """Starts a span, child of `parent` (a new trace when None)."""


class _NoSpan(Span):
    """The span of no trace."""

    @property
    def trace_id(self) -> str:
        return ""

    @property
    def span_id(self) -> str:
        return ""

    def set_attributes(
        self, attributes: Mapping[str, contract.Attribute]
    ) -> None:
        del attributes  # Unused.

    def fail(self, error: BaseException | str) -> None:
        del error  # Unused.

    def end(self) -> None:
        """Closes the span: nothing to close."""


class NoopTracer(ITracer):
    """Records nothing: the default."""

    def start_span(
        self,
        name: str,
        attributes: Mapping[str, contract.Attribute],
        parent: Span | None,
    ) -> Span:
        del name, attributes, parent  # Unused.
        return _NO_SPAN


_NO_SPAN = _NoSpan()
_tracer: ITracer = NoopTracer()
_CURRENT: contextvars.ContextVar[Span | None] = contextvars.ContextVar(
    "etl_span", default=None
)


def set_tracer(installed: ITracer | None) -> None:
    """Installs the process's tracer; None for none."""
    global _tracer  # pylint: disable=global-statement
    _tracer = installed if installed is not None else NoopTracer()


def tracer() -> ITracer:
    """The process's tracer."""
    return _tracer


def current() -> Span | None:
    """The current span, if any."""
    return _CURRENT.get()


@contextlib.contextmanager
def span(
    name: str,
    attributes: Mapping[str, contract.Attribute] | None = None,
) -> Generator[Span]:
    """Runs the block in a span, child of the current one.

    An exception leaving the block marks the span failed, and goes on.
    """
    opened = _tracer.start_span(name, attributes or {}, _CURRENT.get())
    token = _CURRENT.set(opened)
    fields = (
        {"trace.id": opened.trace_id, "span.id": opened.span_id}
        if opened.trace_id
        else {}
    )
    try:
        with log_context.log_context(**fields):
            yield opened
    except BaseException as exc:
        opened.fail(exc)
        raise
    finally:
        _CURRENT.reset(token)
        opened.end()
