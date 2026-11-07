"""The context logs carry: the run, the step, the batch.

    with observability.log_context(**{"etl.step": "customer"}):
        log.info("loaded")   # carries etl.step, and the run's fields

Fields live in a `contextvars.ContextVar`: each thread and each asyncio task
has its own. Code starting threads or loops copies the context
(`contextvars.copy_context().run`), as the orchestrator does for its steps.
`ContextFilter` captures the fields on each record where it is logged:
handlers working in another thread (a queue) still see them.
"""

from __future__ import annotations

from collections.abc import Generator, Mapping
import contextlib
import contextvars
import logging
import types

from core import contract

FIELDS_ATTRIBUTE = "etl_context"

# Indexed by every backend (logs, spans, metrics): renaming breaks dashboards.
PIPELINE = "etl.pipeline"
RUN_ID = "etl.run_id"
STEP = "etl.step"
ATTEMPT = "etl.attempt"
STATUS = "etl.status"
EVENT = "etl.event"

_EMPTY: Mapping[str, object] = types.MappingProxyType({})
_FIELDS: contextvars.ContextVar[Mapping[str, object]] = contextvars.ContextVar(
    "etl_log_fields", default=_EMPTY
)


def current() -> Mapping[str, object]:
    """Returns the fields of the current context."""
    return _FIELDS.get()


@contextlib.contextmanager
def log_context(**fields: object) -> Generator[None]:
    """Adds fields to every log of the block, over the current ones."""
    token = _FIELDS.set(types.MappingProxyType({**_FIELDS.get(), **fields}))
    try:
        yield
    finally:
        _FIELDS.reset(token)


def run_fields(ctx: contract.ETLContext) -> dict[str, object]:
    """Returns the fields identifying a run."""
    return {
        PIPELINE: ctx.run.pipeline_name,
        RUN_ID: ctx.run.run_id,
        "etl.dry_run": ctx.run.dry_run,
    }


class ContextFilter(logging.Filter):
    """Copies the context's fields onto each record, where it is logged."""

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, FIELDS_ATTRIBUTE):
            setattr(record, FIELDS_ATTRIBUTE, dict(_FIELDS.get()))
        return True
