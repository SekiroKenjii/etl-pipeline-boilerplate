"""Observability: the context logs carry, secrets kept out of them, the
ports of traces and metrics.

`core.observability.tracing` and `core.observability.metrics` record nothing
until the composition root installs OpenTelemetry. The listeners of pipeline
runs depend on the engine: import them as `core.observability.listener`.
"""

from core.observability.context import ContextFilter
from core.observability.context import current
from core.observability.context import FIELDS_ATTRIBUTE
from core.observability.context import log_context
from core.observability.context import run_fields
from core.observability.redaction import ERROR_ATTRIBUTE
from core.observability.redaction import is_secret_name
from core.observability.redaction import logged_error
from core.observability.redaction import LoggedError
from core.observability.redaction import MASK
from core.observability.redaction import redact_text
from core.observability.redaction import redact_value
from core.observability.redaction import RedactingFilter
from core.observability.redaction import SECRET_NAMES

__all__ = [
    "ERROR_ATTRIBUTE",
    "FIELDS_ATTRIBUTE",
    "MASK",
    "SECRET_NAMES",
    "ContextFilter",
    "LoggedError",
    "RedactingFilter",
    "current",
    "is_secret_name",
    "log_context",
    "logged_error",
    "redact_text",
    "redact_value",
    "run_fields",
]
