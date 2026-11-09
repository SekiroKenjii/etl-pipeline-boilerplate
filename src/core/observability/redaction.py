"""Keeping secrets, and the data of the rows, out of logs.

`redact_text` hides what text may hold: passwords of URLs, `name=value`
pairs of secret names (connection strings, headers, query strings, JSON),
bearer tokens, private keys, and the values database errors quote (the
parameters SQLAlchemy prints, the rows and keys Postgres, SQL Server and
MySQL quote). `RedactingFilter` applies it to every part of a log record:
message, extra fields, and the exception and its traceback, which it
renders once, redacted, for every formatter and handler. `describe_error`
renders an exception the same way for the other sinks (traces).

It is a safety net, not a guarantee: it hides what it recognises. Data
quoted in free text (`ValueError(f"bad email {email}")`) is not
recognised: do not put row values in exception messages.
"""

from __future__ import annotations

from collections.abc import Collection, Sequence
import dataclasses
import functools
import logging
import re
import traceback
import typing

MASK = "***"

# A key holding one is a secret (`ssl_key_password`); `_` and `-` match `_`,
# `-` or nothing: `api_key` matches `apikey` and `api-key` too.
SECRET_NAMES = (
    "password",
    "passwd",
    "pwd",
    "secret",
    "token",
    "api_key",
    "credential",
    "private_key",
    "authorization",
    "account_key",
    "signature",
)
_WHOLE_KEY_SECRET_NAMES = ("sig",)

# Holds the record's exception, already redacted (`logged_error`).
ERROR_ATTRIBUTE = "_etl_error"

# Every repetition below is bounded: an unbounded one makes a long log line
# cost quadratic time.
_URL_PASSWORD = re.compile(
    r"(?<![a-z0-9+.\-])"
    r"(?P<head>[a-z][a-z0-9+.\-]{0,31}://[^:/@\s]{0,256}:)[^@\s]{1,512}@",
    re.I,
)
_BEARER = re.compile(r"(?P<head>\bBearer\s+)[\w.~+/=\-]+", re.I)
_PRIVATE_KEY = re.compile(
    r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?"
    r"(?:-----END [A-Z ]*PRIVATE KEY-----|\Z)",
    re.S,
)
# A row is hidden up to the line's last `)`, or to the end of the line: a
# pattern that must find its `)` is quadratic on a line lacking one.
_DATA = (
    # SQLAlchemy's `[parameters: ...]`, unless `hide_parameters` is set.
    (re.compile(r"^(\[parameters: ).*$", re.M), rf"\g<1>{MASK}]"),
    # Postgres: `Failing row contains (...)`.
    (
        re.compile(
            r"(\bFailing row contains )\((?:.*\)(?P<end>[.;]?)$|.*)", re.M
        ),
        rf"\g<1>({MASK})\g<end>",
    ),
    # Postgres: `Key (email)=(jane@example.com) already exists`.
    (
        re.compile(
            r"(\bkey \([^)\n]{0,512}\)=)\(.{0,2048}?\)(?=[\s.,;]|$)", re.I
        ),
        rf"\g<1>({MASK})",
    ),
    # SQL Server: `The duplicate key value is (...)`.
    (
        re.compile(
            r"(\bThe duplicate key value is )\((?:.*\)(?P<end>[.;]?)$|.*)", re.M
        ),
        rf"\g<1>({MASK})\g<end>",
    ),
    # MySQL: `Duplicate entry '...' for key`.
    (
        re.compile(r"(\bDuplicate entry )'.{0,2048}?'(?= for key)"),
        rf"\g<1>'{MASK}'",
    ),
    # MySQL: `Incorrect integer value: '...' for column`.
    (
        re.compile(
            r"(\bIncorrect \w{1,32} value: )'(?:.{0,2048}?'(?= for"
            r" column)|.*$)",
            re.M,
        ),
        rf"\g<1>'{MASK}'",
    ),
    # SQL Server: `Truncated value: '...'`; its bulk copy:
    # `Cannot convert string '...'`.
    (
        re.compile(
            r"(\b(?:Truncated value:|Cannot convert string) )'.*$", re.M
        ),
        rf"\g<1>'{MASK}'",
    ),
    # Postgres: `invalid input syntax for type integer: "..."`, and the line
    # of a COPY: `COPY stage, line 1, column email: "..."`.
    (
        re.compile(
            r"(\b(?:invalid input syntax for type [\w ]{1,64}"
            r"|COPY [^\s,]{1,256}, line \d{1,12}(?:, column [^:\n]{1,256})?)"
            r': )".*$',
            re.M,
        ),
        rf'\g<1>"{MASK}"',
    ),
)


def redact_text(text: str, names: Collection[str] = ()) -> str:
    """Hides secrets and quoted row values in text.

    Hidden: the passwords of URLs; the values of keys holding a secret name
    (`password=...;`, `PWD={...}`, `"token": "..."`, `x-api-key: ...`,
    `Authorization: Bearer ...`, `?sig=...`), for `SECRET_NAMES` and
    `names`, case-insensitively; `Bearer` tokens; PEM private keys; the
    values database errors quote.

    Args:
        text: The text.
        names: More secret names, on top of `SECRET_NAMES`.

    Returns:
        The text, its secrets replaced by `***`.
    """
    text = _URL_PASSWORD.sub(lambda m: f"{m['head']}{MASK}@", text)
    text = _PRIVATE_KEY.sub(MASK, text)
    text = _pairs(_extra(names)).sub(_mask_pair, text)
    text = _BEARER.sub(lambda m: f"{m['head']}{MASK}", text)
    for pattern, replacement in _DATA:
        text = pattern.sub(replacement, text)
    return text


def is_secret_name(key: str, names: Collection[str] = ()) -> bool:
    """Whether a key names a secret: holds one of `SECRET_NAMES` or `names`."""
    return _keys(_extra(names)).search(key) is not None


def redact_value(value: object, names: Collection[str] = ()) -> object:
    """Hides the values of secret keys in nested data, and secrets in text."""
    if isinstance(value, str):
        return redact_text(value, names)
    if isinstance(value, dict):
        return {
            key: (
                MASK
                if is_secret_name(str(key), names)
                else redact_value(item, names)
            )
            for key, item in typing.cast(dict[object, object], value).items()
        }
    if isinstance(value, (list, tuple)):
        items = typing.cast(Sequence[object], value)
        return [redact_value(item, names) for item in items]
    return value


@dataclasses.dataclass(frozen=True, slots=True)
class LoggedError:
    """The exception of a log record, as the sinks may show it.

    Attributes:
        type: The class of the exception, e.g. `IntegrityError`.
        module: The module of its class, e.g. `sqlalchemy.exc`.
        message: What it says, redacted.
        stack_trace: Its traceback, chained exceptions included, redacted.
    """

    type: str
    module: str
    message: str
    stack_trace: str

    @property
    def qualified_type(self) -> str:
        """The class with its module, but for built-in exceptions."""
        if self.module in ("", "builtins"):
            return self.type
        return f"{self.module}.{self.type}"


def logged_error(record: logging.LogRecord) -> LoggedError | None:
    """The exception of a record, redacted; None without one.

    `RedactingFilter` stored it, redacted by the configured names; a record
    that did not go through the filter is redacted by `SECRET_NAMES` here.
    """
    stored = getattr(record, ERROR_ATTRIBUTE, None)
    if isinstance(stored, LoggedError):
        return stored
    if record.exc_info and record.exc_info[1] is not None:
        return describe_error(record.exc_info[1])
    return None


def describe_error(
    error: BaseException, names: Collection[str] = ()
) -> LoggedError:
    """An exception as the sinks may show it: its text and stack redacted.

    Every sink outside the process (logs, traces) shows this, never the
    exception itself. An exception whose `str()` raises is described by its
    type.

    Args:
        error: The exception.
        names: More secret names, on top of `SECRET_NAMES`.
    """
    return LoggedError(
        type=type(error).__qualname__,
        module=type(error).__module__,
        message=redact_text(_text_of(error), names),
        stack_trace=redact_text(
            "".join(traceback.format_exception(error)), names
        ),
    )


def _text_of(error: BaseException) -> str:
    try:
        return str(error)
    # What a broken `__str__` raises may quote the data: never shown.
    except Exception:  # pylint: disable=broad-exception-caught
        return f"<{type(error).__qualname__}: str() failed>"


class RedactingFilter(logging.Filter):
    """Hides secrets in every part of a record, before any handler sees it.

    The message, the arguments and the extra fields are redacted. The
    exception, if any, is rendered and redacted once: its text goes to the
    `ERROR_ATTRIBUTE` attribute (`logged_error`) and to `exc_text`, which
    `logging.Formatter` prints, and `exc_info` is cleared, so that no
    handler can print the raw traceback.

    Attributes:
        names: More secret names, on top of `SECRET_NAMES`.
    """

    _STANDARD_ATTRIBUTES = frozenset(vars(logging.makeLogRecord({}))) | {
        "message"
    }

    def __init__(self, names: Collection[str] = ()) -> None:
        super().__init__()
        self.names = tuple(names)

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = redact_text(record.getMessage(), self.names)
        record.args = None
        # Safe while iterating: only existing attributes are set.
        for key, value in vars(record).items():
            if key in self._STANDARD_ATTRIBUTES:
                continue
            if is_secret_name(key, self.names):
                setattr(record, key, MASK)
            else:
                setattr(record, key, redact_value(value, self.names))
        if record.exc_info and record.exc_info[1] is not None:
            error = describe_error(record.exc_info[1], self.names)
            setattr(record, ERROR_ATTRIBUTE, error)
            record.exc_text = error.stack_trace.rstrip("\n")
        elif record.exc_text:
            record.exc_text = redact_text(record.exc_text, self.names)
        record.exc_info = None
        if record.stack_info:
            record.stack_info = redact_text(record.stack_info, self.names)
        return True


def _extra(names: Collection[str]) -> tuple[str, ...]:
    """The names not in `SECRET_NAMES`, lowered and sorted: a cache key."""
    return _extra_of(tuple(names)) if names else ()


@functools.lru_cache(maxsize=32)
def _extra_of(names: tuple[str, ...]) -> tuple[str, ...]:
    builtin = {_loose(n) for n in SECRET_NAMES}
    return tuple(sorted({n.lower() for n in names if _loose(n) not in builtin}))


def _loose(name: str) -> str:
    """A name as a pattern: `_` and `-` match either, or nothing."""
    return "[_-]?".join(re.escape(part) for part in re.split(r"[_\-]", name))


def _names(extra: tuple[str, ...]) -> str:
    """The alternatives of the secret names, longest first."""
    loose = sorted(
        {_loose(n) for n in (*SECRET_NAMES, *extra) if n}, key=len, reverse=True
    )
    return "|".join(loose)


@functools.lru_cache(maxsize=32)
def _keys(extra: tuple[str, ...]) -> re.Pattern[str]:
    """Matches a key holding a secret name, e.g. `etl.sasl.password`."""
    whole = "|".join(re.escape(n) for n in _WHOLE_KEY_SECRET_NAMES)
    return re.compile(rf"(?:{_names(extra)})|^(?:{whole})$", re.I)


@functools.lru_cache(maxsize=32)
def _pairs(extra: tuple[str, ...]) -> re.Pattern[str]:
    """Matches `key=value` and `key: value` where the key is a secret."""
    whole = "|".join(re.escape(n) for n in _WHOLE_KEY_SECRET_NAMES)
    return re.compile(
        # A key, `_`, `-` and `.` included: `client_secret`, `x-api-key`.
        rf"(?<![\w.\-])(?P<key>[\w.\-]{{0,64}}(?:{_names(extra)})"
        rf"[\w.\-]{{0,64}}|(?:{whole}))"
        r"(?P<sep>[\"']?\s*[=:]\s*)"
        # `Authorization: Bearer <token>` keeps its scheme.
        r"(?P<scheme>(?:Bearer|Basic|Digest|Negotiate)\s+)?"
        r"(?:(?P<quote>[\"'])[^\"'\n]*"  # 'quoted', to the closing quote
        r"|(?P<brace>\{)[^}\n]*\}?"  # {braced}, as ODBC quotes values
        r"|[^\s;,\"'&}]+)",  # plain, to the next separator
        re.I,
    )


def _mask_pair(match: re.Match[str]) -> str:
    """The key and separator of a pair, its value hidden."""
    if match["brace"]:
        value = "{" + MASK + "}"
    else:
        value = (match["quote"] or "") + MASK
    return f"{match['key']}{match['sep']}{match['scheme'] or ''}{value}"
