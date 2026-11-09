"""Target types, written the way database administrators write them.

`VARCHAR(200)`, `NUMBER(10,2)`, `DATE`... are turned into a cast, plus the
checks the type implies: length, digits.
"""

from __future__ import annotations

from collections.abc import Callable
import dataclasses
import datetime
import decimal
import enum
import functools
import re

from core.mapping import types

__all__ = [
    "FORMAT_TOKENS",
    "SUPPORTED_TYPE_NAMES",
    "CastError",
    "TargetType",
    "TypeKind",
    "format_problems",
    "parse_type",
    "text_parser",
    "to_strftime",
    "two_digit_year",
]


class TypeKind(enum.StrEnum):
    """What a value becomes once cast."""

    STRING = "string"
    INTEGER = "integer"
    DECIMAL = "decimal"
    FLOAT = "float"
    DATE = "date"
    DATETIME = "datetime"
    BOOLEAN = "boolean"


_STRING_NAMES = (
    "VARCHAR",
    "VARCHAR2",
    "NVARCHAR",
    "NVARCHAR2",
    "CHAR",
    "NCHAR",
    "TEXT",
    "CLOB",
    "NCLOB",
    "STRING",
    "UUID",
)
_NAMES: dict[str, TypeKind] = {
    **dict.fromkeys(_STRING_NAMES, TypeKind.STRING),
    **dict.fromkeys(
        ["INT", "INTEGER", "SMALLINT", "BIGINT", "TINYINT"], TypeKind.INTEGER
    ),
    **dict.fromkeys(["NUMBER", "DECIMAL", "NUMERIC"], TypeKind.DECIMAL),
    **dict.fromkeys(
        ["FLOAT", "DOUBLE", "REAL", "BINARY_DOUBLE", "BINARY_FLOAT"],
        TypeKind.FLOAT,
    ),
    "DATE": TypeKind.DATE,
    **dict.fromkeys(["TIMESTAMP", "DATETIME", "DATETIME2"], TypeKind.DATETIME),
    **dict.fromkeys(["BOOLEAN", "BOOL", "BIT"], TypeKind.BOOLEAN),
}

SUPPORTED_TYPE_NAMES: tuple[str, ...] = tuple(sorted(_NAMES))

_TYPE = re.compile(
    r"^\s*(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*"
    r"(?:\(\s*(?P<a>\d+)\s*(?:,\s*(?P<b>\d+)\s*)?\))?\s*$"
)
_TRUE = frozenset({"1", "true", "t", "y", "yes"})
_FALSE = frozenset({"0", "false", "f", "n", "no"})


class CastError(ValueError):
    """A value does not fit its target type: an issue of that record.

    Attributes:
        redacted: The message without the value, for personal data.
    """

    def __init__(self, message: str, redacted: str | None = None) -> None:
        super().__init__(message)
        self.redacted = message if redacted is None else redacted


def _bad(value: object, reason: str) -> CastError:
    """Returns the error of a value: `'x' <reason>`, redacted `the value
    <reason>`."""
    return CastError(f"{value!r} {reason}", f"the value {reason}")


@dataclasses.dataclass(frozen=True, slots=True)
class TargetType:
    """A parsed target type.

    Attributes:
        raw: The type as written, e.g. `NUMBER(10,2)`.
        kind: What values become.
        max_length: The most characters of a text type.
        precision: The most digits of a number.
        scale: The most decimals of a number.
    """

    raw: str
    kind: TypeKind
    max_length: int | None = None
    precision: int | None = None
    scale: int | None = None

    @property
    def is_temporal(self) -> bool:
        return self.kind in (TypeKind.DATE, TypeKind.DATETIME)

    def cast(self, value: object, source_format: str | None = None) -> object:
        """Converts a value to this type.

        Args:
            value: The value; never None.
            source_format: How dates are written in the source, e.g.
                `dd/MM/yyyy`; ISO 8601 when None.

        Returns:
            The converted value.

        Raises:
            CastError: If the value does not fit the type.
        """
        return self.caster(source_format)(value)

    def caster(self, source_format: str | None = None) -> types.Caster:
        """Returns the function converting values to this type.

        It is built once per field, with the date format already compiled:
        a row costs one call. `cast` is `caster(source_format)(value)`.

        Args:
            source_format: How dates are written in the source, e.g.
                `dd/MM/yyyy`; ISO 8601 when None.
        """
        match self.kind:
            case TypeKind.STRING:
                return self._string
            case TypeKind.INTEGER:
                return self._integer
            case TypeKind.DECIMAL:
                return self._decimal
            case TypeKind.FLOAT:
                return self._float
            case TypeKind.DATE:
                moment = _moment_caster(source_format)
                return lambda value: moment(value).date()
            case TypeKind.DATETIME:
                return _moment_caster(source_format)
            case TypeKind.BOOLEAN:
                return self._boolean

    def _string(self, value: object) -> str:
        text = value if isinstance(value, str) else str(value)
        if self.max_length is not None and len(text) > self.max_length:
            raise CastError(
                f"{len(text)} characters, more than the {self.max_length} of"
                f" {self.raw}"
            )
        return text

    def _integer(self, value: object) -> int:
        """Casts to a whole number within the precision."""
        number = _to_decimal(value, self.raw)
        if number != number.to_integral_value():
            raise _bad(value, f"is not a whole number ({self.raw})")
        integer = int(number)
        if (
            self.precision is not None
            and len(str(abs(integer))) > self.precision
        ):
            raise CastError(
                f"{integer} has more than {self.precision} digits ({self.raw})",
                f"the value has more than {self.precision} digits ({self.raw})",
            )
        return integer

    def _decimal(self, value: object) -> decimal.Decimal:
        """Casts to a decimal, never rounding silently."""
        number = _to_decimal(value, self.raw)
        if self.scale is not None:
            rounded = number.quantize(decimal.Decimal(1).scaleb(-self.scale))
            if rounded != number:
                raise _bad(
                    value,
                    f"has more than {self.scale} decimals ({self.raw}), it"
                    " would be rounded",
                )
            number = rounded
        if self.precision is not None:
            integer_digits = len(str(abs(int(number)))) if int(number) else 0
            if integer_digits > self.precision - (self.scale or 0):
                raise _bad(value, f"does not fit in {self.raw}")
        return number

    def _float(self, value: object) -> float:
        if not isinstance(value, (str, int, float, decimal.Decimal)):
            raise _bad(value, f"is not a number ({self.raw})")
        try:
            return float(value)
        except ValueError as exc:
            raise _bad(value, f"is not a number ({self.raw})") from exc

    def _boolean(self, value: object) -> bool:
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        if text in _TRUE:
            return True
        if text in _FALSE:
            return False
        raise _bad(value, f"is not a boolean ({self.raw})")


def parse_type(raw: str) -> TargetType:
    """Parses a database-style type: `NUMBER(10,2)` is a decimal(10, 2).

    Args:
        raw: The type as written.

    Returns:
        The parsed type. `NUMBER(p)` and `NUMBER(p,0)` are integers; `UUID`
        is a text of 36 characters.

    Raises:
        ValueError: If the type is unknown or malformed.
    """
    match = _TYPE.match(raw)
    if match is None:
        raise ValueError(
            f"{raw!r} is not a type like VARCHAR(200) or NUMBER(10,2)"
        )

    name = match["name"].upper()
    kind = _NAMES.get(name)
    if kind is None:
        raise ValueError(f"unknown type {name!r}")

    a = int(match["a"]) if match["a"] else None
    b = int(match["b"]) if match["b"] else None

    if name == "UUID":
        if a is not None:
            raise ValueError(f"{raw!r}: UUID takes no size")
        return TargetType(raw, kind, max_length=36)
    if kind is TypeKind.STRING:
        if b is not None:
            raise ValueError(
                f"{raw!r}: a text type takes one size, e.g. VARCHAR(200)"
            )
        return TargetType(raw, kind, max_length=a)
    if kind is TypeKind.DECIMAL and a is not None and not b:
        return TargetType(raw, TypeKind.INTEGER, precision=a)
    if kind in (TypeKind.DECIMAL, TypeKind.INTEGER):
        if b is not None and a is not None and b > a:
            raise ValueError(f"{raw!r}: the scale cannot exceed the precision")
        return TargetType(raw, kind, precision=a, scale=b)
    if a is not None:
        raise ValueError(f"{raw!r}: {name} takes no size")
    return TargetType(raw, kind)


_STRFTIME = {
    "yyyy": "%Y",
    "yy": "%y",
    "MM": "%m",
    "dd": "%d",
    "HH": "%H",
    "mm": "%M",
    "ss": "%S",
}
# Longest first: `yyyy` must win over `yy`.
_TOKENS = "|".join(sorted(_STRFTIME, key=len, reverse=True))
# A token, a quoted literal ('T'), or a run of letters no token starts.
_FORMAT_PARTS = re.compile(rf"{_TOKENS}|'[^']*'|(?!{_TOKENS})[A-Za-z]+")
FORMAT_TOKENS: tuple[str, ...] = tuple(_STRFTIME)


def to_strftime(source_format: str) -> str:
    """Translates a date format written as in Excel into a strftime pattern.

    `dd/MM/yyyy HH:mm` becomes `%d/%m/%Y %H:%M`, and `'T'` the letter T; a
    pattern that already holds `%` is returned as is. Letters that are no
    token (see `format_problems`) are kept as they are.
    """
    if "%" in source_format:
        return source_format

    def translate(match: re.Match[str]) -> str:
        part = match.group()
        if part in _STRFTIME:
            return _STRFTIME[part]
        if part.startswith("'"):
            return part[1:-1]
        return part

    return _FORMAT_PARTS.sub(translate, source_format)


def format_problems(source_format: str) -> list[str]:
    """Returns what is wrong with a date format written as in Excel.

    Only the tokens yyyy, yy, MM, dd, HH, mm and ss are understood; other
    letters are refused (`DD/MM/YYYY` would match no date), unless quoted
    (`'T'`). `mm` is minutes: without hours, it is a month written in the
    wrong case.
    """
    if "%" in source_format:
        return []
    parts = [m.group() for m in _FORMAT_PARTS.finditer(source_format)]
    unknown = sorted(
        {p for p in parts if p not in _STRFTIME and not p.startswith("'")}
    )
    problems: list[str] = []
    if unknown:
        problems.append(
            f"unknown token(s) {', '.join(unknown)} in the format"
            f" {source_format!r}: use {', '.join(FORMAT_TOKENS)} (case"
            " matters), e.g. dd/MM/yyyy"
        )
    if "mm" in parts and "HH" not in parts:
        problems.append(
            f"mm is minutes, and {source_format!r} has no hours: write"
            " months MM"
        )
    if not unknown and not any(p in ("yyyy", "yy") for p in parts):
        problems.append(f"the format {source_format!r} has no year")
    return problems


def two_digit_year(source_format: str) -> bool:
    """Whether a format reads years in 2 digits: 69-99 are read 1969-1999,
    00-68 are read 2000-2068 (a birth date in 1960 becomes 2060)."""
    if "%" in source_format:
        return "%y" in source_format
    parts = [m.group() for m in _FORMAT_PARTS.finditer(source_format)]
    return "yy" in parts


def _moment_caster(
    source_format: str | None,
) -> Callable[[object], datetime.datetime]:
    """Returns the function casting values to a datetime, text read with
    `source_format`."""
    parse = text_parser(source_format)
    expected = source_format or "ISO 8601 (yyyy-MM-dd)"

    def cast(value: object) -> datetime.datetime:
        if isinstance(value, str):
            text = value.strip()
        elif isinstance(value, datetime.datetime):
            return value
        elif isinstance(value, datetime.date):
            return datetime.datetime(value.year, value.month, value.day)
        else:
            text = str(value).strip()
        try:
            return parse(text)
        except ValueError as exc:
            raise _bad(value, f"does not match the format {expected}") from exc

    return cast


@functools.cache
def text_parser(
    source_format: str | None,
) -> Callable[[str], datetime.datetime]:
    """Returns the function reading a datetime written as `source_format`.

    A format written as in Excel is compiled once into a regular expression
    (`strptime` would compile its own at every call); a Python `%` format,
    and the few forms the compiled parser does not take, are read by
    `strptime`. Both accept and refuse the same texts. ISO 8601 when None.

    Raises:
        ValueError: From the function, for a text that is no such date.
    """
    if not source_format:
        return datetime.datetime.fromisoformat
    pattern = to_strftime(source_format)
    parse = _compiled_parser(pattern)
    if parse is not None:
        return parse

    def with_strptime(text: str) -> datetime.datetime:
        return datetime.datetime.strptime(text, pattern)

    return with_strptime


# What `strptime` accepts for each directive (Python 3.14, `_strptime`):
# one or two digits, a day or hour that may be padded with a space.
_DIRECTIVES: dict[str, str] = {
    "Y": r"(?P<Y>\d\d\d\d)",
    "y": r"(?P<y>\d\d)",
    "m": r"(?P<m>1[0-2]|0[1-9]|[1-9])",
    "d": r"(?P<d>3[0-1]|[1-2]\d|0[1-9]|[1-9]| [1-9])",
    "H": r"(?P<H>2[0-3]|[0-1]\d|\d| \d)",
    "M": r"(?P<M>[0-5]\d|\d)",
    "S": r"(?P<S>6[0-1]|[0-5]\d|\d)",
}
_REGEX_CHARS = re.compile(r"([\\.^$*+?\(\){}\[\]|])")
_SPACES = re.compile(r"\s+")


def _compiled_parser(
    pattern: str,
) -> Callable[[str], datetime.datetime] | None:
    """Returns the parser of a strftime `pattern` made of the directives of
    `_DIRECTIVES`, each at most once, one of them a year; None for any
    other pattern, which `strptime` reads as it always did.

    The text, the literals and the directives are matched as `strptime`
    matches them: letters in either case, a run of spaces for one, the
    match starting at the first character and reaching the last.
    """
    literal, *directives = pattern.split("%")
    regex = _literal(literal)
    seen: set[str] = set()
    for directive in directives:
        name = directive[:1]
        if name not in _DIRECTIVES or name in seen:
            return None
        seen.add(name)
        regex += _DIRECTIVES[name] + _literal(directive[1:])
    if len(seen & {"Y", "y"}) != 1:
        return None
    compiled = re.compile(regex, re.IGNORECASE)

    def parse(text: str) -> datetime.datetime:
        found = compiled.match(text)
        if found is None or found.end() != len(text):
            raise ValueError(f"{text!r} does not match the format {pattern!r}")
        parts = found.groupdict()
        if "Y" in parts:
            year = int(parts["Y"])
        else:
            year = int(parts["y"])
            year += 2000 if year <= 68 else 1900
        return datetime.datetime(
            year,
            int(parts.get("m", 1)),
            int(parts.get("d", 1)),
            int(parts.get("H", 0)),
            int(parts.get("M", 0)),
            int(parts.get("S", 0)),
        )

    return parse


def _literal(text: str) -> str:
    """A literal part of a pattern, as `strptime` matches it."""
    escaped = _SPACES.sub(r"\\s+", _REGEX_CHARS.sub(r"\\\1", text))
    return escaped.replace("'", "['\u02bc]")


def _to_decimal(value: object, raw: str) -> decimal.Decimal:
    if isinstance(value, bool):
        raise _bad(value, f"is not a number ({raw})")
    try:
        number = decimal.Decimal(str(value).strip())
    except decimal.InvalidOperation as exc:
        raise _bad(value, f"is not a number ({raw})") from exc
    if not number.is_finite():
        raise _bad(value, f"is not a number ({raw})")
    return number
