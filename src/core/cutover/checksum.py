"""Canonical values and row digests, equal across databases.

The equivalences are deliberate, and only these: integral numbers whatever
their type (`1`, `1.0`, `Decimal("1.00")`, `True`), a decimal and its float,
a date and its midnight (Oracle's DATE holds a time), the empty string and
NULL (Oracle stores one as the other), NFC and NFD text, a `UUID` and its
text, an aware timestamp and the naive UTC one. Whitespace, case, and text
against number (`"1"` and `1`, `"1.5"` and `1.5`) always differ.

Timestamps compare to the microsecond by default. With a coarser
`resolution` (300 for SQL Server `datetime`, 1 for whole seconds, 1000 for
milliseconds), both sides are rounded to its nearest tick, as those databases
round what they store.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import datetime
import decimal
import hashlib
import json
import math
import typing
from typing import NamedTuple
import unicodedata
import uuid

# Digests of other versions do not compare; 1 equalled a fraction to its text.
FORM = 2
MICROSECONDS = 1_000_000


class Number(NamedTuple):
    """A number that is not an integer, by its shortest decimal text.

    It differs from that text: JSON writes it as a one-item list
    (`["1.5"]`).
    """

    value: str

    def __repr__(self) -> str:
        return self.value


type Canonical = None | int | str | Number


def canonical_value(value: object, resolution: int = MICROSECONDS) -> Canonical:
    """Returns the canonical form of a column value.

    Integral numbers become `int` (booleans 0 and 1); other numbers a
    `Number` of their shortest decimal text; timestamps their naive UTC
    ISO 8601 text, a midnight one its date; text is NFC-normalized, and the
    empty string is NULL, as Oracle stores it.

    Args:
        value: The value, as a driver returns it.
        resolution: The ticks per second timestamps and times are rounded
            to, the nearest one: 1_000_000 (microseconds) keeps them as they
            are; 300 rounds to SQL Server `datetime`'s, 1 to whole seconds.

    Raises:
        ValueError: If `resolution` is not between 1 and 1_000_000, for a
            timestamp or a time.
    """
    match value:
        case None:
            return None
        case int():  # Booleans too: 0 and 1.
            return int(value)
        case float():
            return _number(decimal.Decimal(repr(value)))
        case decimal.Decimal():
            return _number(value)
        case datetime.datetime():
            return _timestamp(value, resolution)
        case datetime.time():
            moment = datetime.datetime.combine(datetime.date.min, value)
            return _rounded(moment, resolution).timetz().isoformat()
        case datetime.date():
            return value.isoformat()
        case uuid.UUID():
            return str(value)
        case bytes() | bytearray() | memoryview():
            return bytes(typing.cast(bytes, value)).hex()
        case _:
            return unicodedata.normalize("NFC", str(value)) or None


def _timestamp(value: datetime.datetime, resolution: int) -> str:
    """A timestamp as naive UTC ISO 8601 text; a midnight one, its date."""
    if value.tzinfo is not None:
        value = value.astimezone(datetime.UTC).replace(tzinfo=None)
    value = _rounded(value, resolution)
    if value.time() == datetime.time():
        # Oracle's DATE holds a time: a date reads back as midnight.
        return value.date().isoformat()
    return value.isoformat()


def canonical_row(
    row: Mapping[str, object],
    columns: Sequence[str],
    resolution: int = MICROSECONDS,
) -> tuple[Canonical, ...]:
    """The canonical values of `columns` of a row; a missing one is NULL.

    See `canonical_value` for `resolution`.
    """
    return tuple(
        canonical_value(row.get(column), resolution) for column in columns
    )


def text(values: Sequence[Canonical]) -> str:
    """A stable text of canonical values: the identity of a key."""
    return json.dumps(values, ensure_ascii=False, separators=(",", ":"))


def shown(value: Canonical) -> None | int | float | str:
    """A canonical value for a report: a `Number` as a JSON number, or as
    its text when a float cannot hold it exactly."""
    if not isinstance(value, Number):
        return value
    number = float(value.value)
    if math.isfinite(number) and decimal.Decimal(
        repr(number)
    ) == decimal.Decimal(value.value):
        return number
    return value.value


def digest(values: Sequence[Canonical]) -> int:
    """A 128-bit digest of canonical values."""
    hashed = hashlib.blake2b(text(values).encode(), digest_size=16)
    return int.from_bytes(hashed.digest(), "big")


def bucket(key: str, buckets: int) -> int:
    """The bucket of a key text, among `buckets`."""
    hashed = hashlib.blake2b(key.encode(), digest_size=8)
    return int.from_bytes(hashed.digest(), "big") % buckets


def _rounded(value: datetime.datetime, resolution: int) -> datetime.datetime:
    """The timestamp at the nearest tick of `resolution` per second."""
    if resolution == MICROSECONDS:
        return value
    if not 1 <= resolution < MICROSECONDS:
        raise ValueError(
            f"resolution must be 1 to {MICROSECONDS} ticks per second"
        )
    if not value.microsecond:
        return value
    # Half a tick rounds up, as SQL Server and MySQL round what they store.
    tick = (2 * value.microsecond * resolution + MICROSECONDS) // (
        2 * MICROSECONDS
    )
    whole = value.replace(microsecond=0)
    if tick == resolution:
        if whole == datetime.datetime.max.replace(microsecond=0):
            return whole
        return whole + datetime.timedelta(seconds=1)
    return whole.replace(microsecond=tick * MICROSECONDS // resolution)


def _number(value: decimal.Decimal) -> int | Number:
    if value.is_nan():
        return Number("NaN")
    if value.is_infinite():
        return Number(str(value))
    if value == value.to_integral_value():
        return int(value)
    return Number(format(value.normalize(), "f"))
