"""Date and time helpers."""

from __future__ import annotations

import datetime
from typing import cast


def utc_now() -> datetime.datetime:
    """Returns the current time as a timezone-aware UTC datetime.

    Use it instead of `datetime.datetime.now()`, which is naive.
    """
    return datetime.datetime.now(datetime.timezone.utc)


def like_timezone[T](value: T, reference: object) -> T:
    """Returns `value` with the timezone-awareness of `reference`.

    Naive datetimes mean UTC: an aware value becomes naive UTC when
    `reference` is naive, a naive value becomes UTC when it is aware. Anything
    else, or a value already alike, is returned as it is. Use it before
    comparing or querying with a moment given by a user (aware) against values
    of a column (naive or aware, by database type).
    """
    if not isinstance(value, datetime.datetime) or not isinstance(
        reference, datetime.datetime
    ):
        return value
    if reference.tzinfo is None and value.tzinfo is not None:
        naive = value.astimezone(datetime.UTC).replace(tzinfo=None)
        return cast(T, naive)
    if reference.tzinfo is not None and value.tzinfo is None:
        return cast(T, value.replace(tzinfo=datetime.UTC))
    return value
