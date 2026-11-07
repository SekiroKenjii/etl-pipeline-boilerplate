from __future__ import annotations

import datetime

import pytest

from shared import utils

NAIVE = datetime.datetime(2026, 10, 1, 12)
AWARE = NAIVE.replace(tzinfo=datetime.UTC)
PARIS = datetime.datetime(
    2026, 10, 1, 14, tzinfo=datetime.timezone(datetime.timedelta(hours=2))
)


@pytest.mark.parametrize(
    ("value", "reference", "expected"),
    [
        (PARIS, NAIVE, NAIVE),  # Aware against naive: naive UTC.
        (NAIVE, AWARE, AWARE),  # Naive against aware: naive means UTC.
        (PARIS, AWARE, PARIS),
        (NAIVE, NAIVE, NAIVE),
        (None, NAIVE, None),
        (PARIS, 42, PARIS),  # Not a time: as it is.
    ],
)
def test_like_timezone(
    value: object, reference: object, expected: object
) -> None:
    result = utils.like_timezone(value, reference)

    assert result == expected
    if isinstance(result, datetime.datetime) and isinstance(
        expected, datetime.datetime
    ):
        assert (result.tzinfo is None) == (expected.tzinfo is None)
