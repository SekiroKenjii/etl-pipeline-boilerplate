"""The types the modules of `core.mapping` share: a cast, a transform, an
anonymizer."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Protocol

__all__ = ["Anonymizer", "Caster", "TransformFn"]

type Caster = Callable[[object], object]
type TransformFn = Callable[..., object]


class Anonymizer(Protocol):
    """Replaces a personal value by a fake one."""

    def __call__(self, value: object, record: Sequence[str] = ()) -> object:
        """Returns the fake value of `value`.

        Args:
            value: The value, cast to its target type; None stays None.
            record: Identifies the record, e.g. its canonical natural key:
                the `date` kind shifts every date of a record by the same
                number of days.
        """
        ...
