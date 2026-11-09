"""The types the operators share, and the ones their callers pass."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
import datetime
from typing import Protocol, Self, TypedDict

from core import contract

__all__ = [
    "ExtractorOptions",
    "Fetch",
    "Ordered",
    "Params",
    "SourceRecord",
]

type Params = Mapping[str, object] | Callable[
    [contract.ETLContext], Mapping[str, object]
]


# What a pipeline corrects: a source row, or a change of one.
type SourceRecord = contract.Row | contract.ChangeEvent


type Fetch[T] = Callable[
    [Sequence[contract.QuarantineEntry], contract.ETLContext], Iterable[T]
]


class Ordered(Protocol):
    """A value rows are read in the order of: a watermark, a key."""

    def __lt__(self, other: Self, /) -> bool:
        ...

    def __le__(self, other: Self, /) -> bool:
        ...

    def __gt__(self, other: Self, /) -> bool:
        ...

    def __ge__(self, other: Self, /) -> bool:
        ...


class ExtractorOptions(TypedDict, total=False):
    """The options of a `TableExtractor` that its variants pass on."""

    watermark: str | None
    columns: Sequence[str]
    lookback: datetime.timedelta | int | None
    snapshot: bool
