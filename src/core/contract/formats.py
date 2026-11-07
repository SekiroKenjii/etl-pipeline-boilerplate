"""The port of record formats: CSV, JSON Lines, Parquet..."""

from __future__ import annotations

from collections.abc import Iterator
from typing import BinaryIO, Protocol

type Row = dict[str, object]


class IRecordFormat(Protocol):
    """Turns a binary stream into rows, without reading it all at once."""

    def read(self, stream: BinaryIO) -> Iterator[Row]:
        """Yields the rows of `stream`; the caller owns and closes it."""
        ...
