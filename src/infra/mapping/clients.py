"""What the workbook adapters use of openpyxl's objects."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Literal, Protocol

__all__ = ["WorksheetLike"]


class WorksheetLike(Protocol):
    """What is read of an openpyxl sheet."""

    @property
    def title(self) -> str:
        ...

    def iter_rows(
        self, *, values_only: Literal[True]
    ) -> Iterator[tuple[object, ...]]:
        ...
