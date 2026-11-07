"""A transformer letting records through as they are."""

from __future__ import annotations

from collections.abc import Iterable

from core import contract
from core import pipeline


class Passthrough[T](pipeline.IDataTransformer[T, T]):
    """Lets every record through unchanged.

    For steps whose records are already in their target shape: replayed
    parked records, copies of a table.
    """

    def transform(
        self, records: Iterable[T], ctx: contract.ETLContext
    ) -> Iterable[T]:
        del ctx  # Unused.
        return records
