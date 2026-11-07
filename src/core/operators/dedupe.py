"""A transformer dropping duplicate records."""

from __future__ import annotations

from collections.abc import Callable, Hashable, Iterable, Iterator

from core import contract
from core import pipeline


class DropDuplicates[T](pipeline.IDataTransformer[T, T]):
    """Keeps the first record of each key over the whole run.

    The others are counted as `filtered`. The keys seen stay in memory for the
    run: deduplicate upstream for very large sources, and prefer a quality rule
    when duplicates are bad data.

        transformer.then(DropDuplicates(lambda r: r.data["customer_id"]))
    """

    def __init__(self, key: Callable[[T], Hashable]) -> None:
        self._key = key
        self._seen: set[Hashable] = set()
        self._dropped = 0

    def setup(self, ctx: contract.ETLContext) -> None:
        """Forgets the keys of a previous run."""
        self._seen.clear()
        self._dropped = 0

    def transform(
        self, records: Iterable[T], ctx: contract.ETLContext
    ) -> Iterator[T]:
        for record in records:
            key = self._key(record)
            if key in self._seen:
                self._dropped += 1
                continue
            self._seen.add(key)
            yield record

    def collect_stats(self) -> contract.ETLStats:
        stats, self._dropped = contract.ETLStats(filtered=self._dropped), 0
        return stats
