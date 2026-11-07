"""A loader that registers the keys of what it loaded."""

from __future__ import annotations

from collections.abc import Callable, Sequence

from core import contract
from core import pipeline


class RegisteringLoader[T](pipeline.IDataLoader[T]):
    """Loads with another loader, then registers the keys of the batch.

    Children checked later then find their parents in the crosswalk. Keys are
    registered only when the whole batch loaded (`failed == 0`): a loader
    reporting failed records does not say which, and registering a parent that
    is not in the target would let its orphans through. Loaders are expected
    to be all-or-nothing per batch (one transaction), as most bulk loaders are.

    Attributes:
        inner: The loader that writes the target.
        registry: The crosswalk the keys are registered in.
    """

    def __init__(
        self,
        inner: pipeline.IDataLoader[T],
        registry: contract.IKeyRegistry,
        key: Callable[[T], contract.KeyEntry | None],
    ) -> None:
        """Initializes the loader.

        Args:
            inner: The loader that writes the target.
            registry: The crosswalk the keys are registered in.
            key: Gets the key entry of a record, None for a record without key
                (e.g. `MappedRecord.key_entry`).
        """
        self.inner = inner
        self.registry = registry
        self._key = key

    @property
    def name(self) -> str:
        return f"{type(self).__name__}({self.inner.name})"

    def setup(self, ctx: contract.ETLContext) -> None:
        self.inner.setup(ctx)

    def teardown(self, ctx: contract.ETLContext) -> None:
        self.inner.teardown(ctx)

    def load(
        self, records: Sequence[T], ctx: contract.ETLContext
    ) -> contract.ETLStats:
        """Loads the batch, then registers its keys if all of it loaded."""
        stats = self.inner.load(records, ctx)
        if stats.failed == 0:
            entries = [
                entry
                for record in records
                if (entry := self._key(record)) is not None
            ]
            if entries:
                self.registry.register(entries)
        return stats
