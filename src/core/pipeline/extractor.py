"""The extractor interfaces: synchronous and asyncio."""

from __future__ import annotations

import abc
from collections.abc import AsyncIterator, Iterable, Sequence

from core import contract


class IDataExtractor[TOut](contract.Lifecycle, abc.ABC):
    """Reads records from a source.

    Return a lazy iterable (a generator), so that large sources are streamed
    instead of loaded into memory. Use `ctx.run.since` and `ctx.run.until` for
    incremental extraction.
    """

    @abc.abstractmethod
    def extract(self, ctx: contract.ETLContext) -> Iterable[TOut]:
        """Yields the records of the source."""

    def acknowledge(
        self, records: Sequence[TOut], ctx: contract.ETLContext
    ) -> None:
        """Confirms that a batch of records is fully handled.

        Called once the batch is loaded or rejected into the quarantine; never
        for a failed batch nor in a dry run. Confirm consumption or save
        progress here, not while extracting: that gives at-least-once delivery.

        Args:
            records: The records of the batch, as extracted.
            ctx: The run context.
        """


class IAsyncDataExtractor[TOut](
    contract.Lifecycle, contract.AsyncLifecycle, abc.ABC
):
    """Reads records with asyncio, for I/O-bound sources.

    Wrap it in `AsyncExtractorAdapter` to use it in an `ETLJob`. Async
    resources (e.g. `httpx.AsyncClient`) must be created and closed on its loop:
    in `asetup` and `ateardown`, not `setup` and `teardown`.
    """

    @abc.abstractmethod
    def extract(self, ctx: contract.ETLContext) -> AsyncIterator[TOut]:
        """Yields the records of the source; implement as an async generator."""

    async def acknowledge(
        self, records: Sequence[TOut], ctx: contract.ETLContext
    ) -> None:
        """See `IDataExtractor.acknowledge`; awaited on the extractor's loop."""
