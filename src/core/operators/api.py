"""An extractor of asynchronous record sources, such as REST endpoints."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Mapping
from typing import Any

from core import contract
from core import pipeline

type Params = Mapping[str, Any] | Callable[
    [contract.ETLContext], Mapping[str, Any]
]


class ApiExtractor[T](pipeline.IAsyncDataExtractor[T]):
    """Extracts the records of an `IAsyncRecordSource` adapter.

        ApiExtractor(
            orders_api.endpoint("/v1/orders", records_path="data"),
            params=lambda ctx: {"updated_since": ctx.run.since.isoformat()},
        )

    The source's async resources live on this extractor's event loop: use it as
    `AsyncExtractorAdapter(ApiExtractor(...))`.

    Attributes:
        source: The adapter records come from.
    """

    def __init__(
        self,
        source: contract.IAsyncRecordSource[T],
        params: Params | None = None,
    ) -> None:
        """Initializes the extractor.

        Args:
            source: The adapter records come from.
            params: The parameters of the source, fixed or computed from the
                run context (e.g. from `ctx.run.since` for incremental runs).
        """
        self.source = source
        self._params = params

    @property
    def name(self) -> str:
        return f"{type(self).__name__}({self.source.name})"

    async def asetup(self, ctx: contract.ETLContext) -> None:
        await self.source.asetup(ctx)

    async def ateardown(self, ctx: contract.ETLContext) -> None:
        await self.source.ateardown(ctx)

    # pylint takes this async generator for a coroutine.
    async def extract(  # pylint: disable=invalid-overridden-method
        self, ctx: contract.ETLContext
    ) -> AsyncIterator[T]:
        async for record in self.source.records(self.params_for(ctx)):
            yield record

    def params_for(self, ctx: contract.ETLContext) -> Mapping[str, Any]:
        """Returns the parameters of the source for this run."""
        if self._params is None:
            return {}
        if callable(self._params):
            return self._params(ctx)
        return self._params
