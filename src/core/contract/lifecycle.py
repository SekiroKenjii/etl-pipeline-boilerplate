"""Setup and teardown of the components of a step, around each run of it.

Only extractors, transformers, loaders and the asynchronous sources an
extractor drives have them; ports are ready once built, and released by
`close`.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Generator
import contextlib

from core.contract import etl


class Lifecycle:
    """Setup and teardown of a step's component, around a run of the step.

    Override `setup` and `teardown` to prepare and release what the
    component needs for one run: per-run state, a source to close...
    """

    @property
    def name(self) -> str:
        return type(self).__name__

    def setup(self, ctx: etl.ETLContext) -> None:
        """Acquires the resources of the component; does nothing by default."""

    def teardown(self, ctx: etl.ETLContext) -> None:
        """Releases the resources of the component; does nothing by default."""

    @contextlib.contextmanager
    def lifecycle(self, ctx: etl.ETLContext) -> Generator[None]:
        """Runs `setup`, then always `teardown` on exit."""
        self.setup(ctx)
        try:
            yield
        finally:
            self.teardown(ctx)


class AsyncLifecycle:
    """The asyncio counterpart of `Lifecycle`.

    Resources bound to an event loop (`httpx.AsyncClient`) must be created and
    closed on the loop that uses them: in `asetup` and `ateardown`.
    """

    @property
    def name(self) -> str:
        return type(self).__name__

    async def asetup(self, ctx: etl.ETLContext) -> None:
        """Acquires the resources of the component; does nothing by default."""

    async def ateardown(self, ctx: etl.ETLContext) -> None:
        """Releases the resources of the component; does nothing by default."""

    @contextlib.asynccontextmanager
    async def alifecycle(self, ctx: etl.ETLContext) -> AsyncGenerator[None]:
        """Runs `asetup`, then always `ateardown` on exit."""
        await self.asetup(ctx)
        try:
            yield
        finally:
            await self.ateardown(ctx)
