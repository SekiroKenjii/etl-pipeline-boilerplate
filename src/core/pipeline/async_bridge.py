"""Runs an asyncio extractor behind the synchronous extractor interface."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Coroutine, Generator, Sequence
import contextvars
import dataclasses
import queue
import threading
from typing import Any

from core import contract
from core.pipeline import extractor as pipeline_extractor

_PUT_RETRY_SECONDS = 0.005
_GET_POLL_SECONDS = 0.1


class AsyncExtractorAdapter[TOut](pipeline_extractor.IDataExtractor[TOut]):
    """Exposes an `IAsyncDataExtractor` as a synchronous `IDataExtractor`.

    The async extractor runs on its own event loop, in a background thread.
    Records cross in chunks of `chunk_size` through a queue of at most
    `buffer_size` chunks: a slower job makes the source wait (backpressure).
    The loop lives from the first `extract` until `teardown`, so `acknowledge`
    still runs on it after the last batch; stopping cancels the extractor, then
    runs its `ateardown`.

        ETLJob(AsyncExtractorAdapter(MyApiExtractor()), transformer, loader)

    Attributes:
        extractor: The adapted asyncio extractor.
    """

    def __init__(
        self,
        extractor: pipeline_extractor.IAsyncDataExtractor[TOut],
        *,
        buffer_size: int = 8,
        chunk_size: int = 500,
    ) -> None:
        if buffer_size < 1:
            raise ValueError("buffer_size must be >= 1")
        if chunk_size < 1:
            raise ValueError("chunk_size must be >= 1")

        self.extractor = extractor
        self._buffer_size = buffer_size
        self._chunk_size = chunk_size
        self._bridge: _Bridge[TOut] | None = None

    @property
    def name(self) -> str:
        return self.extractor.name

    def setup(self, ctx: contract.ETLContext) -> None:
        self.extractor.setup(ctx)

    def teardown(self, ctx: contract.ETLContext) -> None:
        try:
            self._stop()
        finally:
            self.extractor.teardown(ctx)

    def extract(self, ctx: contract.ETLContext) -> Generator[TOut]:
        """Starts the event loop and yields the records it produces.

        Closing the generator early stops the source.

        Raises:
            RuntimeError: If it is already extracting.
        """
        if self._bridge is not None:
            raise RuntimeError(
                f"{self.name} is already extracting: call teardown() between"
                " runs"
            )

        bridge = _Bridge(
            self.extractor, ctx, self._buffer_size, self._chunk_size
        )
        self._bridge = bridge
        bridge.start()

        try:
            while (chunk := bridge.get()) is not None:
                yield from chunk
        except GeneratorExit:
            # The consumer gave up early: stop the source now.
            self._stop()
            raise

    def acknowledge(
        self, records: Sequence[TOut], ctx: contract.ETLContext
    ) -> None:
        if self._bridge is None:
            raise RuntimeError(
                f"{self.name}.acknowledge() called outside of a run"
            )

        self._bridge.run(self.extractor.acknowledge(records, ctx))

    def _stop(self) -> None:
        bridge, self._bridge = self._bridge, None
        if bridge is not None:
            bridge.stop()


@dataclasses.dataclass(frozen=True, slots=True)
class _End:
    """The end of the source, with the error that ended it, if any."""

    error: BaseException | None = None


class _Bridge[T]:
    """One event loop in one thread, producing chunks for one run."""

    def __init__(
        self,
        extractor: pipeline_extractor.IAsyncDataExtractor[T],
        ctx: contract.ETLContext,
        buffer_size: int,
        chunk_size: int,
    ) -> None:
        self._extractor = extractor
        self._ctx = ctx
        self._chunk_size = chunk_size
        self._queue: queue.Queue[list[T] | _End] = queue.Queue(
            maxsize=buffer_size
        )
        self._ready = threading.Event()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._task: asyncio.Task[None] | None = None
        self._teardown_error: Exception | None = None
        # The loop runs in a copy of the caller's context: its log fields.
        context = contextvars.copy_context()
        self._thread = threading.Thread(
            target=context.run,
            args=(self._run,),
            name=f"async-extract:{extractor.name}",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()
        self._ready.wait()

    def get(self) -> list[T] | None:
        """Returns the next chunk, or None once the source is exhausted.

        Raises:
            RuntimeError: If the loop thread died without finishing.
            Exception: Whatever the source raised.
        """
        while True:
            try:
                item = self._queue.get(timeout=_GET_POLL_SECONDS)
            except queue.Empty:
                if not self._thread.is_alive():
                    raise RuntimeError(
                        "async extractor stopped unexpectedly"
                    ) from None
                continue

            if isinstance(item, _End):
                if item.error is not None:
                    raise item.error
                return None
            return item

    def run(self, coroutine: Coroutine[Any, Any, None]) -> None:
        """Runs `coroutine` on the extractor's loop and waits for it.

        Raises:
            RuntimeError: If the loop is not running.
        """
        loop = self._loop
        if loop is None or loop.is_closed() or not self._thread.is_alive():
            coroutine.close()
            raise RuntimeError("the async extractor is not running")

        asyncio.run_coroutine_threadsafe(coroutine, loop).result()

    def stop(self) -> None:
        """Cancels the source and waits for the thread to end.

        Raises:
            Exception: What the extractor's `ateardown` raised, if anything.
        """
        loop, task = self._loop, self._task
        if loop is not None and task is not None and self._thread.is_alive():
            try:
                loop.call_soon_threadsafe(task.cancel)
            except RuntimeError:  # the loop closed in the meantime
                pass

        self._thread.join()
        if self._teardown_error is not None:
            raise self._teardown_error

    def _run(self) -> None:
        try:
            asyncio.run(self._main())
        except asyncio.CancelledError:
            pass

    async def _main(self) -> None:
        """Produces every chunk, then serves `acknowledge` until cancelled."""
        self._loop = asyncio.get_running_loop()
        self._task = asyncio.current_task()
        self._ready.set()
        set_up = False

        try:
            try:
                await self._extractor.asetup(self._ctx)
                set_up = True
                await self._produce()
                end = _End()
            except Exception as exc:  # pylint: disable=broad-exception-caught
                # Handed over to the job's thread, which re-raises it.
                end = _End(exc)

            await self._put(end)
            # Keep the loop alive for `acknowledge` until `stop()` cancels us.
            await asyncio.Event().wait()
        finally:
            if set_up:
                try:
                    await self._extractor.ateardown(self._ctx)
                # Re-raised by stop(), in the job's thread.
                except Exception as exc:  # pylint: disable=broad-except
                    self._teardown_error = exc

    async def _produce(self) -> None:
        """Queues the records of the source in chunks, until it is exhausted."""
        iterator = self._extractor.extract(self._ctx)
        chunk: list[T] = []
        try:
            async for item in iterator:
                chunk.append(item)
                if len(chunk) >= self._chunk_size:
                    await self._put(chunk)
                    chunk = []
                if self._ctx.is_cancelled:
                    break

            if chunk:
                await self._put(chunk)
        finally:
            if isinstance(iterator, AsyncGenerator):
                await iterator.aclose()

    async def _put(self, item: list[T] | _End) -> None:
        # A blocking put would freeze the loop: poll, cancellably.
        while True:
            try:
                self._queue.put_nowait(item)
                return
            except queue.Full:
                await asyncio.sleep(_PUT_RETRY_SECONDS)
