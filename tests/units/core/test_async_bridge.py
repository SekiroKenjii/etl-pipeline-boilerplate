from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
import threading
import time

import pytest

from core import contract
from core import pipeline
from tests.factories import fakes


class Numbers(pipeline.IAsyncDataExtractor[int]):
    """Yields 0..count-1 (forever if count is None), like a paginated API."""

    def __init__(self, count: int | None, fail_at: int | None = None) -> None:
        self.count = count
        self.fail_at = fail_at
        self.events: list[str] = []
        self.produced = 0
        self.acked: list[list[int]] = []
        self.loop: asyncio.AbstractEventLoop | None = None
        self.ack_loops: list[asyncio.AbstractEventLoop] = []

    def setup(self, ctx: contract.ETLContext) -> None:
        self.events.append("setup")

    def teardown(self, ctx: contract.ETLContext) -> None:
        self.events.append("teardown")

    async def asetup(self, ctx: contract.ETLContext) -> None:
        self.loop = asyncio.get_running_loop()
        self.events.append("asetup")

    async def ateardown(self, ctx: contract.ETLContext) -> None:
        self.events.append("ateardown")

    # pylint: disable-next=invalid-overridden-method  # An async generator.
    async def extract(self, ctx: contract.ETLContext) -> AsyncIterator[int]:
        index = 0
        while self.count is None or index < self.count:
            if index == self.fail_at:
                raise contract.RetryableError("rate limited")
            await asyncio.sleep(0)
            self.produced += 1
            yield index
            index += 1

    async def acknowledge(
        self, records: Sequence[int], ctx: contract.ETLContext
    ) -> None:
        self.ack_loops.append(asyncio.get_running_loop())
        self.acked.append(list(records))


def test_streams_every_record_in_order() -> None:
    source = Numbers(10)
    adapter = pipeline.AsyncExtractorAdapter(source, chunk_size=3)
    ctx = fakes.make_context()

    with adapter.lifecycle(ctx):
        assert list(adapter.extract(ctx)) == list(range(10))

    assert source.events == ["setup", "asetup", "ateardown", "teardown"]


def test_source_errors_reach_the_job_thread() -> None:
    source = Numbers(10, fail_at=4)
    adapter = pipeline.AsyncExtractorAdapter(source, chunk_size=2)
    ctx = fakes.make_context()
    lifecycle = adapter.lifecycle(ctx)
    records = adapter.extract(ctx)

    with pytest.raises(contract.RetryableError, match="rate limited"):
        with lifecycle:
            list(records)

    assert source.events[-2:] == ["ateardown", "teardown"]


def test_a_slow_consumer_holds_the_source_back() -> None:
    source = Numbers(None)  # endless
    adapter = pipeline.AsyncExtractorAdapter(
        source, chunk_size=10, buffer_size=2
    )
    ctx = fakes.make_context()

    with adapter.lifecycle(ctx):
        records = adapter.extract(ctx)
        assert next(records) == 0
        time.sleep(0.1)  # let the producer run ahead as far as it can

        # at most: the chunk being read + a full buffer + the chunk being built
        assert source.produced <= 10 * (1 + 2 + 1)

    # teardown cancelled the endless source and still ran its async teardown
    assert source.events[-2:] == ["ateardown", "teardown"]


def test_closing_the_iterator_early_stops_the_source() -> None:
    source = Numbers(None)
    adapter = pipeline.AsyncExtractorAdapter(source, chunk_size=5)
    ctx = fakes.make_context()

    records = adapter.extract(ctx)
    next(records)
    records.close()

    assert "ateardown" in source.events
    assert not any(
        t.name.startswith("async-extract") for t in threading.enumerate()
    )


def test_acknowledge_runs_on_the_extractor_loop_after_the_last_batch() -> None:
    source = Numbers(3)
    adapter = pipeline.AsyncExtractorAdapter(source)
    ctx = fakes.make_context()

    with adapter.lifecycle(ctx):
        records = list(adapter.extract(ctx))  # source exhausted
        adapter.acknowledge(records, ctx)

    assert source.acked == [[0, 1, 2]]
    assert source.ack_loops == [source.loop]


def test_runs_inside_an_etl_job_and_is_acknowledged_per_batch() -> None:
    users = _UserSource(Numbers(5))
    loader = fakes.InMemoryLoader()
    job = pipeline.ETLJob(
        pipeline.AsyncExtractorAdapter(users, chunk_size=2),
        fakes.UserTransformer(),
        loader,
        batch_size=2,
    )

    result = job(fakes.make_context(), {})

    assert result.status is contract.ETLStatus.SUCCESS
    assert sorted(loader.rows) == ["0", "1", "2", "3", "4"]
    assert users.acked == [["0", "1"], ["2", "3"], ["4"]]


class _UserSource(pipeline.IAsyncDataExtractor[fakes.RawUser]):

    def __init__(self, numbers: Numbers) -> None:
        self.numbers = numbers
        self.acked: list[list[str]] = []

    # pylint: disable-next=invalid-overridden-method  # An async generator.
    async def extract(
        self, ctx: contract.ETLContext
    ) -> AsyncIterator[fakes.RawUser]:
        async for index in self.numbers.extract(ctx):
            yield fakes.RawUser(str(index), f"u{index}@x.io", 30)

    async def acknowledge(
        self, records: Sequence[fakes.RawUser], ctx: contract.ETLContext
    ) -> None:
        self.acked.append([record.id for record in records])


def test_extract_twice_without_teardown_is_refused() -> None:
    adapter = pipeline.AsyncExtractorAdapter(Numbers(1))
    ctx = fakes.make_context()

    with adapter.lifecycle(ctx):
        list(adapter.extract(ctx))
        second = adapter.extract(ctx)
        with pytest.raises(RuntimeError, match="already extracting"):
            next(second)


def test_acknowledge_outside_a_run_is_refused() -> None:
    adapter = pipeline.AsyncExtractorAdapter(Numbers(1))
    ctx = fakes.make_context()

    with pytest.raises(RuntimeError, match="outside of a run"):
        adapter.acknowledge([0], ctx)


def test_rejects_invalid_sizes() -> None:
    source = Numbers(1)

    with pytest.raises(ValueError):
        pipeline.AsyncExtractorAdapter(source, buffer_size=0)
    with pytest.raises(ValueError):
        pipeline.AsyncExtractorAdapter(source, chunk_size=0)
