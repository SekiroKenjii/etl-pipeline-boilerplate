from __future__ import annotations

import asyncio
from collections.abc import Iterator
import pathlib
import threading

import pytest

from shared import utils


def _counting(limit: int, consumed: list[int]) -> Iterator[int]:
    for item in range(limit):
        consumed.append(item)
        yield item


class TestBoundedMap:

    def test_keeps_input_order_by_default(self) -> None:
        assert list(
            utils.bounded_map(lambda x: x * 2, range(10), max_workers=4)
        ) == [x * 2 for x in range(10)]

    def test_unordered_returns_every_result(self) -> None:
        results = utils.bounded_map(
            lambda x: x, range(20), max_workers=4, ordered=False
        )

        assert sorted(results) == list(range(20))

    def test_runs_calls_concurrently(self) -> None:
        barrier = threading.Barrier(3, timeout=2)

        def meet(x: int) -> int:
            barrier.wait()  # only passes if 3 calls run at the same time
            return x

        assert sorted(utils.bounded_map(meet, range(3), max_workers=3)) == [
            0,
            1,
            2,
        ]

    def test_consumes_the_input_lazily(self) -> None:
        consumed: list[int] = []
        results = utils.bounded_map(
            lambda x: x,
            _counting(1_000_000, consumed),
            max_workers=2,
            max_pending=4,
        )

        assert next(results) == 0
        assert len(consumed) <= 5  # not the whole input, unlike Executor.map
        results.close()

    def test_reraises_the_error_of_a_call(self) -> None:
        def explode(x: int) -> int:
            if x == 3:
                raise ValueError("bad item")
            return x

        with pytest.raises(ValueError, match="bad item"):
            list(utils.bounded_map(explode, range(10), max_workers=2))

    def test_rejects_invalid_sizes(self) -> None:
        with pytest.raises(ValueError):
            list(utils.bounded_map(lambda x: x, [1], max_workers=0))
        with pytest.raises(ValueError):
            list(
                utils.bounded_map(
                    lambda x: x, [1], max_workers=1, max_pending=0
                )
            )


class _Clock:

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class TestTokenBucket:

    def test_allows_a_burst_then_spaces_calls_out(self) -> None:
        clock = _Clock()
        bucket = utils.TokenBucket(rate=2, capacity=2, clock=clock)

        assert [bucket.reserve() for _ in range(4)] == [0.0, 0.0, 0.5, 1.0]

    def test_refills_over_time(self) -> None:
        clock = _Clock()
        bucket = utils.TokenBucket(rate=2, capacity=2, clock=clock)
        bucket.reserve(2)

        clock.now = 1.0

        assert bucket.reserve(2) == 0.0

    def test_acquire_sleeps_for_the_reserved_delay(self) -> None:
        slept: list[float] = []
        bucket = utils.TokenBucket(rate=4, capacity=1, clock=_Clock())

        bucket.acquire(sleep=slept.append)
        bucket.acquire(sleep=slept.append)

        assert slept == [0.25]

    def test_acquire_async(self) -> None:
        bucket = utils.TokenBucket(rate=1000, capacity=1)

        async def twice() -> None:
            await bucket.acquire_async()
            await bucket.acquire_async()

        asyncio.run(twice())

    def test_rejects_invalid_settings(self) -> None:
        with pytest.raises(ValueError):
            utils.TokenBucket(rate=0)
        bucket = utils.TokenBucket(rate=1, capacity=1)
        with pytest.raises(ValueError):
            bucket.reserve(2)


class TestAtomicWriteText:

    def test_writes_and_replaces(self, tmp_path: pathlib.Path) -> None:
        path = tmp_path / "nested" / "state.json"

        utils.atomic_write_text(path, "first")
        utils.atomic_write_text(path, "second")

        assert path.read_text(encoding="utf-8") == "second"
        assert list(path.parent.iterdir()) == [path]  # no temp file left

    def test_failed_write_keeps_the_old_content(
        self, tmp_path: pathlib.Path
    ) -> None:
        path = tmp_path / "state.json"
        utils.atomic_write_text(path, "safe")

        with pytest.raises(UnicodeEncodeError):
            utils.atomic_write_text(path, "café", encoding="ascii")

        assert path.read_text(encoding="utf-8") == "safe"
        assert list(tmp_path.iterdir()) == [path]
