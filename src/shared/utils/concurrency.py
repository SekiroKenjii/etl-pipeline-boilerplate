"""Bounded, lazy parallelism over threads."""

from __future__ import annotations

import collections
from collections.abc import Callable, Generator, Iterable
from concurrent import futures


def bounded_map[T, R](
    fn: Callable[[T], R],
    items: Iterable[T],
    max_workers: int,
    *,
    ordered: bool = True,
    max_pending: int | None = None,
) -> Generator[R]:
    """Applies `fn` to `items` in a thread pool, lazily.

    Unlike `Executor.map`, which submits every item up front, at most
    `max_pending` calls are in flight: `items` is consumed only as fast as
    results are read (backpressure), so it can be a huge or endless stream.
    Closing the generator cancels the calls that have not started yet.

    Args:
        fn: The function to apply to each item.
        items: The items, consumed lazily.
        max_workers: The number of threads.
        ordered: Whether results come in the order of `items`. When False,
            each result comes as soon as it is ready.
        max_pending: The most calls in flight at once. Defaults to
            `2 * max_workers`.

    Yields:
        The result of `fn` for each item.

    Raises:
        ValueError: If `max_workers` or `max_pending` is below 1.
        Exception: Whatever `fn` raised, when its result is reached.
    """
    if max_workers < 1:
        raise ValueError("max_workers must be >= 1")

    limit = max_pending if max_pending is not None else 2 * max_workers
    if limit < 1:
        raise ValueError("max_pending must be >= 1")

    pool = futures.ThreadPoolExecutor(max_workers=max_workers)
    in_flight: collections.deque[futures.Future[R]] = collections.deque()
    try:
        for item in items:
            in_flight.append(pool.submit(fn, item))
            if len(in_flight) >= limit:
                yield _next_result(in_flight, ordered)

        while in_flight:
            yield _next_result(in_flight, ordered)
    finally:
        for future in in_flight:
            future.cancel()
        pool.shutdown(wait=True, cancel_futures=True)


def _next_result[R](
    in_flight: collections.deque[futures.Future[R]], ordered: bool
) -> R:
    if ordered:
        return in_flight.popleft().result()

    done, _ = futures.wait(in_flight, return_when=futures.FIRST_COMPLETED)
    future = next(iter(done))
    in_flight.remove(future)
    return future.result()
