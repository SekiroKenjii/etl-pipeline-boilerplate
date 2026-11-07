"""Rate limiting."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
import threading
import time


class TokenBucket:
    """A token-bucket rate limiter, thread-safe, for sync and async code.

    On average `rate` acquisitions per second, with bursts of up to
    `capacity`. Callers reserve their tokens immediately and then wait for
    them, so waiting is first come, first served and never happens while
    holding the lock.

    Attributes:
        rate: The tokens added per second.
        capacity: The most tokens the bucket holds (the largest burst).
    """

    def __init__(
        self,
        rate: float,
        capacity: float | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """Initializes a full bucket.

        Args:
            rate: The tokens added per second.
            capacity: The most tokens the bucket holds. Defaults to `rate`.
            clock: A monotonic clock, in seconds.

        Raises:
            ValueError: If `rate` or `capacity` is not positive.
        """
        if rate <= 0:
            raise ValueError("rate must be > 0")

        self.rate = rate
        self.capacity = capacity if capacity is not None else rate
        if self.capacity <= 0:
            raise ValueError("capacity must be > 0")

        self._clock = clock
        self._tokens = self.capacity
        self._updated_at = clock()
        self._lock = threading.Lock()

    def reserve(self, tokens: float = 1) -> float:
        """Takes `tokens` now.

        Args:
            tokens: How many tokens to take.

        Returns:
            How many seconds to wait before using them.

        Raises:
            ValueError: If `tokens` is more than the capacity.
        """
        if tokens > self.capacity:
            raise ValueError(
                f"cannot take {tokens} tokens from a bucket of {self.capacity}"
            )

        with self._lock:
            now = self._clock()
            elapsed = now - self._updated_at
            self._tokens = min(
                self.capacity, self._tokens + elapsed * self.rate
            )
            self._updated_at = now
            self._tokens -= tokens

            return 0.0 if self._tokens >= 0 else -self._tokens / self.rate

    def acquire(
        self, tokens: float = 1, sleep: Callable[[float], None] = time.sleep
    ) -> None:
        """Takes `tokens`, sleeping until they are available."""
        delay = self.reserve(tokens)
        if delay > 0:
            sleep(delay)

    async def acquire_async(self, tokens: float = 1) -> None:
        """Takes `tokens`, awaiting until they are available."""
        delay = self.reserve(tokens)
        if delay > 0:
            await asyncio.sleep(delay)
