"""Sending HTTP requests with rate limiting and retries."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
import dataclasses

import httpx

from core import contract
from shared import utils


@dataclasses.dataclass(frozen=True, slots=True)
class HttpRetry:
    """Request-level retries, for transient failures.

    Network errors and the statuses in `retry_on` are retried. A `Retry-After`
    header, in seconds, wins over the backoff. Once the attempts are exhausted
    a `RetryableError` is raised, so that the step's own `RetryPolicy` can
    still retry the whole step later.

    Attributes:
        max_attempts: The most attempts per request.
        backoff_seconds: The delay before the second attempt.
        backoff_multiplier: How much the delay grows after each attempt.
        max_backoff_seconds: The longest delay, `Retry-After` included.
        retry_on: The HTTP statuses worth a retry.
    """

    max_attempts: int = 5
    backoff_seconds: float = 0.5
    backoff_multiplier: float = 2.0
    max_backoff_seconds: float = 30.0
    retry_on: frozenset[int] = frozenset({408, 429, 500, 502, 503, 504})

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")

    def delay_for(self, attempt: int, response: httpx.Response | None) -> float:
        """Returns the delay before the attempt after `attempt` (from 1)."""
        retry_after = response.headers.get("Retry-After") if response else None
        if retry_after is not None and retry_after.isdigit():
            return min(float(retry_after), self.max_backoff_seconds)

        delay = self.backoff_seconds * self.backoff_multiplier ** (attempt - 1)
        return min(delay, self.max_backoff_seconds)


class HttpRequester:
    """Sends requests through a rate limiter, retrying transient failures.

    - network error or retryable status: retried, then `RetryableError`;
    - any other 4xx: `FatalError` right away, retrying a bad request is
      pointless;
    - any other 5xx: `RetryableError` right away.

    Attributes:
        client: The HTTP client, bound to one event loop.
        retry: The retry settings.
        rate_limit: Throttles every attempt, if set.
    """

    def __init__(
        self,
        client: httpx.AsyncClient,
        retry: HttpRetry | None = None,
        rate_limit: utils.TokenBucket | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.client = client
        self.retry = retry or HttpRetry()
        self.rate_limit = rate_limit
        self._sleep = sleep

    async def send(self, request: httpx.Request) -> httpx.Response:
        """Sends `request`, retrying transient failures.

        Args:
            request: The request.

        Returns:
            A successful response.

        Raises:
            RetryableError: On a transient failure that outlasted the retries.
            FatalError: On a client error (4xx) that is not retryable.
        """
        attempt = 0
        while True:
            attempt += 1
            if self.rate_limit is not None:
                await self.rate_limit.acquire_async()

            response: httpx.Response | None = None
            try:
                response = await self.client.send(request)
            except httpx.TransportError as exc:
                failure = f"{type(exc).__name__}: {exc}"
            else:
                if response.status_code not in self.retry.retry_on:
                    return _checked(response)
                failure = f"HTTP {response.status_code}"

            if attempt >= self.retry.max_attempts:
                raise contract.RetryableError(
                    f"{request.method} {request.url} failed after {attempt}"
                    f" attempt(s): {failure}"
                )
            await self._sleep(self.retry.delay_for(attempt, response))


def _checked(response: httpx.Response) -> httpx.Response:
    """Returns a successful response; raises on any other."""
    if response.is_success:
        return response

    message = (
        f"{response.request.method} {response.request.url}: HTTP"
        f" {response.status_code}"
    )
    if 400 <= response.status_code < 500:
        raise contract.FatalError(message)
    raise contract.RetryableError(message)
