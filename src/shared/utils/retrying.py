"""Trying a call again, with backoff and jitter."""

from __future__ import annotations

from collections.abc import Callable
import random
import time


def retry[T](
    call: Callable[[], T],
    *,
    when: Callable[[BaseException], bool],
    attempts: int = 5,
    delay: float = 0.05,
) -> T:
    """Returns the result of `call`, trying again on the errors it allows.

    The waits grow (`delay`, then twice, four times...) and are jittered, so
    that callers racing each other do not collide again at the same moment.

    Args:
        call: What to try.
        when: Whether an error is worth another try.
        attempts: The tries at most, the first one included.
        delay: The base wait between tries, in seconds.

    Returns:
        What `call` returned.

    Raises:
        ValueError: If `attempts` is below 1.
        BaseException: The last error, or the first one `when` refuses.
    """
    if attempts < 1:
        raise ValueError("attempts must be >= 1")
    for attempt in range(1, attempts + 1):
        try:
            return call()
        except Exception as exc:  # pylint: disable=broad-exception-caught
            if attempt == attempts or not when(exc):
                raise
            backoff = delay * pow(2, attempt - 1)
            # Spreads retries: no security depends on it.
            jitter = random.uniform(0.5, 1.5)  # nosec B311
            time.sleep(backoff * jitter)
    raise AssertionError("unreachable")  # pragma: no cover
