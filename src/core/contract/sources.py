"""The port of asynchronous record sources: REST endpoints, queues..."""

from __future__ import annotations

import abc
from collections.abc import AsyncIterator, Mapping

from core.contract import lifecycle


class IAsyncRecordSource[T](lifecycle.AsyncLifecycle, abc.ABC):
    """Yields records for a set of parameters, asynchronously.

    A source knows the protocol of the system (pagination, retries...) but
    nothing of ETL: no context, no batches, no acknowledgement.
    """

    @abc.abstractmethod
    def records(self, params: Mapping[str, object]) -> AsyncIterator[T]:
        """Yields the records for `params`; implement as an async generator."""
