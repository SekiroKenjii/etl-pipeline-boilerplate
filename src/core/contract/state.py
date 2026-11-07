"""The port of the state kept between runs.

Watermarks, processed objects, API cursors... Update the state only once the
data it describes is loaded, that is from the extractor's `acknowledge` hook:
updating it while extracting loses data when the run fails before loading.
"""

from __future__ import annotations

import abc

type StateValue = (
    str | int | float | bool | None | list[StateValue] | dict[str, StateValue]
)


class IStateStore(abc.ABC):
    """Durable key-value state, partitioned by namespace.

    The namespace is usually the pipeline name. Implementations must be safe
    to use from several threads, as steps may run in parallel.
    """

    @abc.abstractmethod
    def get(self, namespace: str, key: str) -> StateValue:
        """Returns a copy of the value of `key`, or None when it is not set."""

    @abc.abstractmethod
    def set(self, namespace: str, key: str, value: StateValue) -> None:
        """Saves `value` durably."""

    @abc.abstractmethod
    def delete(self, namespace: str, key: str) -> None:
        """Forgets `key`; deleting a missing key does nothing."""
