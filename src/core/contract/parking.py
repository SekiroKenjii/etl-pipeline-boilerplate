"""The port of the parking lot: records set aside, to be retried later.

A child row may arrive before its parent (late-arriving parent). Rejecting it
loses it until someone replays the quarantine; parking it keeps it aside and
retries it on the following runs, until its parent is loaded.
"""

from __future__ import annotations

import abc
from collections.abc import Iterator, Sequence
import dataclasses
import datetime

from core.contract import state


@dataclasses.dataclass(frozen=True, slots=True)
class ParkedEntry:
    """A record in the parking lot.

    Attributes:
        identity: Identifies the record in its queue: parking it again
            replaces the entry, and counts one more attempt.
        payload: The record, encoded as JSON-compatible data.
        reason: Why it was parked.
        attempts: How many times it was parked.
        parked_at: When it was first parked; set by the lot.
    """

    identity: str
    payload: state.StateValue
    reason: str
    attempts: int = 1
    parked_at: datetime.datetime | None = None


class IParkingLot(abc.ABC):
    """Durable queues of parked records, by name (usually the mapping).

    Implementations must be thread-safe.
    """

    @abc.abstractmethod
    def park(self, queue: str, entries: Sequence[ParkedEntry]) -> None:
        """Parks records; an entry already parked gets one more attempt."""

    @abc.abstractmethod
    def entries(self, queue: str) -> Iterator[ParkedEntry]:
        """Yields the entries of `queue`, by identity."""

    @abc.abstractmethod
    def release(self, queue: str, entries: Sequence[ParkedEntry]) -> None:
        """Removes entries that were retried successfully.

        An entry parked again since it was read (its attempts changed) is
        kept: it is still waiting.
        """
