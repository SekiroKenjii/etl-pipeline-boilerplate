"""The port of message logs and queues, read run after run.

A run reads the messages after the last handled one, up to the end of the
log at its start, and confirms what it loaded. Positions are opaque: where a
message is is the source's business.
"""

from __future__ import annotations

import abc
from collections.abc import Iterator, Mapping
import dataclasses
import datetime

from core.contract import state


@dataclasses.dataclass(frozen=True, slots=True)
class Message:
    """A message of a log or queue.

    Attributes:
        id: Identifies the message in its source, for people and dead
            letters: `<topic>[<partition>]@<offset>` on Kafka, the message
            id of a queue.
        key: Its key, as sent; None if it has none.
        value: Its value, as sent; None for a tombstone.
        headers: Its headers (attributes, properties).
        timestamp: When it was produced, if known.
        position: Where the source is once this message is handled: what
            `IMessageSource.confirm` takes and the next run starts after
            (the next offset of every partition read, the tokens to
            acknowledge...). Opaque, JSON-compatible; only the source
            compares positions.
    """

    id: str
    key: bytes | None
    value: bytes | None
    headers: Mapping[str, bytes] = dataclasses.field(
        default_factory=dict[str, bytes]
    )
    timestamp: datetime.datetime | None = None
    position: state.StateValue = None


class IMessageSource(abc.ABC):
    """A topic or queue, read from a position.

    Positions are opaque, JSON-compatible values the source returns and
    compares (`covers`). A source may hold a connection, opened on first use:
    whoever asked for it (`IMessageLog.topic`) closes it (`close`).
    """

    @property
    @abc.abstractmethod
    def topic(self) -> str:
        """The topic, queue or subscription read."""

    @abc.abstractmethod
    def beginning(self) -> state.StateValue:
        """Returns the position of the oldest message kept."""

    @abc.abstractmethod
    def end(self) -> state.StateValue:
        """Returns the position after the newest message: "now"."""

    @abc.abstractmethod
    def messages(
        self, after: state.StateValue, until: state.StateValue = None
    ) -> Iterator[Message]:
        """Yields the messages from `after` up to `until` (excluded).

        Messages come in the order of the log, or of each of its
        partitions; partitions interleave.

        Raises:
            FatalError: If `after` is older than what the log keeps.
        """

    @abc.abstractmethod
    def covers(self, position: state.StateValue, message: Message) -> bool:
        """Whether `position` is past `message`: handling up to `position`
        handled it. False for no position."""

    @abc.abstractmethod
    def confirm(self, position: state.StateValue) -> None:
        """Tells the broker the messages up to `position` are handled: the
        consumer group's offsets, or acknowledgements."""

    def close(self) -> None:
        """Releases the connection of the source, if any; the next use opens
        another. Does nothing by default."""


class IMessageLog(abc.ABC):
    """A message broker, or a cluster of it: a source per topic.

    Lets a business module read topics without knowing the broker; the
    composition root picks it from the settings. It holds no connection
    itself: each source it returns does, until closed.
    """

    @abc.abstractmethod
    def topic(self, name: str) -> IMessageSource:
        """Returns the source of topic `name`."""


class IDeadLetters(abc.ABC):
    """Where messages that cannot be handled go, so that the others can.

    A message that does not decode would stop its topic for good: it goes
    aside, as it was, with why and where it came from, for someone to look
    into, and the run goes on.
    """

    @abc.abstractmethod
    def send(self, message: Message, reason: str) -> None:
        """Keeps a message that cannot be handled, and why."""
