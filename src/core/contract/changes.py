"""The port of change data capture (CDC): the changes of a table, in order.

Change sources read the database's own change log (SQL Server Change
Tracking, Postgres logical replication, MongoDB change streams): deletes
included, which a watermark cannot see.
"""

from __future__ import annotations

import abc
from collections.abc import Iterator
import dataclasses
import datetime
import enum
from typing import Any

from core.contract import exceptions
from core.contract import formats
from core.contract import state


class ChangeOperation(enum.StrEnum):
    """What happened to a row."""

    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"


@dataclasses.dataclass(frozen=True, slots=True)
class ChangeEvent:
    """A change of one row, or a row of the initial snapshot (an INSERT).

    Attributes:
        table: The table, or collection, the row belongs to.
        operation: What happened to it.
        row: The row after the change; for a DELETE, its key columns (or
            the row before, when the source has it).
        key: The values of the primary key of the row.
        position: Where the change log is after this change, to resume after
            it; None for a row of the snapshot. JSON-compatible.
        committed_at: When the change was committed, if the source says.
    """

    table: str
    operation: ChangeOperation
    row: formats.Row
    key: tuple[Any, ...]
    position: state.StateValue = None
    committed_at: datetime.datetime | None = None

    @property
    def deleted(self) -> bool:
        return self.operation is ChangeOperation.DELETE


class IChangeSource(abc.ABC):
    """The change log of one table, read from a position.

    Positions are opaque JSON-compatible values that only the source compares. A
    source may hold a connection, opened on first use: whoever asked for it
    (`IChangeLog.source`) closes it.
    """

    @property
    @abc.abstractmethod
    def table(self) -> str:
        """The table whose changes are read."""

    @abc.abstractmethod
    def position(self) -> state.StateValue:
        """Returns the current end of the change log: "now".

        Taken before an initial snapshot, it is where the changes start.
        """

    @abc.abstractmethod
    def changes(
        self, after: state.StateValue, until: state.StateValue = None
    ) -> Iterator[ChangeEvent]:
        """Yields the changes after `after` and up to `until`, in order.

        Args:
            after: A position this source returned.
            until: Where to stop, a position of `position()`; the end of the
                log when None. A run reads up to the end at its start, so
                that it ends.

        Raises:
            FatalError: If the log no longer goes back to `after` (retention
                expired): a new snapshot is needed.
        """

    def beginning(self) -> state.StateValue:
        """Returns the position of the oldest change the log keeps.

        For logs holding their own initial snapshot (Debezium's `r` events):
        a first run reads them from there. Most logs keep no history.

        Raises:
            PipelineConfigError: If the log cannot be read from its
                beginning.
        """
        raise exceptions.PipelineConfigError(
            f"The change log of {self.table} has no beginning to read from:"
            " take a snapshot instead"
        )

    def confirm(self, position: state.StateValue) -> None:
        """Tells the source that changes up to `position` are loaded.

        Sources holding their log for the reader (a Postgres replication slot)
        release it; others do nothing.
        """

    def skipped_to(self) -> state.StateValue:
        """Returns where the last `changes` ended, past its last change.

        A log may end with entries that are no changes (a Debezium tombstone,
        a message sent to the dead letters): no change carries a position
        past them, and every run would read them again. None when the last
        change was the last entry read (the default).
        """
        return None

    def flush(self, position: state.StateValue) -> None:
        """Stores what was set aside while reading up to `position`.

        Called once the changes up to `position` are loaded, before the
        position is saved: what a source sets aside (dead letters) is stored
        at least once, and once only for a batch read again after a failure.
        Does nothing by default.
        """

    def close(self) -> None:
        """Releases the connection of the source, if any; the next use opens
        another. Does nothing by default."""


class IChangeLog(abc.ABC):
    """The change log of a database: a change source per table.

    It holds no connection itself: each source it returns may, until closed.
    """

    @abc.abstractmethod
    def source(self, table: str) -> IChangeSource:
        """Returns the change source of `table`.

        Raises:
            PipelineConfigError: If the database has no usable change log.
        """
