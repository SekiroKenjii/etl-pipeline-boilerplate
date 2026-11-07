"""Change data capture: a snapshot, then the changes, deletes included."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
import dataclasses
import datetime
import enum

from core import contract
from core import pipeline
from core.observability import metrics
from shared import utils


@dataclasses.dataclass(frozen=True, slots=True)
class Snapshot:
    """How to read the initial snapshot of a table.

    Attributes:
        reader: Reads the table.
        query: What to read: usually the whole table, by primary key.
    """

    reader: contract.ITableReader
    query: contract.TableQuery


class ChangeExtractor(pipeline.IDataExtractor[contract.ChangeEvent]):
    """Extracts a snapshot of a table, then its changes, run after run.

    The first run takes the position of the change log, reads the snapshot (as
    INSERT events), then the changes since that position: nothing is missed.
    Later runs read the changes after the last loaded one, up to the end of the
    log at their start.

        ChangeExtractor(
            relational.SqlServerChangeTracking(db, "dbo.customer", ["id"]),
            state=state,
            snapshot=Snapshot(reader, TableQuery("dbo.customer", ("id",))),
        )

    Positions are saved when their batch is acknowledged (loaded): changes are
    delivered at least once, so load them with upserts (`ChangeLoader`). A run
    failing during the snapshot reads it again from the start. The source
    flushes what it set aside (`IChangeSource.flush`: dead letters) before a
    position is saved; entries skipped after the last change are passed once
    every record of the run is acknowledged (`skipped_to`).

    Attributes:
        source: The change log.
        snapshot: The initial snapshot, if any; without one, the first run
            starts from the current end of the log.
    """

    def __init__(
        self,
        source: contract.IChangeSource,
        *,
        state: contract.IStateStore,
        state_key: str | None = None,
        snapshot: Snapshot | None = None,
        from_beginning: bool = False,
    ) -> None:
        """Initializes the extractor.

        Args:
            source: The change log.
            state: Saves the position, between runs.
            state_key: The state key; `changes:<table>` by default.
            snapshot: The initial snapshot, if any.
            from_beginning: Whether the first run reads the log from its
                beginning instead of now: for logs holding their own snapshot,
                like a Debezium topic. Exclusive with `snapshot`.

        Raises:
            ValueError: If both `snapshot` and `from_beginning` are given.
        """
        if snapshot is not None and from_beginning:
            raise ValueError("snapshot and from_beginning exclude each other")
        self.source = source
        self.snapshot = snapshot
        self._from_beginning = from_beginning
        self._state = state
        self._state_key = state_key or f"changes:{source.table}"
        self._start: contract.StateValue = None
        self._snapshot_total: int | None = None
        self._snapshot_acked = 0
        self._snapshot_saved = False
        self._yielded = 0
        self._acked = 0
        self._skipped_to: contract.StateValue = None

    @property
    def name(self) -> str:
        return f"{type(self).__name__}({self.source.table})"

    def teardown(self, ctx: contract.ETLContext) -> None:
        self.source.close()

    def saved_position(self, ctx: contract.ETLContext) -> contract.StateValue:
        """Returns the position after the last loaded change; None at first."""
        saved = self._state.get(ctx.run.pipeline_name, self._state_key)
        return saved.get("position") if isinstance(saved, dict) else None

    def extract(
        self, ctx: contract.ETLContext
    ) -> Iterator[contract.ChangeEvent]:
        self._snapshot_total = None
        self._snapshot_acked = 0
        self._snapshot_saved = False
        self._yielded = self._acked = 0
        self._skipped_to = None
        after = self.saved_position(ctx)

        if after is None and self._from_beginning:
            self._start = after = self.source.beginning()
            if not ctx.run.dry_run:
                self._save(ctx, after)
        elif after is None:
            # Before the snapshot: the changes start here.
            self._start = after = self.source.position()
            if self.snapshot is None and not ctx.run.dry_run:
                self._save(ctx, after)
            else:
                count = 0
                for count, event in enumerate(self._snapshot(), start=1):
                    self._yielded += 1
                    yield event
                self._snapshot_total = count
                # Acknowledged as pulled: maybe already, or there were none.
                if self._snapshot_acked >= count and not ctx.run.dry_run:
                    self._save(ctx, after)

        until = self.source.position()
        for event in self.source.changes(after, until):
            self._yielded += 1
            yield event
        # Moving past trailing tombstones and dead letters waits for every ack.
        skipped_to = self.source.skipped_to()
        if skipped_to is not None and not ctx.run.dry_run:
            if self._acked >= self._yielded:
                self._loaded(ctx, skipped_to)
            else:
                self._skipped_to = skipped_to

    def acknowledge(
        self, records: Sequence[contract.ChangeEvent], ctx: contract.ETLContext
    ) -> None:
        """Saves the position of the last change of the batch, and confirms it.

        The snapshot counts as loaded once all its rows are acknowledged. The
        source stores what it set aside up to the position first (`flush`).
        """
        self._acked += len(records)
        last: contract.StateValue = None
        committed: datetime.datetime | None = None
        for event in records:
            if event.position is None:
                self._snapshot_acked += 1
            else:
                last = event.position
                committed = event.committed_at or committed
        if committed is not None:
            metrics.meter().gauge(
                "etl.changes.lag",
                max(0.0, (utils.utc_now() - committed).total_seconds()),
                {
                    "etl.pipeline": ctx.run.pipeline_name,
                    "etl.table": self.source.table,
                },
                unit="s",
                description="How old the last change loaded was",
            )
        if (
            not self._snapshot_saved
            and self._snapshot_total is not None
            and self._snapshot_acked >= self._snapshot_total
        ):
            self._save(ctx, self._start)
        if self._skipped_to is not None and self._acked >= self._yielded:
            last, self._skipped_to = self._skipped_to, None
        if last is not None:
            self._loaded(ctx, last)

    def _snapshot(self) -> Iterator[contract.ChangeEvent]:
        """Yields the rows of the snapshot as INSERT events."""
        if self.snapshot is None:
            return
        query = self.snapshot.query
        for row in self.snapshot.reader.read(query):
            yield contract.ChangeEvent(
                table=self.source.table,
                operation=contract.ChangeOperation.INSERT,
                row=row,
                key=tuple(row[column] for column in query.order_by),
            )

    def _loaded(
        self, ctx: contract.ETLContext, position: contract.StateValue
    ) -> None:
        """Moves to `position`: what is up to it is loaded."""
        self.source.flush(position)
        self._save(ctx, position)
        self.source.confirm(position)

    def _save(
        self, ctx: contract.ETLContext, position: contract.StateValue
    ) -> None:
        self._state.set(
            ctx.run.pipeline_name, self._state_key, {"position": position}
        )
        self._snapshot_saved = True


class Deletes(enum.StrEnum):
    """What a loader does with a deleted row."""

    HARD = "hard"
    SOFT = "soft"
    IGNORE = "ignore"


class ChangeLoader[T](pipeline.IDataLoader[T]):
    """Applies changes to a table: upserts, and deletes as the target wants.

    Changes apply in the order of the batch: a row updated then deleted stays
    deleted, a row deleted then written again comes back. A deleted record's row
    holds the columns to match: the key, or a parent key
    (`MappingTransformer(scope=...)`) to replace the children of a row.

        ChangeLoader(
            writer,
            contract.TableTarget("crm.customer", key=("customer_key",)),
            row=lambda record: record.data,
            deleted=lambda record: record.deleted,
            deletes=Deletes.SOFT,
        )

    Soft deletes set a flag column (`is_deleted`), cleared when the row comes
    back, and optionally a timestamp column. A batch is several writes: a
    failure in between leaves the first ones done, and the retried batch is
    idempotent.

    Attributes:
        writer: The adapter rows are written with.
        target: The table and how upserts are written.
        deletes: What is done with deleted rows.
        deleted_flag: The flag column of soft deletes.
        deleted_at: The timestamp column of soft deletes, if any.
    """

    def __init__(
        self,
        writer: contract.ITableWriter,
        target: contract.TableTarget,
        *,
        row: Callable[[T], contract.Row],
        deleted: Callable[[T], bool],
        deletes: Deletes = Deletes.HARD,
        deleted_flag: str = "is_deleted",
        deleted_at: str | None = None,
    ) -> None:
        """Initializes the loader.

        Args:
            writer: The adapter rows are written with.
            target: The table and how upserts are written.
            row: Gets the row of a record.
            deleted: Whether a record is a deletion.
            deletes: What is done with deleted rows.
            deleted_flag: The flag column of soft deletes.
            deleted_at: The timestamp column of soft deletes, if any.
        """
        self.writer = writer
        self.target = target
        self.deletes = deletes
        self.deleted_flag = deleted_flag
        self.deleted_at = deleted_at
        self._row = row
        self._deleted = deleted

    @property
    def name(self) -> str:
        return f"{type(self).__name__}({self.target.table})"

    def load(
        self, records: Sequence[T], ctx: contract.ETLContext
    ) -> contract.ETLStats:
        stats = contract.ETLStats(successful=len(records))
        segment = _Segment()
        for record in records:
            row = self._row(record)
            if not self._deleted(record):
                segment.upserts.append(row)
            elif segment.touches(row):
                # The delete comes after an upsert of its row: write both first.
                self._apply(segment, stats)
                segment = _Segment()
                segment.removals.setdefault(tuple(row), []).append(row)
            else:
                segment.removals.setdefault(tuple(row), []).append(row)
        self._apply(segment, stats)
        return stats

    def _apply(self, segment: _Segment, stats: contract.ETLStats) -> None:
        """Writes the deletes of a segment, then its upserts."""
        for columns, rows in segment.removals.items():
            stats.deleted += self._remove(columns, rows)
        upserts = segment.upserts
        if upserts:
            if self.deletes is Deletes.SOFT:
                upserts = [self._revived(row) for row in upserts]
            written = self.writer.write(self.target, upserts)
            stats.created += written.created
            stats.updated += written.updated

    def _remove(
        self, columns: tuple[str, ...], rows: list[contract.Row]
    ) -> int:
        """Applies the deletion of rows matched on `columns`."""
        if self.deletes is Deletes.IGNORE:
            return 0
        if self.deletes is Deletes.HARD:
            return self.writer.delete(self.target.table, rows)
        flags: dict[str, object] = {self.deleted_flag: True}
        if self.deleted_at is not None:
            flags[self.deleted_at] = utils.utc_now()
        written = self.writer.write(
            contract.TableTarget(
                self.target.table,
                key=columns,
                mode=contract.WriteMode.UPDATE,
                update_columns=tuple(flags),
            ),
            [{**row, **flags} for row in rows],
        )
        return written.updated

    def _revived(self, row: contract.Row) -> contract.Row:
        """A row written again is not deleted anymore."""
        revived: contract.Row = {**row, self.deleted_flag: False}
        if self.deleted_at is not None:
            revived[self.deleted_at] = None
        return revived


class _Segment:
    """Changes that can be written as deletes first, then upserts.

    A delete joins the segment unless it may match one of its upserts: rows
    are compared loosely (case and trailing blanks ignored, as some collations
    do), so a doubt starts a new segment rather than reorders the changes.

    Attributes:
        removals: The deleted rows, by the columns they are matched on.
        upserts: The rows to write, in order.
    """

    def __init__(self) -> None:
        self.removals: dict[tuple[str, ...], list[contract.Row]] = {}
        self.upserts: list[contract.Row] = []
        # columns -> (their values in the upserts, upserts indexed so far).
        self._indexes: dict[tuple[str, ...], tuple[set[object], int]] = {}

    def touches(self, deleted: contract.Row) -> bool:
        """Whether a deleted row may match one of the upserts."""
        if not self.upserts:
            return False
        columns = tuple(deleted)
        index, indexed = self._indexes.get(columns, (set(), 0))
        try:
            for row in self.upserts[indexed:]:
                index.add(_loose(row, columns))
            self._indexes[columns] = (index, len(self.upserts))
            return _loose(deleted, columns) in index
        except (KeyError, TypeError):
            # An upsert without a column, or a value that cannot be hashed.
            return True


def _loose(row: contract.Row, columns: tuple[str, ...]) -> tuple[object, ...]:
    """The values of `columns`, strings without case or trailing blanks."""
    return tuple(
        value.casefold().rstrip(" ") if isinstance(value, str) else value
        for value in (row[column] for column in columns)
    )
