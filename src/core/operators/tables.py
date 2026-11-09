"""Extracting from and loading into database tables, incrementally."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
import dataclasses
import datetime
import decimal
import typing
from typing import cast

from core import contract
from core import pipeline
from core.operators import types as operators_types
from shared import utils

type _Position = tuple[operators_types.Ordered, ...]


@dataclasses.dataclass(frozen=True, slots=True)
class SavedPosition:
    """The saved state of a `TableExtractor`.

    Attributes:
        position: The furthest row acknowledged; every row before it was read.
        recheck: Whether the rows at its watermark are to be read again.
        resume: The last row acknowledged by an unfinished read again.
    """

    position: _Position | None = None
    recheck: bool = False
    resume: _Position | None = None


class TableExtractor(pipeline.IDataExtractor[contract.Row]):
    """Extracts the rows of a table, incrementally by watermark.

    Rows are read in the order `(watermark, *key)`, by keyset pagination. With a
    state store, the position of the last acknowledged (loaded) row is saved and
    the next run starts right after it: a failed run starts again from the last
    loaded batch (at-least-once: load with upserts).

        TableExtractor(
            reader, "crm.customer", watermark="updated_at", key=["id"],
            state=state,
        )

    Without a watermark, rows are read by key (append-only tables). Without a
    state store, every run reads the whole table.

    Each run reads up to the largest watermark at its start; the next run reads
    the rows at that watermark again, for late commits with a smaller key, until
    one run has read them all: a run that fails or is cancelled half-way leaves
    the rest to the next. Rows committed late with an older watermark are missed
    unless `lookback` reads a window before the saved watermark.

    `ctx.run.since` and `ctx.run.until` override the saved position (a
    backfill). A backfill moves the saved position only when it starts at or
    before it, and never backwards: one that starts after it, or before any
    position is saved, saves nothing, as it skipped the rows before its start.
    An aware time against a naive column is compared as naive UTC.

    Index `(watermark, *key)` in the source.

    Attributes:
        reader: The adapter rows come from.
        table: The table, `schema.table`.
        watermark: The column tracking changes, e.g. `updated_at`.
        key: The columns identifying a row, the tie-breaker of the watermark.
        order_by: The order rows are read in: watermark, then key.
    """

    def __init__(
        self,
        reader: contract.ITableReader,
        table: str,
        *,
        key: Sequence[str],
        watermark: str | None = None,
        columns: Sequence[str] = (),
        filters: Sequence[contract.Condition] = (),
        state: contract.IStateStore | None = None,
        state_key: str | None = None,
        lookback: datetime.timedelta | int | None = None,
        snapshot: bool = True,
    ) -> None:
        """Initializes the extractor.

        Args:
            reader: The adapter rows come from.
            table: The table, `schema.table`.
            key: The columns identifying a row: unique, never NULL.
            watermark: The column tracking changes, never NULL: a last update
                time, a version, an increasing id.
            columns: The columns to read, every column by default.
            filters: Only rows matching these conditions, e.g.
                `Condition("tenant_id", Operator.EQ, "acme")`.
            state: Saves the position, for incremental runs.
            state_key: The state key; `table:<table>` by default.
            lookback: Reads again rows up to this much before the saved
                watermark: a duration for a time, a number for a version.
            snapshot: Whether to read only up to the largest watermark at the
                start of the run.

        Raises:
            ValueError: If `key` is empty, or `lookback` is set without a
                watermark.
        """
        if not key:
            raise ValueError("key must name at least one column")
        if lookback is not None and watermark is None:
            raise ValueError("lookback needs a watermark")

        self.reader = reader
        self.table = table
        self.watermark = watermark
        self.key = tuple(key)
        self.order_by = (watermark, *self.key) if watermark else self.key
        missing = [c for c in self.order_by if columns and c not in columns]
        if missing:
            raise ValueError(f"columns must include {missing}")

        self._columns = tuple(columns)
        self._filters = tuple(filters)
        self._state = state
        self._state_key = state_key or f"table:{table}"
        self._lookback = lookback
        self._snapshot = snapshot
        self._saved_now = SavedPosition()
        self._run_high_watermark: operators_types.Ordered | None = None
        self._rechecking: operators_types.Ordered | None = None
        self._moves_position = True
        self._reading_again = False
        self._last_read: contract.Row | None = None
        self._last_acknowledged: _Position | None = None
        self._exhausted = False

    @property
    def name(self) -> str:
        return f"{type(self).__name__}({self.table})"

    def query(self, ctx: contract.ETLContext) -> contract.TableQuery | None:
        """Returns what this run reads, None when there is nothing to read.

        Raises:
            FatalError: If the saved position was taken with other columns.
        """
        saved = self._saved(ctx)
        self._start(saved)
        since: operators_types.Ordered | None = ctx.run.since
        until: operators_types.Ordered | None = ctx.run.until
        high: operators_types.Ordered | None = None
        if self.watermark is not None and (
            self._snapshot or since is not None or until is not None
        ):
            high = cast(
                operators_types.Ordered | None,
                self.reader.max_value(
                    self.table, self.watermark, self._filters
                ),
            )
            if high is None:
                return None
            # Naive columns hold UTC times: compare and query like with like.
            since = utils.like_timezone(since, high)
            until = utils.like_timezone(until, high)

        after: _Position | None = None
        lower: operators_types.Ordered | None = None
        position = saved.position
        if self.watermark is not None and since is not None:
            lower = since
            self._moves_position = position is not None and since <= position[0]
        elif position is not None and self._lookback is not None:
            lower = self._before(position[0], self._lookback)
        elif position is not None and saved.recheck:
            # The rows at the end of the last run, with those committed since.
            after = saved.resume
            lower = None if after else position[0]
        else:
            after = position

        upper: operators_types.Ordered | None = None
        if self.watermark is not None:
            upper = until
            if self._snapshot and high is not None:
                upper = high if upper is None else min(upper, high)
                self._run_high_watermark = high
        if (
            saved.recheck
            and position is not None
            and self._moves_position
            and (upper is None or upper >= position[0])
        ):
            self._rechecking = position[0]
            self._reading_again = True

        return contract.TableQuery(
            table=self.table,
            order_by=self.order_by,
            columns=self._columns,
            after=after,
            lower=lower,
            upper=upper,
            filters=self._filters,
        )

    def extract(self, ctx: contract.ETLContext) -> Iterator[contract.Row]:
        query = self.query(ctx)
        if query is not None:
            yield from self._read(query, ctx)

    def acknowledge(
        self, records: Sequence[contract.Row], ctx: contract.ETLContext
    ) -> None:
        """Saves the position of the last row of the batch.

        Batches are acknowledged in order, and rows come in `order_by` order:
        the last row of a batch is the furthest. The position only moves
        forwards. At the largest watermark of the run, the next run is to read
        its rows again, unless this run did; a read again half-way saves how
        far it went.
        """
        if self._state is None or not records:
            return
        last = self._position_of(records[-1])
        self._last_acknowledged = last
        if not self._moves_position:
            return

        position = self._saved_now.position
        if position is None or last > position:
            self._reading_again = False
            self._save(ctx, SavedPosition(last, self._at_high_watermark(last)))
        elif self._reading_again:
            self._save(ctx, SavedPosition(position, True, last))
        self._finish_reading_again(ctx)

    def saved_position(self, ctx: contract.ETLContext) -> _Position | None:
        """Returns the position of the last acknowledged row, if any.

        Raises:
            FatalError: If it was saved for other `order_by` columns: the
                position would not mean the same. Delete the state key to
                start over.
        """
        return self._saved(ctx).position

    def _start(self, saved: SavedPosition) -> None:
        """Starts the bookkeeping of a run from the saved state."""
        self._saved_now = saved
        self._run_high_watermark = self._rechecking = None
        self._moves_position = True
        self._reading_again = False
        self._last_read = self._last_acknowledged = None
        self._exhausted = False

    def _read(
        self, query: contract.TableQuery, ctx: contract.ETLContext
    ) -> Iterator[contract.Row]:
        """Yields the rows of `query`, noting how far the read went."""
        for row in self.reader.read(query):
            self._last_read = row
            yield row
        self._exhausted = True
        # Every batch may be acknowledged already: the last one was full.
        self._finish_reading_again(ctx)

    def _finish_reading_again(self, ctx: contract.ETLContext) -> None:
        """Clears the read again once every row of it was acknowledged."""
        if (
            self._reading_again
            and self._exhausted
            and not ctx.run.dry_run
            and self._read_is_acknowledged()
        ):
            self._reading_again = False
            self._save(ctx, SavedPosition(self._saved_now.position))

    def _read_is_acknowledged(self) -> bool:
        if self._last_read is None:
            return self._last_acknowledged is None
        return self._position_of(self._last_read) == self._last_acknowledged

    def _at_high_watermark(self, position: _Position) -> bool:
        """Whether the next run is to read the rows at `position` again."""
        return (
            self._run_high_watermark is not None
            and position[0] == self._run_high_watermark
            and self._run_high_watermark != self._rechecking
        )

    def _position_of(self, row: contract.Row) -> _Position:
        return tuple(
            cast(operators_types.Ordered, row[column])
            for column in self.order_by
        )

    def _before(
        self,
        watermark: operators_types.Ordered,
        lookback: datetime.timedelta | int,
    ) -> operators_types.Ordered:
        """Returns `watermark` minus `lookback`.

        Raises:
            FatalError: If the lookback does not apply to the watermark: a
                duration for a time, a number for a version.
        """
        if isinstance(lookback, datetime.timedelta) and isinstance(
            watermark, datetime.date
        ):
            return watermark - lookback
        if isinstance(lookback, int) and isinstance(
            watermark, (int, float, decimal.Decimal)
        ):
            return watermark - lookback
        raise contract.FatalError(
            f"{self.name}: a lookback of {lookback!r} does not apply to"
            f" {self.watermark}, a {type(watermark).__name__}"
        )

    def _save(self, ctx: contract.ETLContext, saved: SavedPosition) -> None:
        """Saves `saved` as the state, unless it is saved already."""
        if self._state is None or saved == self._saved_now:
            return
        value: dict[str, contract.StateValue] = {
            "columns": list(self.order_by),
            "position": cast(
                contract.StateValue, utils.to_tagged(list(saved.position or ()))
            ),
            "recheck": saved.recheck,
        }
        if saved.resume is not None:
            value["resume"] = cast(
                contract.StateValue, utils.to_tagged(list(saved.resume))
            )
        self._state.set(ctx.run.pipeline_name, self._state_key, value)
        self._saved_now = saved

    def _saved(self, ctx: contract.ETLContext) -> SavedPosition:
        """Returns the saved state, empty when there is none.

        Raises:
            FatalError: If the position was saved for other columns.
        """
        if self._state is None:
            return SavedPosition()
        saved = self._state.get(ctx.run.pipeline_name, self._state_key)
        if not isinstance(saved, dict):
            return SavedPosition()

        if saved.get("columns") != list(self.order_by):
            raise contract.FatalError(
                f"{self.name}: the saved position of state key"
                f" {self._state_key!r} is on {saved.get('columns')}, not on"
                f" {list(self.order_by)}. Delete it to extract from scratch."
            )
        recheck = saved.get("recheck") is True
        return SavedPosition(
            self._tuple(saved["position"]),
            recheck,
            self._tuple(saved["resume"])
            if recheck and "resume" in saved
            else None,
        )

    def _tuple(self, value: contract.StateValue) -> _Position:
        found = utils.from_tagged(value)
        if not isinstance(found, list):
            raise contract.FatalError(
                f"{self.name}: state key {self._state_key!r} is corrupted"
            )
        return tuple(typing.cast(list[operators_types.Ordered], found))


class TableLoader[T](pipeline.IDataLoader[T]):
    """Loads records into a table, through any `ITableWriter` adapter.

    Each batch is one transaction, in the mode of the target (UPSERT by default:
    a retried batch does not duplicate rows).

        TableLoader(
            writer,
            contract.TableTarget("crm.customer", key=("customer_key",)),
            row=lambda record: record.data,
        )

    Attributes:
        writer: The adapter rows are written with.
        target: The table and how to write it.
    """

    def __init__(
        self,
        writer: contract.ITableWriter,
        target: contract.TableTarget,
        row: Callable[[T], contract.Row] | None = None,
    ) -> None:
        """Initializes the loader.

        Args:
            writer: The adapter rows are written with.
            target: The table and how to write it.
            row: Gets the row of a record, by column; the record itself by
                default (a dict).
        """
        self.writer = writer
        self.target = target
        self._row = row

    @property
    def name(self) -> str:
        return f"{type(self).__name__}({self.target.table})"

    def load(
        self, records: Sequence[T], ctx: contract.ETLContext
    ) -> contract.ETLStats:
        """Writes the batch; unchanged rows count as successful.

        Raises:
            FatalError: If a record is not a row and no `row` was given.
        """
        rows = [self._as_row(record) for record in records]
        written = self.writer.write(self.target, rows)
        return contract.ETLStats(
            successful=len(rows),
            created=written.created,
            updated=written.updated,
        )

    def _as_row(self, record: T) -> contract.Row:
        if self._row is not None:
            return self._row(record)
        if not isinstance(record, dict):
            raise contract.FatalError(
                f"{self.name} expects rows as dicts, got"
                f" {type(record).__name__}: pass `row=` to extract them"
            )
        return typing.cast(contract.Row, record)
