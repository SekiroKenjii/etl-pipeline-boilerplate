"""Extracting from and loading into database tables, incrementally."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
import datetime
import typing
from typing import Any, cast

from core import contract
from core import pipeline
from shared import utils


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
    the rows at that watermark again, once, for late commits with a smaller key.
    Rows committed late with an older watermark are missed unless `lookback`
    reads a window before the saved watermark.

    `ctx.run.since` and `ctx.run.until` override the saved position (a
    backfill); the saved watermark never moves backwards. An aware time against
    a naive column is compared as naive UTC.

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
        self._position: tuple[Any, ...] | None = None
        self._run_high_watermark: Any = None
        self._rechecked: Any = None
        self._recheck_saved_watermark = False

    @property
    def name(self) -> str:
        return f"{type(self).__name__}({self.table})"

    def query(self, ctx: contract.ETLContext) -> contract.TableQuery | None:
        """Returns what this run reads, None when there is nothing to read.

        Raises:
            FatalError: If the saved position was taken with other columns.
        """
        saved, recheck = self._saved(ctx)
        self._position, self._recheck_saved_watermark = saved, recheck
        self._run_high_watermark = self._rechecked = None
        since, until, high = ctx.run.since, ctx.run.until, None
        if self.watermark is not None and (
            self._snapshot or since is not None or until is not None
        ):
            high = self.reader.max_value(
                self.table, self.watermark, self._filters
            )
            if high is None:
                return None
            # Naive columns hold UTC times: compare and query like with like.
            since = utils.like_timezone(since, high)
            until = utils.like_timezone(until, high)

        after: tuple[Any, ...] | None = None
        lower: Any = None
        upper: Any = None
        if self.watermark is not None and since is not None:
            lower = since
        elif saved is not None and self._lookback is not None:
            lower = saved[0] - self._lookback
        elif saved is not None and recheck:
            # The rows at the end of the last run, with those committed since.
            lower = self._rechecked = saved[0]
        else:
            after = saved

        if self.watermark is not None:
            upper = until
            if self._snapshot:
                upper = high if upper is None else min(upper, high)
                self._run_high_watermark = high

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
            yield from self.reader.read(query)

    def acknowledge(
        self, records: Sequence[contract.Row], ctx: contract.ETLContext
    ) -> None:
        """Saves the position of the last row of the batch.

        Batches are acknowledged in order, and rows come in `order_by` order:
        the last row of a batch is the furthest. The position only moves
        forwards: a backfill or a lookback never moves it back. At the
        largest watermark of the run, the next run is to read its rows again,
        unless this run did.
        """
        if self._state is None or not records:
            return

        position = tuple(records[-1][column] for column in self.order_by)
        if self._position is not None and position <= self._position:
            position = self._position
        recheck = (
            self._run_high_watermark is not None
            and position[0] == self._run_high_watermark
            and self._run_high_watermark != self._rechecked
        )
        if (
            position == self._position
            and recheck == self._recheck_saved_watermark
        ):
            return

        self._state.set(
            ctx.run.pipeline_name,
            self._state_key,
            {
                "columns": list(self.order_by),
                "position": cast(
                    contract.StateValue, utils.to_tagged(list(position))
                ),
                "recheck": recheck,
            },
        )
        self._position, self._recheck_saved_watermark = position, recheck

    def saved_position(
        self, ctx: contract.ETLContext
    ) -> tuple[Any, ...] | None:
        """Returns the position of the last acknowledged row, if any.

        Raises:
            FatalError: If it was saved for other `order_by` columns: the
                position would not mean the same. Delete the state key to
                start over.
        """
        return self._saved(ctx)[0]

    def _saved(
        self, ctx: contract.ETLContext
    ) -> tuple[tuple[Any, ...] | None, bool]:
        """Returns the saved position, and whether to read its rows again.

        Raises:
            FatalError: If the position was saved for other columns.
        """
        if self._state is None:
            return None, False
        saved = self._state.get(ctx.run.pipeline_name, self._state_key)
        if not isinstance(saved, dict):
            return None, False

        if saved.get("columns") != list(self.order_by):
            raise contract.FatalError(
                f"{self.name}: the saved position of state key"
                f" {self._state_key!r} is on {saved.get('columns')}, not on"
                f" {list(self.order_by)}. Delete it to extract from scratch."
            )
        position = utils.from_tagged(saved["position"])
        if not isinstance(position, list):
            raise contract.FatalError(
                f"{self.name}: state key {self._state_key!r} is corrupted"
            )
        return (
            tuple(typing.cast(list[object], position)),
            saved.get("recheck") is True,
        )


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
