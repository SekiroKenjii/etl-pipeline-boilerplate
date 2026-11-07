"""Extracting a large table in parallel, by ranges of its key.

    partitions = KeyPartitions(reader, "dbo.orders", key=["id"], count=8,
                               state=state)
    steps = [
        PipelineStep("orders:ranges", partitions.prepare),
        *(
            PipelineStep(
                f"orders:{i}",
                ETLJob(extractor, transformer, loader),
                dependencies=("orders:ranges",),
            )
            for i, extractor in enumerate(
                partitions.extractors(watermark="ts")
            )
        ),
    ]

Range bounds are quantiles of the key (`ITableReader.keys_at`), computed by
the `prepare` step, saved with the state and never changed: a row never moves
to a range whose watermark is already past it. A run of some steps only
(`--step`) fails until `prepare` has run, rather than split the table itself
and miss the rows between its bounds and its neighbours'.

The first run reads each range in key order; later runs read by watermark:
index `(watermark, *key)` in the source.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
import dataclasses
import threading
from typing import Any, cast

from core import contract
from core.operators import tables
from shared import utils


@dataclasses.dataclass(frozen=True, slots=True)
class KeyRange:
    """The rows whose first key column is in `[lower, upper)`.

    Attributes:
        lower: The smallest value, included; None for no bound.
        upper: The value after the largest, excluded; None for no bound.
    """

    lower: Any = None
    upper: Any = None

    def conditions(self, column: str) -> tuple[contract.Condition, ...]:
        """The conditions selecting the range on `column`."""
        found: list[contract.Condition] = []
        if self.lower is not None:
            found.append(
                contract.Condition(column, contract.Operator.GE, self.lower)
            )
        if self.upper is not None:
            found.append(
                contract.Condition(column, contract.Operator.LT, self.upper)
            )
        return tuple(found)


def split(
    reader: contract.ITableReader,
    table: str,
    key: Sequence[str],
    count: int,
    filters: Sequence[contract.Condition] = (),
) -> list[KeyRange]:
    """Splits the rows of a table into `count` ranges of its first key column.

    Fewer ranges come back when the column has fewer distinct values, or the
    table few rows; one, unbounded, for an empty table. The bounds are the
    keys at the quantiles (`ITableReader.keys_at`): the database skips to
    them on its key index, where the reader can.

    Raises:
        ValueError: If `count` is below 1, or `key` is empty.
    """
    if count < 1 or not key:
        raise ValueError("count must be >= 1, and key name a column")
    total = reader.count(table, filters)
    if count == 1 or total < 2 * count:
        return [KeyRange()]
    marks = sorted({round(total * i / count) for i in range(1, count)})
    bounds: list[Any] = []
    for found in reader.keys_at(table, key, marks, filters):
        if not bounds or found[0] != bounds[-1]:
            bounds.append(found[0])
    edges = [None, *bounds, None]
    return [KeyRange(edges[i], edges[i + 1]) for i in range(len(edges) - 1)]


class KeyPartitions:
    """The ranges of a table's key, computed once and kept in the state.

    Attributes:
        reader: Reads the table.
        table: The table.
        key: Its key; ranges are on its first column.
        count: How many ranges.
    """

    def __init__(
        self,
        reader: contract.ITableReader,
        table: str,
        *,
        key: Sequence[str],
        count: int,
        state: contract.IStateStore,
        state_key: str | None = None,
        filters: Sequence[contract.Condition] = (),
    ) -> None:
        if count < 1 or not key:
            raise ValueError("count must be >= 1, and key name a column")
        self.reader = reader
        self.table = table
        self.key = tuple(key)
        self.count = count
        self.state = state
        self.state_key = state_key or f"table:{table}"
        self.filters = tuple(filters)
        self._ranges: list[KeyRange] | None = None
        self._lock = threading.Lock()

    def ranges(
        self, ctx: contract.ETLContext, *, compute: bool = True
    ) -> list[KeyRange]:
        """Returns the ranges: saved ones, or computed and saved now.

        Args:
            ctx: The run.
            compute: Whether to compute them when none are saved.

        Raises:
            FatalError: If the saved ranges are for another key or count:
                reset the state of the table to split it again. Or if none
                are saved, and `compute` is False.
        """
        with self._lock:
            if self._ranges is None:
                self._ranges = self._load(ctx, compute=compute)
            return self._ranges

    def prepare(
        self,
        ctx: contract.ETLContext,
        upstream: Mapping[str, contract.ETLResult],
    ) -> contract.ETLResult:
        """A step computing the ranges, and saving them, before any is read.

        Raises:
            FatalError: If the saved ranges are for another key or count.
        """
        del upstream  # Unused.
        found = self.ranges(ctx)
        result = contract.ETLResult()
        result.data["ranges"] = len(found)
        result.complete(
            contract.ETLStatus.SUCCESS,
            f"{self.table}: {len(found)} range(s) of {self.key[0]}",
        )
        return result

    def range(self, index: int, ctx: contract.ETLContext) -> KeyRange | None:
        """The range of partition `index`; None when there are fewer.

        Raises:
            FatalError: If no ranges are saved yet, and the run selects its
                steps: the `prepare` step must run first.
        """
        found = self.ranges(ctx, compute=not ctx.run.steps)
        return found[index] if index < len(found) else None

    def extractors(self, **options: Any) -> list[PartitionExtractor]:
        """One extractor per partition; `options` of `TableExtractor`
        (`watermark`, `columns`...), the state and filters aside."""
        return [
            PartitionExtractor(self, index, **options)
            for index in range(self.count)
        ]

    def _load(
        self, ctx: contract.ETLContext, *, compute: bool
    ) -> list[KeyRange]:
        """The saved ranges, or new ones, saved (not in a dry run)."""
        name = f"{self.state_key}:ranges"
        saved = self.state.get(ctx.run.pipeline_name, name)
        if isinstance(saved, dict):
            if saved.get("key") != list(self.key) or saved.get("count") != (
                self.count
            ):
                raise contract.FatalError(
                    f"{self.table}: the ranges saved under {name!r} are for"
                    f" {saved.get('count')} partition(s) of {saved.get('key')};"
                    " reset that state key to split the table again"
                )
            bounds = cast(list[Any], utils.from_tagged(saved["bounds"]))
            edges = [None, *bounds, None]
            return [
                KeyRange(edges[i], edges[i + 1]) for i in range(len(edges) - 1)
            ]
        if not compute:
            raise contract.FatalError(
                f"{self.table}: its key ranges are not saved yet ({name!r}):"
                " run the step that splits it (its ranges step) first, or with"
                " the steps selected"
            )
        ranges = split(
            self.reader, self.table, self.key, self.count, self.filters
        )
        if not ctx.run.dry_run:
            self.state.set(
                ctx.run.pipeline_name,
                name,
                {
                    "key": list(self.key),
                    "count": self.count,
                    "bounds": cast(
                        contract.StateValue,
                        utils.to_tagged([r.lower for r in ranges[1:]]),
                    ),
                },
            )
        return ranges


@dataclasses.dataclass(slots=True)
class _FirstPass:
    """The first read of a range, in key order, up to a watermark.

    Attributes:
        until: The largest watermark at its start: it reads up to it.
        acknowledged: The key of the last acknowledged row; None before any.
        read: The key of the last row read.
        done: Whether every row was read.
    """

    until: Any
    acknowledged: tuple[Any, ...] | None
    read: tuple[Any, ...] | None = None
    done: bool = False

    def __post_init__(self) -> None:
        self.read = self.acknowledged

    @property
    def complete(self) -> bool:
        """Whether every row was read, and acknowledged."""
        return self.done and self.read == self.acknowledged


class PartitionExtractor(tables.TableExtractor):
    """A `TableExtractor` of one range of a `KeyPartitions`.

    Its state is `<state_key>:<index>`. With a watermark, the first run reads
    the range in key order, up to the largest watermark at its start, and a
    failed first run resumes after the last key acknowledged. Later runs read by
    watermark, as `TableExtractor` does; so do runs with `snapshot=False` and
    backfills (`ctx.run.since`, `until`). A backfill during a first read in key
    order saves no position.
    """

    def __init__(
        self, partitions: KeyPartitions, index: int, **options: Any
    ) -> None:
        super().__init__(
            partitions.reader,
            partitions.table,
            key=partitions.key,
            state=partitions.state,
            state_key=f"{partitions.state_key}:{index}",
            filters=partitions.filters,
            **options,
        )
        self.partitions = partitions
        self.index = index
        self._first: _FirstPass | None = None
        self._keep_saved_state = False

    @property
    def name(self) -> str:
        return f"{type(self).__name__}({self.table}#{self.index})"

    def query(self, ctx: contract.ETLContext) -> contract.TableQuery | None:
        """Returns what this run reads, None when there is nothing to read.

        Raises:
            FatalError: If no ranges are saved yet and the run selects its
                steps; if the saved position was taken with other columns;
                if a row of the range has no watermark.
        """
        found = self.partitions.range(self.index, ctx)
        if found is None:
            return None  # The table split into fewer ranges.
        self._filters = (
            *self.partitions.filters,
            *found.conditions(self.key[0]),
        )
        self._first, self._keep_saved_state = None, False
        saved = self._saved_state(ctx)
        first = None if saved is None else saved.get("first")
        backfill = ctx.run.since is not None or ctx.run.until is not None
        if (
            self.watermark is None
            or not self._snapshot
            or backfill
            or (saved is not None and first is None)
        ):
            self._keep_saved_state = backfill and first is not None
            return super().query(ctx)

        if first is not None:
            if not isinstance(first, dict):
                raise contract.FatalError(
                    f"{self.name}: state key {self._state_key!r} is corrupted"
                )
            until = utils.from_tagged(first["until"])
            after = tuple(cast(list[Any], utils.from_tagged(first["after"])))
        else:
            until = self.reader.max_value(
                self.table, self.watermark, self._filters
            )
            if until is None:
                return None
            after = None
        self._check_watermark()
        self._first = _FirstPass(until, after)
        return contract.TableQuery(
            table=self.table,
            order_by=self.key,
            columns=self._columns,
            after=after,
            filters=(
                *self._filters,
                contract.Condition(self.watermark, contract.Operator.LE, until),
            ),
        )

    def extract(self, ctx: contract.ETLContext) -> Iterator[contract.Row]:
        query = self.query(ctx)
        if query is None:
            return
        first = self._first
        if first is None:
            yield from self.reader.read(query)
            return
        for row in self.reader.read(query):
            first.read = tuple(row[column] for column in self.key)
            yield row
        first.done = True
        if first.complete:  # The last batch was acknowledged already.
            self._finish(ctx, first)

    def acknowledge(
        self, records: Sequence[contract.Row], ctx: contract.ETLContext
    ) -> None:
        """Saves the position of the last row of the batch.

        In a first read in key order, the position is its key, until the
        last row: then the range is read up to the watermark of the start.
        """
        first = self._first
        if self._keep_saved_state or self._state is None or not records:
            return
        if first is None:
            super().acknowledge(records, ctx)
            return
        first.acknowledged = tuple(records[-1][column] for column in self.key)
        if first.complete:
            self._finish(ctx, first)
            return
        self._state.set(
            ctx.run.pipeline_name,
            self._state_key,
            {
                "columns": list(self.order_by),
                "first": {
                    "until": cast(
                        contract.StateValue, utils.to_tagged(first.until)
                    ),
                    "after": cast(
                        contract.StateValue,
                        utils.to_tagged(list(first.acknowledged)),
                    ),
                },
            },
        )

    def _finish(self, ctx: contract.ETLContext, first: _FirstPass) -> None:
        """Saves the end of the first read: the range is read up to its
        watermark, whose rows the next run reads again, once."""
        if self._state is None or ctx.run.dry_run:
            return
        if first.acknowledged is None:
            return  # No row: the next run starts afresh.
        self._state.set(
            ctx.run.pipeline_name,
            self._state_key,
            {
                "columns": list(self.order_by),
                "position": cast(
                    contract.StateValue,
                    utils.to_tagged([first.until, *first.acknowledged]),
                ),
                "recheck": True,
            },
        )

    def _saved(
        self, ctx: contract.ETLContext
    ) -> tuple[tuple[Any, ...] | None, bool]:
        """See `TableExtractor._saved`; no position during a first read."""
        saved = self._saved_state(ctx)
        if saved is not None and "first" in saved:
            return None, False
        return super()._saved(ctx)

    def _saved_state(
        self, ctx: contract.ETLContext
    ) -> dict[str, contract.StateValue] | None:
        """Returns the saved state of the range, None if there is none.

        Raises:
            FatalError: If it was saved for other columns.
        """
        if self._state is None:
            return None
        saved = self._state.get(ctx.run.pipeline_name, self._state_key)
        if not isinstance(saved, dict):
            return None
        if saved.get("columns") != list(self.order_by):
            raise contract.FatalError(
                f"{self.name}: the saved position of state key"
                f" {self._state_key!r} is on {saved.get('columns')}, not on"
                f" {list(self.order_by)}. Delete it to extract from scratch."
            )
        return saved

    def _check_watermark(self) -> None:
        """Fails if a row of the range has no watermark.

        Reading by key, the filter on the watermark would skip it silently;
        reading by watermark, the reader refuses it.

        Raises:
            FatalError: If a row of the range has NULL as its watermark.
        """
        if self.watermark is None:
            return
        missing = self.reader.count(
            self.table,
            (
                *self._filters,
                contract.Condition(self.watermark, contract.Operator.IS_NULL),
            ),
        )
        if missing:
            raise contract.FatalError(
                f"{self.name}: {missing} row(s) with NULL in {self.watermark}:"
                " they cannot be read by watermark, and would be skipped."
                " Filter them out, or use a NOT NULL watermark."
            )
