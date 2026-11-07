"""Data diff: the target rows expected from the source, against the real ones.

    report = cutover.reconcile(reconciliation)
    report.missing, report.extra, report.different

The source rows go through the module's mapping (no load); both sides are
compared in canonical form (`checksum`).

A first pass sums row digests per partition of the keys: ranges of the
target's key, of about `ROWS_PER_PARTITION` rows, when the target orders its
keys as their canonical forms sort; hash buckets otherwise (SQL Server
orders a `uniqueidentifier` by its last bytes). A second pass compares row by
row only the partitions that differ: the target reads those ranges again
(with hash buckets: all of it), the source all of it, its rows of those
partitions spilled sorted to disk past `SPILL_ROWS`. Memory never holds a
table.
"""

from __future__ import annotations

import array
import bisect
from collections.abc import Collection, Iterable, Iterator, Mapping, Sequence
import contextlib
import dataclasses
import decimal
import heapq
import itertools
import json
import math
import operator
import pathlib
import shutil
import tempfile
import time
from typing import Any, Literal

from core import contract
from core.cutover import acceptance
from core.cutover import checksum

# Digests are summed per partition: unlike XOR, two equal rows do not cancel.
_MASK = (1 << 64) - 1
ROWS_PER_PARTITION = 100
# Memory: 32 bytes a partition, and a key a range.
MAX_PARTITIONS = 500_000
SPILL_ROWS = 50_000
_FAN_IN = 64

type Kind = Literal["missing", "extra", "different", "duplicate"]
type Status = Literal["match", "match-with-rejects", "mismatch", "counted"]
type Partitioning = Literal["", "range", "hash"]


@dataclasses.dataclass(frozen=True, slots=True)
class Difference:
    """A row that differs between what is expected and the target.

    Attributes:
        kind: `missing` from the target, `extra` in it, `different` there, or
            a key found more than once on a side (`duplicate`).
        key: The canonical values of its key.
        columns: For a `different` row, the columns that differ, with their
            expected and actual canonical values.
    """

    kind: Kind
    key: tuple[checksum.Canonical, ...]
    columns: dict[str, tuple[checksum.Canonical, checksum.Canonical]] = (
        dataclasses.field(
            default_factory=dict[
                str, tuple[checksum.Canonical, checksum.Canonical]
            ]
        )
    )


@dataclasses.dataclass(slots=True)
class EntityReport:
    """How one target entity reconciles with its source.

    `source_rows = rejected + (rows mapped)`, and the rows mapped are the
    `expected_rows`, unless a filter dropped some or a split made more; then
    `expected_rows = matched + missing + different` and `target_rows =
    matched + extra + different`.

    Attributes:
        entity: The entity.
        source_rows: The rows of the source.
        rejected: The source rows the mapping rejects: never loadable.
        rejections: Why, by reason.
        accepted: The rejected rows the BAs accept to leave behind
            (`AcceptedRejections`); the others are a mismatch.
        expected_rows: The target rows expected.
        target_rows: The rows of the target.
        matched: Rows equal on both sides.
        missing: Expected rows not in the target.
        extra: Target rows not expected.
        different: Rows in both, with other values.
        duplicates: Keys found more than once on a side.
        parked: The entries of its orphans' parking queue. None when unknown.
        parked_missing: The missing rows whose key is parked: missing for a
            known reason, their parent.
        samples: Some of the differences, to look into.
        counted_only: Whether only the rows were counted, not compared.
        partitioning: How the diff split the keys: `range` or `hash`.
        seconds: How long the reconciliation took.
    """

    entity: str
    source_rows: int = 0
    rejected: int = 0
    rejections: dict[str, int] = dataclasses.field(
        default_factory=dict[str, int]
    )
    accepted: int = 0
    expected_rows: int = 0
    target_rows: int = 0
    matched: int = 0
    missing: int = 0
    extra: int = 0
    different: int = 0
    duplicates: int = 0
    parked: int | None = None
    parked_missing: int = 0
    samples: list[Difference] = dataclasses.field(
        default_factory=list[Difference]
    )
    counted_only: bool = False
    partitioning: Partitioning = ""
    seconds: float = 0.0

    @property
    def unexplained_missing(self) -> int:
        """The missing rows that are not parked orphans."""
        return self.missing - self.parked_missing

    @property
    def unaccepted(self) -> int:
        """The rejected rows nobody accepted to leave behind."""
        return self.rejected - self.accepted

    @property
    def status(self) -> Status:
        """`match` when nothing unexplained differs, `match-with-rejects`
        when only accepted rejected rows are missing, `counted` if not
        compared, `mismatch` otherwise."""
        if self.counted_only:
            return "counted"
        unexplained = (
            self.unexplained_missing
            + self.extra
            + self.different
            + self.duplicates
            + self.unaccepted
        )
        if unexplained:
            return "mismatch"
        return "match-with-rejects" if self.accepted else "match"


@dataclasses.dataclass(slots=True)
class _Sums:
    """The number of rows and the sum of their digests, per partition."""

    counts: array.array[int] = dataclasses.field(
        default_factory=lambda: array.array("Q")
    )
    sums: array.array[int] = dataclasses.field(
        default_factory=lambda: array.array("Q")
    )

    @classmethod
    def of(cls, size: int) -> _Sums:
        return cls(array.array("Q", [0]) * size, array.array("Q", [0]) * size)

    def grow(self) -> None:
        """Adds a partition."""
        self.counts.append(0)
        self.sums.append(0)

    def add(self, index: int, digest: int) -> None:
        self.counts[index] += 1
        self.sums[index] = (self.sums[index] + digest) & _MASK

    def __len__(self) -> int:
        return len(self.counts)


# A row in canonical form: its key text, key, digest and compared values.
type _Row = tuple[
    str, tuple[checksum.Canonical, ...], int, tuple[checksum.Canonical, ...]
]
# Likewise, with its partition first.
type _Placed = tuple[
    int,
    str,
    tuple[checksum.Canonical, ...],
    int,
    tuple[checksum.Canonical, ...],
]
# How a canonical key sorts: None, numbers, NaN, then text.
type _Order = tuple[tuple[int, int | decimal.Decimal | str], ...]

_partition_and_key = operator.itemgetter(0, 1)


class _Unordered(Exception):
    """The target's order of its keys is not the order of their canonical
    forms: ranges of it cannot be told apart."""


@dataclasses.dataclass(slots=True)
class _Ranges:
    """Ranges of the target's key, in its order: where each one starts.

    Range 0 has no lower bound; range `i > 0` starts at `lows[i - 1]`,
    whose canonical order is `orders[i - 1]`.
    """

    lows: list[Any] = dataclasses.field(default_factory=list[Any])
    orders: list[_Order] = dataclasses.field(default_factory=list[_Order])

    def of(self, key: tuple[checksum.Canonical, ...]) -> int:
        """The range of a key."""
        return bisect.bisect_right(self.orders, _order(key))


@dataclasses.dataclass(slots=True)
class _Tally:
    """What the second pass found."""

    matched: int = 0
    missing: int = 0
    parked_missing: int = 0
    extra: int = 0
    different: int = 0
    duplicates: int = 0
    samples: list[Difference] = dataclasses.field(
        default_factory=list[Difference]
    )


class _Diff:
    """One reconciliation, its two passes."""

    def __init__(
        self,
        reconciliation: contract.Reconciliation,
        *,
        partitions: int | None,
        samples: int,
        parked: Collection[str],
        accepted: Mapping[str, int],
        resolution: int,
        ranges: bool,
        spill_rows: int,
    ) -> None:
        self.rec = reconciliation
        self.partitions = partitions
        self.samples = samples
        self.parked = parked
        self.accepted = accepted
        self.resolution = resolution
        self.spill_rows = spill_rows
        self.columns = tuple(
            dict.fromkeys((*reconciliation.key, *reconciliation.columns))
        )
        self.key_positions = tuple(
            self.columns.index(column) for column in reconciliation.key
        )
        self.query = dataclasses.replace(
            reconciliation.target_query, columns=self.columns
        )
        # Ranges are read with `lower` and `upper`: on the first column of
        # the order, which must be the first of the key, and free.
        self.use_ranges = (
            ranges
            and self.query.order_by[0] == reconciliation.key[0]
            and self.query.lower is None
            and self.query.upper is None
            and self.query.after is None
        )
        self.ranges: _Ranges | None = None
        self.buckets = 1
        self.expected = _Sums()
        self.actual = _Sums()
        self.report = EntityReport(reconciliation.entity)

    def run(self) -> EntityReport:
        """Compares partition by partition, then the rows of those that
        differ."""
        started = time.monotonic()
        rows = self.rec.target.count(self.query.table, self.query.filters)
        size = self.partitions or min(
            MAX_PARTITIONS, max(1, math.ceil(rows / ROWS_PER_PARTITION))
        )
        self._scan_target(rows, size)
        self.expected = _Sums.of(len(self.actual))
        # Ranges place a row by its key: its text is not needed yet.
        keyed = self.ranges is None
        for row in self._expected(tally=True, keyed=keyed):
            self.report.expected_rows += 1
            self.expected.add(self._partition(row), row[2])

        differing = [
            index
            for index in range(len(self.actual))
            if self.expected.counts[index] != self.actual.counts[index]
            or self.expected.sums[index] != self.actual.sums[index]
        ]
        self.report.matched = sum(self.expected.counts) - sum(
            self.expected.counts[index] for index in differing
        )
        if differing:
            try:
                tally = self._compare(differing, by_range=self.use_ranges)
            except _Unordered:
                # The database ranges otherwise than the first pass did.
                tally = self._compare(differing, by_range=False)
            self._apply(tally)
        self.report.seconds = time.monotonic() - started
        return self.report

    def _scan_target(self, rows: int, size: int) -> None:
        """Sums the target rows per range of keys, else per hash bucket."""
        if self.use_ranges:
            try:
                self._scan_ranges(max(1, math.ceil(rows / size)))
                self.report.partitioning = "range"
                return
            except _Unordered:
                self.use_ranges = False
                self.report.target_rows = 0
        self.ranges = None
        self.buckets = size
        self.actual = _Sums.of(size)
        for row in self._target():
            self.report.target_rows += 1
            self.actual.add(checksum.bucket(row[0], size), row[2])
        self.report.partitioning = "hash"

    def _scan_ranges(self, per_range: int) -> None:
        """Sums the target rows per range of `per_range` rows or so.

        Raises:
            _Unordered: If the target's order is not the canonical one.
        """
        ranges = _Ranges()
        self.ranges = ranges
        self.actual = _Sums()
        self.actual.grow()
        first = self.rec.key[0]
        previous: _Order | None = None
        in_range = 0
        for raw in self.rec.target.read(self.query):
            row = self._canonical(raw, keyed=False)
            order = _order(row[1])
            if previous is not None:
                if order < previous:
                    raise _Unordered
                # A key never spans two ranges.
                if in_range >= per_range and order != previous:
                    ranges.lows.append(raw[first])
                    ranges.orders.append(order)
                    self.actual.grow()
                    in_range = 0
            previous = order
            in_range += 1
            self.report.target_rows += 1
            self.actual.add(len(self.actual) - 1, row[2])

    def _partition(self, row: _Row) -> int:
        if self.ranges is not None:
            return self.ranges.of(row[1])
        return checksum.bucket(row[0], self.buckets)

    def _compare(self, differing: list[int], *, by_range: bool) -> _Tally:
        """Compares the rows of the partitions that differ, key by key."""
        tally = _Tally()
        wanted = set(differing)
        with contextlib.ExitStack() as stack:
            sorter = _Sorter(self.spill_rows, stack)
            expected = sorter.sort(self._placed(self._expected(), wanted))
            actual = (
                self._target_ranges(differing)
                if by_range
                else sorter.sort(self._placed(self._target(), wanted))
            )
            for mine, theirs in _merge(expected, actual):
                self._compare_key(tally, mine, theirs)
        return tally

    def _compare_key(
        self, tally: _Tally, mine: list[_Placed], theirs: list[_Placed]
    ) -> None:
        """Compares the rows of one key, expected and found."""
        if len(mine) > 1 or len(theirs) > 1:
            tally.duplicates += 1
            self._sample(tally, "duplicate", (mine or theirs)[0][2])
        elif not theirs:
            tally.missing += 1
            if mine[0][1] in self.parked:
                tally.parked_missing += 1
            else:
                self._sample(tally, "missing", mine[0][2])
        elif not mine:
            tally.extra += 1
            self._sample(tally, "extra", theirs[0][2])
        elif mine[0][3] != theirs[0][3]:
            tally.different += 1
            self._sample(
                tally,
                "different",
                mine[0][2],
                {
                    column: (want, got)
                    for column, want, got in zip(
                        self.columns, mine[0][4], theirs[0][4]
                    )
                    if want != got
                },
            )
        else:
            tally.matched += 1

    def _apply(self, tally: _Tally) -> None:
        report = self.report
        report.matched += tally.matched
        report.missing += tally.missing
        report.parked_missing += tally.parked_missing
        report.extra += tally.extra
        report.different += tally.different
        report.duplicates += tally.duplicates
        report.samples.extend(tally.samples)

    def _placed(
        self, rows: Iterable[_Row], wanted: Collection[int]
    ) -> Iterator[_Placed]:
        """The rows of the `wanted` partitions, each with its partition."""
        for row in rows:
            partition = self._partition(row)
            if partition in wanted:
                yield (partition, *row)

    def _target_ranges(self, differing: Sequence[int]) -> Iterator[_Placed]:
        """The target rows of the `differing` ranges, read in the database,
        sorted range by range.

        Raises:
            _Unordered: If the database's ranges are not those of the first
                pass.
        """
        ranges = self.ranges
        if ranges is None:
            raise _Unordered
        last_range = len(self.actual) - 1
        for first, last in _runs(differing):
            # Inclusive: a collation telling apart what canonical forms do
            # not reads a key equal to a bound either way.
            query = dataclasses.replace(
                self.query,
                lower=ranges.lows[first - 1] if first else None,
                upper=ranges.lows[last] if last < last_range else None,
            )
            current = first
            pending: list[_Placed] = []
            for raw in self.rec.target.read(query):
                row = self._canonical(raw)
                index = ranges.of(row[1])
                if not first <= index <= last:
                    continue
                if index < current:
                    raise _Unordered
                while current < index:
                    yield from self._checked(current, pending)
                    current, pending = current + 1, []
                pending.append((index, *row))
            while current <= last:
                yield from self._checked(current, pending)
                current, pending = current + 1, []

    def _checked(self, index: int, rows: list[_Placed]) -> list[_Placed]:
        """The rows of a range, sorted, once they are those of the first
        pass.

        Raises:
            _Unordered: If they are not.
        """
        total = 0
        for row in rows:
            total = (total + row[3]) & _MASK
        if (len(rows), total) != (
            self.actual.counts[index],
            self.actual.sums[index],
        ):
            raise _Unordered
        rows.sort(key=_partition_and_key)
        return rows

    def _sample(
        self,
        tally: _Tally,
        kind: Kind,
        key: tuple[checksum.Canonical, ...],
        columns: (
            dict[str, tuple[checksum.Canonical, checksum.Canonical]] | None
        ) = None,
    ) -> None:
        if len(tally.samples) < self.samples:
            tally.samples.append(Difference(kind, key, columns or {}))

    def _expected(
        self, tally: bool = False, keyed: bool = True
    ) -> Iterator[_Row]:
        """The target rows the source rows map to; counts the rejects."""
        rejections: dict[str, int] = {}
        budget = acceptance.Budget(self.accepted)
        for source in self.rec.source.read(self.rec.source_query):
            expected = self.rec.expect(source)
            if tally:
                self.report.source_rows += 1
            if expected.rejected:
                if tally:
                    self.report.rejected += 1
                    for reason in dict.fromkeys(expected.rejected):
                        rejections[reason] = rejections.get(reason, 0) + 1
                    if budget.accept(expected.rejected):
                        self.report.accepted += 1
                continue
            for row in expected.rows:
                yield self._canonical(row, keyed)
        if tally:
            self.report.rejections = dict(
                sorted(rejections.items(), key=lambda item: -item[1])
            )

    def _target(self) -> Iterator[_Row]:
        for row in self.rec.target.read(self.query):
            yield self._canonical(row)

    def _canonical(self, row: contract.Row, keyed: bool = True) -> _Row:
        """A row in canonical form; the text of its key empty unless
        `keyed`."""
        values = checksum.canonical_row(row, self.columns, self.resolution)
        key = tuple(values[i] for i in self.key_positions)
        return (
            checksum.text(key) if keyed else "",
            key,
            checksum.digest(values),
            values,
        )


def _order(key: tuple[checksum.Canonical, ...]) -> _Order:
    """How a canonical key sorts, whatever the types of its values."""
    return tuple(map(_rank, key))


def _rank(value: checksum.Canonical) -> tuple[int, int | decimal.Decimal | str]:
    match value:
        case None:
            return (0, 0)
        case int():
            return (1, value)
        case checksum.Number():
            number = decimal.Decimal(value.value)
            return (2, 0) if number.is_nan() else (1, number)
        case _:
            return (3, value)


def _runs(indexes: Sequence[int]) -> Iterator[tuple[int, int]]:
    """The runs of consecutive indexes, as (first, last)."""
    for _, run in itertools.groupby(
        enumerate(indexes), key=lambda pair: pair[1] - pair[0]
    ):
        found = [index for _, index in run]
        yield found[0], found[-1]


def _merge(
    left: Iterable[_Placed], right: Iterable[_Placed]
) -> Iterator[tuple[list[_Placed], list[_Placed]]]:
    """The rows of each key on both sides, from two sorted streams."""

    def grouped(
        rows: Iterable[_Placed],
    ) -> Iterator[tuple[tuple[int, str], list[_Placed]]]:
        for position, group in itertools.groupby(rows, key=_partition_and_key):
            yield position, list(group)

    lefts, rights = grouped(left), grouped(right)
    mine, theirs = next(lefts, None), next(rights, None)
    while mine is not None or theirs is not None:
        if mine is not None and (theirs is None or mine[0] < theirs[0]):
            yield mine[1], []
            mine = next(lefts, None)
        elif theirs is not None and (mine is None or theirs[0] < mine[0]):
            yield [], theirs[1]
            theirs = next(rights, None)
        elif mine is not None and theirs is not None:
            yield mine[1], theirs[1]
            mine, theirs = next(lefts, None), next(rights, None)


class _Sorter:
    """Sorts rows by partition and key: in memory up to `limit` rows, then
    in files merged back; the stack removes them."""

    def __init__(self, limit: int, stack: contextlib.ExitStack) -> None:
        self.limit = limit
        self.stack = stack
        self.directory: pathlib.Path | None = None
        self.files = 0

    def sort(self, rows: Iterable[_Placed]) -> Iterator[_Placed]:
        """Yields the rows sorted, once it has read them all."""
        buffer: list[_Placed] = []
        runs: list[pathlib.Path] = []
        for row in rows:
            buffer.append(row)
            if len(buffer) >= self.limit:
                buffer.sort(key=_partition_and_key)
                runs.append(self._write(buffer))
                buffer = []
                if len(runs) >= _FAN_IN:
                    runs = [self._write(self._merged(runs))]
        buffer.sort(key=_partition_and_key)
        if not runs:
            yield from buffer
            return
        if buffer:
            runs.append(self._write(buffer))
            buffer = []
        yield from self._merged(runs)

    def _merged(self, runs: list[pathlib.Path]) -> Iterator[_Placed]:
        """The rows of sorted files, sorted."""
        yield from heapq.merge(
            *(self._read(path) for path in runs), key=_partition_and_key
        )

    def _write(self, rows: Iterable[_Placed]) -> pathlib.Path:
        """Writes sorted rows to a new file; returns its path.

        The rows hold the data compared, personal data included: the
        directory is the owner's only (`mkdtemp`), and goes with the diff.
        """
        if self.directory is None:
            self.directory = pathlib.Path(
                tempfile.mkdtemp(prefix="etl-cutover-")
            )
            self.stack.callback(shutil.rmtree, self.directory, True)
        self.files += 1
        path = self.directory / f"{self.files}.jsonl"
        # Text is as a driver gave it: a lone surrogate too.
        with path.open("w", encoding="utf-8", errors="surrogatepass") as file:
            for row in rows:
                file.write(
                    json.dumps(row, ensure_ascii=False, separators=(",", ":"))
                )
                file.write("\n")
        return path

    def _read(self, path: pathlib.Path) -> Iterator[_Placed]:
        """Yields the rows of a file, then removes it."""
        with path.open(encoding="utf-8", errors="surrogatepass") as file:
            for line in file:
                index, text, key, digest, values = json.loads(line)
                yield (
                    index,
                    text,
                    checksum.restored(key),
                    digest,
                    checksum.restored(values),
                )
        path.unlink()


def reconcile(
    reconciliation: contract.Reconciliation,
    *,
    buckets: int | None = None,
    samples: int = 20,
    parked: int | None = None,
    parked_keys: Collection[str] = (),
    accepted: Mapping[str, int] | None = None,
    resolution: int = checksum.MICROSECONDS,
    ranges: bool = True,
    spill_rows: int = SPILL_ROWS,
) -> EntityReport:
    """Compares the target rows expected from the source with the target's.

    Args:
        reconciliation: What to compare.
        buckets: How many partitions the keys split into; by default one per
            `ROWS_PER_PARTITION` target rows, up to `MAX_PARTITIONS`. More
            partitions read fewer rows again in the second pass.
        samples: How many differences the report shows.
        parked: The orphans waiting in the parking lot, if known.
        parked_keys: The key texts (`checksum.text`) of the parked orphans:
            the missing rows they explain.
        accepted: The rejected rows accepted, by reason
            (`AcceptedRejections.of`).
        resolution: The ticks per second timestamps compare at
            (`checksum.canonical_value`).
        ranges: Whether to partition by ranges of the target's key, when its
            order allows; by hash buckets otherwise.
        spill_rows: The rows of the second pass held in memory before they
            are sorted on disk.

    Raises:
        ValueError: If an argument is out of range.
    """
    if (buckets is not None and buckets < 1) or samples < 0 or spill_rows < 1:
        raise ValueError("buckets and spill_rows must be >= 1, samples >= 0")
    if not 1 <= resolution <= checksum.MICROSECONDS:
        raise ValueError(
            f"resolution must be 1 to {checksum.MICROSECONDS} ticks per second"
        )
    report = _Diff(
        reconciliation,
        partitions=buckets,
        samples=samples,
        parked=frozenset(parked_keys),
        accepted=accepted or {},
        resolution=resolution,
        ranges=ranges,
        spill_rows=spill_rows,
    ).run()
    report.parked = parked
    return report


def count(
    reconciliation: contract.Reconciliation,
    *,
    parked: int | None = None,
) -> EntityReport:
    """Counts the rows of both sides, in the databases: fast, compares none."""
    started = time.monotonic()
    source, target = reconciliation.source_query, reconciliation.target_query
    report = EntityReport(
        reconciliation.entity,
        source_rows=reconciliation.source.count(source.table, source.filters),
        target_rows=reconciliation.target.count(target.table, target.filters),
        parked=parked,
        counted_only=True,
    )
    report.seconds = time.monotonic() - started
    return report
