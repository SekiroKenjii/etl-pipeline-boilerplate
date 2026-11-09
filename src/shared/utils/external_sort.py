"""Sorting more items than memory holds: runs on disk, merged back.

    with utils.ExternalSorter(key, spill_rows=50_000) as sorter:
        sorter.extend(items)
        for item in sorter:
            ...

    for item in utils.sorted_spilling(items, key, spill_rows=50_000):
        ...

Items are kept in memory up to `spill_rows`, then sorted and written as a
run; a k-way merge (`heapq.merge`) of the runs yields them sorted, stable
on equal keys, as `sorted` would. Runs are tiered: `fan_in` runs of a tier
are merged into one run of the next, so that a run is rewritten once per
tier, about log(items / spill_rows) / log(fan_in) times in all. Memory
holds `spill_rows` items, and a merge a chunk of each run it reads: at most
`fan_in` runs, `spill_rows` items in all.
"""

from __future__ import annotations

from collections.abc import Callable, Generator, Iterable, Iterator
import heapq
import itertools
import pathlib
import pickle  # nosec B403: only its own runs are read, see `PickleFormat`.
import shutil
import tempfile
from typing import BinaryIO, Protocol, Self
import weakref

FAN_IN = 64


class Comparable(Protocol):
    """What a sort key must be: a value ordered against its own kind."""

    def __lt__(self, other: Self, /) -> bool:
        ...

    def __gt__(self, other: Self, /) -> bool:
        ...

    def __le__(self, other: Self, /) -> bool:
        ...

    def __ge__(self, other: Self, /) -> bool:
        ...


class SpillFormat[T](Protocol):
    """How the items of a run are written to a file, and read back."""

    def dump(self, items: Iterable[T], file: BinaryIO) -> None:
        """Writes `items` to `file`, in their order."""
        ...

    def load(self, file: BinaryIO) -> Iterator[T]:
        """Yields the items of `file`, in their order."""
        ...


class PickleFormat[T]:
    """Runs of pickles, `chunk` items each: any picklable item, as it was.

    Reading a run holds one chunk: a pickle per item would cost a reader
    per item, several times the dump and the merge together.
    """

    def __init__(self, chunk: int = 1000) -> None:
        """Initializes the format.

        Raises:
            ValueError: If `chunk` is below 1.
        """
        if chunk < 1:
            raise ValueError("chunk must be >= 1")
        self.chunk = chunk

    def dump(self, items: Iterable[T], file: BinaryIO) -> None:
        for batch in itertools.batched(items, self.chunk):
            pickle.dump(batch, file, protocol=pickle.HIGHEST_PROTOCOL)

    def load(self, file: BinaryIO) -> Iterator[T]:
        # The file was written by `dump` in a directory of the process
        # owner only, and read by the same process: as trusted as its code.
        while True:
            try:
                batch: tuple[T, ...] = pickle.load(file)  # nosec B301
            except EOFError:
                return
            yield from batch


class ExternalSorter[T]:
    """Sorts items by key; those past `spill_rows` go to disk meanwhile.

    Add the items, then iterate once: the sorter yields them sorted, and
    removes each run as it is read. Closing it (`close`, the context
    manager, or garbage collection) removes what is left on disk.

    Attributes:
        key: The sort key of an item.
        spill_rows: The most items held in memory.
        fan_in: The most runs merged at once: that many runs of a tier
            make one run of the next.
    """

    def __init__(
        self,
        key: Callable[[T], Comparable],
        *,
        spill_rows: int,
        fan_in: int = FAN_IN,
        directory: pathlib.Path | None = None,
        prefix: str = "etl-sort-",
        spill_format: SpillFormat[T] | None = None,
    ) -> None:
        """Initializes an empty sorter.

        Args:
            key: The sort key of an item.
            spill_rows: The most items held in memory before they are
                written to disk.
            fan_in: The most runs merged at once.
            directory: Where the runs go: a directory of its own, readable
                by its owner only, is made there; in the temporary directory
                (`$TMPDIR`) by default.
            prefix: The name of that directory starts with it.
            spill_format: How items are written and read back; by default
                pickles, in chunks small enough for the runs read at once
                to hold `spill_rows` items in all.

        Raises:
            ValueError: If `spill_rows` is below 1, or `fan_in` below 2.
        """
        if spill_rows < 1 or fan_in < 2:
            raise ValueError("spill_rows must be >= 1, and fan_in >= 2")
        self.key = key
        self.spill_rows = spill_rows
        self.fan_in = fan_in
        self._directory = directory
        self._prefix = prefix
        self._format: SpillFormat[T] = spill_format or PickleFormat[T](
            max(1, spill_rows // fan_in)
        )
        self._buffer: list[T] = []
        # Tier 0 holds runs of `spill_rows` items, tier `i` runs of
        # `fan_in` runs of tier `i - 1`; each tier in the order written.
        self._tiers: list[list[pathlib.Path]] = []
        self._count = 0
        self._written = 0
        self._spill_directory: pathlib.Path | None = None
        self._remove: Callable[[], object] | None = None

    @property
    def spill_directory(self) -> pathlib.Path | None:
        """Where the runs are; None until an item is written to disk."""
        return self._spill_directory

    @property
    def runs_written(self) -> int:
        """How many runs were written to disk so far."""
        return self._written

    def __len__(self) -> int:
        """The items added."""
        return self._count

    def add(self, item: T) -> None:
        """Adds an item; a full memory is sorted and written as a run."""
        self._buffer.append(item)
        self._count += 1
        if len(self._buffer) >= self.spill_rows:
            self._spill()

    def extend(self, items: Iterable[T]) -> None:
        """Adds every item."""
        for item in items:
            self.add(item)

    def __iter__(self) -> Iterator[T]:
        """Yields the items sorted, stable on equal keys; each run is
        removed once read, so the items come out once."""
        self._buffer.sort(key=self.key)
        if not any(self._tiers):
            yield from self._buffer
            return
        if self._buffer:
            self._place(self._write(self._buffer), 0)
            self._buffer = []
        yield from self._merged(self._gathered())

    def close(self) -> None:
        """Removes the runs on disk, and forgets the items."""
        self._buffer = []
        self._tiers = []
        if self._remove is not None:
            self._remove()
            self._remove = None
        self._spill_directory = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _spill(self) -> None:
        self._buffer.sort(key=self.key)
        self._place(self._write(self._buffer), 0)
        self._buffer = []

    def _place(self, run: pathlib.Path, tier: int) -> None:
        """Keeps a run in its tier; `fan_in` of them make one of the next."""
        if tier == len(self._tiers):
            self._tiers.append([])
        runs = self._tiers[tier]
        runs.append(run)
        if len(runs) >= self.fan_in:
            self._tiers[tier] = []
            self._place(self._write(self._merged(runs)), tier + 1)

    def _gathered(self) -> list[pathlib.Path]:
        """Every run, oldest first, at most `fan_in` of them: the newest,
        the smallest, are merged until they fit.

        Stable: a merge takes runs that follow one another in time.
        """
        runs = [run for tier in reversed(self._tiers) for run in tier]
        self._tiers = []
        while len(runs) > self.fan_in:
            newest = runs[-self.fan_in :]
            runs = [*runs[: -self.fan_in], self._write(self._merged(newest))]
        return runs

    def _merged(self, runs: list[pathlib.Path]) -> Iterator[T]:
        """The items of sorted runs, sorted; the earlier run first on
        equal keys."""
        if len(runs) == 1:
            return self._read(runs[0])
        return iter(
            heapq.merge(*(self._read(path) for path in runs), key=self.key)
        )

    def _write(self, items: Iterable[T]) -> pathlib.Path:
        """Writes a run of sorted items; returns its path.

        Items may hold personal data: the directory is the owner's only
        (`mkdtemp`), and goes with the sorter, even one never closed.
        """
        if self._spill_directory is None:
            self._spill_directory = pathlib.Path(
                tempfile.mkdtemp(prefix=self._prefix, dir=self._directory)
            )
            self._remove = weakref.finalize(
                self, shutil.rmtree, self._spill_directory, True
            )
        self._written += 1
        path = self._spill_directory / f"{self._written}.run"
        with path.open("wb") as file:
            self._format.dump(items, file)
        return path

    def _read(self, path: pathlib.Path) -> Iterator[T]:
        """Yields the items of a run, then removes it."""
        with path.open("rb") as file:
            yield from self._format.load(file)
        path.unlink()


def sorted_spilling[T](
    items: Iterable[T],
    key: Callable[[T], Comparable],
    *,
    spill_rows: int,
    fan_in: int = FAN_IN,
    directory: pathlib.Path | None = None,
    spill_format: SpillFormat[T] | None = None,
) -> Generator[T]:
    """Yields `items` sorted by `key`, through an `ExternalSorter`.

    The runs on disk go when the iterator ends, is closed, or is garbage
    collected. See `ExternalSorter` for the arguments.

    Raises:
        ValueError: If `spill_rows` is below 1, or `fan_in` below 2.
    """
    with ExternalSorter[T](
        key,
        spill_rows=spill_rows,
        fan_in=fan_in,
        directory=directory,
        spill_format=spill_format,
    ) as sorter:
        sorter.extend(items)
        yield from sorter
