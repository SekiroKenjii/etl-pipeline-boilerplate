"""A key cache in tiers: memory, then a local disk, then the registry."""

from __future__ import annotations

import collections
from collections.abc import Callable, Collection, Iterator, Sequence
import itertools
import json
import logging
import os
import pathlib
import shutil
import sqlite3
import sys
import tempfile
import threading
from typing import Self
import weakref

from core import contract

_LOG = logging.getLogger(__name__)

SAMPLE_SIZE = 1_000
RESAMPLE_EVERY = 100_000
# What the map costs per entry on top of the entry's own objects: the dict
# slot and index, and the recency links (measured on CPython 3.14).
_SLOT_BYTES = 96
# Below SQLite's lowest default limit of variables in a statement (999).
_IN_CHUNK = 900

type _Key = tuple[str, str]
type _Natural = tuple[str, ...] | None
type _Entry = tuple[_Key, _Natural]


def entry_bytes(key: _Key, natural_key: _Natural) -> int:
    """The bytes an entry costs in memory: its key tuple, its surrogate key,
    its natural key tuple and the strings of it, and the map's slot.

    The entity name is not counted: the entries of an entity share it.
    """
    size = sys.getsizeof(key) + sys.getsizeof(key[1]) + _SLOT_BYTES
    if natural_key is not None:
        size += sys.getsizeof(natural_key)
        size += sum(sys.getsizeof(component) for component in natural_key)
    return size


class TieredKeyCache(contract.IKeyRegistry):
    """A cache in front of a slower registry, such as a database: the keys
    most recently used in memory, the ones evicted from it on a local disk.

    Memory holds `memory_budget` bytes of entries: the cost of an entry is
    measured on the first 1,000 and again every 100,000. What memory evicts
    goes, in batches, to a SQLite file in a temporary directory of the
    process owner only, up to `disk_budget` bytes; past that the cache logs
    one warning and drops what it evicts, which the registry answers for.
    The registry is the truth: `register` writes it first, and nothing is
    lost with either tier. Only positive answers are cached: a key unknown
    now may be registered a moment later by another step or host. A key
    found on disk stays there: brought back to memory, it would push another
    key to disk, and a pass over more keys than memory holds would rewrite
    them all.

    Thread-safe, by one lock; the registry is called outside it. Closing
    the cache (`close`, the context manager, garbage collection, exit)
    removes the directory.

    Attributes:
        inner: The cached registry.
        memory_budget: The most bytes of entries kept in memory.
        disk_budget: The most bytes kept on disk; 0 for no disk tier.
        memory_hits: The keys `known` found in memory.
        disk_hits: The keys `known` found on disk.
        misses: The keys `known` had to ask the registry about.
    """

    def __init__(
        self,
        inner: contract.IKeyRegistry,
        *,
        memory_budget: int,
        disk_budget: int = 0,
        directory: pathlib.Path | None = None,
        prefix: str = "etl-keys-",
    ) -> None:
        """Initializes an empty cache.

        Args:
            inner: The registry to cache.
            memory_budget: The most bytes of entries kept in memory.
            disk_budget: The most bytes kept on disk; 0 for no disk tier.
            directory: Where the disk tier goes: a directory of its own,
                readable by its owner only, is made there on the first
                eviction; in the temporary directory (`$TMPDIR`) by default.
            prefix: The name of that directory starts with it.

        Raises:
            ValueError: If `memory_budget` is below 1, or `disk_budget`
                below 0.
        """
        if memory_budget < 1 or disk_budget < 0:
            raise ValueError("memory_budget must be >= 1, and disk_budget >= 0")
        self.inner = inner
        self.memory_budget = memory_budget
        self.disk_budget = disk_budget
        # (entity, surrogate key) -> natural key, or None when not fetched yet.
        self._memory: collections.OrderedDict[_Key, _Natural] = (
            collections.OrderedDict()
        )
        self._disk = _DiskTier(disk_budget, directory, prefix)
        self._lock = threading.Lock()
        self._max_entries = 1
        self._puts = 0
        self._sampled_bytes = 0
        self._sampled = 0
        self._window_bytes = 0
        self._window = 0
        self.memory_hits = 0
        self.disk_hits = 0
        self.misses = 0

    @property
    def max_entries(self) -> int:
        """The most entries memory holds, from the budget and the measured
        cost of an entry."""
        return self._max_entries

    @property
    def spill_directory(self) -> pathlib.Path | None:
        """Where the disk tier is; None until memory evicted an entry."""
        return self._disk.directory

    @property
    def disk_entries(self) -> int:
        """How many entries were written to disk."""
        return self._disk.rows

    def register(self, entries: Sequence[contract.KeyEntry]) -> None:
        """See `contract.IKeyRegistry.register`; the registry gets the
        entries no tier holds. A key `known` found counts as held: the
        surrogate key is a function of the natural key."""
        with self._lock:
            missing: dict[str, list[contract.KeyEntry]] = {}
            for entry in entries:
                if (entry.entity, entry.surrogate_key) not in self._memory:
                    missing.setdefault(entry.entity, []).append(entry)
            on_disk = {
                entity: self._disk.known(
                    entity, [e.surrogate_key for e in batch]
                )
                for entity, batch in missing.items()
            }
        new = [
            entry
            for batch in missing.values()
            for entry in batch
            if entry.surrogate_key not in on_disk[entry.entity]
        ]
        if new:
            self.inner.register(new)
        with self._lock:
            evicted: list[_Entry] = []
            for entry in entries:
                key = (entry.entity, entry.surrogate_key)
                if (
                    key in self._memory
                    or entry.surrogate_key not in on_disk.get(entry.entity, ())
                ):
                    self._put(key, entry.natural_key, evicted)
            self._disk.write(evicted)

    def known(self, entity: str, surrogate_keys: Collection[str]) -> set[str]:
        """See `contract.IKeyRegistry.known`."""
        found: set[str] = set()
        missing: list[str] = []
        with self._lock:
            for key in surrogate_keys:
                if (entity, key) in self._memory:
                    self._memory.move_to_end((entity, key))
                    found.add(key)
                else:
                    missing.append(key)
            self.memory_hits += len(found)
            if missing:
                on_disk = self._disk.known(entity, missing)
                found |= on_disk
                missing = [key for key in missing if key not in on_disk]
                self.disk_hits += len(on_disk)
                self.misses += len(missing)

        if missing:
            confirmed = self.inner.known(entity, missing)
            with self._lock:
                evicted: list[_Entry] = []
                for key in confirmed:
                    if (entity, key) not in self._memory:
                        self._put((entity, key), None, evicted)
                self._disk.write(evicted)
            found |= confirmed
        return found

    def entries(self, entity: str) -> Iterator[contract.KeyEntry]:
        """See `contract.IKeyRegistry.entries`; read from the registry."""
        return self.inner.entries(entity)

    def natural_key(
        self, entity: str, surrogate_key: str
    ) -> tuple[str, ...] | None:
        key = (entity, surrogate_key)
        with self._lock:
            cached = self._memory.get(key)
            if cached is None:
                cached = self._disk.natural_key(entity, surrogate_key)
        if cached is not None:
            return cached

        natural = self.inner.natural_key(entity, surrogate_key)
        if natural is not None:
            with self._lock:
                evicted: list[_Entry] = []
                self._put(key, natural, evicted)
                self._disk.write(evicted)
        return natural

    def forget(self, entity: str, natural_key: Sequence[str]) -> list[str]:
        gone = self.inner.forget(entity, natural_key)
        with self._lock:
            for key in gone:
                self._memory.pop((entity, key), None)
            self._disk.forget(entity, gone)
        return gone

    def close(self) -> None:
        """Removes the disk tier; memory keeps serving."""
        with self._lock:
            self._disk.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _put(self, key: _Key, natural: _Natural, evicted: list[_Entry]) -> None:
        """Keeps an entry in memory, as the most recent; the entries it
        pushes out go to `evicted`."""
        self._memory[key] = natural
        self._memory.move_to_end(key)
        self._measure(key, natural)
        while len(self._memory) > self._max_entries:
            evicted.append(self._memory.popitem(last=False))

    def _measure(self, key: _Key, natural: _Natural) -> None:
        """Samples the cost of an entry, and sizes memory in entries.

        The cost is the higher of the mean over every sample and the mean
        of the latest window: a window of small entries (keys confirmed
        without their natural key) does not raise the limit while memory
        still holds the bigger ones.
        """
        position = self._puts % RESAMPLE_EVERY
        self._puts += 1
        if position >= SAMPLE_SIZE:
            return
        size = entry_bytes(key, natural)
        if position == 0:
            self._window_bytes = self._window = 0
        self._sampled_bytes += size
        self._sampled += 1
        self._window_bytes += size
        self._window += 1
        cost = max(
            self._sampled_bytes / self._sampled,
            self._window_bytes / self._window,
        )
        self._max_entries = max(1, int(self.memory_budget // cost))


def _discard(connection: sqlite3.Connection, directory: pathlib.Path) -> None:
    connection.close()
    shutil.rmtree(directory, ignore_errors=True)


class _DiskTier:
    """The entries on disk: a SQLite table in a directory of its own.

    A row is found by the process's `hash` of its key, an integer: probing
    integers costs less than comparing texts, and the file lives and dies
    with the process. Two keys with one hash share a row, the later one
    keeping it; a row answers for a key only when its entity and surrogate
    key are the one asked, so a lost key costs a registry call, never a
    wrong answer.

    Opened on the first write. Reads go on once the budget is reached; a
    failure of the disk or of SQLite (no directory to write in, a full disk,
    a corrupt file: there is no journal) closes the tier, after one warning,
    and the registry answers.
    """

    def __init__(
        self, budget: int, directory: pathlib.Path | None, prefix: str
    ) -> None:
        self.budget = budget
        self.rows = 0
        self._parent = directory
        self._prefix = prefix
        self._connection: sqlite3.Connection | None = None
        self._path: pathlib.Path | None = None
        self._remove: Callable[[], object] | None = None
        self._full = False
        self._closed = False

    @property
    def directory(self) -> pathlib.Path | None:
        """Where the file is; None before the first write, and after close."""
        return self._path.parent if self._path is not None else None

    def write(self, entries: Sequence[_Entry]) -> None:
        """Keeps `entries`, the newest value of a key winning; drops them
        once the budget is reached, with one warning."""
        if not entries or self._full or self._closed or self.budget == 0:
            return
        try:
            connection = self._connection or self._open()
            connection.execute("BEGIN")
            connection.executemany(
                "INSERT OR REPLACE INTO keys VALUES (?, ?, ?, ?)",
                (
                    (
                        hash(key),
                        key[0],
                        key[1],
                        None if natural is None else json.dumps(natural),
                    )
                    for key, natural in entries
                ),
            )
            connection.execute("COMMIT")
            self.rows += len(entries)
            size = os.stat(self._path or "").st_size
        except (OSError, sqlite3.Error) as exc:
            self._give_up(exc)
            return
        if size > self.budget:
            self._full = True
            _LOG.warning(
                "the key cache reached its disk budget of %d bytes with %d"
                " keys on disk: the keys its memory evicts from now on are"
                " asked of the key registry again",
                self.budget,
                self.rows,
            )

    def known(self, entity: str, surrogate_keys: Sequence[str]) -> set[str]:
        """The keys of `surrogate_keys` on disk for `entity`."""
        if self._connection is None:
            return set()
        wanted = set(surrogate_keys)
        found: set[str] = set()
        try:
            for chunk in itertools.batched(wanted, _IN_CHUNK):
                marks = ",".join("?" * len(chunk))
                rows = self._connection.execute(
                    "SELECT surrogate FROM keys WHERE id IN"
                    f" ({marks}) AND entity = ?",  # nosec B608: placeholders.
                    (*(hash((entity, key)) for key in chunk), entity),
                )
                found.update(row[0] for row in rows if row[0] in wanted)
        except sqlite3.Error as exc:
            self._give_up(exc)
        return found

    def natural_key(self, entity: str, surrogate_key: str) -> _Natural:
        """The natural key on disk, None when absent or not fetched yet."""
        if self._connection is None:
            return None
        try:
            row = self._connection.execute(
                "SELECT natural_key FROM keys WHERE id = ? AND entity = ? AND"
                " surrogate = ?",
                (hash((entity, surrogate_key)), entity, surrogate_key),
            ).fetchone()
        except sqlite3.Error as exc:
            self._give_up(exc)
            return None
        if row is None or row[0] is None:
            return None
        return tuple(json.loads(row[0]))

    def forget(self, entity: str, surrogate_keys: Sequence[str]) -> None:
        """Drops the keys of `surrogate_keys` for `entity`."""
        if self._connection is None or not surrogate_keys:
            return
        try:
            self._connection.execute("BEGIN")
            self._connection.executemany(
                "DELETE FROM keys WHERE id = ? AND entity = ? AND"
                " surrogate = ?",
                ((hash((entity, key)), entity, key) for key in surrogate_keys),
            )
            self._connection.execute("COMMIT")
        except sqlite3.Error as exc:
            self._give_up(exc)

    def close(self) -> None:
        """Removes the directory; nothing is written or read afterwards."""
        self._closed = True
        if self._remove is not None:
            self._remove()
            self._remove = None
        self._connection = None
        self._path = None

    def _open(self) -> sqlite3.Connection:
        """Makes the directory and the table.

        Keys may be personal data: the directory is the owner's only
        (`mkdtemp`), and goes with the tier, even one never closed.
        """
        directory = pathlib.Path(
            tempfile.mkdtemp(prefix=self._prefix, dir=self._parent)
        )
        try:
            connection = sqlite3.connect(
                directory / "keys.sqlite3",
                isolation_level=None,
                check_same_thread=False,
            )
        except sqlite3.Error:
            shutil.rmtree(directory, ignore_errors=True)
            raise
        self._path = directory / "keys.sqlite3"
        self._remove = weakref.finalize(self, _discard, connection, directory)
        # A cache: nothing to recover after a crash, so no journal, no fsync.
        connection.execute("PRAGMA journal_mode = OFF")
        connection.execute("PRAGMA synchronous = OFF")
        connection.execute("PRAGMA locking_mode = EXCLUSIVE")
        connection.execute("PRAGMA temp_store = MEMORY")
        connection.execute(
            "CREATE TABLE keys (id INTEGER PRIMARY KEY, entity TEXT NOT NULL,"
            " surrogate TEXT NOT NULL, natural_key TEXT)"
        )
        self._connection = connection
        return connection

    def _give_up(self, error: Exception) -> None:
        _LOG.warning(
            "the key cache's disk tier failed and is closed; the key registry"
            " answers for the keys it held: %s",
            error,
        )
        self.close()
