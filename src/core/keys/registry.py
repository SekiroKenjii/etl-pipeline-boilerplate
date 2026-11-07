"""Key registries (crosswalks): in memory, JSON Lines files, and a cache."""

from __future__ import annotations

import collections
from collections.abc import Collection, Iterator, Sequence
import json
import pathlib
import threading

from core import contract
from shared import utils


class InMemoryKeyRegistry(contract.IKeyRegistry):
    """Keeps the crosswalk in memory, for tests and local development."""

    def __init__(self) -> None:
        self._keys: dict[str, dict[str, tuple[str, ...]]] = {}
        self._lock = threading.Lock()

    def register(self, entries: Sequence[contract.KeyEntry]) -> None:
        with self._lock:
            for entry in entries:
                self._keys.setdefault(entry.entity, {})[
                    entry.surrogate_key
                ] = entry.natural_key

    def known(self, entity: str, surrogate_keys: Collection[str]) -> set[str]:
        with self._lock:
            keys = self._keys.get(entity, {})
            return {key for key in surrogate_keys if key in keys}

    def entries(self, entity: str) -> Iterator[contract.KeyEntry]:
        with self._lock:
            keys = dict(self._keys.get(entity, {}))
        for surrogate, natural in keys.items():
            yield contract.KeyEntry(entity, surrogate, natural)

    def natural_key(
        self, entity: str, surrogate_key: str
    ) -> tuple[str, ...] | None:
        with self._lock:
            return self._keys.get(entity, {}).get(surrogate_key)

    def forget(self, entity: str, natural_key: Sequence[str]) -> list[str]:
        wanted = tuple(natural_key)
        with self._lock:
            keys = self._keys.get(entity, {})
            gone = [k for k, natural in keys.items() if natural == wanted]
            for key in gone:
                del keys[key]
        return gone


class JsonLinesKeyRegistry(contract.IKeyRegistry):
    """Keeps one append-only file per entity: `{directory}/{entity}.keys.jsonl`.

    Each line is `{"surrogate_key": ..., "natural_key": [...]}`. The file of an
    entity is loaded on first use and kept in memory; only new keys are
    appended. Good for a single host and moderate volumes; use a database
    backed registry, with upserts and shared by hosts, beyond that.

    Attributes:
        directory: Where the files are written.
    """

    def __init__(self, directory: pathlib.Path | str) -> None:
        self.directory = pathlib.Path(directory)
        self._keys: dict[str, dict[str, tuple[str, ...]]] = {}
        self._lock = threading.Lock()

    def path_for(self, entity: str) -> pathlib.Path:
        """Returns the file of `entity`."""
        return self.directory / f"{entity}.keys.jsonl"

    def register(self, entries: Sequence[contract.KeyEntry]) -> None:
        """See `contract.IKeyRegistry.register`; appends the new keys only."""
        with self._lock:
            new: dict[str, list[contract.KeyEntry]] = {}
            for entry in entries:
                keys = self._load(entry.entity)
                if entry.surrogate_key not in keys:
                    keys[entry.surrogate_key] = entry.natural_key
                    new.setdefault(entry.entity, []).append(entry)

            for entity, added in new.items():
                utils.append_json_lines(
                    self.path_for(entity),
                    (
                        {
                            "surrogate_key": e.surrogate_key,
                            "natural_key": list(e.natural_key),
                        }
                        for e in added
                    ),
                )

    def known(self, entity: str, surrogate_keys: Collection[str]) -> set[str]:
        with self._lock:
            keys = self._load(entity)
            return {key for key in surrogate_keys if key in keys}

    def entries(self, entity: str) -> Iterator[contract.KeyEntry]:
        with self._lock:
            keys = dict(self._load(entity))
        for surrogate, natural in keys.items():
            yield contract.KeyEntry(entity, surrogate, natural)

    def natural_key(
        self, entity: str, surrogate_key: str
    ) -> tuple[str, ...] | None:
        with self._lock:
            return self._load(entity).get(surrogate_key)

    def forget(self, entity: str, natural_key: Sequence[str]) -> list[str]:
        """See `contract.IKeyRegistry.forget`; rewrites the entity's file."""
        wanted = tuple(natural_key)
        with self._lock:
            keys = self._load(entity)
            gone = [k for k, natural in keys.items() if natural == wanted]
            if not gone:
                return []
            for key in gone:
                del keys[key]
            utils.atomic_write_text(
                self.path_for(entity),
                "".join(
                    json.dumps(
                        {"surrogate_key": key, "natural_key": list(natural)},
                        ensure_ascii=False,
                    )
                    + "\n"
                    for key, natural in keys.items()
                ),
            )
        return gone

    def _load(self, entity: str) -> dict[str, tuple[str, ...]]:
        """Returns the keys of `entity`, reading its file on first use."""
        keys = self._keys.get(entity)
        if keys is None:
            keys = {}
            path = self.path_for(entity)
            if path.exists():
                for line in path.read_text(encoding="utf-8").splitlines():
                    if line.strip():
                        item = json.loads(line)
                        keys[item["surrogate_key"]] = tuple(item["natural_key"])
            self._keys[entity] = keys
        return keys


class CachedKeyRegistry(contract.IKeyRegistry):
    """An LRU cache in front of a slower registry, such as a database.

    Written through on `register`. Only positive answers are cached: a key
    unknown now may be registered a moment later by another step or host.
    `known` costs one call to the inner registry for all the misses of a
    batch; natural keys are fetched only when asked for.

    Attributes:
        inner: The cached registry.
        max_entries: The most keys kept in the cache.
        hits: The keys `known` found in the cache.
        misses: The keys `known` had to ask the inner registry about.
    """

    def __init__(
        self, inner: contract.IKeyRegistry, max_entries: int = 100_000
    ) -> None:
        """Initializes an empty cache.

        Args:
            inner: The registry to cache.
            max_entries: The most keys kept in the cache.

        Raises:
            ValueError: If `max_entries` is below 1.
        """
        if max_entries < 1:
            raise ValueError("max_entries must be >= 1")
        self.inner = inner
        self.max_entries = max_entries
        # (entity, surrogate key) -> natural key, or None when not fetched yet.
        self._cache: collections.OrderedDict[
            tuple[str, str], tuple[str, ...] | None
        ] = collections.OrderedDict()
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0

    def register(self, entries: Sequence[contract.KeyEntry]) -> None:
        self.inner.register(entries)
        with self._lock:
            for entry in entries:
                self._put(
                    (entry.entity, entry.surrogate_key), entry.natural_key
                )

    def known(self, entity: str, surrogate_keys: Collection[str]) -> set[str]:
        """See `contract.IKeyRegistry.known`."""
        found: set[str] = set()
        missing: list[str] = []
        with self._lock:
            for key in surrogate_keys:
                if (entity, key) in self._cache:
                    self._cache.move_to_end((entity, key))
                    found.add(key)
                else:
                    missing.append(key)
            self.hits += len(found)
            self.misses += len(missing)

        if missing:
            confirmed = self.inner.known(entity, missing)
            with self._lock:
                for key in confirmed:
                    if (entity, key) not in self._cache:
                        self._put((entity, key), None)
            found |= confirmed
        return found

    def entries(self, entity: str) -> Iterator[contract.KeyEntry]:
        """See `contract.IKeyRegistry.entries`; read from the inner registry."""
        return self.inner.entries(entity)

    def natural_key(
        self, entity: str, surrogate_key: str
    ) -> tuple[str, ...] | None:
        with self._lock:
            cached = self._cache.get((entity, surrogate_key))
        if cached is not None:
            return cached

        natural = self.inner.natural_key(entity, surrogate_key)
        if natural is not None:
            with self._lock:
                self._put((entity, surrogate_key), natural)
        return natural

    def forget(self, entity: str, natural_key: Sequence[str]) -> list[str]:
        gone = self.inner.forget(entity, natural_key)
        with self._lock:
            for key in gone:
                self._cache.pop((entity, key), None)
        return gone

    def _put(
        self, key: tuple[str, str], natural: tuple[str, ...] | None
    ) -> None:
        self._cache[key] = natural
        self._cache.move_to_end(key)
        while len(self._cache) > self.max_entries:
            self._cache.popitem(last=False)
