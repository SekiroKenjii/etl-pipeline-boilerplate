"""The tiered key cache: memory, then a local disk, then the registry."""

from __future__ import annotations

import concurrent.futures
import gc
import logging
import os
import pathlib
import signal
import stat
import subprocess
import sys
import tempfile
import time

from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st
import pytest

from core import contract
from core import keys as core_keys
from core.keys import tiered
from tests.factories import registries

ROOT = pathlib.Path(__file__).resolve().parents[3]
ENTITY = "crm.customer"
ENTITIES = ("crm.customer", "crm.order")
ENOUGH = 10**9


def _entry(key: str, natural: str = "N") -> contract.KeyEntry:
    return contract.KeyEntry(ENTITY, key, (natural,))


def _memory_for(entries: int) -> int:
    """A memory budget holding `entries` entries the size of `_entry`'s."""
    return entries * tiered.entry_bytes((ENTITY, "k1"), ("N",))


def _cache(
    inner: contract.IKeyRegistry,
    *,
    entries: int,
    disk_budget: int = ENOUGH,
    directory: pathlib.Path | None = None,
) -> core_keys.TieredKeyCache:
    return core_keys.TieredKeyCache(
        inner,
        memory_budget=_memory_for(entries),
        disk_budget=disk_budget,
        directory=directory,
    )


class TestTieredKeyCache:

    def test_hits_are_served_from_memory(self) -> None:
        inner = registries.SpyRegistry()
        inner.register([_entry("k1"), _entry("k2")])
        cache = _cache(inner, entries=1000, disk_budget=0)

        assert cache.known(ENTITY, ["k1", "k2", "k3"]) == {"k1", "k2"}
        assert cache.known(ENTITY, ["k1", "k2"]) == {"k1", "k2"}

        assert inner.known_calls == [["k1", "k2", "k3"]]  # one call, all misses
        assert (cache.memory_hits, cache.disk_hits, cache.misses) == (2, 0, 3)

    def test_unknown_keys_are_not_cached(self) -> None:
        inner = core_keys.InMemoryKeyRegistry()
        cache = _cache(inner, entries=1000)
        assert cache.known(ENTITY, ["k1"]) == set()

        inner.register([_entry("k1")])  # e.g. by another step or host

        assert cache.known(ENTITY, ["k1"]) == {"k1"}

    def test_memory_holds_as_many_entries_as_the_budget_pays_for(
        self, tmp_path: pathlib.Path
    ) -> None:
        inner = registries.SpyRegistry()
        with _cache(inner, entries=2, directory=tmp_path) as cache:
            cache.register([_entry("k1"), _entry("k2"), _entry("k3")])

            assert cache.max_entries == 2
            assert cache.disk_entries == 1
            assert cache.known(ENTITY, ["k2", "k3"]) == {"k2", "k3"}
            assert cache.memory_hits == 2

    def test_evicted_keys_are_served_from_disk_without_the_registry(
        self, tmp_path: pathlib.Path
    ) -> None:
        inner = registries.SpyRegistry()
        with _cache(inner, entries=2, directory=tmp_path) as cache:
            cache.register([_entry("k1"), _entry("k2"), _entry("k3")])

            assert cache.known(ENTITY, ["k1"]) == {"k1"}
            assert cache.natural_key(ENTITY, "k1") == ("N",)
            assert cache.known("crm.order", ["k1"]) == set()

            assert inner.known_calls == [["k1"]]  # the other entity only
            assert (cache.memory_hits, cache.disk_hits, cache.misses) == (
                0,
                1,
                1,
            )

    def test_without_a_disk_tier_evicted_keys_go_to_the_registry(self) -> None:
        inner = registries.SpyRegistry()
        cache = _cache(inner, entries=2, disk_budget=0)
        cache.register([_entry("k1"), _entry("k2"), _entry("k3")])

        assert cache.known(ENTITY, ["k1"]) == {"k1"}
        assert inner.known_calls == [["k1"]]  # k1 was evicted, k2 / k3 kept
        assert cache.spill_directory is None

    def test_register_writes_through_with_the_natural_key(self) -> None:
        inner = core_keys.InMemoryKeyRegistry()
        cache = _cache(inner, entries=1000)

        cache.register([_entry("k1", "C001")])

        assert inner.known(ENTITY, ["k1"]) == {"k1"}
        assert cache.natural_key(ENTITY, "k1") == ("C001",)

    def test_keys_in_the_cache_are_not_registered_again(self) -> None:
        inner = registries.CountingRegistry()
        cache = _cache(inner, entries=1000)
        batch = [_entry("k1", "C001"), _entry("k2", "C002")]

        cache.register(batch)
        cache.register(batch)
        cache.register([_entry("k2", "C002"), _entry("k3", "C003")])
        cache.register([])

        # Only the keys no tier holds reach the slower registry.
        assert inner.registered == [["k1", "k2"], ["k3"]]
        assert inner.known(ENTITY, ["k1", "k2", "k3"]) == {"k1", "k2", "k3"}

    def test_keys_on_disk_are_not_registered_again(
        self, tmp_path: pathlib.Path
    ) -> None:
        inner = registries.CountingRegistry()
        with _cache(inner, entries=2, directory=tmp_path) as cache:
            batch = [_entry(f"k{i}") for i in range(5)]
            keys = [e.surrogate_key for e in batch]

            cache.register(batch)
            cache.register(batch)
            cache.register(list(reversed(batch)))

            assert inner.registered == [keys]
            assert cache.known(ENTITY, keys) == set(keys)
            # The keys on disk stayed there: no churn between the tiers.
            assert cache.disk_entries == 3
            assert cache.memory_hits == 2
            assert cache.disk_hits == 3

    def test_a_key_found_known_is_not_registered_again(self) -> None:
        inner = registries.CountingRegistry()
        inner.register([_entry("k1", "C001")])
        inner.registered.clear()
        cache = _cache(inner, entries=1000)
        cache.known(ENTITY, ["k1"])  # Cached without its natural key.

        cache.register([_entry("k1", "C001")])

        assert inner.registered == []
        # The natural key given is kept: no lookup for it.
        assert cache.natural_key(ENTITY, "k1") == ("C001",)
        assert inner.looked_up == []

    def test_a_natural_key_not_fetched_yet_is_fetched_once(
        self, tmp_path: pathlib.Path
    ) -> None:
        inner = registries.CountingRegistry()
        inner.register([_entry("k1", "C001"), _entry("k2", "C002")])
        with _cache(inner, entries=1, directory=tmp_path) as cache:
            cache.known(ENTITY, ["k1", "k2"])  # k1 evicted to disk, unfetched

            assert cache.natural_key(ENTITY, "k1") == ("C001",)
            assert cache.natural_key(ENTITY, "k1") == ("C001",)
            assert cache.natural_key(ENTITY, "nope") is None

            assert inner.looked_up == ["k1", "nope"]

    def test_a_memory_smaller_than_the_batch_registers_every_key(
        self,
    ) -> None:
        inner = registries.CountingRegistry()
        cache = _cache(inner, entries=2, disk_budget=0)
        batch = [_entry(f"k{i}") for i in range(5)]
        keys = [e.surrogate_key for e in batch]

        cache.register(batch)
        cache.register(batch)

        assert inner.known(ENTITY, keys) == set(keys)
        assert cache.known(ENTITY, keys) == set(keys)
        # Without a disk tier, the keys memory had to evict go again: no
        # key is ever lost to an eviction.
        assert inner.registered == [keys, ["k0", "k1", "k2"]]

    def test_nothing_is_cached_while_the_inner_registry_fails(self) -> None:
        inner = registries.CountingRegistry()
        inner.failing = True
        cache = _cache(inner, entries=1000)
        batch = [_entry("k1")]

        with pytest.raises(contract.RetryableError):
            cache.register(batch)
        assert cache.known(ENTITY, ["k1"]) == set()

        inner.failing = False
        cache.register([_entry("k1")])

        assert inner.registered == [["k1"], ["k1"]]
        assert cache.known(ENTITY, ["k1"]) == {"k1"}


class TestTheDiskTier:

    def test_the_disk_budget_reached_warns_once_and_the_registry_answers(
        self, tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        inner = registries.SpyRegistry()
        cache = _cache(inner, entries=1, disk_budget=1, directory=tmp_path)
        with caplog.at_level(logging.WARNING, logger="core.keys.tiered"):
            cache.register([_entry("k1"), _entry("k2"), _entry("k3")])
            cache.register([_entry("k4")])
            cache.register([_entry("k5")])
            found = cache.known(ENTITY, ["k1", "k2", "k3", "k4", "k5"])

        assert found == {"k1", "k2", "k3", "k4", "k5"}
        assert cache.disk_entries == 2  # k1, k2: the batch that filled it
        assert inner.known_calls == [["k3", "k4"]]  # dropped; k5 in memory
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert "disk budget of 1 bytes" in warnings[0].getMessage()
        assert [cache.natural_key(ENTITY, k) for k in ("k1", "k3")] == [
            ("N",),
            ("N",),
        ]
        cache.close()

    def test_a_failing_disk_tier_closes_and_the_registry_answers(
        self, tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        inner = registries.SpyRegistry()
        cache = _cache(inner, entries=1, directory=tmp_path)
        cache.register([_entry("k1"), _entry("k2")])
        directory = cache.spill_directory
        assert directory is not None
        # SQLite fails from here on, as on a full disk or a corrupt file.
        cache._disk._connection.close()  # type: ignore[union-attr]

        with caplog.at_level(logging.WARNING, logger="core.keys.tiered"):
            found = cache.known(ENTITY, ["k1", "k2"])

        assert found == {"k1", "k2"}
        assert inner.known_calls == [["k1"]]
        assert not directory.exists()
        assert cache.spill_directory is None
        assert len(caplog.records) == 1
        assert "failed and is closed" in caplog.records[0].getMessage()

    def test_a_directory_that_cannot_be_made_leaves_the_registry_to_answer(
        self, tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        inner = registries.SpyRegistry()
        cache = _cache(inner, entries=1, directory=tmp_path / "missing")

        with caplog.at_level(logging.WARNING, logger="core.keys.tiered"):
            cache.register([_entry("k1"), _entry("k2")])
            cache.register([_entry("k3")])
            found = cache.known(ENTITY, ["k1", "k2", "k3"])

        assert found == {"k1", "k2", "k3"}
        assert inner.known_calls == [["k1", "k2"]]
        assert cache.spill_directory is None
        assert len(caplog.records) == 1
        assert "failed and is closed" in caplog.records[0].getMessage()
        assert not (tmp_path / "missing").exists()

    def test_forget_drops_the_key_from_every_tier(
        self, tmp_path: pathlib.Path
    ) -> None:
        inner = registries.SpyRegistry()
        with _cache(inner, entries=1, directory=tmp_path) as cache:
            registries.check_forget(cache)

            # k1 was on disk before `forget`: asked of the registry since.
            assert cache.known(ENTITY, ["k1"]) == set()
            assert inner.known_calls[-1] == ["k1"]

    def test_the_directory_is_the_owners_only_and_goes_with_close(
        self, tmp_path: pathlib.Path
    ) -> None:
        cache = _cache(
            core_keys.InMemoryKeyRegistry(), entries=1, directory=tmp_path
        )
        assert cache.disk_entries == 0
        cache.register([_entry("k1"), _entry("k2")])
        directory = cache.spill_directory

        assert directory is not None
        assert directory.parent == tmp_path
        assert directory.name.startswith("etl-keys-")
        assert stat.S_IMODE(directory.stat().st_mode) == 0o700
        assert (directory / "keys.sqlite3").exists()

        cache.close()

        assert not directory.exists()
        assert cache.spill_directory is None
        # Closed: memory keeps serving, nothing is written to disk again.
        cache.register([_entry("k3")])
        assert cache.known(ENTITY, ["k3"]) == {"k3"}
        assert cache.spill_directory is None
        assert list(tmp_path.iterdir()) == []

    def test_a_cache_garbage_collected_removes_its_directory(
        self, tmp_path: pathlib.Path
    ) -> None:
        cache = _cache(
            core_keys.InMemoryKeyRegistry(), entries=1, directory=tmp_path
        )
        cache.register([_entry("k1"), _entry("k2")])
        directory = cache.spill_directory
        assert directory is not None

        del cache
        gc.collect()

        assert not directory.exists()


class TestThreadsAndSizing:

    def test_threads_registering_the_same_keys_at_once(self) -> None:
        inner = registries.CountingRegistry()
        cache = _cache(inner, entries=1000)
        batch = [_entry(f"k{i}") for i in range(100)]
        keys = [e.surrogate_key for e in batch]

        with concurrent.futures.ThreadPoolExecutor(8) as threads:
            list(threads.map(lambda _: cache.register(batch), range(8)))

        assert inner.known(ENTITY, keys) == set(keys)
        assert cache.known(ENTITY, keys) == set(keys)
        assert all(sorted(given) == sorted(keys) for given in inner.registered)

    def test_threads_registering_and_looking_up_through_every_tier(
        self, tmp_path: pathlib.Path
    ) -> None:
        inner = core_keys.InMemoryKeyRegistry()
        with _cache(inner, entries=5, directory=tmp_path) as cache:

            def work(thread: int) -> tuple[set[str], list[tuple[str, ...]]]:
                keys = [f"k{thread}-{i}" for i in range(50)]
                for key in keys:
                    cache.register([_entry(key)])
                found = cache.known(ENTITY, keys)
                naturals = [
                    cache.natural_key(ENTITY, key) or () for key in keys
                ]
                return found, naturals

            with concurrent.futures.ThreadPoolExecutor(8) as threads:
                results = list(threads.map(work, range(8)))

            every = [f"k{t}-{i}" for t in range(8) for i in range(50)]
            assert all(
                found == {f"k{t}-{i}" for i in range(50)}
                and naturals == [("N",)] * 50
                for t, (found, naturals) in enumerate(results)
            )
            assert cache.known(ENTITY, every) == set(every)
            assert cache.disk_entries >= len(every) - 5

    def test_the_cost_of_an_entry_is_sampled_again_as_entries_come(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(tiered, "RESAMPLE_EVERY", 10)
        monkeypatch.setattr(tiered, "SAMPLE_SIZE", 2)
        cache = _cache(core_keys.InMemoryKeyRegistry(), entries=4)
        cache.register([_entry(f"k{i}") for i in range(10)])
        assert cache.max_entries == 4

        cache.register([_entry(f"k{i}", "N" * 500) for i in range(10, 20)])

        assert cache.max_entries < 4

    def test_a_window_of_small_entries_does_not_raise_the_limit(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(tiered, "RESAMPLE_EVERY", 10)
        monkeypatch.setattr(tiered, "SAMPLE_SIZE", 2)
        cache = _cache(core_keys.InMemoryKeyRegistry(), entries=4)
        cache.register([_entry(f"k{i}", "N" * 500) for i in range(10)])
        assert cache.max_entries == 1

        cache.register([_entry(f"k{i}") for i in range(10, 20)])

        # Big entries may still be in memory: the limit follows every
        # sample, not the latest small ones alone.
        assert cache.max_entries < 4

    @pytest.mark.parametrize(("memory", "disk"), [(0, 0), (-1, 10), (10, -1)])
    def test_the_bounds_are_checked(self, memory: int, disk: int) -> None:
        inner = core_keys.InMemoryKeyRegistry()
        with pytest.raises(ValueError, match="memory_budget"):
            core_keys.TieredKeyCache(
                inner,
                memory_budget=memory,
                disk_budget=disk,
            )


# The script of a process holding a cache that spilled, then signalled; as
# `etl run` ends on SIGTERM: the kernel cancels the run and the process
# exits, and the directory goes with the cache, closed or not.
_SPILLING = """
import pathlib, signal, sys, time
from core import contract, keys

cache = keys.TieredKeyCache(
    keys.InMemoryKeyRegistry(), memory_budget=1, disk_budget=10**6
)
cache.register([contract.KeyEntry("e", f"k{i}", ("n",)) for i in range(3)])
pathlib.Path(sys.argv[1]).write_text(str(cache.spill_directory))
signal.signal(signal.SIGTERM, lambda *_: sys.exit(130))
while True:
    time.sleep(0.05)
"""


def test_a_process_ending_on_sigterm_leaves_no_directory(
    tmp_path: pathlib.Path,
) -> None:
    temporary = tmp_path / "tmp"
    temporary.mkdir()
    spilled = tmp_path / "spilled"
    with subprocess.Popen(
        [sys.executable, "-c", _SPILLING, str(spilled)],
        cwd=ROOT,
        env={**os.environ, "PYTHONPATH": "src", "TMPDIR": str(temporary)},
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    ) as process:
        try:
            deadline = time.monotonic() + 60
            while not spilled.exists() and process.poll() is None:
                assert time.monotonic() < deadline, "no spill"
                time.sleep(0.02)
            directory = pathlib.Path(spilled.read_text())
            assert directory.parent == temporary
            assert directory.exists()
            os.kill(process.pid, signal.SIGTERM)
            output, _ = process.communicate(timeout=60)
        finally:
            if process.poll() is None:
                process.kill()

    assert process.returncode == 130, output
    assert not directory.exists()
    assert list(temporary.iterdir()) == []


def _natural_of(key: str) -> tuple[str, ...]:
    """The natural key of a surrogate key: the surrogate is a function of
    it, so the same surrogate always comes with the same natural key."""
    return (f"N{key}",)


_KEYS = st.sampled_from([f"k{i}" for i in range(6)])
_OPERATIONS = st.lists(
    st.tuples(
        st.sampled_from(["register", "known", "natural_key", "forget"]),
        st.sampled_from(ENTITIES),
        st.lists(_KEYS, max_size=6),
    ),
    max_size=40,
)


def _apply(
    registry: contract.IKeyRegistry,
    operation: str,
    entity: str,
    keys: list[str],
) -> object:
    if operation == "register":
        registry.register(
            [contract.KeyEntry(entity, key, _natural_of(key)) for key in keys]
        )
        return None
    if operation == "known":
        return registry.known(entity, keys)
    if not keys:
        return None
    if operation == "natural_key":
        return registry.natural_key(entity, keys[0])
    return registry.forget(entity, _natural_of(keys[0]))


@given(
    operations=_OPERATIONS,
    entries=st.sampled_from([1, 3, 1000]),
    disk_budget=st.sampled_from([0, 1, ENOUGH]),
)
@settings(max_examples=150, deadline=None)
def test_the_tiers_answer_like_the_registry_alone(
    operations: list[tuple[str, str, list[str]]], entries: int, disk_budget: int
) -> None:
    oracle = core_keys.InMemoryKeyRegistry()
    with tempfile.TemporaryDirectory() as temporary:
        with _cache(
            core_keys.InMemoryKeyRegistry(),
            entries=entries,
            disk_budget=disk_budget,
            directory=pathlib.Path(temporary),
        ) as cache:
            for operation, entity, keys in operations:
                expected = _apply(oracle, operation, entity, keys)
                assert _apply(cache, operation, entity, keys) == expected

            for entity in ENTITIES:
                assert sorted(
                    cache.entries(entity), key=lambda e: e.surrogate_key
                ) == sorted(
                    oracle.entries(entity), key=lambda e: e.surrogate_key
                )
