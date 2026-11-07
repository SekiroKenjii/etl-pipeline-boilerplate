from __future__ import annotations

from collections.abc import Collection, Sequence
import datetime
import decimal
import pathlib
import uuid

import pytest

from core import contract
from core import keys as core_keys
from shared import utils
from tests.factories import fakes
from tests.factories import registries

CUSTOMERS = core_keys.key_namespace("legacy.CUST_MASTER", "crm.customer")


class TestCanonicalForm:

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("  C001 ", "C001"),
            ("Café", "Café"),  # decomposed accent -> NFC
            (42, "42"),
            (42.0, "42"),
            (1.5, "1.5"),
            (decimal.Decimal("10.50"), "10.5"),
            (decimal.Decimal("100"), "100"),
            (datetime.date(2026, 10, 31), "2026-10-31"),
            (True, "true"),
        ],
    )
    def test_components(self, value: object, expected: str) -> None:
        assert core_keys.canonical_component(value) == expected

    @pytest.mark.parametrize("value", [None, "", "   "])
    def test_empty_components_have_no_key(self, value: object) -> None:
        with pytest.raises(core_keys.MissingKeyComponentError):
            core_keys.canonical_component(value)


class TestKeyMaker:

    def test_keys_never_change(self) -> None:
        # Golden values: if this fails, every key in production would change.
        maker = core_keys.KeyMaker(CUSTOMERS)

        assert (
            maker.key(core_keys.canonical(["C001"]))
            == "858acae5-adfc-5502-bc36-0230c61c28e2"
        )
        assert (
            maker.key(core_keys.canonical(["VN", 42]))
            == "a4ce2dc8-0ef7-543a-a60c-cbeed1f058cf"
        )

    def test_the_same_natural_key_written_differently_gives_the_same_key(
        self,
    ) -> None:
        maker = core_keys.KeyMaker(CUSTOMERS)

        assert maker.key(core_keys.canonical([" C001"])) == maker.key(
            core_keys.canonical(["C001 "])
        )
        assert maker.key(core_keys.canonical([42])) == maker.key(
            core_keys.canonical(["42"])
        )

    def test_composite_keys_do_not_collide(self) -> None:
        maker = core_keys.KeyMaker(CUSTOMERS)

        assert maker.key(("A|B", "C")) != maker.key(("A", "B|C"))

    def test_namespaces_separate_systems_entities_and_versions(self) -> None:
        keys = {
            core_keys.KeyMaker(
                core_keys.key_namespace(system, entity, version)
            ).key(("1",))
            for system, entity, version in [
                ("legacy.CUST_MASTER", "crm.customer", 1),
                ("legacy.ERP", "crm.customer", 1),
                ("legacy.CUST_MASTER", "crm.order", 1),
                ("legacy.CUST_MASTER", "crm.customer", 2),
            ]
        }

        assert len(keys) == 4

    def test_uuid5_by_default_hmac_with_a_secret(self) -> None:
        public = core_keys.KeyMaker(CUSTOMERS).key(("079123456789",))
        keyed = core_keys.KeyMaker(CUSTOMERS, secret=b"s3cret").key((
            "079123456789",
        ))
        other = core_keys.KeyMaker(CUSTOMERS, secret=b"other").key((
            "079123456789",
        ))

        assert uuid.UUID(public).version == 5
        assert uuid.UUID(keyed).version == 8
        assert len({public, keyed, other}) == 3


def _entry(key: str, natural: str = "N") -> contract.KeyEntry:
    return contract.KeyEntry("crm.customer", key, (natural,))


@pytest.fixture(params=["memory", "jsonl"])
def registry(
    request: pytest.FixtureRequest, tmp_path: pathlib.Path
) -> contract.IKeyRegistry:
    if request.param == "memory":
        return core_keys.InMemoryKeyRegistry()
    return core_keys.JsonLinesKeyRegistry(tmp_path)


class TestRegistries:

    def test_register_known_and_natural_key(
        self, registry: contract.IKeyRegistry
    ) -> None:
        registry.register([_entry("k1", "C001"), _entry("k2", "C002")])
        registry.register([_entry("k1", "C001")])  # idempotent

        assert registry.known("crm.customer", ["k1", "k3"]) == {"k1"}
        assert registry.known("crm.order", ["k1"]) == set()
        assert registry.natural_key("crm.customer", "k2") == ("C002",)
        assert registry.natural_key("crm.customer", "k3") is None

    def test_concurrent_registrations(
        self, registry: contract.IKeyRegistry
    ) -> None:
        list(
            utils.bounded_map(
                lambda i: registry.register([_entry(f"k{i}")]),
                range(100),
                max_workers=8,
            )
        )

        assert (
            len(registry.known("crm.customer", [f"k{i}" for i in range(100)]))
            == 100
        )


def test_jsonl_registry_persists_and_appends_only_new_keys(
    tmp_path: pathlib.Path,
) -> None:
    first = core_keys.JsonLinesKeyRegistry(tmp_path)
    first.register([_entry("k1", "C001")])
    first.register([_entry("k1", "C001"), _entry("k2", "C002")])

    reopened = core_keys.JsonLinesKeyRegistry(tmp_path)

    assert reopened.known("crm.customer", ["k1", "k2"]) == {"k1", "k2"}
    assert len(reopened.path_for("crm.customer").read_text().splitlines()) == 2


class SpyRegistry(core_keys.InMemoryKeyRegistry):

    def __init__(self) -> None:
        super().__init__()
        self.known_calls: list[list[str]] = []

    def known(self, entity: str, surrogate_keys: Collection[str]) -> set[str]:
        self.known_calls.append(sorted(surrogate_keys))
        return super().known(entity, surrogate_keys)


class TestCachedKeyRegistry:

    def test_hits_are_served_from_the_cache(self) -> None:
        inner = SpyRegistry()
        inner.register([_entry("k1"), _entry("k2")])
        cached = core_keys.CachedKeyRegistry(inner)

        assert cached.known("crm.customer", ["k1", "k2", "k3"]) == {"k1", "k2"}
        assert cached.known("crm.customer", ["k1", "k2"]) == {"k1", "k2"}

        assert inner.known_calls == [
            ["k1", "k2", "k3"]
        ]  # one call for all the misses
        assert (cached.hits, cached.misses) == (2, 3)

    def test_unknown_keys_are_not_cached(self) -> None:
        inner = core_keys.InMemoryKeyRegistry()
        cached = core_keys.CachedKeyRegistry(inner)
        assert cached.known("crm.customer", ["k1"]) == set()

        inner.register([_entry("k1")])  # e.g. by another step or host

        assert cached.known("crm.customer", ["k1"]) == {"k1"}

    def test_least_recently_used_keys_are_evicted(self) -> None:
        inner = SpyRegistry()
        cached = core_keys.CachedKeyRegistry(inner, max_entries=2)
        cached.register([_entry("k1"), _entry("k2"), _entry("k3")])

        cached.known("crm.customer", ["k1"])

        assert inner.known_calls == [["k1"]]  # k1 was evicted, k2 / k3 kept

    def test_register_writes_through_with_the_natural_key(self) -> None:
        inner = core_keys.InMemoryKeyRegistry()
        cached = core_keys.CachedKeyRegistry(inner)

        cached.register([_entry("k1", "C001")])

        assert inner.known("crm.customer", ["k1"]) == {"k1"}
        assert cached.natural_key("crm.customer", "k1") == ("C001",)


class TestRegisteringLoader:

    def test_keys_are_registered_once_loaded(self) -> None:
        registry = core_keys.InMemoryKeyRegistry()
        loader = core_keys.RegisteringLoader(
            fakes.CollectingLoader[contract.KeyEntry](),
            registry,
            key=lambda e: e,
        )

        loader.load([_entry("k1")], fakes.make_context())

        assert registry.known("crm.customer", ["k1"]) == {"k1"}

    def test_nothing_is_registered_when_records_failed(self) -> None:
        class PartialLoader(fakes.CollectingLoader[contract.KeyEntry]):

            def load(
                self,
                records: Sequence[contract.KeyEntry],
                ctx: contract.ETLContext,
            ) -> contract.ETLStats:
                return contract.ETLStats(successful=1, failed=1)

        registry = core_keys.InMemoryKeyRegistry()
        loader = core_keys.RegisteringLoader(
            PartialLoader(), registry, key=lambda e: e
        )

        loader.load([_entry("k1"), _entry("k2")], fakes.make_context())

        assert registry.known("crm.customer", ["k1", "k2"]) == set()

    def test_nothing_is_registered_when_the_load_raises(self) -> None:
        registry = core_keys.InMemoryKeyRegistry()
        loader = core_keys.RegisteringLoader(
            fakes.CollectingLoader[contract.KeyEntry](fail_on_batch=1),
            registry,
            key=lambda e: e,
        )
        entries = [_entry("k1")]
        ctx = fakes.make_context()

        with pytest.raises(Exception, match="warehouse unavailable"):
            loader.load(entries, ctx)

        assert registry.known("crm.customer", ["k1"]) == set()


@pytest.mark.parametrize("kind", ["memory", "files", "cached"])
def test_registries_forget_a_natural_key(
    kind: str, tmp_path: pathlib.Path
) -> None:
    registry: contract.IKeyRegistry
    if kind == "memory":
        registry = core_keys.InMemoryKeyRegistry()
    elif kind == "files":
        registry = core_keys.JsonLinesKeyRegistry(tmp_path)
    else:
        registry = core_keys.CachedKeyRegistry(core_keys.InMemoryKeyRegistry())

    registries.check_forget(registry)

    if kind == "files":  # The file forgot too.
        reopened = core_keys.JsonLinesKeyRegistry(tmp_path)
        assert reopened.known("crm.customer", ["k1", "k2"]) == {"k2"}
