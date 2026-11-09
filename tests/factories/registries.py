"""Key registries for tests: the forget check every registry passes, and
in-memory registries that count what a cache asks of them."""

from __future__ import annotations

from collections.abc import Collection, Sequence

from core import contract
from core import keys as core_keys


def check_forget(registry: contract.IKeyRegistry) -> None:
    """Forgets the keys of one natural key, and only those."""
    registry.register([
        contract.KeyEntry("crm.customer", "k1", ("1001",)),
        contract.KeyEntry("crm.customer", "k2", ("1002",)),
        contract.KeyEntry("crm.order", "k3", ("1001",)),
    ])
    assert registry.known("crm.customer", ["k1"]) == {"k1"}

    assert registry.forget("crm.customer", ("1001",)) == ["k1"]
    assert registry.forget("crm.customer", ("1001",)) == []

    assert registry.known("crm.customer", ["k1", "k2"]) == {"k2"}
    assert registry.natural_key("crm.customer", "k1") is None
    assert registry.known("crm.order", ["k3"]) == {"k3"}
    assert [e.surrogate_key for e in registry.entries("crm.customer")] == ["k2"]


class SpyRegistry(core_keys.InMemoryKeyRegistry):
    """Remembers the keys each `known` call asked about."""

    def __init__(self) -> None:
        super().__init__()
        self.known_calls: list[list[str]] = []

    def known(self, entity: str, surrogate_keys: Collection[str]) -> set[str]:
        self.known_calls.append(sorted(surrogate_keys))
        return super().known(entity, surrogate_keys)


class CountingRegistry(core_keys.InMemoryKeyRegistry):
    """Remembers the batches `register` was given, and the natural keys
    looked up; fails every `register` while `failing`."""

    def __init__(self) -> None:
        super().__init__()
        self.registered: list[list[str]] = []
        self.looked_up: list[str] = []
        self.failing = False

    def register(self, entries: Sequence[contract.KeyEntry]) -> None:
        self.registered.append(sorted(e.surrogate_key for e in entries))
        if self.failing:
            raise contract.RetryableError("the database is down")
        super().register(entries)

    def natural_key(
        self, entity: str, surrogate_key: str
    ) -> tuple[str, ...] | None:
        self.looked_up.append(surrogate_key)
        return super().natural_key(entity, surrogate_key)
