"""What every key registry must do to forget a key: one check for all."""

from __future__ import annotations

from core import contract


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
