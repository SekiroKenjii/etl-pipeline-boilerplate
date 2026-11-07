"""Referential integrity checks ask the key registry per batch, not per row."""

from __future__ import annotations

from collections.abc import Collection

from core import contract
from core import keys as core_keys
from core import mapping
from core import quality
from tests.factories import fakes
from tests.factories import mapping_specs

field = mapping_specs.field

CUSTOMER = mapping_specs.mapping(
    field("customer_key", "CUST_NO", primary_key=True),
    key_hashing=contract.KeyHashing.UUID5,
)
ORDER = mapping_specs.mapping(
    field("order_key", "ORD_NO", primary_key=True),
    field("customer_key", "CUST_NO", references="customer"),
    name="order",
    source="legacy.ORD",
    target="crm.order",
    key_hashing=contract.KeyHashing.UUID5,
)


class CountingRegistry(core_keys.InMemoryKeyRegistry):
    """Counts the calls of `known`, and the keys asked about."""

    def __init__(self) -> None:
        super().__init__()
        self.calls: list[int] = []

    def known(self, entity: str, surrogate_keys: Collection[str]) -> set[str]:
        self.calls.append(len(surrogate_keys))
        return super().known(entity, surrogate_keys)


def _compiled() -> dict[str, mapping.CompiledMapping]:
    return mapping.compile_catalog(
        mapping_specs.catalog(CUSTOMER, ORDER), mapping_specs.registry
    )


def test_registry_calls_per_batch_are_bounded_by_chunks_not_rows() -> None:
    compiled = _compiled()
    registry = CountingRegistry()
    # Customers 0..599 are loaded; orders reference customers 0..1199, each
    # twice: half of them are orphans, every orphan twice.
    entries = [
        compiled["customer"].apply({"CUST_NO": str(n)}).key_entry()
        for n in range(600)
    ]
    registry.register([entry for entry in entries if entry is not None])
    orders = [
        compiled["order"].apply({"ORD_NO": str(n), "CUST_NO": str(n % 1200)})
        for n in range(2400)
    ]
    gate = quality.QualityGate(
        {
            contract.Checkpoint.AFTER_TRANSFORM: quality.CheckSet(
                mapping.reference_rules(compiled["order"], registry)
            )
        },
        quarantine=quality.InMemoryQuarantine(),
    )

    outcome = gate.inspect(
        contract.Checkpoint.AFTER_TRANSFORM, orders, fakes.make_context()
    )

    # 1,200 distinct keys, 500 per call: 3 calls for 2,400 rows.
    assert registry.calls == [500, 500, 200]
    assert len(outcome.records) == 1200
    assert outcome.report is not None
    assert outcome.report.rules["references:order.customer_key"].violations == (
        1200
    )


def test_orphans_are_not_remembered_across_batches() -> None:
    compiled = _compiled()
    registry = CountingRegistry()
    (rule,) = mapping.reference_rules(compiled["order"], registry)
    order = compiled["order"].apply({"ORD_NO": "1", "CUST_NO": "7"})
    ctx = fakes.make_context()

    rule.prepare_batch([order], ctx)
    assert list(rule.check_record(order, ctx))
    # Its parent is loaded before the next batch, e.g. a self-reference.
    parent = compiled["customer"].apply({"CUST_NO": "7"}).key_entry()
    assert parent is not None
    registry.register([parent])
    rule.prepare_batch([order], ctx)

    assert not list(rule.check_record(order, ctx))


def test_a_record_of_no_prepared_batch_is_looked_up_alone() -> None:
    compiled = _compiled()
    registry = CountingRegistry()
    (rule,) = mapping.reference_rules(compiled["order"], registry)
    order = compiled["order"].apply({"ORD_NO": "1", "CUST_NO": "7"})

    violations = list(rule.check_record(order, fakes.make_context()))

    assert [v.message for v in violations] == [
        "references a crm.customer that is not loaded"
    ]
    assert registry.calls == [1]
