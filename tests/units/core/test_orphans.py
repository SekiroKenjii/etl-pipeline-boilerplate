from __future__ import annotations

from collections.abc import Iterator, Sequence
import datetime
import decimal
from typing import Any

import pytest

from core import contract
from core import keys as core_keys
from core import mapping
from core import operators
from core import pipeline
from core import quality
from tests.factories import fake_ports
from tests.factories import fakes
from tests.factories import mapping_specs

CUSTOMER = mapping_specs.mapping(
    mapping_specs.field(
        "customer_key", "CUST_NO", target_type="UUID", primary_key=True
    ),
    mapping_specs.field("name", "NAME", row=3),
    name="customer",
)
ORDER = mapping_specs.mapping(
    mapping_specs.field(
        "order_key", "ORD_NO", target_type="UUID", primary_key=True
    ),
    mapping_specs.field(
        "customer_key",
        "CUST_NO",
        target_type="UUID",
        references="customer",
        row=3,
    ),
    mapping_specs.field("total", "TOTAL", target_type="DECIMAL(10,2)", row=4),
    name="order",
)
COMPILED = mapping.compile_catalog(
    mapping_specs.catalog(CUSTOMER, ORDER), mapping_specs.registry
)
PARKED_RULE = mapping.reference_rule_name("order", "customer_key")


def _order(
    number: str, customer: str, total: str = "1"
) -> mapping.MappedRecord:
    (record,) = COMPILED["order"].map(
        {"ORD_NO": number, "CUST_NO": customer, "TOTAL": total}
    )
    return record


class Rows(pipeline.IDataExtractor[dict[str, Any]]):

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def extract(self, ctx: contract.ETLContext) -> Iterator[dict[str, Any]]:
        yield from self.rows


class TestForeignKeys:

    def test_records_carry_the_natural_keys_of_their_parents(self) -> None:
        record = _order("O-1", " C001 ")

        assert record.foreign_keys == {"customer_key": ("C001",)}

    def test_an_empty_foreign_key_has_no_natural_key(self) -> None:
        assert _order("O-1", "").foreign_keys == {}


class TestRecordCodec:

    def test_records_survive_the_parking_lot(self) -> None:
        record = _order("O-1", "C001", "12.50")
        record = mapping.MappedRecord(
            {
                **record.data,
                "at": datetime.datetime(2026, 1, 2, 3, 4, tzinfo=datetime.UTC),
            },
            (mapping.MappingIssue("total", "odd", decimal.Decimal("1")),),
            record.mapping,
            record.version,
            "row-7",
            1,
            record.entity,
            record.key,
            record.natural_key,
            record.foreign_keys,
        )

        decoded = mapping.decode_record(mapping.encode_record(record))

        assert decoded == record
        assert isinstance(decoded.data["total"], decimal.Decimal)
        assert isinstance(decoded.data["at"], datetime.datetime)

    def test_garbage_is_not_a_record(self) -> None:
        with pytest.raises(ValueError, match="Not an encoded"):
            mapping.decode_record([1, 2])

    def test_identity_is_the_key_else_the_source_row(self) -> None:
        record = _order("O-1", "C001")
        anonymous = mapping.MappedRecord({}, (), "order", 1, "row-7", part=2)
        unidentified = mapping.MappedRecord({}, (), "order", 1)

        assert mapping.record_identity(record) == record.key
        assert mapping.record_identity(anonymous) == "row-7#2"
        with pytest.raises(ValueError, match="cannot be parked"):
            mapping.record_identity(unidentified)


class TestParkingLot:

    def test_parking_again_counts_one_more_attempt(self) -> None:
        lot = quality.InMemoryParkingLot()
        lot.park("order", [contract.ParkedEntry("a", {"v": 1}, "late")])
        (first,) = lot.entries("order")

        lot.park("order", [contract.ParkedEntry("a", {"v": 2}, "late")])
        (second,) = lot.entries("order")

        assert (second.attempts, second.payload) == (2, {"v": 2})
        assert second.parked_at == first.parked_at

    def test_release_keeps_what_was_parked_again_meanwhile(self) -> None:
        lot = quality.InMemoryParkingLot()
        lot.park(
            "order",
            [
                contract.ParkedEntry("a", 1, "late"),
                contract.ParkedEntry("b", 2, "late"),
            ],
        )
        read = list(lot.entries("order"))
        lot.park("order", [contract.ParkedEntry("b", 2, "still late")])

        lot.release("order", read)

        assert [e.identity for e in lot.entries("order")] == ["b"]


class TestParkingQuarantine:

    def _rejected(self, *rules: str) -> contract.RejectedRecord[Any]:
        issues = tuple(
            contract.QualityIssue(
                rule, contract.Checkpoint.AFTER_TRANSFORM, action, "bad"
            )
            for rule, action in [
                *((r, contract.QualityAction.REJECT) for r in rules),
                ("style", contract.QualityAction.WARN),
            ]
        )
        return contract.RejectedRecord(
            _order("O-1", "C404"), contract.Checkpoint.AFTER_TRANSFORM, issues
        )

    def test_only_records_rejected_by_parking_rules_are_parked(self) -> None:
        lot = quality.InMemoryParkingLot()
        fallback = quality.InMemoryQuarantine()
        quarantine = quality.ParkingQuarantine(
            lot,
            "order",
            rules={PARKED_RULE},
            encode=mapping.encode_record,
            identity=mapping.record_identity,
            fallback=fallback,
        )

        quarantine.put(
            [
                self._rejected(PARKED_RULE),
                self._rejected(PARKED_RULE, "mapping_issues"),
            ],
            fakes.make_context(),
        )

        (parked,) = lot.entries("order")
        assert parked.reason == f"{PARKED_RULE}: bad"
        assert [r.issues[1].rule for r in fallback.records] == [
            "mapping_issues"
        ]


def test_orphans_are_parked_then_loaded_once_their_parent_is() -> None:
    registry = core_keys.InMemoryKeyRegistry()
    lot = quality.InMemoryParkingLot()
    loaded = fakes.CollectingLoader[mapping.MappedRecord]()
    orders = [
        {"ORD_NO": "O-1", "CUST_NO": "C001", "TOTAL": "5"},
        {"ORD_NO": "O-2", "CUST_NO": "C002", "TOTAL": "7"},
    ]

    def gate() -> quality.QualityGate:
        return quality.QualityGate(
            {
                contract.Checkpoint.AFTER_TRANSFORM: quality.CheckSet(
                    mapping.reference_rules(COMPILED["order"], registry)
                )
            },
            quarantine=quality.ParkingQuarantine(
                lot,
                "order",
                rules={PARKED_RULE},
                encode=mapping.encode_record,
                identity=mapping.record_identity,
                fallback=quality.InMemoryQuarantine(),
            ),
        )

    loader = core_keys.RegisteringLoader(
        loaded, registry, key=mapping.MappedRecord.key_entry
    )

    def load_customers(*numbers: str) -> None:
        loader.load(
            [
                r
                for n in numbers
                for r in COMPILED["customer"].map({"CUST_NO": n, "NAME": n})
            ],
            fakes.make_context(),
        )

    def replay(max_attempts: int | None = None) -> contract.ETLResult:
        extractor = operators.ParkedRecordExtractor(
            lot, "order", mapping.decode_record, max_attempts=max_attempts
        )
        return pipeline.ETLJob(
            extractor,
            operators.Passthrough[mapping.MappedRecord](),
            loader,
            gate(),
        )(fakes.make_context(), {})

    load_customers("C001")
    pipeline.ETLJob(
        Rows(orders),
        mapping.MappingTransformer(COMPILED["order"]),
        loader,
        gate(),
    )(fakes.make_context(), {})
    assert [e.attempts for e in lot.entries("order")] == [1]

    still_waiting = replay()
    assert still_waiting.stats.rejected == 1
    assert [e.attempts for e in lot.entries("order")] == [2]
    assert replay(max_attempts=2).stats.total == 0

    load_customers("C002")
    result = replay()

    assert result.stats.successful == 1
    assert list(lot.entries("order")) == []
    assert [r.data["total"] for r in loaded.rows if r.mapping == "order"] == [
        decimal.Decimal("5.00"),
        decimal.Decimal("7.00"),
    ]


class TestInferringLoader:

    def _loader(
        self, registry: contract.IKeyRegistry
    ) -> tuple[mapping.InferringLoader, fake_ports.InMemoryTableWriter]:
        writer = fake_ports.InMemoryTableWriter()
        parents = operators.TableLoader[contract.Row](
            writer,
            contract.TableTarget(
                "crm.customer",
                key=("customer_key",),
                mode=contract.WriteMode.INSERT_MISSING,
            ),
        )
        loader = mapping.InferringLoader(
            fakes.CollectingLoader[mapping.MappedRecord](),
            COMPILED["order"],
            registry,
            [
                mapping.InferredParent(
                    "customer_key",
                    parents,
                    lambda key, natural: {
                        "customer_key": key,
                        "legacy_no": natural[0],
                        "is_inferred": True,
                    },
                )
            ],
        )
        return loader, writer

    def test_missing_parents_are_created_and_registered(self) -> None:
        registry = core_keys.InMemoryKeyRegistry()
        (known,) = COMPILED["customer"].map({"CUST_NO": "C001", "NAME": "Ana"})
        entry = known.key_entry()
        assert entry is not None
        registry.register([entry])
        loader, writer = self._loader(registry)
        batch: Sequence[mapping.MappedRecord] = [
            _order("O-1", "C001"),
            _order("O-2", "C404"),
            _order("O-3", "C404"),
        ]

        stats = loader.load(batch, fakes.make_context())

        (placeholder,) = writer.tables["crm.customer"].values()
        assert placeholder == {
            "customer_key": str(batch[1].data["customer_key"]),
            "legacy_no": "C404",
            "is_inferred": True,
        }
        assert registry.natural_key(
            "crm.customer", str(batch[1].data["customer_key"])
        ) == ("C404",)
        assert (stats.successful, loader.inferred) == (3, 1)

        loader.load([_order("O-4", "C404")], fakes.make_context())
        assert loader.inferred == 1

    def test_only_foreign_keys_can_be_inferred(self) -> None:
        loader = fakes.CollectingLoader[mapping.MappedRecord]()
        registry = core_keys.InMemoryKeyRegistry()
        parents = [
            mapping.InferredParent(
                "total",
                fakes.CollectingLoader[contract.Row](),
                lambda key, natural: {},
            )
        ]

        with pytest.raises(contract.PipelineConfigError, match="total"):
            mapping.InferringLoader(
                loader, COMPILED["order"], registry, parents
            )

    def test_inferred_foreign_keys_are_not_checked(self) -> None:
        rules = mapping.reference_rules(
            COMPILED["order"],
            core_keys.InMemoryKeyRegistry(),
            exclude={"customer_key"},
        )

        assert rules == []
