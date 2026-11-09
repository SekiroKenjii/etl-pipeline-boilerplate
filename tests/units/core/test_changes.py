from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest

from core import contract
from core import keys as core_keys
from core import mapping
from core import operators
from core import pipeline
from core import state as core_state
from tests.factories import fake_ports
from tests.factories import fakes
from tests.factories import mapping_specs

INSERT = contract.ChangeOperation.INSERT
UPDATE = contract.ChangeOperation.UPDATE
DELETE = contract.ChangeOperation.DELETE


def _event(operation: contract.ChangeOperation, row: dict[str, Any]) -> Any:
    return contract.ChangeEvent("t", operation, row, (row["id"],), 1)


class TestChangeExtractor:

    def _run(
        self,
        source: fake_ports.InMemoryChangeLog,
        state: contract.IStateStore,
        table: list[dict[str, Any]] | None = None,
        loader: Any = None,
        batch_size: int = 2,
    ) -> list[tuple[str, Any]]:
        snapshot = None
        if table is not None:
            snapshot = operators.Snapshot(
                fake_ports.InMemoryTableReader({"t": table}),
                contract.TableQuery("t", order_by=("id",)),
            )
        loader = loader or fakes.CollectingLoader[contract.ChangeEvent]()
        pipeline.ETLJob(
            operators.ChangeExtractor(source, state=state, snapshot=snapshot),
            operators.Passthrough[contract.ChangeEvent](),
            loader,
            batch_size=batch_size,
        )(fakes.make_context("sync"), {})
        return [(e.operation.value, e.row["id"]) for e in loader.rows]

    def test_a_snapshot_then_the_changes_made_since(self) -> None:
        source = fake_ports.InMemoryChangeLog("t")
        source.append(INSERT, {"id": 1})  # Before: in the snapshot only.
        state = core_state.InMemoryStateStore()

        first = self._run(source, state, table=[{"id": 1}, {"id": 2}])
        source.append(UPDATE, {"id": 2})
        source.append(DELETE, {"id": 1})
        second = self._run(source, state, table=[{"id": 9}])

        assert first == [("insert", 1), ("insert", 2)]
        assert second == [("update", 2), ("delete", 1)]
        assert source.confirmed == [3]
        assert self._run(source, state) == []

    def test_without_snapshot_the_first_run_starts_now(self) -> None:
        source = fake_ports.InMemoryChangeLog("t")
        source.append(INSERT, {"id": 1})
        state = core_state.InMemoryStateStore()

        assert self._run(source, state) == []
        source.append(INSERT, {"id": 2})
        assert self._run(source, state) == [("insert", 2)]

    def test_a_failed_snapshot_is_read_again(self) -> None:
        source = fake_ports.InMemoryChangeLog("t")
        state = core_state.InMemoryStateStore()
        failing = fakes.CollectingLoader[contract.ChangeEvent](fail_on_batch=2)

        with pytest.raises(contract.RetryableError):
            self._run(
                source,
                state,
                table=[{"id": 1}, {"id": 2}, {"id": 3}],
                loader=failing,
            )

        assert self._run(
            source, state, table=[{"id": 1}, {"id": 2}, {"id": 3}]
        ) == [
            ("insert", 1),
            ("insert", 2),
            ("insert", 3),
        ]

    def test_a_failed_batch_of_changes_is_read_again(self) -> None:
        source = fake_ports.InMemoryChangeLog("t")
        state = core_state.InMemoryStateStore()
        self._run(source, state)
        for i in range(4):
            source.append(INSERT, {"id": i})
        failing = fakes.CollectingLoader[contract.ChangeEvent](fail_on_batch=2)

        with pytest.raises(contract.RetryableError):
            self._run(source, state, loader=failing)

        assert self._run(source, state) == [("insert", 2), ("insert", 3)]

    def test_an_empty_snapshot_still_starts_the_changes(self) -> None:
        source = fake_ports.InMemoryChangeLog("t")
        state = core_state.InMemoryStateStore()

        assert self._run(source, state, table=[]) == []
        source.append(INSERT, {"id": 5})
        assert self._run(source, state, table=[]) == [("insert", 5)]


class TestChangeLoader:

    def _loader(
        self, deletes: operators.Deletes, writer: fake_ports.InMemoryTableWriter
    ) -> operators.ChangeLoader[contract.ChangeEvent]:
        return operators.ChangeLoader[contract.ChangeEvent](
            writer,
            contract.TableTarget("t", key=("id",)),
            row=lambda e: e.row,
            deleted=lambda e: e.deleted,
            deletes=deletes,
            deleted_at="deleted_at",
        )

    def _seed(
        self, writer: fake_ports.InMemoryTableWriter, extra: dict[str, Any]
    ) -> None:
        writer.write(
            contract.TableTarget("t", key=("id",)),
            [{"id": 1, "v": "a", **extra}, {"id": 2, "v": "b", **extra}],
        )

    def test_hard_deletes(self) -> None:
        writer = fake_ports.InMemoryTableWriter()
        self._seed(writer, {})

        stats = self._loader(operators.Deletes.HARD, writer).load(
            [_event(DELETE, {"id": 1}), _event(UPDATE, {"id": 2, "v": "B"})],
            fakes.make_context(),
        )

        assert (stats.deleted, stats.updated, stats.successful) == (1, 1, 2)
        assert list(writer.tables["t"].values()) == [{"id": 2, "v": "B"}]

    def test_soft_deletes_flag_and_revive(self) -> None:
        writer = fake_ports.InMemoryTableWriter()
        self._seed(writer, {"is_deleted": False, "deleted_at": None})
        loader = self._loader(operators.Deletes.SOFT, writer)

        deleted = loader.load([_event(DELETE, {"id": 1})], fakes.make_context())
        flagged = dict(writer.tables["t"][(1,)])
        loader.load(
            [_event(INSERT, {"id": 1, "v": "back"})], fakes.make_context()
        )

        assert deleted.deleted == 1
        assert flagged["is_deleted"] is True
        assert flagged["deleted_at"] is not None
        assert writer.tables["t"][(1,)] == {
            "id": 1,
            "v": "back",
            "is_deleted": False,
            "deleted_at": None,
        }

    def test_ignored_deletes(self) -> None:
        writer = fake_ports.InMemoryTableWriter()
        self._seed(writer, {})

        stats = self._loader(operators.Deletes.IGNORE, writer).load(
            [_event(DELETE, {"id": 1})], fakes.make_context()
        )

        assert stats.deleted == 0
        assert len(writer.tables["t"]) == 2

    def _replay(
        self,
        deletes: operators.Deletes,
        writer: fake_ports.InMemoryTableWriter,
        changes: list[tuple[contract.ChangeOperation, dict[str, Any]]],
    ) -> None:
        """Loads changes as one batch, through `Passthrough` (no dedup)."""
        source = fake_ports.InMemoryChangeLog("t")
        state = core_state.InMemoryStateStore()
        job = pipeline.ETLJob(
            operators.ChangeExtractor(source, state=state),
            operators.Passthrough[contract.ChangeEvent](),
            self._loader(deletes, writer),
            batch_size=100,
        )
        job(fakes.make_context("sync"), {})  # The first run starts now.
        for operation, row in changes:
            source.append(operation, row)
        job(fakes.make_context("sync"), {})

    @pytest.mark.parametrize(
        "deletes", [operators.Deletes.HARD, operators.Deletes.SOFT]
    )
    def test_a_row_updated_then_deleted_in_a_batch_stays_deleted(
        self, deletes: operators.Deletes
    ) -> None:
        writer = fake_ports.InMemoryTableWriter()
        self._seed(writer, {"is_deleted": False, "deleted_at": None})

        self._replay(
            deletes,
            writer,
            [(UPDATE, {"id": 1, "v": "new"}), (DELETE, {"id": 1})],
        )

        row = writer.tables["t"].get((1,))
        if deletes is operators.Deletes.HARD:
            assert row is None
        else:
            assert row is not None
            assert (row["v"], row["is_deleted"]) == ("new", True)
            assert row["deleted_at"] is not None

    @pytest.mark.parametrize(
        "deletes", [operators.Deletes.HARD, operators.Deletes.SOFT]
    )
    def test_a_row_deleted_then_written_in_a_batch_comes_back(
        self, deletes: operators.Deletes
    ) -> None:
        writer = fake_ports.InMemoryTableWriter()
        self._seed(writer, {"is_deleted": False, "deleted_at": None})

        self._replay(
            deletes,
            writer,
            [(DELETE, {"id": 1}), (INSERT, {"id": 1, "v": "back"})],
        )

        row = writer.tables["t"][(1,)]
        assert row["v"] == "back"
        if deletes is operators.Deletes.SOFT:
            assert (row["is_deleted"], row["deleted_at"]) == (False, None)

    def test_the_last_of_many_changes_of_a_row_wins(self) -> None:
        writer = fake_ports.InMemoryTableWriter()
        self._seed(writer, {})

        self._replay(
            operators.Deletes.HARD,
            writer,
            [
                (UPDATE, {"id": 1, "v": "x"}),
                (DELETE, {"id": 1}),
                (INSERT, {"id": 1, "v": "y"}),
                (DELETE, {"id": 2}),
                (INSERT, {"id": 2, "v": "z"}),
                (DELETE, {"id": 2}),
            ],
        )

        assert writer.tables["t"] == {(1,): {"id": 1, "v": "y"}}

    def test_changes_of_other_rows_are_one_write_of_each_kind(self) -> None:
        writer = fake_ports.InMemoryTableWriter()
        self._seed(writer, {})
        writer.batches.clear()

        self._replay(
            operators.Deletes.HARD,
            writer,
            [
                (UPDATE, {"id": 2, "v": "B"}),
                (DELETE, {"id": 1}),
                (INSERT, {"id": 3, "v": "c"}),
                (DELETE, {"id": 4}),
            ],
        )

        assert writer.batches == [("t", 2)]
        assert writer.deletes == [("t", 2)]
        assert sorted(writer.tables["t"]) == [(2,), (3,)]

    def test_a_doubtful_match_keeps_the_order(self) -> None:
        # Case-insensitive collations match "A1" and "a1 ": in doubt, the
        # upsert is written before the delete.
        order: list[str] = []

        class Writer(fake_ports.InMemoryTableWriter):

            def write(
                self, target: contract.TableTarget, rows: Sequence[Any]
            ) -> contract.WriteResult:
                order.append("write")
                return super().write(target, rows)

            def delete(self, table: str, rows: Sequence[Any]) -> int:
                order.append("delete")
                return super().delete(table, rows)

        writer = Writer()
        loader = operators.ChangeLoader[dict[str, Any]](
            writer,
            contract.TableTarget("t", key=("id",)),
            row=lambda r: r["row"],
            deleted=lambda r: r["deleted"],
        )

        loader.load(
            [
                {"row": {"id": "A1", "v": 1}, "deleted": False},
                {"row": {"id": "a1 "}, "deleted": True},
            ],
            fakes.make_context(),
        )

        assert order == ["write", "delete"]


CUSTOMER = mapping_specs.mapping(
    mapping_specs.field(
        "customer_key", "_id", target_type="UUID", primary_key=True
    ),
    mapping_specs.field("name", "name", row=3),
    name="customer",
    ignored_source_fields=("addresses",),
)
ADDRESS = mapping_specs.mapping(
    mapping_specs.field(
        "address_key", "_id", "addresses#", target_type="UUID", primary_key=True
    ),
    mapping_specs.field(
        "customer_key", "_id", target_type="UUID", references="customer", row=3
    ),
    mapping_specs.field("city", "addresses.city", row=4),
    name="address",
    row_split="explode(addresses)",
    ignored_source_fields=("name",),
)
COMPILED = mapping.compile_catalog(
    mapping_specs.catalog(CUSTOMER, ADDRESS), mapping_specs.registry
)


def _doc_event(
    operation: contract.ChangeOperation, row: dict[str, Any]
) -> contract.ChangeEvent:
    return contract.ChangeEvent("customers", operation, row, (row["_id"],), 1)


class TestMappingChanges:

    def _map(
        self, name: str, events: Sequence[contract.ChangeEvent], **options: Any
    ) -> list[mapping.MappedRecord]:
        transformer = mapping.MappingTransformer[contract.ChangeEvent](
            COMPILED[name], **options
        )
        return list(transformer.transform(events, fakes.make_context()))

    def test_a_delete_becomes_the_deletion_of_the_target_key(self) -> None:
        (inserted,) = self._map(
            "customer", [_doc_event(INSERT, {"_id": "c1", "name": "a"})]
        )
        (deleted,) = self._map("customer", [_doc_event(DELETE, {"_id": "c1"})])

        assert deleted.deleted
        assert deleted.data == {"customer_key": inserted.key}
        assert deleted.key_entry() is None

    def test_the_last_change_of_a_row_wins(self) -> None:
        records = self._map(
            "customer",
            [
                _doc_event(INSERT, {"_id": "c1", "name": "a"}),
                _doc_event(UPDATE, {"_id": "c2", "name": "b"}),
                _doc_event(UPDATE, {"_id": "c1", "name": "A"}),
            ],
        )

        assert [r.data["name"] for r in records] == ["b", "A"]

    def test_children_are_replaced_by_their_parent_key(self) -> None:
        records = self._map(
            "address",
            [
                _doc_event(
                    UPDATE, {"_id": "c1", "addresses": [{"city": "Genève"}]}
                ),
                _doc_event(DELETE, {"_id": "c2"}),
                _doc_event(
                    INSERT, {"_id": "c3", "addresses": [{"city": "Oslo"}]}
                ),
            ],
            scope="customer_key",
        )

        (parent1,) = self._map(
            "customer", [_doc_event(INSERT, {"_id": "c1", "name": "x"})]
        )
        assert [(r.deleted, sorted(r.data)) for r in records] == [
            (True, ["customer_key"]),
            (False, ["address_key", "city", "customer_key"]),
            (True, ["customer_key"]),
            # A new row has no children to replace.
            (False, ["address_key", "city", "customer_key"]),
        ]
        assert records[0].data["customer_key"] == parent1.key

    def test_split_rows_need_a_scope_to_be_deleted(self) -> None:
        events = [_doc_event(DELETE, {"_id": "c1"})]

        with pytest.raises(contract.FatalError, match="scope"):
            self._map("address", events)
        with pytest.raises(contract.PipelineConfigError, match="scope"):
            mapping.MappingTransformer(
                COMPILED["customer"], scope="customer_key"
            )

    def test_deletions_survive_the_parking_lot(self) -> None:
        (deleted,) = self._map("customer", [_doc_event(DELETE, {"_id": "c1"})])

        assert mapping.decode_record(mapping.encode_record(deleted)) == deleted


def test_missing_source_rows_are_found_from_the_crosswalk() -> None:
    registry = core_keys.InMemoryKeyRegistry()
    registry.register([
        contract.KeyEntry("crm.customer", "k1", ("C1",)),
        contract.KeyEntry("crm.customer", "k2", ("C2",)),
        contract.KeyEntry("crm.order", "k9", ("C2",)),
    ])
    reader = fake_ports.InMemoryTableReader(
        {"legacy.cust": [{"cust_no": "c1 "}]}
    )
    extractor = operators.MissingSourceRows(
        reader,
        "legacy.cust",
        key=["cust_no"],
        registry=registry,
        entity="crm.customer",
        case=contract.KeyCase.UPPER,
        max_share=None,
    )

    events = list(extractor.extract(fakes.make_context()))

    assert [(e.operation, e.row) for e in events] == [
        (DELETE, {"cust_no": "C2"})
    ]


class TestMissingSourceRowsSafety:

    def _registry(self, count: int) -> core_keys.InMemoryKeyRegistry:
        registry = core_keys.InMemoryKeyRegistry()
        registry.register([
            contract.KeyEntry("crm.customer", f"k{i}", (f"C{i}",))
            for i in range(count)
        ])
        return registry

    def _extractor(
        self, source: list[dict[str, Any]], registry: Any, **options: Any
    ) -> operators.MissingSourceRows:
        return operators.MissingSourceRows(
            fake_ports.InMemoryTableReader({"legacy.cust": source}),
            "legacy.cust",
            key=["cust_no"],
            registry=registry,
            entity="crm.customer",
            **options,
        )

    def _context(self, **params: str) -> contract.ETLContext:
        return contract.ETLContext(contract.ETLRunInfo("crm", params=params))

    def test_an_empty_source_deletes_nothing(self) -> None:
        writer = fake_ports.InMemoryTableWriter()
        writer.write(
            contract.TableTarget("crm.customer", key=("cust_no",)),
            [{"cust_no": "C0"}, {"cust_no": "C1"}],
        )
        job = pipeline.ETLJob(
            self._extractor([], self._registry(2), max_share=None),
            operators.Passthrough[contract.ChangeEvent](),
            operators.ChangeLoader[contract.ChangeEvent](
                writer,
                contract.TableTarget("crm.customer", key=("cust_no",)),
                row=lambda e: e.row,
                deleted=lambda e: e.deleted,
            ),
        )
        context = self._context()

        with pytest.raises(contract.FatalError, match="source has no row"):
            job(context, {})

        assert len(writer.tables["crm.customer"]) == 2
        assert writer.deletes == []

    def test_too_many_deletes_are_refused_until_confirmed(self) -> None:
        source = [{"cust_no": "C0"}]
        registry = self._registry(4)
        unconfirmed = self._extractor(source, registry)
        context = self._context()
        too_few = self._extractor(source, registry)
        too_few_context = self._context(confirm_deletes="crm.customer:2")

        with pytest.raises(
            contract.FatalError,
            match=r"3 of the 4 .*: 75\.0% of them, above 10\.0%.*"
            r"--param confirm_deletes=crm.customer:3",
        ):
            list(unconfirmed.extract(context))
        with pytest.raises(contract.FatalError, match="refusing"):
            list(too_few.extract(too_few_context))
        confirmed = self._extractor(source, registry).extract(
            self._context(confirm_deletes="crm.order:1, crm.customer:3")
        )

        assert [e.row["cust_no"] for e in confirmed] == ["C1", "C2", "C3"]

    def test_a_fifth_of_the_rows_is_refused_by_default(self) -> None:
        source = [{"cust_no": f"C{i}"} for i in range(8)]
        extractor = self._extractor(source, self._registry(10))
        context = self._context()

        with pytest.raises(contract.FatalError, match=r"above 10\.0%"):
            list(extractor.extract(context))

    def test_a_refusal_is_for_the_operator_not_a_retry(self) -> None:
        extractor = self._extractor([], self._registry(2))
        context = self._context()

        with pytest.raises(contract.PipelineConfigError, match="refusing"):
            list(extractor.extract(context))

    def test_deletes_within_the_share_need_no_confirmation(self) -> None:
        source = [{"cust_no": f"C{i}"} for i in range(9)]

        events = self._extractor(
            source, self._registry(10), max_share=0.1
        ).extract(self._context())

        assert [e.row["cust_no"] for e in events] == ["C9"]

    def test_a_confirmation_must_name_the_entity_and_count(self) -> None:
        extractor = self._extractor([], self._registry(1))
        context = self._context(confirm_deletes="3")

        with pytest.raises(contract.FatalError, match="<entity>:<count>"):
            list(extractor.extract(context))

    def test_max_share_is_a_share(self) -> None:
        registry = self._registry(1)

        with pytest.raises(ValueError, match="max_share"):
            self._extractor([], registry, max_share=2.0)
