"""The MongoDB adapters against a real replica set.

Runs when `ETL_TEST_MONGODB_URL` is set (`make db-up` starts one). Each test
gets its own database, dropped afterwards.
"""

from __future__ import annotations

from collections.abc import Generator
import concurrent.futures
import datetime
import decimal
import io
import json
import os
from typing import Any
import uuid

from bson import objectid
import pytest

from cli import database as database_cli
from core import contract
from core import keys
from core import operators
from core import pipeline
from infra.database import mongodb
from shared import utils
from tests.factories import fakes
from tests.factories import ledgers
from tests.factories import quarantines
from tests.factories import registries

T0 = datetime.datetime(2026, 10, 1, 8, 0, tzinfo=datetime.UTC)


def _at(minutes: int) -> datetime.datetime:
    return T0 + datetime.timedelta(minutes=minutes)


@pytest.fixture
def mongo() -> Generator[mongodb.MongoDatabase]:
    url = os.environ.get("ETL_TEST_MONGODB_URL")
    if not url:
        pytest.skip("set ETL_TEST_MONGODB_URL to run the MongoDB tests")
    db = mongodb.MongoDatabase(url, database=f"t{uuid.uuid4().hex[:10]}")
    try:
        yield db
    finally:
        db.client.drop_database(db.name)
        db.close()


class TestReader:

    @pytest.fixture
    def customers(self, mongo: mongodb.MongoDatabase) -> list[Any]:
        ids = [objectid.ObjectId() for _ in range(5)]
        mongo.collection("customers").insert_many([
            {
                "_id": ids[i],
                "updatedAt": _at(minutes),
                "name": f"n{i}",
                "address": {"city": "Genève" if i != 2 else "Zürich"},
                **({"vip": True} if i == 4 else {}),
            }
            for i, minutes in enumerate([1, 1, 1, 2, 3])
        ])
        return ids

    def _names(self, mongo: mongodb.MongoDatabase, **query: Any) -> list[Any]:
        query.setdefault("order_by", ("updatedAt", "_id"))
        reader = mongodb.MongoTableReader(mongo, page_size=2)
        return [
            row["name"]
            for row in reader.read(contract.TableQuery("customers", **query))
        ]

    @pytest.mark.usefixtures("customers")
    def test_pages_never_skip_nor_repeat_documents_on_ties(
        self, mongo: mongodb.MongoDatabase
    ) -> None:
        assert self._names(mongo) == ["n0", "n1", "n2", "n3", "n4"]

    def test_after_lower_upper_and_filters(
        self, mongo: mongodb.MongoDatabase, customers: list[Any]
    ) -> None:
        city = contract.Condition(
            "address.city", contract.Operator.EQ, "Genève"
        )
        names = contract.Condition(
            "name", contract.Operator.IN, ["n0", "n2", "n4"]
        )

        assert self._names(mongo, after=(_at(1), customers[1])) == [
            "n2",
            "n3",
            "n4",
        ]
        assert self._names(mongo, lower=_at(2), upper=_at(2)) == ["n3"]
        assert self._names(mongo, filters=(city, names)) == ["n0", "n4"]

    @pytest.mark.usefixtures("customers")
    def test_columns_give_every_document_the_same_shape(
        self, mongo: mongodb.MongoDatabase
    ) -> None:
        reader = mongodb.MongoTableReader(mongo)
        query = contract.TableQuery(
            "customers",
            order_by=("updatedAt", "_id"),
            columns=("updatedAt", "_id", "name", "vip"),
        )

        rows = list(reader.read(query))

        assert [sorted(row) for row in rows] == [
            ["_id", "name", "updatedAt", "vip"]
        ] * 5
        assert [row["vip"] for row in rows] == [None] * 4 + [True]

    @pytest.mark.usefixtures("customers")
    def test_the_max_of_a_watermark(self, mongo: mongodb.MongoDatabase) -> None:
        reader = mongodb.MongoTableReader(mongo)
        nobody = contract.Condition("name", contract.Operator.EQ, "zz")

        assert reader.max_value("customers", "updatedAt") == (_at(3))
        assert reader.max_value("customers", "updatedAt", [nobody]) is None

    @pytest.mark.usefixtures("customers")
    def test_counts(self, mongo: mongodb.MongoDatabase) -> None:
        reader = mongodb.MongoTableReader(mongo)
        vip = contract.Condition("vip", contract.Operator.EQ, True)

        assert reader.count("customers") == 5
        assert reader.count("customers", [vip]) == 1

    def test_documents_without_the_order_fields_are_refused(
        self, mongo: mongodb.MongoDatabase
    ) -> None:
        mongo.collection("events").insert_many(
            [{"_id": 1, "at": T0}, {"_id": 2}]
        )
        reader = mongodb.MongoTableReader(mongo)
        query = contract.TableQuery("events", order_by=("at", "_id"))

        with pytest.raises(contract.FatalError, match="without"):
            list(reader.read(query))


class TestWriter:

    def _write(
        self,
        mongo: mongodb.MongoDatabase,
        rows: list[contract.Row],
        **target: Any,
    ) -> contract.WriteResult:
        target.setdefault("key", ("_id",))
        writer = mongodb.MongoTableWriter(mongo)
        return writer.write(contract.TableTarget("customers", **target), rows)

    def _documents(self, mongo: mongodb.MongoDatabase) -> list[Any]:
        return list(mongo.collection("customers").find(sort=[("_id", 1)]))

    def test_upserts_count_what_changed(
        self, mongo: mongodb.MongoDatabase
    ) -> None:
        self._write(
            mongo,
            [
                {"_id": 1, "name": "a"},
                {"_id": 2, "name": "b"},
                {"_id": 4, "name": None},
            ],
        )

        result = self._write(
            mongo,
            [
                {"_id": 1, "name": "a"},
                {"_id": 2, "name": "old"},
                {"_id": 2, "name": "B"},
                {"_id": 3, "name": "c"},
                {"_id": 4, "name": "from null"},
            ],
        )

        assert (result.created, result.updated, result.unchanged) == (1, 2, 2)
        assert [(d["_id"], d["name"]) for d in self._documents(mongo)] == [
            (1, "a"),
            (2, "B"),
            (3, "c"),
            (4, "from null"),
        ]

    def test_flat_rows_become_nested_documents(
        self, mongo: mongodb.MongoDatabase
    ) -> None:
        self._write(
            mongo,
            [{
                "_id": 1,
                "name": "a",
                "address.city": "Genève",
                "address.zip": "49",
            }],
        )
        self._write(mongo, [{"_id": 1, "address.city": "Zürich"}])

        (document,) = self._documents(mongo)
        assert document["address"] == {"city": "Zürich", "zip": "49"}

    def test_insert_missing_and_update_columns(
        self, mongo: mongodb.MongoDatabase
    ) -> None:
        self._write(mongo, [{"_id": 1, "name": "real", "since": T0}])

        missing = self._write(
            mongo,
            [{"_id": 1, "name": "placeholder"}, {"_id": 2, "name": "new"}],
            mode=contract.WriteMode.INSERT_MISSING,
        )
        self._write(
            mongo,
            [{"_id": 1, "name": "renamed", "since": _at(9)}],
            update_columns=("name",),
        )

        assert (missing.created, missing.unchanged) == (1, 1)
        assert [
            (d["name"], d.get("since")) for d in self._documents(mongo)
        ] == [
            ("renamed", T0),
            ("new", None),
        ]

    def test_a_failing_batch_writes_nothing(
        self, mongo: mongodb.MongoDatabase
    ) -> None:
        self._write(mongo, [{"_id": 1, "name": "a"}])

        with pytest.raises(contract.FatalError, match="duplicate key"):
            self._write(
                mongo,
                [{"_id": 2, "name": "b"}, {"_id": 1, "name": "again"}],
                mode=contract.WriteMode.INSERT,
                key=(),
            )

        # One transaction per batch, on a replica set.
        assert [d["_id"] for d in self._documents(mongo)] == [1]

    def test_writers_racing_on_the_same_keys(
        self, mongo: mongodb.MongoDatabase
    ) -> None:
        writer = mongodb.MongoTableWriter(mongo)
        target = contract.TableTarget("customers", key=("_id",))

        def write(round_: int) -> contract.WriteResult:
            rows: list[contract.Row] = [
                {"_id": i, "name": f"r{round_ % 2}"} for i in range(50)
            ]
            return writer.write(target, rows)

        with concurrent.futures.ThreadPoolExecutor(4) as threads:
            results = list(threads.map(write, range(8)))

        assert sum(r.created for r in results) == 50
        assert len(self._documents(mongo)) == 50

    def test_values_of_every_common_type(
        self, mongo: mongodb.MongoDatabase
    ) -> None:
        row: contract.Row = {
            "_id": uuid.UUID(int=7),
            "amount": decimal.Decimal("10.50"),
            "day": datetime.date(2026, 1, 2),
            "at": T0,
            "raw": b"\x00\x01",
            "tags": ["a", "b"],
            "label": "Zoë Ångström",
        }

        self._write(mongo, [row])

        (document,) = self._documents(mongo)
        assert document == {
            **row,
            # BSON has no date: midnight UTC.
            "day": datetime.datetime(2026, 1, 2, tzinfo=datetime.UTC),
        }


def test_incremental_extraction_by_object_id(
    mongo: mongodb.MongoDatabase,
) -> None:
    events = mongo.collection("events")
    events.insert_many([{"n": i} for i in range(3)])
    state = mongodb.MongoStateStore(mongo)
    loaded: list[Any] = []

    def run() -> list[Any]:
        loader = fakes.CollectingLoader[contract.Row]()
        extractor = operators.TableExtractor(
            mongodb.MongoTableReader(mongo), "events", key=["_id"], state=state
        )
        pipeline.ETLJob(
            extractor,
            operators.Passthrough[contract.Row](),
            loader,
            batch_size=2,
        )(fakes.make_context("events"), {})
        loaded.extend(loader.rows)
        return [row["n"] for row in loader.rows]

    assert run() == [0, 1, 2]
    events.insert_one({"n": 3})
    # The position, an ObjectId, survived the state store.
    assert run() == [3]
    assert run() == []


class TestBookkeeping:

    def test_an_idempotent_crosswalk_safe_under_concurrency(
        self, mongo: mongodb.MongoDatabase
    ) -> None:
        mongodb.create_etl_collections(mongo)
        registry = mongodb.MongoKeyRegistry(mongo)
        entries = [
            contract.KeyEntry("crm.customer", f"k{i}", (str(i), "Zürich"))
            for i in range(600)
        ]

        def use(r: keys.TieredKeyCache) -> Any:
            with concurrent.futures.ThreadPoolExecutor(4) as threads:
                list(threads.map(lambda _: r.register(entries), range(4)))
            return (
                len(
                    r.known("crm.customer", [e.surrogate_key for e in entries])
                ),
                r.known("crm.customer", ["k1", "nope"]),
                r.natural_key("crm.customer", "k1"),
                r.natural_key("crm.order", "k1"),
            )

        # Memory for about 200 of the 600 keys: the rest on disk.
        with keys.TieredKeyCache(
            registry, memory_budget=64 * 1024, disk_budget=10**6
        ) as cached:
            assert use(cached) == (600, {"k1"}, ("1", "Zürich"), None)
            assert cached.disk_entries > 0

    def test_state_round_trips(self, mongo: mongodb.MongoDatabase) -> None:
        store = mongodb.MongoStateStore(mongo)
        value: contract.StateValue = {"position": [1, "a", None], "n": 2.5}

        def use(s: mongodb.MongoStateStore) -> Any:
            s.set("crm", "watermark", {"old": True})
            s.set("crm", "watermark", value)
            found = s.get("crm", "watermark")
            s.delete("crm", "watermark")
            return found, s.get("crm", "watermark")

        assert use(store) == (value, None)

    def test_the_parking_lot_counts_attempts(
        self, mongo: mongodb.MongoDatabase
    ) -> None:
        lot = mongodb.MongoParkingLot(mongo)

        def use(parking: mongodb.MongoParkingLot) -> Any:
            parking.park(
                "order",
                [
                    contract.ParkedEntry("a", {"v": 1}, "late"),
                    contract.ParkedEntry("b", {"v": 2}, "late"),
                ],
            )
            read = list(parking.entries("order"))
            parking.park(
                "order", [contract.ParkedEntry("b", {"v": 3}, "still")]
            )
            parking.release("order", read)
            return list(parking.entries("order"))

        (left,) = use(lot)
        assert (left.identity, left.payload, left.attempts) == (
            "b",
            {"v": 3},
            2,
        )
        assert left.parked_at is not None

    def test_quarantine_and_issues(self, mongo: mongodb.MongoDatabase) -> None:
        issue = contract.QualityIssue(
            "positive",
            contract.Checkpoint.AFTER_TRANSFORM,
            contract.QualityAction.REJECT,
            "must be > 0",
            value=decimal.Decimal("-1"),
        )
        ctx = fakes.make_context("orders")
        quarantine = mongodb.MongoQuarantine(mongo)
        sink = mongodb.MongoIssueSink(mongo)

        quarantine.put(
            [
                contract.RejectedRecord(
                    fakes.User("u1", "a@x", True),
                    issue.checkpoint,
                    (issue,),
                )
            ],
            ctx,
        )
        sink.write([issue], ctx)

        (stored,) = mongo.collection("etl_quarantine").find()
        assert stored["record"] == {
            "id": "u1",
            "email": "a@x",
            "is_adult": True,
        }
        assert stored["run_id"] == ctx.run.run_id
        assert mongo.collection("etl_quality_issues").count_documents({}) == 1

    def test_the_crosswalk_forgets_a_natural_key(
        self, mongo: mongodb.MongoDatabase
    ) -> None:
        registries.check_forget(mongodb.MongoKeyRegistry(mongo))

    def test_the_quarantine_is_read_resolved_and_purged(
        self, mongo: mongodb.MongoDatabase
    ) -> None:
        quarantines.check_quarantine(mongodb.MongoQuarantine(mongo))


class TestInspector:

    @pytest.fixture
    def legacy(self, mongo: mongodb.MongoDatabase) -> None:
        ids = [objectid.ObjectId() for _ in range(3)]
        mongo.collection("customers").insert_many([
            {"_id": ids[0], "code": "C1", "address": {"city": "Genève"}},
            {"_id": ids[1], "code": "C2", "address": {"city": "Genève"}},
            {"_id": ids[2], "code": "C3"},
        ])
        mongo.collection("customers").create_index("code", unique=True)
        mongo.collection("orders").insert_many([
            {"_id": 1, "customer_id": ids[0], "total": decimal.Decimal("5")},
            {"_id": 2, "customer_id": ids[1], "total": 7},
            {"_id": 3, "customer_id": objectid.ObjectId(), "total": 1},
        ])

    @pytest.mark.usefixtures("legacy")
    def test_collections_are_described_from_samples(
        self, mongo: mongodb.MongoDatabase
    ) -> None:
        inspector = mongodb.MongoSchemaInspector(mongo)

        customers, orders = inspector.tables(mongo.name)

        assert customers.name == f"{mongo.name}.customers"
        assert (customers.primary_key, customers.unique) == (
            ("_id",),
            (("code",),),
        )
        city = customers.column("address.city")
        assert (city.family, city.nullable) == (contract.TypeFamily.TEXT, True)
        assert (
            orders.column("customer_id").family is contract.TypeFamily.OBJECT_ID
        )
        assert orders.column("total").family is contract.TypeFamily.DECIMAL
        assert orders.estimated_rows == 3

    @pytest.mark.usefixtures("legacy")
    def test_relationships_are_discovered(
        self, mongo: mongodb.MongoDatabase
    ) -> None:
        inspector = mongodb.MongoSchemaInspector(mongo)

        report = keys.discover_relationships(
            inspector, mongo.name, min_coverage=0.5
        )

        (relation,) = report.relationships
        assert (
            relation.table,
            relation.columns,
            relation.referenced_table,
            relation.referenced_columns,
            relation.evidence,
            relation.orphans,
        ) == (
            f"{mongo.name}.orders",
            ("customer_id",),
            f"{mongo.name}.customers",
            ("_id",),
            keys.Evidence.INFERRED,
            1,
        )


def test_a_standalone_writer_still_works_without_transactions(
    mongo: mongodb.MongoDatabase,
) -> None:
    writer = mongodb.MongoTableWriter(mongo, transactions=False)
    target = contract.TableTarget("t", key=("_id",))

    result = writer.write(target, [{"_id": 1, "v": 1}])

    assert result.created == 1


def test_an_unreachable_server_is_retryable() -> None:
    db = mongodb.MongoDatabase(
        "mongodb://127.0.0.1:1/x?serverSelectionTimeoutMS=200"
    )
    reader = mongodb.MongoTableReader(db)

    try:
        with pytest.raises(contract.RetryableError):
            reader.max_value("t", "v")
    finally:
        db.close()


def test_a_database_is_required() -> None:
    with pytest.raises(contract.FatalError, match="No MongoDB database"):
        mongodb.MongoDatabase("mongodb://127.0.0.1:1")


def test_object_id_positions_survive_the_tagged_codec() -> None:
    position = objectid.ObjectId()

    encoded = json.loads(json.dumps(utils.to_tagged([position])))

    assert utils.from_tagged(encoded) == [position]


def test_the_cli_discovers_a_mongodb_database(
    mongo: mongodb.MongoDatabase,
) -> None:
    mongo.collection("customers").insert_one({"_id": 1})
    mongo.collection("orders").insert_one({"_id": 1, "customer_id": 1})
    url = os.environ["ETL_TEST_MONGODB_URL"]
    out = io.StringIO()

    assert (
        database_cli.main(["init", "--url", url, "--schema", mongo.name], out)
        == 0
    )
    assert database_cli.main(["discover", mongo.name, "--url", url], out) == 0

    assert (
        f"{mongo.name}.orders (customer_id) -> {mongo.name}.customers (_id)"
        "  inferred  coverage 100.0%"
    ) in out.getvalue()


class TestDeletesAndUpdates:

    def test_writer_deletes_and_updates_by_any_fields(
        self, mongo: mongodb.MongoDatabase
    ) -> None:
        mongo.collection("items").insert_many([
            {"_id": i, "parent": i // 10, "flag": False} for i in (1, 2, 11, 12)
        ])
        writer = mongodb.MongoTableWriter(mongo)
        target = contract.TableTarget(
            "items",
            key=("parent",),
            mode=contract.WriteMode.UPDATE,
            update_columns=("flag",),
        )

        def use(w: mongodb.MongoTableWriter) -> Any:
            updated = w.write(
                target,
                [{"parent": 1, "flag": True}, {"parent": 7, "flag": True}],
            )
            deleted = w.delete("items", [{"_id": 1}]) + w.delete(
                "items", [{"parent": 1}]
            )
            return updated, deleted

        updated, deleted = use(writer)

        # Both parent-1 documents flagged; 1 deleted by key, then 11 and 12
        # by parent.
        assert (updated.created, updated.updated, deleted) == (0, 2, 3)
        assert [d["_id"] for d in mongo.collection("items").find()] == [2]

    def test_null_in_a_field_to_delete_by_is_refused(
        self, mongo: mongodb.MongoDatabase
    ) -> None:
        # `{"parent": None}` matches every document without a parent too.
        mongo.collection("items").insert_many([
            {"_id": 1, "parent": 1},
            {"_id": 2},
            {"_id": 3, "parent": None},
        ])
        writer = mongodb.MongoTableWriter(mongo)

        with pytest.raises(contract.FatalError, match="NULL"):
            writer.delete("items", [{"parent": 1}, {"parent": None}])

        assert mongo.collection("items").count_documents({}) == 3

    def test_the_crosswalk_lists_its_keys(
        self, mongo: mongodb.MongoDatabase
    ) -> None:
        registry = mongodb.MongoKeyRegistry(mongo)
        entries = [
            contract.KeyEntry("crm.customer", f"k{i:04}", (str(i),))
            for i in range(1500)
        ]

        def use(r: mongodb.MongoKeyRegistry) -> list[contract.KeyEntry]:
            r.register(entries)
            return list(r.entries("crm.customer"))

        assert use(registry) == entries


def test_change_stream(mongo: mongodb.MongoDatabase) -> None:
    customers = mongo.collection("customers")
    customers.insert_one({"_id": 1, "name": "a"})
    source = mongodb.MongoChangeStream(mongo, "customers")
    snapshot = operators.Snapshot(
        mongodb.MongoTableReader(mongo),
        contract.TableQuery("customers", order_by=("_id",)),
    )
    state = mongodb.MongoStateStore(mongo)

    def run(
        loader: fakes.CollectingLoader[contract.ChangeEvent] | None = None,
    ) -> list[tuple[str, Any, Any]]:
        loader = loader or fakes.CollectingLoader[contract.ChangeEvent]()
        pipeline.ETLJob(
            operators.ChangeExtractor(source, state=state, snapshot=snapshot),
            operators.Passthrough[contract.ChangeEvent](),
            loader,
            batch_size=1,
        )(fakes.make_context("sync"), {})
        return [
            (e.operation.value, e.key[0], e.row.get("name"))
            for e in loader.rows
        ]

    assert run() == [("insert", 1, "a")]

    customers.insert_one({"_id": 2, "name": "b"})
    customers.update_one({"_id": 1}, {"$set": {"name": "A"}})
    customers.delete_one({"_id": 2})
    failing = fakes.CollectingLoader[contract.ChangeEvent](fail_on_batch=2)
    with pytest.raises(contract.RetryableError):
        run(failing)

    # The insert of 2 was loaded; 2 is gone since: its later insert is
    # skipped (no document), its delete comes.
    assert run() == [("update", 1, "A"), ("delete", 2, None)]
    assert run() == []


def test_the_run_ledger_and_lock(mongo: mongodb.MongoDatabase) -> None:
    ledger = mongodb.MongoRunLedger(mongo)
    lock = mongodb.MongoRunLock(mongo)
    ctx = fakes.make_context("crm.load")
    result = contract.PipelineResult(
        pipeline_name="crm.load", run_id=ctx.run.run_id
    )
    result.complete(contract.ETLStatus.SUCCESS)

    def use(_: Any) -> Any:
        ledger.start(ctx.run, "h", {"user": "an"})
        ledger.finish(result)
        taken = lock.acquire("p", "a", 60), lock.acquire("p", "b", 60)
        held = lock.holder("p")
        lock.release("p", "a")
        retaken = lock.acquire("p", "b", 60)
        lock.acquire("q", "dead", -1)
        expired = lock.holder("q")
        lock.acquire("q", "retry", 60)
        lock.release("q", "dead")
        return (
            ledger.recent("crm.load"),
            taken,
            held,
            retaken,
            expired,
            (lock.renew("q", "dead", 60), lock.holder("q")),
        )

    (record,), taken, held, retaken, expired, back = use(ledger)
    assert record.status is contract.ETLStatus.SUCCESS
    assert record.meta == {"user": "an"}
    assert (taken, held, retaken, expired) == ((True, False), "a", True, None)
    # The first owner, back: it frees and renews nothing of the retry's.
    assert back == (False, "retry")


def test_the_steps_of_a_run(mongo: mongodb.MongoDatabase) -> None:
    mongodb.create_etl_collections(mongo)
    ledgers.check_steps(mongodb.MongoRunLedger(mongo))


def test_the_latest_run_that_loaded(mongo: mongodb.MongoDatabase) -> None:
    mongodb.create_etl_collections(mongo)
    ledgers.check_latest(mongodb.MongoRunLedger(mongo))


def test_the_last_run_of_a_label(mongo: mongodb.MongoDatabase) -> None:
    mongodb.create_etl_collections(mongo)
    ledgers.check_last_run(mongodb.MongoRunLedger(mongo))
