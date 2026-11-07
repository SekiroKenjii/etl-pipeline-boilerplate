"""A MongoDB-to-SQL migration, end to end.

Customers embed their addresses (an array), orders point to customers by
ObjectId. The target is relational (SQLite here): customer, address (one row
per element of the array) and order tables, with real foreign keys. Rows are
read incrementally by `updatedAt`; the watermark is kept in MongoDB.
"""

from __future__ import annotations

from collections.abc import Sequence
import datetime
import os
import pathlib
from typing import Any
import uuid

from bson import objectid
import pytest
import sqlalchemy as sa

from core import contract
from core import keys as core_keys
from core import mapping
from core import operators
from core import pipeline
from core import quality
from infra.database import mongodb
from infra.database import relational
from tests.factories import fakes
from tests.factories import mapping_specs

T0 = datetime.datetime(2026, 10, 1, 8, 0, tzinfo=datetime.UTC)
_IGNORED = ("updatedAt",)

CUSTOMER = mapping_specs.mapping(
    mapping_specs.field(
        "customer_key", "_id", target_type="UUID", primary_key=True
    ),
    mapping_specs.field("legacy_id", "_id", target_type="VARCHAR(24)", row=3),
    mapping_specs.field("name", "name", row=4),
    name="customer",
    target="crm.customer",
    ignored_source_fields=(*_IGNORED, "addresses"),
)
ADDRESS = mapping_specs.mapping(
    mapping_specs.field(
        "address_key", "_id", "addresses#", target_type="UUID", primary_key=True
    ),
    mapping_specs.field(
        "customer_key", "_id", target_type="UUID", references="customer", row=3
    ),
    mapping_specs.field("city", "addresses.city", required=True, row=4),
    name="address",
    target="crm.address",
    row_split="explode(addresses)",
    ignored_source_fields=(*_IGNORED, "name"),
)
ORDER = mapping_specs.mapping(
    mapping_specs.field(
        "order_key", "_id", target_type="UUID", primary_key=True
    ),
    mapping_specs.field(
        "customer_key",
        "customer_id",
        target_type="UUID",
        references="customer",
        row=3,
    ),
    mapping_specs.field("total", "total", target_type="NUMERIC(10,2)", row=4),
    name="order",
    target="crm.order",
    ignored_source_fields=_IGNORED,
)
CRM = mapping_specs.catalog(CUSTOMER, ADDRESS, ORDER)
# The collection each mapping reads.
SOURCES = {"customer": "customers", "address": "customers", "order": "orders"}


@pytest.fixture
def mongo() -> Any:
    url = os.environ.get("ETL_TEST_MONGODB_URL")
    if not url:
        pytest.skip("set ETL_TEST_MONGODB_URL to run the MongoDB tests")
    db = mongodb.MongoDatabase(url, database=f"t{uuid.uuid4().hex[:10]}")
    yield db
    db.client.drop_database(db.name)
    db.close()


def _target(
    path: pathlib.Path,
) -> tuple[relational.Database, dict[str, sa.Table]]:
    db = relational.Database(f"sqlite:///{path}")
    metadata = sa.MetaData()
    customer = sa.Table(
        "customer",
        metadata,
        sa.Column("customer_key", sa.Unicode(36), primary_key=True),
        sa.Column("legacy_id", sa.Unicode(24), nullable=False),
        sa.Column("name", sa.Unicode(50)),
    )
    tables = {
        "customer": customer,
        "address": sa.Table(
            "address",
            metadata,
            sa.Column("address_key", sa.Unicode(36), primary_key=True),
            sa.Column(
                "customer_key",
                sa.Unicode(36),
                sa.ForeignKey(customer.c.customer_key),
                nullable=False,
            ),
            sa.Column("city", sa.Unicode(50), nullable=False),
        ),
        "order": sa.Table(
            "order",
            metadata,
            sa.Column("order_key", sa.Unicode(36), primary_key=True),
            sa.Column(
                "customer_key",
                sa.Unicode(36),
                sa.ForeignKey(customer.c.customer_key),
                nullable=False,
            ),
            sa.Column("total", sa.Numeric(10, 2)),
        ),
    }
    metadata.create_all(db.engine)
    relational.create_etl_tables(db)
    return db, tables


class Migration(pipeline.BaseOrchestrator):
    """Customers first, then their addresses and orders, in parallel."""

    def __init__(
        self, source: mongodb.MongoDatabase, target: relational.Database
    ) -> None:
        super().__init__(max_workers=2)
        self.compiled = mapping.compile_catalog(CRM, mapping_specs.registry)
        self.reader = mongodb.MongoTableReader(source, page_size=2)
        self.state = mongodb.MongoStateStore(source)
        self.writer = relational.RelationalTableWriter(target)
        self.registry = core_keys.CachedKeyRegistry(
            relational.RelationalKeyRegistry(target)
        )
        self.quarantine = relational.RelationalQuarantine(target)

    def steps(self) -> Sequence[contract.PipelineStep]:
        dependencies = mapping.mapping_dependencies(CRM)
        return [
            contract.PipelineStep(
                name, self._job(name), dependencies=tuple(dependencies[name])
            )
            for name in ("customer", "address", "order")
        ]

    def _job(self, name: str) -> pipeline.ETLJob[contract.Row, Any]:
        compiled = self.compiled[name]
        key_field = compiled.key_field
        assert key_field is not None
        columns = {
            "customers": ("updatedAt", "_id", "name", "addresses"),
            "orders": ("updatedAt", "_id", "customer_id", "total"),
        }[SOURCES[name]]
        extractor = operators.TableExtractor(
            self.reader,
            SOURCES[name],
            watermark="updatedAt",
            key=["_id"],
            columns=columns,
            state=self.state,
            # One position per mapping: two mappings read customers.
            state_key=f"collection:{name}",
        )
        loader = core_keys.RegisteringLoader(
            operators.TableLoader[mapping.MappedRecord](
                self.writer,
                contract.TableTarget(name, key=(key_field,)),
                row=lambda record: record.data,
            ),
            self.registry,
            key=mapping.MappedRecord.key_entry,
        )
        gate = quality.QualityGate(
            {
                contract.Checkpoint.AFTER_TRANSFORM: quality.CheckSet([
                    mapping.MappingIssuesRule(),
                    *mapping.reference_rules(compiled, self.registry),
                ])
            },
            quarantine=self.quarantine,
        )
        return pipeline.ETLJob(
            extractor,
            mapping.MappingTransformer[contract.Row](
                compiled, key=lambda row: str(row["_id"])
            ),
            loader,
            gate,
            batch_size=2,
        )


def test_documents_become_related_tables(
    mongo: mongodb.MongoDatabase, tmp_path: pathlib.Path
) -> None:
    target, tables = _target(tmp_path / "crm.db")
    ana, binh = objectid.ObjectId(), objectid.ObjectId()

    def at(minutes: int) -> datetime.datetime:
        return T0 + datetime.timedelta(minutes=minutes)

    def run() -> contract.PipelineResult:
        result = Migration(mongo, target).run(fakes.make_context("crm"))
        for step in result.step_results.values():
            assert not step.errors, step.errors
        return result

    def rows(name: str, *columns: str) -> list[tuple[Any, ...]]:
        table = tables[name]
        with target.engine.connect() as connection:
            selected = [table.c[c] for c in columns]
            return [
                tuple(r)
                for r in connection.execute(
                    sa.select(*selected).order_by(*selected)
                )
            ]

    mongo.collection("customers").insert_many([
        {
            "_id": ana,
            "name": "Ana",
            "updatedAt": at(1),
            "addresses": [{"city": "Genève"}, {"city": "Kraków"}],
        },
        # No address: no address row.
        {"_id": binh, "name": "Bruno", "updatedAt": at(1)},
    ])
    mongo.collection("orders").insert_many([
        {"_id": 1, "customer_id": ana, "total": 10, "updatedAt": at(2)},
        # An order of a customer that does not exist: quarantined.
        {
            "_id": 2,
            "customer_id": objectid.ObjectId(),
            "total": 5,
            "updatedAt": at(2),
        },
    ])

    first = run()

    assert first.step_results["customer"].stats.created == 2
    assert first.step_results["address"].stats.created == 2
    assert first.step_results["address"].stats.filtered == 1
    orders = first.step_results["order"].stats
    assert (orders.created, orders.rejected) == (1, 1)
    assert rows("customer", "legacy_id") == sorted([(str(ana),), (str(binh),)])
    assert rows("address", "city") == [("Genève",), ("Kraków",)]

    # Ana moves her second address and adds a third.
    mongo.collection("customers").update_one(
        {"_id": ana},
        {
            "$set": {
                "addresses": [
                    {"city": "Genève"},
                    {"city": "Malmö"},
                    {"city": "Pécs"},
                ],
                "updatedAt": at(5),
            }
        },
    )

    second = run()

    # The rows at the last watermark of the first run are read again, once:
    # Bruno (no address), and both orders.
    stats = second.step_results["address"].stats
    assert (stats.total, stats.created, stats.updated) == (2, 1, 1)
    assert second.step_results["order"].stats.total == 2
    assert rows("address", "city") == [("Genève",), ("Malmö",), ("Pécs",)]
    target.engine.dispose()


class Sync(pipeline.BaseOrchestrator):
    """Customers and their addresses, kept in sync by change streams.

    Deleted customers are flagged (soft); addresses follow their customer:
    an address gone from the array is deleted.
    """

    def __init__(
        self, source: mongodb.MongoDatabase, target: relational.Database
    ) -> None:
        super().__init__()
        self.source = source
        self.compiled = mapping.compile_catalog(
            mapping_specs.catalog(CUSTOMER, ADDRESS), mapping_specs.registry
        )
        self.writer = relational.RelationalTableWriter(target)
        self.registry = relational.RelationalKeyRegistry(target)
        self.state = mongodb.MongoStateStore(source)
        self.quarantine = relational.RelationalQuarantine(target)

    def steps(self) -> Sequence[contract.PipelineStep]:
        return [
            contract.PipelineStep("customer", self._job("customer")),
            contract.PipelineStep(
                "address", self._job("address"), dependencies=("customer",)
            ),
        ]

    def _job(self, name: str) -> pipeline.ETLJob[contract.ChangeEvent, Any]:
        compiled = self.compiled[name]
        key_field = compiled.key_field
        assert key_field is not None
        extractor = operators.ChangeExtractor(
            mongodb.MongoChangeStream(self.source, "customers"),
            state=self.state,
            state_key=f"changes:{name}",
            snapshot=operators.Snapshot(
                mongodb.MongoTableReader(self.source),
                contract.TableQuery(
                    "customers",
                    order_by=("_id",),
                    columns=("_id", "name", "addresses", "updatedAt"),
                ),
            ),
        )
        loader = core_keys.RegisteringLoader(
            operators.ChangeLoader[mapping.MappedRecord](
                self.writer,
                contract.TableTarget(name, key=(key_field,)),
                row=lambda record: record.data,
                deleted=lambda record: record.deleted,
                deletes=(
                    operators.Deletes.SOFT
                    if name == "customer"
                    else operators.Deletes.HARD
                ),
            ),
            self.registry,
            key=mapping.MappedRecord.key_entry,
        )
        gate = quality.QualityGate(
            {
                contract.Checkpoint.AFTER_TRANSFORM: quality.CheckSet([
                    mapping.MappingIssuesRule(),
                    *mapping.reference_rules(compiled, self.registry),
                ])
            },
            quarantine=self.quarantine,
        )
        return pipeline.ETLJob(
            extractor,
            mapping.MappingTransformer[contract.ChangeEvent](
                compiled,
                key=lambda event: str(event.key[0]),
                scope="customer_key" if name == "address" else None,
            ),
            loader,
            gate,
            batch_size=2,
        )


def test_documents_stay_in_sync_deletes_included(
    mongo: mongodb.MongoDatabase, tmp_path: pathlib.Path
) -> None:
    target, tables = _target(tmp_path / "crm.db")
    with target.engine.begin() as connection:
        connection.exec_driver_sql(
            "ALTER TABLE customer ADD COLUMN is_deleted BOOLEAN"
        )
    tables["customer"] = sa.Table(
        "customer", sa.MetaData(), autoload_with=target.engine
    )
    customers = mongo.collection("customers")
    ana, binh, chi = (objectid.ObjectId() for _ in range(3))
    customers.insert_many([
        {
            "_id": ana,
            "name": "Ana",
            "addresses": [{"city": "Genève"}, {"city": "Kraków"}],
            "updatedAt": T0,
        },
        {"_id": binh, "name": "Bruno", "addresses": [], "updatedAt": T0},
    ])

    def run() -> contract.PipelineResult:
        result = Sync(mongo, target).run(fakes.make_context("sync"))
        for step in result.step_results.values():
            assert not step.errors, step.errors
        return result

    def rows(name: str, *columns: str) -> list[tuple[Any, ...]]:
        table = tables[name]
        with target.engine.connect() as connection:
            selected = [table.c[c] for c in columns]
            return [
                tuple(r)
                for r in connection.execute(
                    sa.select(*selected).order_by(*selected)
                )
            ]

    run()
    assert rows("address", "city") == [("Genève",), ("Kraków",)]

    # Ana keeps one address, renamed; Bruno leaves; Chi arrives.
    customers.update_one(
        {"_id": ana}, {"$set": {"addresses": [{"city": "Malmö"}]}}
    )
    customers.delete_one({"_id": binh})
    customers.insert_one({
        "_id": chi,
        "name": "Chi",
        "addresses": [{"city": "Oslo"}],
        "updatedAt": T0,
    })

    second = run()

    assert second.step_results["customer"].stats.deleted == 1
    assert rows("customer", "name", "is_deleted") == [
        ("Ana", False),
        ("Bruno", True),
        ("Chi", False),
    ]
    assert rows("address", "city") == [("Malmö",), ("Oslo",)]
    assert run().step_results["customer"].stats.total == 0
    target.engine.dispose()
