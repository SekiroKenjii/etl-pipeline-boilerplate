"""The ETL's own records and the catalog, the same way on every database."""

from __future__ import annotations

import concurrent.futures
import decimal
import json
from typing import Any

import pytest
import sqlalchemy as sa

from core import contract
from core import keys
from infra.database import relational
from tests.factories import fakes
from tests.factories import ledgers
from tests.factories import quarantines
from tests.factories import registries
from tests.integrations.databases import conftest
from tests.integrations.databases import legacy_etl_tables


@pytest.fixture(params=["created", "upgraded from 0.1.0"])
def etl(
    request: pytest.FixtureRequest, backend: conftest.Backend
) -> conftest.Backend:
    """The ETL tables with the prefix of the test: created by this release,
    or created by 0.1.0 then upgraded."""
    if request.param != "created":
        legacy_etl_tables.metadata("0001", prefix=backend.prefix).create_all(
            backend.db.engine
        )
    relational.create_etl_tables(backend.db, prefix=backend.prefix)
    return backend


class TestKeyRegistry:

    def test_an_idempotent_crosswalk_safe_under_concurrency(
        self, etl: conftest.Backend
    ) -> None:
        registry = relational.RelationalKeyRegistry(etl.db, prefix=etl.prefix)
        entry = contract.KeyEntry("crm.customer", "k1", ("C001", "Zürich"))
        others = [
            contract.KeyEntry("crm.customer", f"k{i}", (str(i),))
            for i in range(2, 600)
        ]

        def use(r: relational.RelationalKeyRegistry) -> Any:
            r.register([entry, entry])
            # Steps registering the same keys at once never conflict.
            with concurrent.futures.ThreadPoolExecutor(4) as threads:
                list(
                    threads.map(
                        lambda _: r.register([entry, *others]), range(4)
                    )
                )
            return (
                len(r.known("crm.customer", [e.surrogate_key for e in others])),
                r.known("crm.customer", ["k1", "nope"]),
                r.known("crm.customer", []),
                r.natural_key("crm.customer", "k1"),
                r.natural_key("crm.order", "k1"),
            )

        assert use(registry) == (
            598,
            {"k1"},
            set(),
            ("C001", "Zürich"),
            None,
        )

    def test_behind_a_cache(self, etl: conftest.Backend) -> None:
        cached = keys.CachedKeyRegistry(
            relational.RelationalKeyRegistry(etl.db, prefix=etl.prefix)
        )

        def use(r: keys.CachedKeyRegistry) -> Any:
            r.register([contract.KeyEntry("e", "k1", ("1",))])
            return r.known("e", ["k1", "k2"]), r.natural_key("e", "k1")

        assert use(cached) == ({"k1"}, ("1",))


def test_state_round_trips(etl: conftest.Backend) -> None:
    store = relational.RelationalStateStore(etl.db, prefix=etl.prefix)
    value: contract.StateValue = {"position": [1, "a", None], "n": 2.5}

    def use(s: relational.RelationalStateStore) -> Any:
        s.set("crm", "watermark", {"old": True})
        s.set("crm", "watermark", value)
        found = s.get("crm", "watermark")
        s.delete("crm", "watermark")
        s.delete("crm", "missing")
        return found, s.get("crm", "watermark")

    assert use(store) == (value, None)


def test_the_parking_lot_counts_attempts(etl: conftest.Backend) -> None:
    lot = relational.RelationalParkingLot(etl.db, prefix=etl.prefix)

    def use(parking: relational.RelationalParkingLot) -> Any:
        parking.park(
            "order",
            [
                contract.ParkedEntry("a", {"v": 1}, "late"),
                contract.ParkedEntry("b", {"v": 2}, "late"),
                contract.ParkedEntry("b", {"v": 3}, "later"),
            ],
        )
        read = list(parking.entries("order"))
        parking.park("order", [contract.ParkedEntry("b", {"v": 4}, "still")])
        parking.release("order", read)
        return read, list(parking.entries("order"))

    read, left = use(lot)
    assert [(e.identity, e.payload, e.attempts) for e in read] == [
        ("a", {"v": 1}, 1),
        ("b", {"v": 3}, 1),
    ]
    assert [(e.identity, e.reason, e.attempts) for e in left] == [
        ("b", "still", 2)
    ]
    assert left[0].parked_at is not None


def test_quarantine_and_issues_are_stored_with_their_run(
    etl: conftest.Backend,
) -> None:
    issue = contract.QualityIssue(
        "positive",
        contract.Checkpoint.AFTER_TRANSFORM,
        contract.QualityAction.REJECT,
        "must be > 0",
        field="total",
        value=decimal.Decimal("-1"),
    )
    rejected = contract.RejectedRecord(
        fakes.User("u1", "a@x", True), issue.checkpoint, (issue,)
    )
    ctx = fakes.make_context("orders")
    quarantine = relational.RelationalQuarantine(etl.db, prefix=etl.prefix)
    sink = relational.RelationalIssueSink(etl.db, prefix=etl.prefix)

    quarantine.put([rejected], ctx)
    sink.write([issue, issue], ctx)
    quarantine.put([], ctx)
    sink.write([], ctx)

    tables = relational.etl_tables(prefix=etl.prefix)
    (stored,) = etl.rows(tables.quarantine, "pipeline", "run_id", "record")
    assert stored[:2] == ("orders", ctx.run.run_id)
    assert json.loads(stored[2]) == {
        "id": "u1",
        "email": "a@x",
        "is_adult": True,
    }
    assert (
        etl.rows(tables.quality_issues, "rule", "value")
        == [
            ("positive", "\"Decimal('-1')\""),
        ]
        * 2
    )


def test_the_crosswalk_forgets_a_natural_key(etl: conftest.Backend) -> None:
    registries.check_forget(
        relational.RelationalKeyRegistry(etl.db, prefix=etl.prefix)
    )


def test_the_quarantine_is_read_resolved_and_purged(
    etl: conftest.Backend,
) -> None:
    quarantines.check_quarantine(
        relational.RelationalQuarantine(etl.db, prefix=etl.prefix)
    )


@pytest.fixture
def legacy(backend: conftest.Backend) -> conftest.Backend:
    """Legacy tables: one declared foreign key, one undeclared."""
    customers = backend.table(
        "customers",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("cust_no", sa.Unicode(10), unique=True),
    )
    orders = backend.table(
        "orders",
        sa.Column("ord_no", sa.Unicode(10), primary_key=True),
        sa.Column("customer_id", sa.Integer, sa.ForeignKey(customers.c.id)),
        sa.Column("cust_no", sa.Unicode(10)),
    )
    backend.create()
    backend.insert(
        customers, {"id": 1, "cust_no": "C1"}, {"id": 2, "cust_no": "C2"}
    )
    backend.insert(
        orders,
        {"ord_no": "O1", "customer_id": 1, "cust_no": "C1"},
        {"ord_no": "O2", "customer_id": 2, "cust_no": "C2"},
        {"ord_no": "O3", "customer_id": 1, "cust_no": "C9"},
    )
    return backend


class TestSchemaInspector:

    def test_the_catalog_is_read(self, legacy: conftest.Backend) -> None:
        inspector = relational.RelationalSchemaInspector(legacy.db)
        schema, prefix = legacy.schema, legacy.prefix

        pattern = f"{prefix}*"
        tables, fks = (
            inspector.tables(schema, pattern),
            inspector.foreign_keys(schema, pattern),
        )

        mine = {t.name: t for t in tables}
        assert len(mine) == 2
        customers = mine[f"{schema}.{prefix}customers"]
        orders = mine[f"{schema}.{prefix}orders"]
        assert customers.primary_key == ("id",)
        # Exact on SQL Server; other databases estimate once they analyze.
        if legacy.name == "mssql":
            assert orders.estimated_rows == 3
        assert orders.estimated_rows >= -1
        assert customers.unique == (("cust_no",),)
        assert (
            orders.column("customer_id").family is contract.TypeFamily.INTEGER
        )
        assert orders.column("cust_no").family is contract.TypeFamily.TEXT
        (fk,) = [fk for fk in fks if fk.table == orders.name]
        assert (fk.columns, fk.referenced_table, fk.referenced_columns) == (
            ("customer_id",),
            customers.name,
            ("id",),
        )

    def test_columns_are_profiled_and_checked(
        self, legacy: conftest.Backend
    ) -> None:
        inspector = relational.RelationalSchemaInspector(legacy.db)
        orders = f"{legacy.schema}.{legacy.prefix}orders"
        customers = f"{legacy.schema}.{legacy.prefix}customers"

        profiles, sampled, check = (
            inspector.profile(orders, ["ord_no", "customer_id"]),
            inspector.profile(orders, ["customer_id"], sample_rows=2),
            inspector.inclusion(orders, "cust_no", customers, "cust_no"),
        )

        assert profiles["ord_no"].is_unique
        assert profiles["customer_id"] == contract.ColumnProfile(3, 3, 2)
        assert sampled["customer_id"].rows == 2
        assert (check.distinct, check.unmatched) == (3, 1)

    def test_relationships_are_discovered(
        self, legacy: conftest.Backend
    ) -> None:
        inspector = relational.RelationalSchemaInspector(legacy.db)

        report = keys.discover_relationships(
            inspector,
            legacy.schema,
            min_coverage=0.5,
            tables=f"{legacy.prefix}*",
        )

        assert [
            (r.columns, r.evidence, r.orphans) for r in report.relationships
        ] == [
            (("customer_id",), keys.Evidence.DECLARED, 0),
            (("cust_no",), keys.Evidence.INFERRED, 1),
        ]


def test_the_run_ledger_and_lock(etl: conftest.Backend) -> None:
    ledger = relational.RelationalRunLedger(etl.db, prefix=etl.prefix)
    lock = relational.RelationalRunLock(etl.db, prefix=etl.prefix)
    ctx = fakes.make_context("crm.load")
    result = contract.PipelineResult(
        pipeline_name="crm.load", run_id=ctx.run.run_id
    )
    result.stats.total = 3
    result.complete(contract.ETLStatus.PARTIAL_SUCCESS, "1 rejected")

    def use_ledger(l: relational.RelationalRunLedger) -> Any:
        l.start(ctx.run, "host-1", {"user": "an", "build": "v1-3-gabc"})
        running = l.recent("crm.load")
        l.finish(result)
        return running, l.recent("crm.load"), l.recent(None, limit=5)

    running, done, every = use_ledger(ledger)
    assert running[0].status is contract.ETLStatus.IN_PROGRESS
    assert running[0].meta == {"user": "an", "build": "v1-3-gabc"}
    (record,) = done
    assert (record.status, record.stats["total"], record.message) == (
        contract.ETLStatus.PARTIAL_SUCCESS,
        3,
        "1 rejected",
    )
    assert len(every) == 1

    def use_lock(l: relational.RelationalRunLock) -> Any:
        first = l.acquire("crm.load", "a", 60)
        second = l.acquire("crm.load", "b", 60)
        renewed = l.renew("crm.load", "a", 60), l.renew("crm.load", "b", 60)
        l.release("crm.load", "a")
        return (
            first,
            second,
            renewed,
            l.acquire("crm.load", "b", -1),
            l.acquire("crm.load", "c", 60),
        )

    # b's lease is already over: c takes the lock.
    assert use_lock(lock) == (True, False, (True, False), True, True)


def test_the_steps_of_a_run(etl: conftest.Backend) -> None:
    ledgers.check_steps(
        relational.RelationalRunLedger(etl.db, prefix=etl.prefix)
    )
