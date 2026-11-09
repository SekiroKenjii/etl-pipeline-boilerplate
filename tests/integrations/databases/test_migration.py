"""A legacy-to-target migration, end to end, on every database.

Legacy tables are read incrementally by watermark, mapped by the mapping
specs, and upserted into target tables with a real foreign key. Orders whose
customer has not arrived yet get an inferred customer, which the real one
replaces later. The ETL's records (crosswalk, state, quarantine) live in the
same database.
"""

from __future__ import annotations

from collections.abc import Sequence
import datetime
from typing import Any

import sqlalchemy as sa

from core import contract
from core import keys as core_keys
from core import mapping
from core import operators
from core import pipeline
from core import quality
from infra.database import relational
from tests.factories import fakes
from tests.factories import mapping_specs
from tests.integrations.databases import conftest

T0 = datetime.datetime(2026, 10, 1, 8, 0, 0)

CUSTOMER = mapping_specs.mapping(
    mapping_specs.field(
        "customer_key", "cust_no", target_type="UUID", primary_key=True
    ),
    mapping_specs.field("name", "name", row=3),
    mapping_specs.field(
        "is_inferred", target_type="BOOLEAN", default="false", row=4
    ),
    name="customer",
    ignored_source_fields=("updated_at",),
)
ORDER = mapping_specs.mapping(
    mapping_specs.field(
        "order_key", "ord_no", target_type="UUID", primary_key=True
    ),
    mapping_specs.field(
        "customer_key",
        "cust_no",
        target_type="UUID",
        references="customer",
        row=3,
    ),
    mapping_specs.field("total", "total", target_type="NUMERIC(10,2)", row=4),
    name="order",
    target="crm.order",
    ignored_source_fields=("updated_at",),
)
CRM = mapping_specs.catalog(CUSTOMER, ORDER)
# Oracle, SQLite and MySQL have no UUID type: keys are text there.
KEY = sa.Uuid().with_variant(sa.Unicode(36), "oracle", "sqlite", "mysql")


def _tables(backend: conftest.Backend) -> dict[str, sa.Table]:
    tables = {
        "legacy_customer": backend.table(
            "legacy_customer",
            sa.Column("cust_no", sa.Unicode(10), primary_key=True),
            sa.Column("name", sa.Unicode(50)),
            sa.Column("updated_at", sa.DateTime, nullable=False),
        ),
        "legacy_order": backend.table(
            "legacy_order",
            sa.Column("ord_no", sa.Unicode(10), primary_key=True),
            sa.Column("cust_no", sa.Unicode(10)),
            # Legacy systems keep numbers as text, and some are not numbers.
            sa.Column("total", sa.Unicode(20)),
            sa.Column("updated_at", sa.DateTime, nullable=False),
        ),
    }
    tables["customer"] = backend.table(
        "customer",
        sa.Column("customer_key", KEY, primary_key=True),
        sa.Column("name", sa.Unicode(50)),
        sa.Column("is_inferred", sa.Boolean, nullable=False),
    )
    tables["order"] = backend.table(
        "order",
        sa.Column("order_key", KEY, primary_key=True),
        sa.Column(
            "customer_key",
            KEY,
            sa.ForeignKey(tables["customer"].c.customer_key),
            nullable=False,
        ),
        sa.Column("total", sa.Numeric(10, 2)),
    )
    backend.create()
    relational.create_etl_tables(backend.db, prefix=backend.prefix)
    return tables


class Migration(pipeline.BaseOrchestrator):
    """Customers, then orders; orders infer the customers not loaded yet."""

    def __init__(self, backend: conftest.Backend) -> None:
        super().__init__()
        db, prefix = backend.db, backend.prefix
        self.prefix = prefix
        self.compiled = mapping.compile_catalog(CRM, mapping_specs.registry)
        # Memory for a dozen keys: the rest of them go through the disk tier.
        self.registry = core_keys.TieredKeyCache(
            relational.RelationalKeyRegistry(db, prefix=prefix),
            memory_budget=4096,
            disk_budget=10**7,
        )
        self.reader = relational.RelationalTableReader(db, page_size=2)
        self.writer = relational.RelationalTableWriter(db)
        self.state = relational.RelationalStateStore(db, prefix=prefix)
        self.quarantine = relational.RelationalQuarantine(db, prefix=prefix)

    def steps(self) -> Sequence[contract.PipelineStep]:
        dependencies = mapping.mapping_dependencies(CRM)
        return [
            contract.PipelineStep(
                name, self._job(name), dependencies=tuple(dependencies[name])
            )
            for name in ("customer", "order")
        ]

    def _job(self, name: str) -> pipeline.ETLJob[contract.Row, Any]:
        compiled = self.compiled[name]
        key_field = compiled.key_field
        assert key_field is not None
        extractor = operators.TableExtractor(
            self.reader,
            f"{self.prefix}legacy_{name}",
            watermark="updated_at",
            key=[compiled.spec.fields[0].sources[0]],
            state=self.state,
        )
        loader: pipeline.IDataLoader[mapping.MappedRecord] = (
            core_keys.RegisteringLoader(
                operators.TableLoader[mapping.MappedRecord](
                    self.writer,
                    contract.TableTarget(
                        f"{self.prefix}{name}", key=(key_field,)
                    ),
                    row=lambda record: record.data,
                ),
                self.registry,
                key=mapping.MappedRecord.key_entry,
            )
        )
        inferred: set[str] = set()
        if name == "order":
            inferred = {"customer_key"}
            loader = mapping.InferringLoader(
                loader,
                compiled,
                self.registry,
                [
                    mapping.InferredParent(
                        "customer_key",
                        operators.TableLoader[contract.Row](
                            self.writer,
                            contract.TableTarget(
                                f"{self.prefix}customer",
                                key=("customer_key",),
                                mode=contract.WriteMode.INSERT_MISSING,
                            ),
                        ),
                        lambda key, natural: {
                            "customer_key": key,
                            "name": None,
                            "is_inferred": True,
                        },
                    )
                ],
            )
        gate = quality.QualityGate(
            {
                contract.Checkpoint.AFTER_TRANSFORM: quality.CheckSet([
                    mapping.MappingIssuesRule(),
                    *mapping.reference_rules(
                        compiled, self.registry, exclude=inferred
                    ),
                ])
            },
            quarantine=self.quarantine,
        )
        return pipeline.ETLJob(
            extractor,
            mapping.MappingTransformer[contract.Row](
                compiled, key=lambda row: str(row.get("ord_no", row["cust_no"]))
            ),
            loader,
            gate,
            batch_size=2,
        )


def test_incremental_migration_with_inferred_parents(
    backend: conftest.Backend,
) -> None:
    tables = _tables(backend)

    def at(minutes: int) -> datetime.datetime:
        return T0 + datetime.timedelta(minutes=minutes)

    def run() -> contract.PipelineResult:
        result = Migration(backend).run(fakes.make_context("crm"))
        for step in result.step_results.values():
            assert not step.errors, step.errors
        return result

    backend.insert(
        tables["legacy_customer"],
        {"cust_no": "C1", "name": "Ana", "updated_at": at(1)},
        {"cust_no": "C2", "name": "Bruno", "updated_at": at(1)},
    )
    backend.insert(
        tables["legacy_order"],
        {"ord_no": "O1", "cust_no": "C1", "total": "10", "updated_at": at(1)},
        {"ord_no": "O2", "cust_no": "C3", "total": "5", "updated_at": at(2)},
        {"ord_no": "O3", "cust_no": "C1", "total": "n/a", "updated_at": at(2)},
    )

    first = run()

    assert first.step_results["customer"].stats.created == 2
    orders = first.step_results["order"].stats
    # O3 has a bad total: quarantined, the run goes on.
    assert (orders.created, orders.rejected) == (2, 1)
    assert sorted(
        backend.rows(tables["customer"], "is_inferred", "name"),
        key=lambda row: (row[0], row[1] or ""),
    ) == [(False, "Ana"), (False, "Bruno"), (True, None)]

    # The late customer arrives; Ana changes; nothing else does.
    backend.insert(
        tables["legacy_customer"],
        {"cust_no": "C3", "name": "Chi", "updated_at": at(5)},
    )
    customers = tables["legacy_customer"]
    backend.execute(
        customers.update()
        .where(customers.c.cust_no == "C1")
        .values(name="Anna", updated_at=at(6))
    )

    second = run()

    stats = second.step_results["customer"].stats
    # Chi replaces her placeholder: an update, like Anna. Bruno, at the last
    # watermark of the first run, is read again (once), as are the orders.
    assert (stats.total, stats.created, stats.updated) == (3, 0, 2)
    assert second.step_results["order"].stats.total == 2
    order, customer = tables["order"], tables["customer"]
    with backend.db.engine.connect() as connection:
        names = connection.execute(
            sa.select(customer.c.name, customer.c.is_inferred)
            .select_from(
                order.join(
                    customer, order.c.customer_key == customer.c.customer_key
                )
            )
            .order_by(order.c.total)
        ).all()
    assert [tuple(row) for row in names] == [("Chi", False), ("Anna", False)]

    third = run()

    # Anna, at the last watermark of the second run; the orders are done.
    assert third.step_results["customer"].stats.total == 1
    assert third.step_results["order"].stats.total == 0
    quarantined = relational.etl_tables(prefix=backend.prefix).quarantine
    # O3, rejected by both runs that read it (at-least-once): one open entry,
    # refreshed by the second.
    assert len(backend.rows(quarantined, "id")) == 1
