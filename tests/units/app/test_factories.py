from __future__ import annotations

import pathlib
from typing import Any

import pytest
import sqlalchemy as sa

from app import factories
import config
from core import contract
from core import keys
from core import runtime
from core.runtime import names
from infra.database import relational


def _settings(tmp_path: pathlib.Path, **values: Any) -> config.AppSettings:
    return config.load_settings(
        "test", directory=tmp_path / "none", overrides=values
    )


def test_a_sql_database_and_file_bookkeeping(tmp_path: pathlib.Path) -> None:
    settings = _settings(
        tmp_path,
        databases={"crm": {"url": f"sqlite:///{tmp_path / 'crm.db'}"}},
        stores={"landing": {"kind": "local", "path": str(tmp_path)}},
        bookkeeping={"directory": str(tmp_path / "var")},
    )

    with runtime.Resources(factories.resource_specs(settings)) as resources:
        with resources.run_scope() as run:
            reader = run.get(names.reader("crm"), contract.ITableReader)
        resources.get(names.writer("crm"), contract.ITableWriter)
        resources.get(names.store("landing"), contract.IObjectStore)
        registry = resources.get(names.KEYS, contract.IKeyRegistry)
        resources.get(names.STATE, contract.IStateStore)
        health = resources.health()
        db = resources.get(names.database("crm"), relational.Database)

        assert isinstance(reader, relational.RelationalTableReader)
        assert reader.db is db
        assert isinstance(registry, keys.CachedKeyRegistry)
        assert health == {
            names.database("crm"): "ok",
            names.store("landing"): "ok",
        }
        with pytest.raises(
            contract.PipelineConfigError, match="bookkeeping.database"
        ):
            resources.get(names.PARKING, contract.IParkingLot)

    # SQLite in a file: each connection closed once used, none kept open.
    assert isinstance(db.engine.pool, sa.pool.NullPool)


def test_each_run_gets_its_own_table_reader(tmp_path: pathlib.Path) -> None:
    settings = _settings(
        tmp_path,
        databases={"crm": {"url": f"sqlite:///{tmp_path / 'crm.db'}"}},
    )

    with runtime.Resources(factories.resource_specs(settings)) as resources:
        with resources.run_scope() as run:
            first = run.get(names.reader("crm"), contract.ITableReader)
            again = run.get(names.reader("crm"), contract.ITableReader)
        with resources.run_scope() as run:
            second = run.get(names.reader("crm"), contract.ITableReader)

    assert first is again
    assert first is not second
    assert isinstance(first, relational.RelationalTableReader)
    assert isinstance(second, relational.RelationalTableReader)
    assert first.db is second.db


def test_bookkeeping_in_a_database(tmp_path: pathlib.Path) -> None:
    settings = _settings(
        tmp_path,
        databases={"etl": {"url": f"sqlite:///{tmp_path / 'etl.db'}"}},
        bookkeeping={"database": "etl", "create": True, "prefix": "mig_"},
    )

    with runtime.Resources(factories.resource_specs(settings)) as resources:
        lot = resources.get(names.PARKING, contract.IParkingLot)
        state = resources.get(names.STATE, contract.IStateStore)
        state.set("crm", "watermark", 1)

        assert isinstance(lot, relational.RelationalParkingLot)
        assert state.get("crm", "watermark") == 1
        assert lot.tables.parked.name == "mig_parked"


def test_an_unreachable_database_is_unhealthy(tmp_path: pathlib.Path) -> None:
    settings = _settings(
        tmp_path, databases={"down": {"url": "sqlite:////no/such/dir/x.db"}}
    )

    with runtime.Resources(factories.resource_specs(settings)) as resources:
        health = resources.health()

    assert health[names.database("down")].startswith("RetryableError")


def test_debezium_changes_through_a_kafka_cluster(
    tmp_path: pathlib.Path,
) -> None:
    from infra import messaging  # pylint: disable=import-outside-toplevel
    from infra.messaging import kafka  # pylint: disable=import-outside-toplevel

    settings = _settings(
        tmp_path,
        kafka={"main": {"bootstrap_servers": "localhost:1", "group_id": "g"}},
        databases={
            "legacy": {
                "url": f"sqlite:///{tmp_path / 'legacy.db'}",
                "changes": {
                    "kind": "debezium",
                    "kafka": "main",
                    "topic_prefix": "legacy",
                },
            }
        },
    )

    with runtime.Resources(factories.resource_specs(settings)) as resources:
        log = resources.get(names.changes("legacy"), contract.IChangeLog)
        cluster = resources.get(names.kafka("main"), contract.IMessageLog)
        source = log.source("CRM.CUSTOMER")

        assert isinstance(cluster, kafka.KafkaCluster)
        assert cluster.group_id == "g"
        assert isinstance(source, messaging.DebeziumChangeSource)
        assert source.messages.topic == "legacy.CRM.CUSTOMER"
