"""What the settings of a database change in its resources."""

from __future__ import annotations

import pathlib
from typing import Any

import pytest
import sqlalchemy as sa

from app import factories
import config
from core import contract
from core import runtime
from core.runtime import names
from infra.database import relational


def _settings(tmp_path: pathlib.Path, **values: Any) -> config.AppSettings:
    return config.load_settings(
        "test", directory=tmp_path / "none", overrides=values
    )


def test_the_reader_reads_pages_of_the_page_size(
    tmp_path: pathlib.Path,
) -> None:
    url = f"sqlite:///{tmp_path / 'crm.db'}"
    default = _settings(tmp_path, databases={"crm": {"url": url}})
    chosen = _settings(
        tmp_path, databases={"crm": {"url": url, "page_size": 250}}
    )

    for settings, size in ((default, 1_000), (chosen, 250)):
        with (
            runtime.Resources(factories.resource_specs(settings)) as found,
            found.run_scope() as run,
        ):
            reader = run.get(names.reader("crm"), contract.ITableReader)

            assert isinstance(reader, relational.RelationalTableReader)
            assert reader.page_size == size


def test_engine_options_reach_the_engine(tmp_path: pathlib.Path) -> None:
    settings = _settings(
        tmp_path,
        databases={
            "crm": {
                "url": "sqlite://",
                "options": {
                    "echo_pool": True,
                    "connect_args": {"timeout": 7},
                    "execution_options": {"isolation_level": "AUTOCOMMIT"},
                },
            }
        },
    )

    with runtime.Resources(factories.resource_specs(settings)) as resources:
        db = resources.get(names.database("crm"), relational.Database)
        with db.engine.connect() as connection:
            connection.execute(sa.select(1))

        assert db.engine.pool.echo is True
        assert db.engine.get_execution_options()["isolation_level"] == (
            "AUTOCOMMIT"
        )


def test_the_etl_tables_are_created_once(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []
    create = relational.create_etl_tables

    def counted(db: relational.Database, *args: Any) -> None:
        calls.append(db.name)
        create(db, *args)

    monkeypatch.setattr(relational, "create_etl_tables", counted)
    settings = _settings(
        tmp_path,
        databases={"etl": {"url": f"sqlite:///{tmp_path / 'etl.db'}"}},
        bookkeeping={"database": "etl", "create": True},
    )

    with runtime.Resources(factories.resource_specs(settings)) as resources:
        for name in (
            names.STATE,
            names.KEYS,
            names.QUARANTINE,
            names.ISSUES,
            names.PARKING,
            names.LEDGER,
            names.LOCK,
        ):
            resources.resolve(name)

    assert calls == ["sqlite"]
