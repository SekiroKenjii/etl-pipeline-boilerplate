"""The ETL tables at startup: upgraded when `bookkeeping.create`, else
checked, and a run refused while they are behind (review D4)."""

from __future__ import annotations

import datetime
import io
import json
import pathlib

import pytest
import sqlalchemy as sa

from cli import database as database_cli
from cli import run
from core import contract
from infra.database import relational
from infra.database.relational import migrations
from shared.utils import retrying
from tests.integrations.databases import legacy_etl_tables


def _project(tmp_path: pathlib.Path, *, create: bool) -> pathlib.Path:
    """A config directory: a source table, the ETL tables in `etl.db`."""
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'src.db'}")
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "CREATE TABLE source (id INTEGER PRIMARY KEY, name TEXT)"
        )
        connection.exec_driver_sql(
            "CREATE TABLE target (id INTEGER PRIMARY KEY, name TEXT)"
        )
        connection.exec_driver_sql("INSERT INTO source VALUES (1, 'a')")
    engine.dispose()
    directory = tmp_path / "config"
    directory.mkdir()
    (directory / "base.toml").write_text(
        f"""
[databases.src]
url = "sqlite:///{tmp_path / 'src.db'}"
[databases.etl]
url = "sqlite:///{tmp_path / 'etl.db'}"
[bookkeeping]
database = "etl"
create = {str(create).lower()}
[modules.demo]
module = "tests.factories.demo_module:create"
""",
        encoding="utf-8",
    )
    return directory


def _etl(project: pathlib.Path, *argv: str) -> tuple[int, str]:
    out = io.StringIO()
    code = run.main(["--env", "test", "--config", str(project), *argv], out)
    return code, out.getvalue()


def _old_tables(url: str) -> sa.MetaData:
    """The tables of 0.1.0, a run and a quarantined record in them."""
    tables = legacy_etl_tables.metadata("0001")
    engine = sa.create_engine(url)
    now = datetime.datetime(2026, 1, 2, tzinfo=datetime.UTC)
    with engine.begin() as connection:
        tables.create_all(connection)
        connection.execute(
            tables.tables["etl_runs"].insert(),
            {
                "run_id": "r0",
                "pipeline": "demo.copy",
                "status": "success",
                "started_at": now,
                "host": "h",
                "dry_run": False,
            },
        )
        connection.execute(
            tables.tables["etl_quarantine"].insert(),
            {
                "pipeline": "demo.copy",
                "run_id": "r0",
                "checkpoint": "after_transform",
                "record": json.dumps({"id": 7}),
                "issues": "[]",
                "rejected_at": now,
            },
        )
    engine.dispose()
    return tables


def test_a_run_is_refused_until_the_tables_are_upgraded(
    tmp_path: pathlib.Path,
) -> None:
    project = _project(tmp_path, create=False)
    url = f"sqlite:///{tmp_path / 'etl.db'}"
    _old_tables(url)

    code, out = _etl(project, "run", "demo.copy")
    assert code == 2
    assert out.startswith(
        "FAILED bookkeeping.database 'etl': The ETL tables have no version"
        " table"
    )
    assert "Run `etl-db upgrade`" in out
    assert "`etl-db ddl --dialect sqlite --from 0001 --legacy`" in out
    code, out = _etl(project, "check")
    assert code == 5
    assert "FAIL etl.tables: PipelineConfigError: bookkeeping.database" in out

    assert database_cli.main(["upgrade", "--url", url], io.StringIO()) == 0
    assert _etl(project, "run", "demo.copy")[0] == 0
    assert _etl(project, "check")[0] == 0
    code, out = _etl(project, "runs", "demo.copy", "--format", "json")
    assert [r["run_id"] for r in json.loads(out)][-1] == "r0"


def test_empty_databases_are_refused_too(tmp_path: pathlib.Path) -> None:
    project = _project(tmp_path, create=False)

    code, out = _etl(project, "run", "demo.copy")

    assert code == 2
    assert "There are no ETL tables: run `etl-db upgrade`" in out


@pytest.mark.parametrize("old", [False, True])
def test_create_upgrades_the_tables(tmp_path: pathlib.Path, old: bool) -> None:
    project = _project(tmp_path, create=True)
    url = f"sqlite:///{tmp_path / 'etl.db'}"
    if old:
        _old_tables(url)

    assert _etl(project, "run", "demo.copy")[0] == 0

    db = relational.Database(url)
    try:
        assert migrations.status(db).revision == migrations.HEAD
        quarantine = relational.RelationalQuarantine(db)
        assert [e.record for e in quarantine.entries()] == (
            [{"id": 7}] if old else []
        )
    finally:
        db.engine.dispose()


class TestCheckingWhileAnotherProcessUpgrades:
    """Reading the catalog deadlocks with another process's DDL on SQL
    Server: `check` looks again a few times before giving up."""

    @pytest.fixture(autouse=True)
    def _no_delay(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(retrying.time, "sleep", lambda _: None)

    def test_a_transient_catalog_error_is_tried_again(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls: list[int] = []

        def status(*args: object, **kwargs: object) -> migrations.Status:
            del args, kwargs
            calls.append(1)
            if len(calls) == 1:
                raise contract.RetryableError("deadlocked on lock resources")
            return migrations.Status(dialect="sqlite", revision=migrations.HEAD)

        monkeypatch.setattr(migrations, "status", status)

        migrations.check(relational.Database("sqlite://"))

        assert len(calls) == 2

    def test_a_catalog_that_never_answers_fails_after_a_few_attempts(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls: list[int] = []

        def status(*args: object, **kwargs: object) -> migrations.Status:
            del args, kwargs
            calls.append(1)
            raise contract.RetryableError("deadlocked on lock resources")

        monkeypatch.setattr(migrations, "status", status)

        db = relational.Database("sqlite://")
        with pytest.raises(contract.RetryableError):
            migrations.check(db)

        assert len(calls) == migrations._RACING_UPGRADE_ATTEMPTS  # pylint: disable=protected-access
