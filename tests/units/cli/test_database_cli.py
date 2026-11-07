from __future__ import annotations

import io
import json
import pathlib

import pytest
import sqlalchemy as sa

from cli import database as database_cli
from tests.integrations.databases import legacy_etl_tables


def _run(*argv: str) -> tuple[int, str]:
    out = io.StringIO()
    code = database_cli.main(list(argv), out)
    return code, out.getvalue()


def test_ddl_needs_no_database() -> None:
    code, out = _run("ddl", "--dialect", "mssql", "--prefix", "mig_")

    assert code == 0
    assert "CREATE TABLE mig_key_registry" in out


def test_init_then_discover(tmp_path: pathlib.Path) -> None:
    url = f"sqlite:///{tmp_path / 'legacy.db'}"
    engine = sa.create_engine(url)
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)"
        )
        connection.exec_driver_sql(
            "CREATE TABLE orders (ord_no TEXT PRIMARY KEY, customer_id INTEGER)"
        )
        connection.exec_driver_sql("INSERT INTO customers VALUES (1, 'a')")
        connection.exec_driver_sql(
            "INSERT INTO orders VALUES ('O1', 1), ('O2', 9)"
        )
    engine.dispose()
    report = tmp_path / "report.json"

    assert _run("init", "--url", url) == (
        0,
        "ETL tables at revision 0004: created\n",
    )
    code, out = _run(
        "discover",
        "main",
        "--url",
        url,
        "--tables",
        "*s",
        "--min-coverage",
        "0.5",
        "--json",
        str(report),
    )

    assert code == 0
    assert (
        "main.orders (customer_id) -> main.customers (id)  inferred"
        "  coverage 50.0%, 1 orphan value(s)"
    ) in out
    assert json.loads(report.read_text())["schema"] == "main"


def test_the_url_can_come_from_the_environment(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("ETL_DATABASE_URL", f"sqlite:///{tmp_path / 'x.db'}")

    assert _run("init")[0] == 0


def test_a_database_is_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ETL_DATABASE_URL", raising=False)

    code, out = _run("init")

    assert code == 2
    assert "--url" in out


def test_database_errors_fail_cleanly() -> None:
    code, out = _run("init", "--url", "sqlite:////no/such/dir/x.db")

    assert code == 1
    assert out.startswith("FAILED upgrading the ETL tables")


def test_url_shows_the_translation_without_the_password() -> None:
    code, out = _run(
        "url", "Server=db;Database=crm;User Id=u;Password=Secret;Pooling=true"
    )

    assert code == 0
    assert out.splitlines() == [
        "mssql+mssqlpython://u:***@db/crm",
        "  from: ado.net",
        (
            "  database: assumed SQL Server, nothing told it; pass --dialect if"
            " wrong"
        ),
        "  ignored: pooling",
    ]


def test_url_takes_the_dialect() -> None:
    code, out = _run(
        "url", "User Id=u;Password=p;Data Source=PRODDB", "--dialect", "oracle"
    )

    assert (code, out.splitlines()[0]) == (0, "oracle+oracledb://u:***@PRODDB")


def test_untranslatable_connection_strings_are_refused() -> None:
    assert _run("url", "nonsense")[0] == 2
    assert _run("init", "--url", "nonsense")[0] == 2


def test_upgrade_check_and_current_on_tables_of_0_1_0(
    tmp_path: pathlib.Path,
) -> None:
    url = f"sqlite:///{tmp_path / 'etl.db'}"
    engine = sa.create_engine(url)
    legacy_etl_tables.metadata("0001", prefix="old_").create_all(engine)
    engine.dispose()

    code, out = _run("check", "--url", url, "--prefix", "old_")
    assert code == 5
    assert out.splitlines() == [
        "none",
        (
            "The ETL tables have no version table (they were created before"
            " it); they look like revision 0001 (the tables of 0.1.0). Run"
            " `etl-db upgrade`, or have a DBA run the SQL of `etl-db ddl"
            " --dialect sqlite --prefix old_ --from 0001 --legacy`."
        ),
    ]
    assert _run("current", "--url", url, "--prefix", "old_")[0] == 0
    assert _run("upgrade", "--url", url, "--prefix", "old_") == (
        0,
        "ETL tables at revision 0004: upgraded from 0001 (no version table)\n",
    )
    assert _run("check", "--url", url, "--prefix", "old_") == (
        0,
        "0004\nThe ETL tables are at revision 0004, the latest.\n",
    )
    assert _run("upgrade", "--url", url, "--prefix", "old_") == (
        0,
        "ETL tables at revision 0004: nothing to do\n",
    )
    # Another prefix, another version.
    assert _run("check", "--url", url)[0] == 5


def test_check_fails_on_tables_it_cannot_tell(tmp_path: pathlib.Path) -> None:
    url = f"sqlite:///{tmp_path / 'etl.db'}"
    engine = sa.create_engine(url)
    tables = legacy_etl_tables.metadata("0002")
    tables.remove(tables.tables["etl_locks"])
    tables.create_all(engine)
    engine.dispose()

    code, out = _run("check", "--url", url)
    assert code == 1
    assert "they look like revision 0002, without etl_locks" in out
    code, out = _run("upgrade", "--url", url)
    assert code == 1
    assert out.startswith("FAILED The ETL tables have no version table, and")


def test_ddl_from_a_revision() -> None:
    code, out = _run("ddl", "--dialect", "postgresql", "--from", "0002")

    assert code == 0
    assert "CREATE TABLE" not in out
    assert "ALTER TABLE etl_quarantine ADD COLUMN record_key" in out
    assert out.count("UPDATE etl_alembic_version") == 2

    code, out = _run("ddl", "--dialect", "mssql", "--from", "0001", "--legacy")
    assert code == 0
    assert out.index("CREATE TABLE etl_alembic_version") < out.index(
        "ALTER TABLE etl_quarantine ADD status"
    )
    assert _run("ddl", "--dialect", "mysql", "--from", "0004") == (
        0,
        "-- The ETL tables are at the latest revision: nothing to run.\n",
    )


def test_ddl_refuses_what_it_cannot_write() -> None:
    assert _run("ddl", "--dialect", "postgresql", "--legacy")[0] == 2
    assert _run("ddl", "--dialect", "nosuchdb")[0] == 2
    with pytest.raises(SystemExit):
        _run("ddl", "--dialect", "postgresql", "--from", "0009")
