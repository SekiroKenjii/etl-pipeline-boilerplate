"""The sample CRM module, end to end, through the real bootstrap.

On SQLite always; its `sync` on Postgres (logical replication) when
`ETL_TEST_POSTGRES_URL` is set.
"""

from __future__ import annotations

from collections.abc import Generator
import csv
import datetime
import io
import json
import os
import pathlib
import re
from typing import Any
import uuid

import openpyxl
import pytest
import sqlalchemy as sa

from app import bootstrap
from cli import exit_codes
from cli import run as cli_run
from core import contract
from core import runtime
from infra.database import relational
from modules.crm import demo

ROOT = pathlib.Path(__file__).resolve().parents[2]
MAPPINGS = ROOT / "mappings" / "compiled" / "crm.json"


def _settings(legacy: str, crm: str, **crm_settings: Any) -> dict[str, Any]:
    return {
        "databases": {"legacy": {"url": legacy}, "crm": {"url": crm}},
        "bookkeeping": {"database": "crm", "create": True},
        "modules": {
            "crm": {
                "module": "modules.crm:create",
                "settings": {
                    "mappings": str(MAPPINGS),
                    "source_tables": {
                        "customer": "CUST_MASTER",
                        "order": "ORD_MASTER",
                    },
                    "target_tables": {"customer": "customer", "order": "order"},
                    "max_reject_rate": 0.6,
                    "orphans": "park",
                    **crm_settings,
                },
            }
        },
    }


def _rows(engine: sa.Engine, sql: str) -> list[tuple[Any, ...]]:
    with engine.connect() as connection:
        return [tuple(row) for row in connection.exec_driver_sql(sql)]


@pytest.fixture
def sqlite(
    tmp_path: pathlib.Path,
) -> Generator[tuple[sa.Engine, sa.Engine, dict[str, Any]]]:
    legacy = sa.create_engine(f"sqlite:///{tmp_path / 'legacy.db'}")
    crm = sa.create_engine(f"sqlite:///{tmp_path / 'crm.db'}")
    demo.seed(legacy, crm)
    yield legacy, crm, _settings(str(legacy.url), str(crm.url))
    legacy.dispose()
    crm.dispose()


def test_full_load_late_parents_and_deletes(
    sqlite: tuple[sa.Engine, sa.Engine, dict[str, Any]], tmp_path: pathlib.Path
) -> None:
    legacy, crm, settings = sqlite

    def run(pipeline: str, **params: str) -> runtime.RunOutcome:
        with bootstrap.bootstrap(
            "test", directory=tmp_path / "none", overrides=settings, logs=False
        ) as application:
            return application.kernel.run(pipeline, params=params)

    first = run("crm.full_load")

    assert first.exit_code is runtime.ExitCode.PARTIAL_SUCCESS
    assert _rows(
        crm, "SELECT customer_id, country FROM customer ORDER BY 1"
    ) == [
        (1001, "VN"),
        (1002, "VN"),
    ]
    # The order of the missing customer waits in the parking lot.
    assert _rows(crm, "SELECT queue, attempts FROM etl_parked") == [
        ("order", 1)
    ]

    # The customer arrives; the next run loads it, the parked order follows.
    customers = sa.Table("CUST_MASTER", sa.MetaData(), autoload_with=legacy)
    with legacy.begin() as connection:
        connection.execute(
            customers.insert().values(
                CUST_NO="1009",
                FIRST_NM="Greta",
                LAST_NM="Weber",
                STAT_CD="A",
                CRT_DT="01/01/2024",
                CTRY="VN",
                UPDATED_AT=demo.NOW + datetime.timedelta(hours=1),
            )
        )
    # The rows at the last watermark are read again, once: the bad ones are
    # rejected again.
    assert run("crm.full_load").exit_code is runtime.ExitCode.PARTIAL_SUCCESS
    assert run("crm.parked").exit_code is runtime.ExitCode.SUCCESS
    assert _rows(crm, 'SELECT count(*) FROM "order"') == [(3,)]
    assert _rows(crm, "SELECT count(*) FROM etl_parked") == [(0,)]

    # Bruno leaves the legacy system, with his order: soft-deleted.
    orders = sa.Table("ORD_MASTER", sa.MetaData(), autoload_with=legacy)
    with legacy.begin() as connection:
        connection.execute(orders.delete().where(orders.c.CUST_NO == "1002"))
        connection.execute(
            customers.delete().where(customers.c.CUST_NO == "1002")
        )
    # A third of the rows of each entity: above the share deleted unasked.
    refused = run("crm.deletes")
    deletes = run("crm.deletes", confirm_deletes="crm.customer:1,crm.order:1")

    assert refused.exit_code is runtime.ExitCode.INVALID
    assert deletes.result is not None
    assert deletes.result.stats.deleted == 2
    assert _rows(crm, "SELECT customer_id FROM customer WHERE is_deleted") == [(
        1002,
    )]
    assert _rows(crm, 'SELECT count(*) FROM "order" WHERE is_deleted') == [(1,)]

    # The ledger tells the story.
    with bootstrap.bootstrap(
        "test", directory=tmp_path / "none", overrides=settings, logs=False
    ) as application:
        ledger = application.resources.get("etl.runs", object)
        assert [r.status.value for r in ledger.recent(None)][:3] == [  # type: ignore[attr-defined]
            "success",
            "failed",
            "success",
        ]


def _cutover(
    settings: dict[str, Any], tmp_path: pathlib.Path, *arguments: str
) -> tuple[int, str]:
    """Runs `etl cutover report crm <arguments>`: its exit code and output."""
    out = io.StringIO()
    with bootstrap.bootstrap(
        "test", directory=tmp_path / "none", overrides=settings, logs=False
    ) as application:
        args = cli_run._parser().parse_args(  # pylint: disable=protected-access
            ["cutover", "report", "crm", *arguments]
        )
        code = cli_run._command(  # pylint: disable=protected-access
            args, application, out
        )
    return code, out.getvalue()


def _accepted(tmp_path: pathlib.Path, *lines: str) -> str:
    """A CSV of the rejected rows the BAs accept; its path."""
    path = tmp_path / "accepted.csv"
    path.write_text("\n".join(["entity,reason,rows", *lines]) + "\n")
    return str(path)


def test_the_cutover_report_signs_off_or_blocks(
    sqlite: tuple[sa.Engine, sa.Engine, dict[str, Any]], tmp_path: pathlib.Path
) -> None:
    _, crm, settings = sqlite
    settings["modules"]["crm"]["settings"]["verify_loads"] = True
    with bootstrap.bootstrap(
        "test", directory=tmp_path / "none", overrides=settings, logs=False
    ) as application:
        loaded = application.kernel.run("crm.full_load")
    assert loaded.exit_code is runtime.ExitCode.PARTIAL_SUCCESS

    # Two customers cannot be loaded: until the BAs accept to leave them
    # behind, the cutover is blocked.
    code, text = _cutover(settings, tmp_path, "--format", "json")
    assert code == runtime.ExitCode.FAILED
    assert json.loads(text)["entities"][0]["unaccepted"] == 2
    code, text = _cutover(
        settings, tmp_path, "--format", "json",
        "--accept-rejections", _accepted(tmp_path, "customer,*,2"),
    )  # fmt: skip
    report = json.loads(text)
    customer, order = report["entities"]

    assert code == runtime.ExitCode.PARTIAL_SUCCESS
    assert (report["passed"], report["verdict"]) == (
        True,
        "PASSED-WITH-REJECTS",
    )
    # Two customers cannot be loaded, for reasons the BAs read.
    assert (customer["source_rows"], customer["rejected"]) == (4, 2)
    assert (customer["matched"], customer["status"]) == (
        2,
        "match-with-rejects",
    )
    assert sum(customer["rejections"].values()) == 2
    # The order of the missing customer is parked: missing, explained.
    assert (order["expected_rows"], order["matched"]) == (3, 2)
    assert (order["missing"], order["parked"]) == (1, 1)
    assert order["parked_missing"] == 1
    assert [run["pipeline"] for run in report["runs"]] == ["crm.full_load"]

    code, text = _cutover(settings, tmp_path, "--counts-only")
    assert code == runtime.ExitCode.SUCCESS
    assert "| customer | counted | 4 |" in text

    # Someone edits the target by hand: the cutover is blocked.
    with crm.begin() as connection:
        connection.exec_driver_sql(
            "UPDATE customer SET full_name = 'X' WHERE customer_id = 1001"
        )
    workbook = tmp_path / "cutover" / "crm.xlsx"
    code, text = _cutover(
        settings, tmp_path, "--format", "xlsx", "--output", str(workbook)
    )

    assert code == runtime.ExitCode.FAILED
    assert text.startswith("FAILED crm: wrote")
    sheets = openpyxl.load_workbook(workbook)
    assert sheets.sheetnames == [
        "Summary",
        "Differences",
        "Rejected",
        "Provenance",
        "Runs",
    ]
    differences = list(sheets["Differences"].iter_rows(values_only=True))
    assert differences[1][:2] == ("customer", "different")
    assert differences[1][3:] == ("full_name", "Anna Müller", "X")
    summary = list(sheets["Summary"].iter_rows(min_row=5, values_only=True))
    assert [row[:2] for row in summary] == [
        ("customer", "mismatch"),
        ("order", "match"),
    ]


def _etl(
    settings: dict[str, Any], tmp_path: pathlib.Path, *arguments: str
) -> tuple[int, str]:
    """Runs `etl <arguments>` on the settings: its exit code and output."""
    out = io.StringIO()
    with bootstrap.bootstrap(
        "test", directory=tmp_path / "none", overrides=settings, logs=False
    ) as application:
        args = cli_run._parser().parse_args(  # pylint: disable=protected-access
            list(arguments)
        )
        code = cli_run._command(  # pylint: disable=protected-access
            args, application, out
        )
    return code, out.getvalue()


def test_rejected_rows_are_fixed_and_replayed(
    sqlite: tuple[sa.Engine, sa.Engine, dict[str, Any]], tmp_path: pathlib.Path
) -> None:
    legacy, crm, settings = sqlite
    assert _etl(settings, tmp_path, "run", "crm.full_load")[0] == 3

    runs = _etl(settings, tmp_path, "runs")[1]
    assert " user=" in runs
    assert " mappings=" in runs
    code, listed = _etl(settings, tmp_path, "quarantine", "list")
    assert code == 0
    assert "customer 1003" in listed
    assert "customer 1004" in listed
    assert "does not match the format dd/MM/yyyy" in listed

    # The BAs get the rejected rows, and correct 1003 in a reviewed file.
    exported = tmp_path / "rejected.csv"
    assert _etl(
        settings, tmp_path, "quarantine", "export", "-o", str(exported)
    )[1].startswith("Wrote 2 record(s)")
    with exported.open(encoding="utf-8-sig", newline="") as file:
        rows = list(csv.DictReader(file))
    assert [r["source_key"] for r in rows] == ["1003", "1004"]
    corrections = tmp_path / "corrections.csv"
    with corrections.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, [*rows[0], "CRT_DT"])
        writer.writeheader()
        writer.writerow({**rows[0], "CRT_DT": "28/02/2020"})
    settings["modules"]["crm"]["settings"]["corrections"] = str(corrections)
    # 1004 is fixed at the source.
    with legacy.begin() as connection:
        connection.exec_driver_sql(
            'UPDATE "CUST_MASTER" SET "CTRY" = \'VN\' WHERE "CUST_NO" ='
            " '1004'"
        )

    code, text = _etl(settings, tmp_path, "run", "crm.replay")

    assert code == 0, text
    assert "2 created" in text
    assert _rows(
        crm, "SELECT customer_id, created_at FROM customer ORDER BY 1"
    ) == [
        (1001, "2020-01-15"),
        (1002, "2021-03-02"),
        (1003, "2020-02-28"),
        (1004, "2022-10-10"),
    ]
    assert _etl(settings, tmp_path, "quarantine", "list")[1] == ""
    replayed = _etl(
        settings, tmp_path, "quarantine", "list", "--status", "replayed"
    )[1]
    assert replayed.count(" replayed ") == 2
    # Nothing left to replay; the cutover agrees with the corrected target.
    assert "0 read" in _etl(settings, tmp_path, "run", "crm.replay")[1]
    code, text = _cutover(settings, tmp_path, "--format", "json")
    customer = json.loads(text)["entities"][0]
    assert code == 0
    assert (customer["rejected"], customer["matched"]) == (0, 4)


def test_operations_commands(
    sqlite: tuple[sa.Engine, sa.Engine, dict[str, Any]], tmp_path: pathlib.Path
) -> None:
    _, crm, settings = sqlite
    _etl(settings, tmp_path, "run", "crm.full_load")

    code, parked = _etl(settings, tmp_path, "parked", "list", "order")
    assert code == 0
    assert " attempts=1 " in parked
    assert "references a crm.customer that is not loaded" in parked
    identity = parked.split()[0]
    assert _etl(settings, tmp_path, "parked", "list", "crm.order") == (
        exit_codes.ExitCode.NOT_FOUND,
        "crm.order: no such queue (the mappings park to: customer, order)\n",
    )

    state = _etl(
        settings,
        tmp_path,
        "state",
        "show",
        "crm.full_load",
        "full_load:customer",
    )[1]
    assert "2026-10-01" in state
    assert _etl(
        settings,
        tmp_path,
        "state",
        "reset",
        "crm.full_load",
        "full_load:customer",
        "--yes",
    )[1].startswith("Reset crm.full_load full_load:customer")
    assert (
        _etl(
            settings,
            tmp_path,
            "state",
            "show",
            "crm.full_load",
            "full_load:customer",
        )[1].strip()
        == "null"
    )

    (key,) = _rows(
        crm, "SELECT customer_key FROM customer WHERE customer_id = 1001"
    )[0]
    assert (
        _etl(settings, tmp_path, "keys", "lookup", "crm.customer", str(key))[1]
        == f"crm.customer {key}: ['1001']\n"
    )
    assert (
        _etl(settings, tmp_path, "keys", "lookup", "crm.customer", "nope")[0]
        == exit_codes.ExitCode.NOT_FOUND
    )

    ids = [
        line.split()[0]
        for line in _etl(settings, tmp_path, "quarantine", "list")[
            1
        ].splitlines()
    ]
    assert _etl(settings, tmp_path, "quarantine", "close", ids[0])[1] == (
        "Closed 1 record(s)\n"
    )
    assert _etl(settings, tmp_path, "housekeeping")[1].startswith(
        "Purged 0 quarantined record(s)"
    )
    assert (
        _etl(settings, tmp_path, "parked", "release", "order", identity)[1]
        == "Released 1 record(s)\n"
    )
    assert _etl(settings, tmp_path, "parked", "list", "order")[1] == ""


def test_a_full_load_split_in_parallel_ranges(
    sqlite: tuple[sa.Engine, sa.Engine, dict[str, Any]], tmp_path: pathlib.Path
) -> None:
    _, crm, settings = sqlite
    settings["modules"]["crm"]["settings"]["partitions"] = 2
    # Both bad customers fall in one range: its batch rejects all it reads.
    settings["modules"]["crm"]["settings"]["max_reject_rate"] = 1.0
    with bootstrap.bootstrap(
        "test", directory=tmp_path / "none", overrides=settings, logs=False
    ) as application:
        plan = application.kernel.plan("crm.full_load")
        outcome = application.kernel.run("crm.full_load")

    assert [(s.name, s.dependencies) for s in plan] == [
        ("customer:ranges", ()),
        ("order:ranges", ()),
        ("customer:0", ("customer:ranges",)),
        ("customer:1", ("customer:ranges",)),
        ("order:0", ("customer:0", "customer:1", "order:ranges")),
        ("order:1", ("customer:0", "customer:1", "order:ranges")),
    ]
    assert outcome.exit_code is runtime.ExitCode.PARTIAL_SUCCESS
    assert _rows(crm, "SELECT count(*) FROM customer") == [(2,)]
    assert _rows(crm, 'SELECT count(*) FROM "order"') == [(2,)]


def test_uat_gets_fake_names_and_still_reconciles(
    sqlite: tuple[sa.Engine, sa.Engine, dict[str, Any]], tmp_path: pathlib.Path
) -> None:
    _, crm, settings = sqlite
    settings["privacy"] = {"anonymize": True, "secret": "uat-secret"}

    assert _etl(settings, tmp_path, "run", "crm.full_load")[0] == 3
    names = _rows(crm, "SELECT full_name FROM customer ORDER BY customer_id")
    code, text = _cutover(settings, tmp_path, "--format", "json")

    assert ("Anna Müller",) not in names
    assert ("Bruno Costa",) not in names
    assert len(set(names)) == 2
    assert code == runtime.ExitCode.FAILED  # Rejected rows, not accepted.
    assert json.loads(text)["entities"][0]["matched"] == 2
    assert "anonymized=yes" in _etl(settings, tmp_path, "runs")[1]


def test_a_right_to_erasure(
    sqlite: tuple[sa.Engine, sa.Engine, dict[str, Any]], tmp_path: pathlib.Path
) -> None:
    _, _, settings = sqlite
    _etl(settings, tmp_path, "run", "crm.full_load")

    # 1001 was loaded: its key goes. 1003 was rejected: its record goes,
    # and its issue.
    loaded = _etl(settings, tmp_path, "keys", "forget", "crm.customer", "1001")
    rejected = _etl(
        settings, tmp_path, "keys", "forget", "crm.customer", "1003",
        "--case", "upper",
    )  # fmt: skip
    unknown = _etl(settings, tmp_path, "keys", "forget", "crm.customer", "9")

    assert loaded == (
        0,
        (
            "crm.customer: 1 key(s), 0 quarantined and 0 parked record(s),"
            " 0 issue(s) erased\n"
        ),
    )
    assert rejected[1] == (
        "crm.customer: 0 key(s), 1 quarantined and 0 parked record(s),"
        " 1 issue(s) erased\n"
    )
    assert unknown[0] == exit_codes.ExitCode.NOT_FOUND
    listed = _etl(settings, tmp_path, "quarantine", "list")[1]
    assert " 1003 " not in listed
    assert " 1004 " in listed
    audit = _etl(settings, tmp_path, "runs", "etl.erasure")[1]
    assert audit.count("etl.erasure success") == 3
    assert re.search(r"\b1001\b", audit) is None  # Never keeps the key.


def test_rehearsals_are_saved_sealed_and_compared(
    sqlite: tuple[sa.Engine, sa.Engine, dict[str, Any]], tmp_path: pathlib.Path
) -> None:
    legacy, _, settings = sqlite
    saved = tmp_path / "rehearsals"
    _etl(settings, tmp_path, "run", "crm.full_load")
    first = _etl(
        settings, tmp_path, "cutover", "report", "crm", "--save", str(saved)
    )[1]
    # The legacy data is fixed at the source before the second rehearsal.
    with legacy.begin() as connection:
        connection.exec_driver_sql(
            'UPDATE "CUST_MASTER" SET "CTRY" = \'VN\' WHERE "CUST_NO" ='
            " '1004'"
        )
    _etl(settings, tmp_path, "run", "crm.replay")
    page = tmp_path / "report.html"
    _etl(
        settings, tmp_path, "cutover", "report", "crm", "--save", str(saved),
        "--format", "html", "--output", str(page),
    )  # fmt: skip
    # Saved in the same second maybe: the second has more matches.
    before, after = sorted(
        (saved / "crm").glob("*.json"),
        key=lambda p: '"rejected": 1' in p.read_text(),
    )

    compared = cli_run.main(
        ["cutover", "compare", str(before), str(after)], out := io.StringIO()
    )
    verified = _etl(
        settings, tmp_path, "cutover", "verify", str(before), str(after)
    )
    before.write_text(
        before.read_text().replace('"rejected": 2', '"rejected": 0')
    )
    changed = _etl(settings, tmp_path, "cutover", "verify", str(before))

    assert first.startswith(f"Saved {saved / 'crm'}")
    assert compared == runtime.ExitCode.SUCCESS
    assert (
        "| customer | mismatch | rejected -1, unaccepted -1, expected_rows"
        " +1, target_rows +1, matched +1 |"
    ) in out.getvalue()
    assert verified[0] == runtime.ExitCode.SUCCESS, verified[1]
    assert changed[0] == runtime.ExitCode.FAILED
    assert changed[1].startswith("CHANGED")
    assert "SHA-256" in page.read_text(encoding="utf-8")


def test_a_lookup_table_read_from_the_source(
    sqlite: tuple[sa.Engine, sa.Engine, dict[str, Any]], tmp_path: pathlib.Path
) -> None:
    legacy, crm, settings = sqlite
    with legacy.begin() as connection:
        connection.exec_driver_sql(
            'CREATE TABLE "REF_COUNTRY" ("CODE" VARCHAR(20) PRIMARY KEY,'
            ' "ISO2" VARCHAR(2))'
        )
        connection.exec_driver_sql(
            "INSERT INTO \"REF_COUNTRY\" VALUES ('VIETNAM', 'VN'), ('VN',"
            " 'VN'), ('Narnia', 'NR')"
        )
    settings["modules"]["crm"]["settings"]["lookups"] = {
        "countries": {"table": "REF_COUNTRY", "key": "CODE", "value": "ISO2"}
    }

    assert _etl(settings, tmp_path, "run", "crm.full_load")[0] == 3
    code, text = _cutover(settings, tmp_path, "--format", "json")

    # Narnia is known now: only the customer born on 31/02 is rejected.
    assert _rows(
        crm, "SELECT customer_id, country FROM customer ORDER BY 1"
    ) == [
        (1001, "VN"),
        (1002, "VN"),
        (1004, "NR"),
    ]
    assert code == runtime.ExitCode.FAILED  # Rejected, not accepted.
    assert json.loads(text)["entities"][0]["rejected"] == 1


def test_sync_needs_a_change_log(
    sqlite: tuple[sa.Engine, sa.Engine, dict[str, Any]], tmp_path: pathlib.Path
) -> None:
    _, _, settings = sqlite
    with (
        bootstrap.bootstrap(
            "test", directory=tmp_path / "none", overrides=settings, logs=False
        ) as application,
        # A pipeline that cannot be built is invalid (exit 2), not a failed
        # run: nothing is recorded.
        pytest.raises(
            contract.PipelineConfigError, match="No change log read for sqlite"
        ),
    ):
        application.kernel.run("crm.sync")


def test_sync_by_logical_replication_on_postgres(
    tmp_path: pathlib.Path,
) -> None:
    url = os.environ.get("ETL_TEST_POSTGRES_URL")
    if not url:
        pytest.skip("set ETL_TEST_POSTGRES_URL to run the Postgres test")
    schema = f"crm_{uuid.uuid4().hex[:8]}"
    admin = sa.create_engine(url)
    with admin.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA "{schema}"')
    scoped = sa.make_url(url).update_query_dict(
        {"options": f"-csearch_path={schema}"}
    )
    engine = sa.create_engine(scoped)
    prefix = f"{schema}_"
    try:
        demo.seed(engine, engine)
        settings = _settings(
            scoped.render_as_string(False),
            scoped.render_as_string(False),
            deletes="soft",
            verify_loads=True,
        )
        settings["databases"]["legacy"]["replication_prefix"] = prefix
        db = relational.Database(engine)
        for table in ("CUST_MASTER", "ORD_MASTER"):
            name = prefix + table.lower()
            relational.create_replication(
                db, table, slot=name, publication=name
            )

        def sync() -> runtime.RunOutcome:
            with bootstrap.bootstrap(
                "test",
                directory=tmp_path / "none",
                overrides=settings,
                logs=False,
            ) as application:
                return application.kernel.run("crm.sync")

        first = sync()
        assert first.result is not None
        assert first.result.stats.created == 4  # 2 customers, 2 orders.

        customers = sa.Table("CUST_MASTER", sa.MetaData(), autoload_with=engine)
        orders = sa.Table("ORD_MASTER", sa.MetaData(), autoload_with=engine)
        with engine.begin() as connection:
            connection.execute(
                customers.update()
                .where(customers.c.CUST_NO == "1001")
                .values(FIRST_NM="Annie")
            )
            connection.execute(orders.delete().where(orders.c.ORD_NO == "O-2"))
        second = sync()

        assert second.result is not None
        stats = second.result.stats
        assert (stats.updated, stats.deleted) == (1, 1)
        # Postgres reads back uuid and numeric: they reconcile all the same.
        code, text = _cutover(
            settings, tmp_path, "--format", "json",
            "--accept-rejections", _accepted(tmp_path, "customer,*,2"),
        )  # fmt: skip
        entities = {e["entity"]: e for e in json.loads(text)["entities"]}
        assert code == runtime.ExitCode.PARTIAL_SUCCESS, text
        assert entities["customer"]["matched"] == 2
        assert entities["order"]["matched"] == 1  # O-2 is gone.
        assert _rows(
            engine, "SELECT full_name FROM customer WHERE customer_id = 1001"
        ) == [("Annie Müller",)]
        assert _rows(
            engine, 'SELECT count(*) FROM "order" WHERE is_deleted'
        ) == [(1,)]
    finally:
        for table in ("CUST_MASTER", "ORD_MASTER"):
            name = prefix + table.lower()
            relational.drop_replication(
                relational.Database(engine), slot=name, publication=name
            )
        engine.dispose()
        with admin.begin() as connection:
            connection.exec_driver_sql(f'DROP SCHEMA "{schema}" CASCADE')
        admin.dispose()
