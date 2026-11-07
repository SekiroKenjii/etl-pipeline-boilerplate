"""A right to erasure and housekeeping, on the CRM module, end to end.

`etl keys forget` erases what the ETL keeps of a natural key: its keys, its
quarantined and parked records, those of the records referencing it, and
their quality issues; `etl housekeeping` purges them past their retention.
"""

from __future__ import annotations

from collections.abc import Generator
import io
import json
import pathlib
import sys
from typing import Any

import pytest
import sqlalchemy as sa

from cli import exit_codes
from modules.crm import demo
from tests.integrations import test_crm_module as crm_module


@pytest.fixture
def project(
    tmp_path: pathlib.Path,
) -> Generator[tuple[sa.Engine, dict[str, Any]]]:
    """The demo, with an order of customer 1003, whose row is rejected: the
    order is parked, waiting for it."""
    legacy = sa.create_engine(f"sqlite:///{tmp_path / 'legacy.db'}")
    crm = sa.create_engine(f"sqlite:///{tmp_path / 'crm.db'}")
    demo.seed(legacy, crm)
    with legacy.begin() as connection:
        connection.exec_driver_sql(
            'INSERT INTO "ORD_MASTER" VALUES'
            " ('O-4', '1003', '42', '05/02/2024', '2026-10-01 08:00:00')"
        )
    settings = crm_module._settings(  # pylint: disable=protected-access
        str(legacy.url), str(crm.url)
    )
    assert _etl(settings, tmp_path, "run", "crm.full_load")[0] == 3
    yield crm, settings
    legacy.dispose()
    crm.dispose()


def _etl(
    settings: dict[str, Any], tmp_path: pathlib.Path, *arguments: str
) -> tuple[int, str]:
    return crm_module._etl(  # pylint: disable=protected-access
        settings, tmp_path, *arguments
    )


def _rows(engine: sa.Engine, sql: str) -> list[tuple[Any, ...]]:
    with engine.connect() as connection:
        return [tuple(row) for row in connection.exec_driver_sql(sql)]


def test_the_documented_erasure_erases_the_records_referencing_the_key(
    project: tuple[sa.Engine, dict[str, Any]], tmp_path: pathlib.Path
) -> None:
    crm, settings = project
    parked = "SELECT payload FROM etl_parked"
    assert sorted(
        json.loads(row[0])["source_key"] for row in _rows(crm, parked)
    ) == ["O-3", "O-4"]
    issues = "SELECT record_id FROM etl_quality_issues ORDER BY 1"
    assert {"1003", "O-4"} <= {row[0] for row in _rows(crm, issues)}

    # As docs/security.md says; the key case is read from the mapping.
    code, text = _etl(
        settings, tmp_path, "keys", "forget", "crm.customer", "1003",
        "--queue", "order",
    )  # fmt: skip

    assert code == exit_codes.ExitCode.SUCCESS
    assert text == (
        "crm.customer: 0 key(s), 1 quarantined and 1 parked record(s),"
        " 2 issue(s) erased\n"
    )
    assert [  # Waiting for customer 1009.
        json.loads(row[0])["source_key"] for row in _rows(crm, parked)
    ] == ["O-3"]
    assert {row[0] for row in _rows(crm, issues)} == {"1004", "O-3"}
    listed = _etl(settings, tmp_path, "quarantine", "list")[1]
    assert " 1003 " not in listed
    assert " 1004 " in listed


def test_the_queues_come_from_the_catalog_and_the_key_from_stdin(
    project: tuple[sa.Engine, dict[str, Any]],
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    crm, settings = project
    monkeypatch.setenv("ETL_OPERATOR", "jane.doe (ticket DPO-17)")
    monkeypatch.setattr(sys, "stdin", io.StringIO("1003\n"))

    code, text = _etl(
        settings, tmp_path, "keys", "forget", "crm.customer", "--key-file", "-"
    )

    assert code == exit_codes.ExitCode.SUCCESS
    assert "1 quarantined and 1 parked record(s), 2 issue(s)" in text
    audit = _rows(
        crm, "SELECT meta, message FROM etl_runs WHERE pipeline = 'etl.erasure'"
    )
    assert len(audit) == 1
    assert "jane.doe (ticket DPO-17)" in audit[0][0]
    assert "1003" not in str(audit)  # The natural key is never kept.


def test_a_key_is_given_once(
    project: tuple[sa.Engine, dict[str, Any]], tmp_path: pathlib.Path
) -> None:
    _, settings = project
    key_file = tmp_path / "key.txt"
    key_file.write_text("1003\n", encoding="utf-8")

    both = _etl(
        settings, tmp_path, "keys", "forget", "crm.customer", "1003",
        "--key-file", str(key_file),
    )  # fmt: skip
    neither = _etl(settings, tmp_path, "keys", "forget", "crm.customer")
    from_file = _etl(
        settings, tmp_path, "keys", "forget", "crm.customer",
        "--key-file", str(key_file),
    )  # fmt: skip

    assert both[0] == neither[0] == exit_codes.ExitCode.INVALID
    assert from_file[0] == exit_codes.ExitCode.SUCCESS


def test_housekeeping_purges_issues_and_parked_records_too(
    project: tuple[sa.Engine, dict[str, Any]], tmp_path: pathlib.Path
) -> None:
    crm, settings = project
    kept = _etl(settings, tmp_path, "housekeeping")[1]
    assert "Purged 0 quality issue(s)" in kept
    assert "Purged 0 parked record(s)" in kept

    # Past their retention: everything older than now.
    code, text = _etl(settings, tmp_path, "housekeeping", "--days", "0")

    assert code == exit_codes.ExitCode.SUCCESS
    assert "Purged 2 quarantined record(s)" in text
    assert "Purged 2 parked record(s)" in text
    assert _rows(crm, "SELECT count(*) FROM etl_quality_issues") == [(0,)]
    assert _rows(crm, "SELECT count(*) FROM etl_parked") == [(0,)]
    assert _rows(crm, "SELECT count(*) FROM etl_quarantine") == [(0,)]
