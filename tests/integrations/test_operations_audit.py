"""Operations that change what the ETL saved are asked for and recorded;
`etl check` and `etl config show` tell what they can without side effects."""

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
from cli import run as cli_run
from modules.crm import demo
from tests.integrations import test_crm_module as crm_module

STATE = ("state", "reset", "crm.full_load", "full_load:customer")


@pytest.fixture
def project(
    tmp_path: pathlib.Path,
) -> Generator[tuple[sa.Engine, dict[str, Any]]]:
    legacy = sa.create_engine(f"sqlite:///{tmp_path / 'legacy.db'}")
    crm = sa.create_engine(f"sqlite:///{tmp_path / 'crm.db'}")
    demo.seed(legacy, crm)
    settings = crm_module._settings(  # pylint: disable=protected-access
        str(legacy.url), str(crm.url)
    )
    yield crm, settings
    legacy.dispose()
    crm.dispose()


def _etl(
    settings: dict[str, Any], tmp_path: pathlib.Path, *arguments: str
) -> tuple[int, str]:
    return crm_module._etl(  # pylint: disable=protected-access
        settings, tmp_path, *arguments
    )


def _audit(crm: sa.Engine) -> list[tuple[Any, ...]]:
    with crm.connect() as connection:
        return [
            tuple(row)
            for row in connection.exec_driver_sql(
                "SELECT meta, message FROM etl_runs"
                " WHERE pipeline = 'etl.state-reset'"
            )
        ]


def test_a_state_reset_asks_and_is_recorded(
    project: tuple[sa.Engine, dict[str, Any]],
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    crm, settings = project
    _etl(settings, tmp_path, "run", "crm.full_load")
    monkeypatch.setenv("ETL_OPERATOR", "ops-oncall")

    monkeypatch.setattr(sys, "stdin", io.StringIO("n\n"))
    declined = _etl(settings, tmp_path, *STATE)
    monkeypatch.setattr(sys, "stdin", io.StringIO(""))
    closed = _etl(settings, tmp_path, *STATE)
    kept = _etl(settings, tmp_path, "state", "show", *STATE[2:])[1]
    monkeypatch.setattr(sys, "stdin", io.StringIO("y\n"))
    agreed = _etl(settings, tmp_path, *STATE)
    again = _etl(settings, tmp_path, *STATE, "--yes")

    assert declined[0] == closed[0] == exit_codes.ExitCode.INVALID
    assert "Its next run starts over [y/N]" in declined[1]
    assert "2026-10-01" in kept
    assert agreed[0] == exit_codes.ExitCode.SUCCESS
    assert "Reset crm.full_load full_load:customer (was" in agreed[1]
    assert again[0] == exit_codes.ExitCode.NOT_FOUND  # Nothing saved now.
    ((meta, message),) = _audit(crm)
    assert json.loads(meta)["user"] == "ops-oncall"
    assert json.loads(meta)["key"] == "full_load:customer"
    assert message == "reset crm.full_load full_load:customer"


def test_check_is_not_ready_when_a_store_or_endpoint_is_down(
    project: tuple[sa.Engine, dict[str, Any]], tmp_path: pathlib.Path
) -> None:
    _, settings = project
    assert _etl(settings, tmp_path, "check")[0] == exit_codes.ExitCode.SUCCESS
    settings["stores"] = {
        "landing": {"kind": "local", "path": str(tmp_path / "missing")}
    }
    # Port 9 (discard) of this host: nothing listens.
    settings["lineage"] = {
        "endpoint": "http://127.0.0.1:9",
        "timeout_seconds": 1,
    }

    code, text = _etl(settings, tmp_path, "check")

    assert code == exit_codes.ExitCode.NOT_READY
    assert "FAIL store.landing: FileNotFoundError" in text
    assert "FAIL endpoint.lineage: ConnectionRefusedError" in text
    assert "ok   database.crm" in text


def test_config_show_reaches_no_secret_manager(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("VAULT_ADDR", raising=False)
    (tmp_path / "test.toml").write_text(
        "[databases.crm]\n"
        'url = "postgresql://etl:${vault:secret/crm/db#password}@db/crm"\n'
        "[lineage]\n"
        'endpoint = "http://marquez:5000"\n'
        'api_key = "${gcp:projects/acme/secrets/marquez/versions/1}"\n',
        encoding="utf-8",
    )
    out = io.StringIO()

    code = cli_run.main(
        ["--env", "test", "--config", str(tmp_path), "config", "show"], out
    )

    assert code == exit_codes.ExitCode.SUCCESS, out.getvalue()
    # Before: FAILED, VAULT_ADDR is not set. Secrets are hidden anyway.
    shown = json.loads(out.getvalue())
    assert shown["databases"]["crm"]["url"] == "**********"
    assert shown["lineage"]["endpoint"] == "http://marquez:5000"
    assert "acme" not in out.getvalue()
