"""`etl calibrate` measures every database on a scratch table it drops, and
recommends a `[tuning]` snippet."""

from __future__ import annotations

import io
import json
import pathlib

import sqlalchemy as sa

from cli import calibrate
from cli import run
from infra.database.relational import staging
from tests.integrations.databases import conftest


def _tables(backend: conftest.Backend) -> set[str]:
    return {
        name
        for name in sa.inspect(backend.db.engine).get_table_names()
        if name.startswith("etl_calibrate_")
    }


def test_the_measurements_leave_nothing_behind(
    backend: conftest.Backend,
) -> None:
    found = calibrate.calibrate(backend.db, "crm", batch_size=500, repeats=1)

    # Other test workers calibrate at the same time: only this run's
    # scratch tables are checked.
    assert found.scratch_prefix.startswith("etl_calibrate_")
    assert not {
        name
        for name in _tables(backend)
        if name.startswith(found.scratch_prefix)
    }
    assert found.dialect == backend.name
    assert found.batch_size == 500
    assert found.staging[staging.PARAMETERS] > 0
    assert set(found.in_lists) >= {250, 500}
    assert found.in_chunk in found.in_lists
    assert set(found.key_registry) == {"new", "known"}
    assert all(
        seconds > 0
        for by in found.key_registry.values()
        for seconds in by.values()
    )
    if backend.name == "mssql":
        assert found.staging[staging.BULK_COPY] > 0
        assert found.bulk_copy_rows in (500, "auto")
        assert set(found.key_registry["new"]) == {
            staging.BULK_COPY,
            staging.PARAMETERS,
        }
    elif backend.name == "postgresql":
        assert found.staging[staging.BULK_COPY] > 0
        assert found.bulk_copy_rows is None
    else:
        assert staging.BULK_COPY not in found.staging
        assert found.bulk_copy_rows is None


def test_the_command_prints_a_snippet_and_json(
    backend: conftest.Backend, tmp_path: pathlib.Path
) -> None:
    (tmp_path / "test.toml").write_text(
        f'[databases.crm]\nurl = "{backend.url}"\n'
        f'[bookkeeping]\ndirectory = "{tmp_path / "var"}"\n',
        encoding="utf-8",
    )
    text, data = io.StringIO(), io.StringIO()

    code = run.main(
        ["--env", "test", "--config", str(tmp_path), "calibrate", "crm"], text
    )
    json_code = run.main(
        [
            "--env", "test", "--config", str(tmp_path),
            "calibrate", "crm", "--format", "json", "--batch-size", "200",
        ],
        data,
    )  # fmt: skip
    unknown = run.main(
        ["--env", "test", "--config", str(tmp_path), "calibrate", "nope"],
        io.StringIO(),
    )

    assert (code, json_code, unknown) == (0, 0, 2)
    snippet = text.getvalue()
    assert snippet.startswith(f"# etl calibrate crm: {backend.name} ")
    assert "\n[tuning]\n" in snippet
    assert "in_chunk = " in snippet
    assert "# staging 1000 rows: parameters" in snippet
    if backend.name == "mssql":
        assert "bulk copy" in snippet
    found = json.loads(data.getvalue())
    assert found["database"] == "crm"
    assert found["batch_size"] == 200
    assert found["recommended"]["in_chunk"] in (250, 500, 1000)
    assert _tables(backend) == set()
