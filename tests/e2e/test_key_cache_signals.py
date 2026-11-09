"""`etl run` stopped by a signal while its key cache spilled to disk: the
keys on disk, natural keys from the source, go with the process."""

from __future__ import annotations

import os
import pathlib
import signal

import pytest

from tests.e2e import harness

pytestmark = pytest.mark.slow

PIPELINE = "crm.full_load"
# Enough rows for a run of several seconds on SQLite.
CUSTOMERS = 30_000


@pytest.fixture
def project(tmp_path: pathlib.Path) -> harness.Project:
    project = harness.Project(
        legacy=f"sqlite:///{tmp_path / 'legacy.db'}",
        crm=f"sqlite:///{tmp_path / 'crm.db'}",
        extra={
            "ETL__BOOKKEEPING__DIRECTORY": str(tmp_path / "var"),
            # Memory for a handful of keys: every batch spills to disk.
            "ETL__TUNING__KEY_CACHE_MEMORY": "4KiB",
        },
    )
    project.seed()
    harness.add_customers(project.legacy, CUSTOMERS)
    return project


@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT])
def test_a_run_stopped_mid_way_leaves_no_keys_on_disk(
    project: harness.Project, tmp_path: pathlib.Path, signum: signal.Signals
) -> None:
    temporary = tmp_path / "tmp"
    temporary.mkdir()
    with project.start(
        "run", PIPELINE, "--partial-ok", TMPDIR=str(temporary)
    ) as process:
        try:
            harness.wait_for(
                lambda: any(temporary.glob("etl-keys-*")),
                timeout=120,
                what="the key cache to spill",
            )
            spilled = next(temporary.glob("etl-keys-*"))
            assert (spilled / "keys.sqlite3").exists()
            os.kill(process.pid, signum)
            output, _ = process.communicate(timeout=120)
        finally:
            if process.poll() is None:
                process.kill()

    assert process.returncode == 130, output
    assert not spilled.exists()
    assert list(temporary.iterdir()) == []
