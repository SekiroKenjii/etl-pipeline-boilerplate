"""`etl cutover report` stopped by a signal mid-diff: the rows it spilled to
disk, source data, go with it."""

from __future__ import annotations

from collections.abc import Iterator
import contextlib
import os
import pathlib
import signal
import subprocess
import sys

import pytest
import sqlalchemy as sa

from tests.e2e import harness

pytestmark = pytest.mark.slow

# `etl`, its diff spilling every row and stalling after its first file: a
# report of the demo's few rows stopped half-way.
_STALLING = """
import os, pathlib, sys, time
from cli import run
from shared.utils import external_sort

spilled = pathlib.Path(os.environ["SPILLED"])
sorter = external_sort.ExternalSorter
init, write = sorter.__init__, sorter._write

def spill_every_row(self, key, **options):
    init(self, key, **{**options, "spill_rows": 1})

def stall(self, items):
    path = write(self, items)
    spilled.write_text(str(self.spill_directory))
    time.sleep(120)
    return path

sorter.__init__ = spill_every_row
sorter._write = stall
sys.exit(run.main(sys.argv[1:]))
"""


@pytest.fixture
def project(tmp_path: pathlib.Path) -> harness.Project:
    crm = f"sqlite:///{tmp_path / 'crm.db'}"
    project = harness.Project(
        legacy=f"sqlite:///{tmp_path / 'legacy.db'}",
        crm=crm,
        extra={"ETL__BOOKKEEPING__DIRECTORY": str(tmp_path / "var")},
    )
    project.seed()
    loaded = project.etl("run", "crm.full_load", "--partial-ok")
    assert loaded.returncode == 0, loaded.stdout + loaded.stderr
    # Every customer missing: every partition of the diff differs.
    engine = sa.create_engine(crm)
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql("DELETE FROM customer")
    finally:
        engine.dispose()
    return project


@contextlib.contextmanager
def _stalled_report(
    project: harness.Project, tmp_path: pathlib.Path
) -> Iterator[tuple[subprocess.Popen[str], pathlib.Path, pathlib.Path]]:
    """Starts a report, and waits for its first spilled file.

    Yields:
        The process, its temporary directory, and the spill directory.
    """
    temporary = tmp_path / "tmp"
    temporary.mkdir()
    spilled = tmp_path / "spilled"
    with subprocess.Popen(
        [sys.executable, "-c", _STALLING, "cutover", "report", "crm"],
        cwd=harness.ROOT,
        env=project.env(TMPDIR=str(temporary), SPILLED=str(spilled)),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    ) as process:
        try:
            harness.wait_for(
                spilled.exists, timeout=120, what="the first spill"
            )
            directory = pathlib.Path(spilled.read_text())
            assert directory.parent == temporary
            assert list(directory.iterdir())
            yield process, temporary, directory
        finally:
            if process.poll() is None:
                process.kill()


@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT])
def test_a_report_stopped_mid_diff_leaves_no_rows_on_disk(
    project: harness.Project, tmp_path: pathlib.Path, signum: signal.Signals
) -> None:
    with _stalled_report(project, tmp_path) as (process, temporary, spill):
        os.kill(process.pid, signum)
        output, _ = process.communicate(timeout=60)

    assert not spill.exists()
    assert not list(temporary.iterdir())
    assert process.returncode == 130, output


def test_a_second_signal_does_not_cut_the_cleanup_short(
    project: harness.Project, tmp_path: pathlib.Path
) -> None:
    with _stalled_report(project, tmp_path) as (process, temporary, spill):
        os.kill(process.pid, signal.SIGTERM)
        os.kill(process.pid, signal.SIGTERM)
        output, _ = process.communicate(timeout=60)

    assert not spill.exists()
    assert not list(temporary.iterdir())
    assert process.returncode == 130, output
