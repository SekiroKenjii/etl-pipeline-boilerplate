"""Runs the installed `etl` command as a process, as a scheduler does."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
import contextlib
import dataclasses
import datetime
import json
import os
import pathlib
import subprocess
import sys
import time
from typing import Any

from sqlalchemy import exc
import sqlalchemy as sa

from modules.crm import demo

ROOT = pathlib.Path(__file__).resolve().parents[2]
ETL = pathlib.Path(sys.executable).with_name("etl")


@dataclasses.dataclass(frozen=True)
class Project:
    """The settings of an end-to-end test: dev, its databases overridden."""

    legacy: str
    crm: str
    extra: Mapping[str, str] = dataclasses.field(default_factory=dict)

    def env(self, **more: str) -> dict[str, str]:
        """The process environment: `ETL__*` variables over the dev files."""
        return {
            **os.environ,
            "ETL_ENV": "dev",
            "ETL__DATABASES__LEGACY__URL": self.legacy,
            "ETL__DATABASES__CRM__URL": self.crm,
            **self.extra,
            **more,
        }

    def etl(
        self, *arguments: str, timeout: float = 300, **env: str
    ) -> subprocess.CompletedProcess[str]:
        """Runs `etl <arguments>` to its end."""
        return subprocess.run(
            [str(ETL), *arguments],
            cwd=ROOT,
            env=self.env(**env),
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )

    def start(self, *arguments: str, **env: str) -> subprocess.Popen[str]:
        """Starts `etl <arguments>`, without waiting."""
        return subprocess.Popen(
            [str(ETL), *arguments],
            cwd=ROOT,
            env=self.env(**env),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

    def runs(self, pipeline: str) -> list[dict[str, Any]]:
        """The last runs of a pipeline, newest first (`etl runs --format
        json`)."""
        shown = self.etl("runs", pipeline, "--format", "json")
        assert shown.returncode == 0, shown.stdout + shown.stderr
        runs: list[dict[str, Any]] = json.loads(shown.stdout)
        return runs

    def run_of(self, run_id: str) -> dict[str, Any]:
        """A run and its steps, by name (`etl runs --run --format json`)."""
        shown = self.etl("runs", "--run", run_id, "--format", "json")
        assert shown.returncode == 0, shown.stdout + shown.stderr
        run: dict[str, Any] = json.loads(shown.stdout)
        run["steps"] = {step["step"]: step for step in run["steps"]}
        return run

    def seed(self) -> None:
        """Seeds the demo databases, as `python -m modules.crm.demo` does."""
        subprocess.run(
            [sys.executable, "-m", "modules.crm.demo", "--env", "dev"],
            cwd=ROOT,
            env=self.env(),
            check=True,
            capture_output=True,
        )


def add_customers(url: str, count: int) -> None:
    """Adds valid customers to the legacy table, after the demo's."""
    engine = sa.create_engine(url)
    try:
        table = sa.Table("CUST_MASTER", sa.MetaData(), autoload_with=engine)
        rows = [
            {
                "CUST_NO": str(100_000 + i),
                "FIRST_NM": "Test",
                "LAST_NM": f"Customer {i}",
                "STAT_CD": "A",
                "CRT_DT": "01/01/2024",
                "CTRY": "VN",
                "LEGACY_FLAG": None,
                "UPDATED_AT": demo.NOW + datetime.timedelta(seconds=i),
            }
            for i in range(count)
        ]
        with engine.begin() as connection:
            for start in range(0, count, 10_000):
                connection.execute(table.insert(), rows[start : start + 10_000])
    finally:
        engine.dispose()


def count(url: str, sql: str) -> int:
    """Counts with a query; 0 while the table does not exist yet."""
    engine = sa.create_engine(url)
    try:
        with engine.connect() as connection:
            return int(connection.exec_driver_sql(sql).scalar_one())
    except exc.SQLAlchemyError:
        return 0
    finally:
        engine.dispose()


def outcome(output: str) -> dict[str, Any]:
    """How `etl run --format json` ended: its last line, after the logs."""
    last = output.rstrip().rpartition("\n")[2]
    found: dict[str, Any] = json.loads(last)
    return found


def wait_for(
    probe: Callable[[], bool], timeout: float = 60, what: str = "a condition"
) -> None:
    """Waits until `probe()` is true, checking every 50 ms."""
    deadline = time.monotonic() + timeout
    while not probe():
        if time.monotonic() > deadline:
            raise TimeoutError(f"timed out after {timeout}s waiting for {what}")
        time.sleep(0.05)


def leases(url: str, pipeline: str) -> dict[str, float]:
    """The locks of the steps of a pipeline in the bookkeeping database,
    `etl_locks`: when each lease ends (`time.time()`). Empty while the
    table does not exist yet."""
    engine = sa.create_engine(url)
    try:
        with engine.connect() as connection:
            rows = connection.execute(
                sa.text(
                    "SELECT name, expires_at FROM etl_locks"
                    " WHERE name LIKE :steps"
                ),
                {"steps": f"{pipeline}/%"},
            ).all()
    except exc.SQLAlchemyError:
        return {}
    finally:
        engine.dispose()
    return {str(name): float(expires) for name, expires in rows}


@contextlib.contextmanager
def leases_kept(url: str, pipeline: str) -> Iterator[dict[str, float]]:
    """Keeps the leases of the steps of a pipeline running, however slow
    the machine, then gives them back the end they had.

    Yields:
        That end, of each lease.
    """
    ends = leases(url, pipeline)
    assert ends, f"no lock of {pipeline}"
    _end_leases(url, {name: time.time() + 3600 for name in ends})
    try:
        yield ends
    finally:
        _end_leases(url, ends)


def _end_leases(url: str, ends: Mapping[str, float]) -> None:
    """Sets when the leases end, by lock."""
    engine = sa.create_engine(url)
    try:
        with engine.begin() as connection:
            for name, end in ends.items():
                connection.execute(
                    sa.text(
                        "UPDATE etl_locks SET expires_at = :end"
                        " WHERE name = :name"
                    ),
                    {"name": name, "end": end},
                )
    finally:
        engine.dispose()


def expired(url: str, pipeline: str) -> bool:
    """Whether no lease of the steps of a pipeline is running."""
    return all(end <= time.time() for end in leases(url, pipeline).values())
