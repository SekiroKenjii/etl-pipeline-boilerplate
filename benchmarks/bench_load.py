"""Throughput of `crm.full_load`, source to target: rows per second.

    uv run python benchmarks/bench_load.py --rows 1000000 \\
        --source postgresql+psycopg://etl:etl@localhost:55432/bench_legacy \\
        --target postgresql+psycopg://etl:etl@localhost:55432/bench_crm \\
        --partitions 1,4

The pipeline measured is the product's: the CRM module's `full_load`, run by
the kernel as `etl run crm.full_load` runs it (mappings, quality gate, key
registry, quarantine, state, run ledger), with the CRM settings' defaults
(batches of 1,000 rows, rejects into the quarantine) but `partitions`.

Seeds the legacy tables of the CRM demo (`CUST_MASTER`, `ORD_MASTER`) with
`--rows` rows, half customers and half orders, 1% of them dirty (a date that
does not exist, hence an orphan order), indexed on `(UPDATED_AT, key)`: the
order a single range reads in. Then, for each count of partitions, in a
process of its own (its peak memory is its own):

- load: into empty CRM tables, bookkeeping tables in the target database;
- reload: the same after resetting the state, nothing to write (the change
  detection).

Each reports its seconds, rows/s, peak RSS, and where the time went (seconds
of the steps' threads): extract (the source reader), transform + gate
(mapping, checks, quarantine), write (the target upsert), keys (the key
registry), other (state, ledger, the gate's records). The target tables are
dropped and created again: point `--target` at a database of its own.

SQLite by default, in a temporary directory.
"""

from __future__ import annotations

import argparse
import collections
from collections.abc import Callable, Iterator, Mapping
import contextlib
import datetime
import json
import os
import pathlib
import platform
import resource
import subprocess  # nosec B404
import sys
import tempfile
import threading
import time
from typing import Any

import sqlalchemy as sa

from app import bootstrap
from core import contract
from core import keys
from core import operators
from core.observability import context as fields
from core.observability import tracing
from infra.database import relational
from modules.crm import demo

ROOT = pathlib.Path(__file__).resolve().parents[1]
T0 = datetime.datetime(2026, 1, 1)
DIRTY_EVERY = 100
_CHUNK = 50_000
# The engine's spans: a step attempt, and a batch in it.
STEP_SPAN, BATCH_SPAN = "etl.step", "etl.batch"


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(__doc__ or "").splitlines()[0]
    )
    parser.add_argument("--source", help="SQLAlchemy URL (default: SQLite)")
    parser.add_argument("--target", help="SQLAlchemy URL (default: SQLite)")
    parser.add_argument("--rows", type=int, default=200_000)
    parser.add_argument("--partitions", default="1,4")
    parser.add_argument(
        "--batch-size", type=int, help="default: the CRM's (1,000)"
    )
    parser.add_argument(
        "--no-reload", action="store_true", help="measure the load alone"
    )
    parser.add_argument(
        "--keep", action="store_true", help="keep a seeded source of N rows"
    )
    parser.add_argument("--output", type=pathlib.Path, help="JSON results")
    parser.add_argument("--child", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.child:
        return _child(json.loads(args.child))

    with tempfile.TemporaryDirectory() as tmp:
        source = args.source or f"sqlite:///{tmp}/legacy.db"
        target = args.target or f"sqlite:///{tmp}/crm.db"
        seeded = _seed(source, args.rows, keep=args.keep)
        _print_header(seeded)
        runs = [
            _spawn({
                "source": source,
                "target": target,
                "partitions": int(count),
                "batch_size": args.batch_size,
                "reload": not args.no_reload,
            })
            for count in args.partitions.split(",")
        ]
        setup = _setup(source, target)
        if not args.keep:
            _drop_source(source)

    if args.output:
        args.output.write_text(
            json.dumps(
                {
                    "pipeline": "crm.full_load",
                    "setup": setup,
                    "rows": args.rows,
                    "seed": seeded,
                    "runs": runs,
                },
                indent=2,
            )
        )
    return 0


def _spawn(job: Mapping[str, Any]) -> dict[str, Any]:
    found = subprocess.run(  # nosec B603
        [sys.executable, __file__, "--child", json.dumps(job)],
        capture_output=True,
        text=True,
        check=False,
    )
    if found.returncode:
        failed = [
            line
            for line in found.stderr.splitlines()
            if line.startswith("step ") and "failed (attempt" in line
        ]
        error = (failed or found.stderr.splitlines() or ["?"])[-1]
        print(f"{job['partitions']} partition(s) FAILED: {error}", flush=True)
        return {"partitions": job["partitions"], "error": error, "measures": []}
    result: dict[str, Any] = json.loads(found.stdout.splitlines()[-1])
    _print_run(result)
    return result


def _customer(i: int) -> dict[str, Any]:
    dirty = i % DIRTY_EVERY == 0
    return {
        "CUST_NO": str(i),
        "FIRST_NM": f"First{i % 1000}",
        "LAST_NM": f"Last {i}",
        "STAT_CD": "AI"[i % 2],
        "CRT_DT": "31/02/2020" if dirty else f"{i % 28 + 1:02d}/03/2021",
        "CTRY": ("VN", "VIETNAM", "US")[i % 3],
        "LEGACY_FLAG": None,
        "UPDATED_AT": _updated(i),
    }


def _order(i: int) -> dict[str, Any]:
    return {
        "ORD_NO": f"O-{i}",
        "CUST_NO": str(i),
        "AMT": f"{i % 1000}.50",
        "ORD_DT": f"{i % 28 + 1:02d}/02/2024",
        "UPDATED_AT": _updated(i),
    }


def _updated(i: int) -> datetime.datetime:
    # A permutation of the keys: rows updated in any order, as in a real
    # table, so the watermark index is not the key's.
    return T0 + datetime.timedelta(seconds=(i * 7919) % 1_000_003)


def _seed(url: str, rows: int, *, keep: bool) -> dict[str, Any]:
    db = relational.Database(url)
    try:
        metadata = sa.MetaData()
        customers, orders = demo.legacy_tables(metadata)
        for table, key in ((customers, "CUST_NO"), (orders, "ORD_NO")):
            sa.Index(
                f"ix_{table.name}_updated", table.c.UPDATED_AT, table.c[key]
            )
        if keep and _count(db, customers, orders) == rows:
            return {"rows": rows, "seconds": 0.0, "kept": True}
        metadata.drop_all(db.engine)
        metadata.create_all(db.engine)
        writer = relational.RelationalTableWriter(db)
        started = time.perf_counter()
        half = rows // 2
        for table, make, count in (
            (customers, _customer, rows - half),
            (orders, _order, half),
        ):
            target = contract.TableTarget(
                table.name,
                key=tuple(c.name for c in table.primary_key),
                mode=contract.WriteMode.INSERT,
            )
            for start in range(1, count + 1, _CHUNK):
                writer.write(
                    target,
                    [
                        make(i)
                        for i in range(
                            start, min(count, start + _CHUNK - 1) + 1
                        )
                    ],
                )
        _analyze(db, (customers.name, orders.name))
        return {
            "rows": rows,
            "seconds": round(time.perf_counter() - started, 1),
        }
    finally:
        db.close()


def _count(db: relational.Database, *tables: sa.Table) -> int:
    inspector = sa.inspect(db.engine)
    if not all(inspector.has_table(t.name) for t in tables):
        return -1
    with db.engine.connect() as connection:
        return sum(
            connection.execute(
                sa.select(sa.func.count()).select_from(t)
            ).scalar_one()
            for t in tables
        )


def _analyze(db: relational.Database, tables: tuple[str, ...]) -> None:
    """Fresh planner statistics, as a production table has."""
    statement = {
        "postgresql": 'ANALYZE "{}"',
        "sqlite": 'ANALYZE "{}"',
        "mssql": "UPDATE STATISTICS [{}]",
    }.get(db.engine.dialect.name)
    if statement is None:
        return
    with db.engine.begin() as connection:
        for table in tables:
            connection.execute(sa.text(statement.format(table)))


def _drop_source(url: str) -> None:
    db = relational.Database(url)
    try:
        metadata = sa.MetaData()
        demo.legacy_tables(metadata)
        metadata.drop_all(db.engine)
    finally:
        db.close()


def _recreate_target(url: str) -> None:
    """Drops the CRM tables and the ETL's records, then creates the former."""
    db = relational.Database(url)
    try:
        existing = sa.MetaData()
        existing.reflect(
            db.engine,
            only=lambda name, _: name in ("customer", "order")
            or name.startswith("etl_"),
        )
        existing.drop_all(db.engine)
        metadata = sa.MetaData()
        demo.crm_tables(metadata)
        metadata.create_all(db.engine)
    finally:
        db.close()


def _reset_state(url: str) -> None:
    """Forgets the watermarks and ranges: the next run reads everything."""
    db = relational.Database(url)
    try:
        with db.engine.begin() as connection:
            connection.execute(sa.text("DELETE FROM etl_state"))
    finally:
        db.close()


class _Span(tracing.Span):
    """A span that reports its duration to `_Clock`."""

    def __init__(
        self,
        clock: _Clock,
        name: str,
        attributes: Mapping[str, contract.Attribute],
        parent: tracing.Span | None,
    ) -> None:
        self.name = name
        self.step = str(attributes.get(fields.STEP, ""))
        if not self.step and isinstance(parent, _Span):
            self.step = parent.step
        self._clock = clock
        self._started = time.perf_counter()

    @property
    def trace_id(self) -> str:
        return ""

    @property
    def span_id(self) -> str:
        return ""

    def set_attributes(
        self, attributes: Mapping[str, contract.Attribute]
    ) -> None:
        del attributes  # Unused.

    def fail(self, error: BaseException | str) -> None:
        del error  # Unused.

    def end(self) -> None:
        self._clock.add(
            self.step, self.name, time.perf_counter() - self._started
        )


class _Clock(tracing.ITracer):
    """Seconds by step and by what ran: the engine's step and batch spans,
    the reader's and loaders' calls."""

    def __init__(self) -> None:
        self.seconds: dict[tuple[str, str], float] = collections.defaultdict(
            float
        )
        self._lock = threading.Lock()

    def start_span(
        self,
        name: str,
        attributes: Mapping[str, contract.Attribute],
        parent: tracing.Span | None,
    ) -> tracing.Span:
        return _Span(self, name, attributes, parent)

    def add(self, step: str, what: str, seconds: float) -> None:
        with self._lock:
            self.seconds[step, what] += seconds

    def phases(self) -> dict[str, float]:
        """Seconds of the steps reading rows, by phase; then of `ranges`."""
        total: dict[str, float] = collections.defaultdict(float)
        for (step, what), seconds in self.seconds.items():
            if not step.endswith(":ranges"):
                total[what] += seconds
            elif what == STEP_SPAN:
                total["ranges"] += seconds
        write, registering = total["write"], total["registering"]
        found = {
            "extract": total["read"],
            "transform + gate": total[BATCH_SPAN] - registering,
            "write": write,
            "keys": registering - write,
            "other": total[STEP_SPAN] - total[BATCH_SPAN] - total["read"],
            "ranges": total["ranges"],
        }
        return {name: round(value, 2) for name, value in found.items()}

    def install(self) -> None:
        """Times the spans, the reader and the loaders: replaces the tracer
        and those classes' methods for the whole process."""
        tracing.set_tracer(self)
        reader = relational.RelationalTableReader
        setattr(reader, "read", self._timed_read(reader.read))
        writer = operators.TableLoader
        setattr(writer, "load", self._timed("write", writer.load))
        registering = keys.RegisteringLoader
        setattr(
            registering, "load", self._timed("registering", registering.load)
        )

    def _step(self) -> str:
        span = tracing.current()
        return span.step if isinstance(span, _Span) else ""

    def _timed[**P, R](self, what: str, call: Callable[P, R]) -> Callable[P, R]:
        def timed(*args: P.args, **kwargs: P.kwargs) -> R:
            started = time.perf_counter()
            try:
                return call(*args, **kwargs)
            finally:
                self.add(self._step(), what, time.perf_counter() - started)

        return timed

    def _timed_read[S, Q](
        self, read: Callable[[S, Q], Iterator[contract.Row]]
    ) -> Callable[[S, Q], Iterator[contract.Row]]:
        def timed(reader: S, query: Q) -> Iterator[contract.Row]:
            step, seconds = self._step(), 0.0
            started = time.perf_counter()
            rows = read(reader, query)
            seconds += time.perf_counter() - started
            try:
                while True:
                    started = time.perf_counter()
                    try:
                        row = next(rows)
                    except StopIteration:
                        return
                    finally:
                        seconds += time.perf_counter() - started
                    yield row
            finally:
                self.add(step, "read", seconds)

        return timed


def _child(job: dict[str, Any]) -> int:
    _recreate_target(job["target"])
    settings: dict[str, Any] = {
        "source": "legacy",
        "target": "crm",
        "mappings": str(ROOT / "mappings/compiled/crm.json"),
        "source_tables": {"customer": "CUST_MASTER", "order": "ORD_MASTER"},
        "target_tables": {"customer": "customer", "order": "order"},
        "partitions": job["partitions"],
    }
    if job["batch_size"]:
        settings["batch_size"] = job["batch_size"]
    overrides = {
        "databases": {
            "legacy": {"url": job["source"]},
            "crm": {"url": job["target"]},
        },
        "bookkeeping": {"database": "crm", "create": True},
        "modules": {
            "crm": {"module": "modules.crm:create", "settings": settings}
        },
    }
    clock = _Clock()
    base_mb = _peak_mb()
    measures = []
    with bootstrap.bootstrap(
        "bench", directory=ROOT / "config", overrides=overrides, logs=False
    ) as application:
        clock.install()
        names = ["load", "reload"] if job["reload"] else ["load"]
        for name in names:
            if name == "reload":
                _reset_state(job["target"])
            clock.seconds.clear()
            measures.append(_run(application, clock, name))
    print(
        json.dumps({
            "partitions": job["partitions"],
            "batch_size": job["batch_size"] or 1000,
            "base_mb": round(base_mb),
            "measures": measures,
        })
    )
    return 0


def _run(
    application: bootstrap.Application, clock: _Clock, name: str
) -> dict[str, Any]:
    started = time.perf_counter()
    outcome = application.kernel.run("crm.full_load")
    seconds = time.perf_counter() - started
    result = outcome.result
    if result is None or result.status not in (
        contract.ETLStatus.SUCCESS,
        contract.ETLStatus.PARTIAL_SUCCESS,
    ):
        raise RuntimeError(
            f"{name} failed: {outcome.message or (result and result.message)}"
        )
    rejected = sum(
        report.rejected
        for step in result.step_results.values()
        for report in step.quality_reports.values()
    )
    return {
        "name": name,
        "rows": result.stats.total,
        "written": result.stats.created + result.stats.updated,
        "rejected": rejected,
        "seconds": round(seconds, 1),
        "rate": round(result.stats.total / seconds),
        "peak_mb": round(_peak_mb()),
        "thread_seconds": clock.phases(),
    }


def _peak_mb() -> float:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # Kilobytes on Linux, bytes on macOS.
    return peak / (1024 * 1024) if sys.platform == "darwin" else peak / 1024


def _setup(source: str, target: str) -> dict[str, Any]:
    cpu = ""
    with contextlib.suppress(OSError):
        for line in (
            pathlib.Path("/proc/cpuinfo")
            .read_text(encoding="utf-8")
            .splitlines()
        ):
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
    memory = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    return {
        "cpu": cpu,
        "threads": os.cpu_count(),
        "memory_gb": round(memory / 1024**3, 1),
        "python": platform.python_version(),
        "source": _server(source),
        "target": _server(target),
    }


def _server(url: str) -> str:
    engine = sa.create_engine(url)
    try:
        with engine.connect():
            version = engine.dialect.server_version_info or ()
        return f"{engine.dialect.name} {'.'.join(map(str, version))}"
    finally:
        engine.dispose()


def _print_header(seeded: Mapping[str, Any]) -> None:
    print(f"seeded {seeded['rows']:,} rows in {seeded['seconds']} s")
    print(
        f"{'run':<16} {'rows':>9} {'written':>9} {'rejected':>8}"
        f" {'seconds':>8} {'rows/s':>8} {'peak MB':>8}  thread-seconds",
        flush=True,
    )


def _print_run(run: Mapping[str, Any]) -> None:
    for m in run["measures"]:
        phases = ", ".join(
            f"{name} {seconds}" for name, seconds in m["thread_seconds"].items()
        )
        print(
            f"{m['name'] + ', ' + str(run['partitions']) + ' part.':<16}"
            f" {m['rows']:>9} {m['written']:>9} {m['rejected']:>8}"
            f" {m['seconds']:>8} {m['rate']:>8} {m['peak_mb']:>8}  {phases}",
            flush=True,
        )


if __name__ == "__main__":
    sys.exit(main())
