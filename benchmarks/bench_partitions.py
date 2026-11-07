"""Splitting a table into key ranges, and reading the ranges in parallel.

    uv run python benchmarks/bench_partitions.py --rows 1000000 \\
        --source postgresql+psycopg://etl:etl@localhost:55432/etl

Seeds `bench_parts` with N rows whose watermark is not correlated with the
key (rows updated in any order, as in a real table), then measures:

- split: `operators.split`, the bounds of the ranges (the `<mapping>:ranges`
  step);
- first load: every range of `KeyPartitions`, in parallel, from no saved
  position, into a loader that only counts (the reading side alone);
- delta: 1% of the rows updated, then the next run of the ranges;

without, then with, an index on `(updated_at, id)`. SQLite by default, in a
temporary directory. `--keep` keeps the seeded table for the next run.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Iterator, Sequence
import datetime
import json
import pathlib
import sys
import tempfile
import time
from typing import Any

import sqlalchemy as sa

from core import contract
from core import operators
from core import pipeline
from core import state as core_state
from infra.database import relational

TABLE = "bench_parts"
INDEX = "bench_parts_watermark"
T0 = datetime.datetime(2026, 1, 1)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(__doc__ or "").splitlines()[0]
    )
    parser.add_argument("--source", help="SQLAlchemy URL (default: SQLite)")
    parser.add_argument("--rows", type=int, default=200_000)
    parser.add_argument("--partitions", type=int, default=4)
    parser.add_argument("--batch-size", type=int, default=10_000)
    parser.add_argument(
        "--workers",
        type=int,
        help=(
            "ranges read at once (default: all); 1 shows the reading cost"
            " alone, without the threads competing for the interpreter"
        ),
    )
    parser.add_argument(
        "--keep", action="store_true", help="keep a seeded table of N rows"
    )
    parser.add_argument("--output", type=pathlib.Path, help="JSON results")
    args = parser.parse_args()

    results: dict[str, Any] = {}
    with tempfile.TemporaryDirectory() as tmp:
        db = relational.Database(args.source or f"sqlite:///{tmp}/source.db")
        try:
            _seed(db, args.rows, keep=args.keep)
            for indexed in (False, True):
                _index(db, create=indexed)
                label = "with" if indexed else "without"
                results[f"{label} index"] = _measure(
                    db, args.partitions, args.batch_size, args.workers
                )
            _index(db, create=False)
            if not args.keep:
                sa.Table(TABLE, sa.MetaData()).drop(db.engine, checkfirst=True)
        finally:
            db.close()

    for label, measures in results.items():
        print(f"{label} (updated_at, id):")
        for name, value in measures.items():
            print(f"  {name:<12} {value}")
    if args.output:
        args.output.write_text(
            json.dumps(
                {"source": db.name, "rows": args.rows, "results": results},
                indent=2,
                default=str,
            )
        )
    return 0


def _rows(count: int) -> Iterator[dict[str, Any]]:
    # A permutation of the keys: the watermark says nothing of the key.
    for i in range(1, count + 1):
        yield {
            "id": i,
            "updated_at": (
                T0 + datetime.timedelta(seconds=(i * 7919) % count // 10)
            ),
            "name": f"Customer {i}",
            "note": "x" * 60,
        }


def _seed(db: relational.Database, count: int, *, keep: bool) -> None:
    table = sa.Table(
        TABLE,
        sa.MetaData(),
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=False),
        sa.Column("updated_at", sa.DateTime, nullable=False),
        sa.Column("name", sa.Unicode(50), nullable=False),
        sa.Column("note", sa.Unicode(100)),
    )
    if keep and sa.inspect(db.engine).has_table(TABLE):
        with db.engine.connect() as connection:
            found = connection.execute(
                sa.select(sa.func.count()).select_from(table)
            ).scalar_one()
        if found == count:
            return
    table.drop(db.engine, checkfirst=True)
    table.create(db.engine)
    writer = relational.RelationalTableWriter(db)
    target = contract.TableTarget(
        TABLE, key=("id",), mode=contract.WriteMode.INSERT
    )
    started = time.perf_counter()
    batch: list[dict[str, Any]] = []
    for row in _rows(count):
        batch.append(row)
        if len(batch) == 50_000:
            writer.write(target, batch)
            batch = []
    if batch:
        writer.write(target, batch)
    print(f"seeded {count} rows in {time.perf_counter() - started:.1f}s")


def _index(db: relational.Database, *, create: bool) -> None:
    table = sa.Table(TABLE, sa.MetaData(), autoload_with=db.engine)
    index = sa.Index(INDEX, table.c.updated_at, table.c.id)
    index.drop(db.engine, checkfirst=True)
    if create:
        index.create(db.engine)
    # Fresh planner statistics, as a production table has.
    with db.engine.begin() as connection:
        if db.engine.dialect.name in ("sqlite", "postgresql"):
            connection.execute(sa.text(f"ANALYZE {TABLE}"))


class _Count(pipeline.IDataLoader[contract.Row]):
    """Loads nothing: the reading side alone."""

    def load(
        self, records: Sequence[contract.Row], ctx: contract.ETLContext
    ) -> contract.ETLStats:
        return contract.ETLStats(successful=len(records))


class _Ranges(pipeline.BaseOrchestrator):
    """The ranges step, then one step per range, in parallel."""

    def __init__(
        self,
        partitions: operators.KeyPartitions,
        jobs: Sequence[contract.StepHandler],
        workers: int | None,
    ) -> None:
        super().__init__(max_workers=workers or len(jobs))
        self._partitions = partitions
        self._jobs = jobs

    def steps(self) -> Sequence[contract.PipelineStep]:
        return [
            contract.PipelineStep("ranges", self._partitions.prepare),
            *(
                contract.PipelineStep(
                    f"range:{i}", job, dependencies=("ranges",)
                )
                for i, job in enumerate(self._jobs)
            ),
        ]


def _measure(
    db: relational.Database, count: int, batch_size: int, workers: int | None
) -> dict[str, Any]:
    reader = relational.RelationalTableReader(db)
    state = core_state.InMemoryStateStore()
    measures: dict[str, Any] = {}

    bounds, seconds = _timed(
        lambda: operators.split(reader, TABLE, ["id"], count)
    )
    measures["split"] = f"{seconds:.2f}s {[r.lower for r in bounds[1:]]}"

    def run() -> int:
        partitions = operators.KeyPartitions(
            reader, TABLE, key=["id"], count=count, state=state
        )
        jobs = [
            pipeline.ETLJob(
                extractor,
                operators.Passthrough[contract.Row](),
                _Count(),
                batch_size=batch_size,
            )
            for extractor in partitions.extractors(watermark="updated_at")
        ]
        result = _Ranges(partitions, jobs, workers).run(_context("bench.load"))
        if result.status is not contract.ETLStatus.SUCCESS:
            raise RuntimeError(f"load failed: {result.message}")
        return result.stats.total

    rows, seconds = _timed(run)
    measures["first load"] = f"{seconds:.2f}s {rows} rows"

    table = sa.Table(TABLE, sa.MetaData(), autoload_with=db.engine)
    changed = table.c.id % 100 == 7
    with db.engine.begin() as connection:
        seeded = [
            {"key": key, "at": at}
            for key, at in connection.execute(
                sa.select(table.c.id, table.c.updated_at).where(changed)
            )
        ]
        connection.execute(
            table.update()
            .where(changed)
            .values(updated_at=T0 + datetime.timedelta(days=30))
        )
    rows, seconds = _timed(run)
    measures["delta"] = f"{seconds:.2f}s {rows} rows"
    # Back as seeded, for the next measure.
    with db.engine.begin() as connection:
        connection.execute(
            table.update()
            .where(table.c.id == sa.bindparam("key"))
            .values(updated_at=sa.bindparam("at")),
            seeded,
        )
    return measures


def _timed[R](work: Callable[[], R]) -> tuple[R, float]:
    started = time.perf_counter()
    found = work()
    return found, time.perf_counter() - started


def _context(name: str) -> contract.ETLContext:
    return contract.ETLContext(contract.ETLRunInfo(name))


if __name__ == "__main__":
    sys.exit(main())
