"""The cutover diff at scale: time, peak memory and rows read, by drift.

    uv run python benchmarks/bench_diff.py --rows 1000000 --drifts 0,0.001,0.01
    uv run python benchmarks/bench_diff.py --target postgresql+psycopg://...

Seeds a source table of N rows, and for each drift a target table holding
what the source maps to (a surrogate UUID key, as text; a name upper-cased;
an amount; a timestamp), then drifts a fraction of its rows: a third
changed, a third deleted, a third added. Each diff runs in a process of its
own, which reports:

- the seconds `cutover.reconcile` took;
- the peak memory of the process (RSS), and its memory before the diff;
- the rows read from the source and from the target, all passes included;
- how the diff split the keys (`range` or `hash`, `--hash` forces it).

SQLite by default, in a temporary directory.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime
import decimal
import json
import pathlib
import resource
import subprocess  # nosec B404
import sys
import tempfile
import time
from typing import Any
import uuid

import sqlalchemy as sa

from core import contract
from core import cutover
from infra.database import relational

SOURCE = "bench_diff_source"
NAMESPACE = uuid.UUID("6f1c8f8e-8c4f-4c55-9a43-0d6c2e7b7f10")
T0 = datetime.datetime(2026, 1, 1, 8)
_CHUNK = 20_000


@dataclasses.dataclass
class Measure:
    """One diff: its drift, how long, how much memory, how many reads."""

    drift: float
    rows: int
    seconds: float
    peak_mb: float
    base_mb: float
    source_read: int
    target_read: int
    partitioning: str
    status: str
    differences: int


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(__doc__ or "").splitlines()[0]
    )
    parser.add_argument("--source", help="SQLAlchemy URL (default: SQLite)")
    parser.add_argument("--target", help="SQLAlchemy URL (default: SQLite)")
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--drifts", default="0,0.001,0.01")
    parser.add_argument(
        "--hash", action="store_true", help="split the keys by hash"
    )
    parser.add_argument("--output", type=pathlib.Path, help="JSON results")
    parser.add_argument("--child", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.child:
        return _child(json.loads(args.child))

    measures = []
    with tempfile.TemporaryDirectory() as tmp:
        source = args.source or f"sqlite:///{tmp}/source.db"
        target = args.target or f"sqlite:///{tmp}/target.db"
        _seed(relational.Database(source), args.rows)
        for drift in (float(d) for d in args.drifts.split(",")):
            table = f"bench_diff_{str(drift).replace('.', '_')}"
            _target(relational.Database(target), table, args.rows, drift)
            found = subprocess.run(  # nosec B603
                [
                    sys.executable,
                    __file__,
                    "--child",
                    json.dumps({
                        "source": source,
                        "target": target,
                        "table": table,
                        "rows": args.rows,
                        "drift": drift,
                        "hash": args.hash,
                    }),
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            measure = Measure(**json.loads(found.stdout.splitlines()[-1]))
            measures.append(measure)
            print(
                f"drift {measure.drift:>6.2%}: {measure.seconds:7.1f} s,"
                f" peak {measure.peak_mb:7.1f} MB (before"
                f" {measure.base_mb:.0f}), read source"
                f" {measure.source_read:,} target {measure.target_read:,}"
                f" ({measure.partitioning}), {measure.status},"
                f" {measure.differences:,} differences",
                flush=True,
            )
    if args.output:
        args.output.write_text(
            json.dumps([dataclasses.asdict(m) for m in measures], indent=2)
        )
    return 0


def _seed(db: relational.Database, rows: int) -> None:
    """The source: `rows` rows, keyed by an integer."""
    metadata = sa.MetaData()
    table = sa.Table(
        SOURCE,
        metadata,
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("name", sa.Unicode(40)),
        sa.Column("amount", sa.Numeric(12, 2)),
        sa.Column("at", sa.DateTime),
    )
    metadata.drop_all(db.engine)
    metadata.create_all(db.engine)
    with db.engine.begin() as connection:
        for start in range(0, rows, _CHUNK):
            connection.execute(
                table.insert(),
                [_source(i) for i in range(start, min(rows, start + _CHUNK))],
            )


def _source(i: int) -> dict[str, Any]:
    return {
        "id": i,
        "name": f"customer {i}",
        "amount": decimal.Decimal(i % 100_000) / 100,
        "at": T0 + datetime.timedelta(seconds=i),
    }


def _expect(row: contract.Row) -> contract.Expected:
    """The mapping: a surrogate key, the name upper-cased."""
    return contract.Expected(
        rows=[{
            "key": str(uuid.uuid5(NAMESPACE, str(row["id"]))),
            "name": str(row["name"]).upper(),
            "amount": row["amount"],
            "at": row["at"],
        }]
    )


def _target(
    db: relational.Database, name: str, rows: int, drift: float
) -> None:
    """The target: what the source maps to, a fraction of it drifted."""
    metadata = sa.MetaData()
    table = sa.Table(
        name,
        metadata,
        sa.Column("key", sa.Unicode(36), primary_key=True),
        sa.Column("name", sa.Unicode(40)),
        sa.Column("amount", sa.Numeric(12, 2)),
        sa.Column("at", sa.DateTime),
    )
    metadata.drop_all(db.engine)
    metadata.create_all(db.engine)
    # Every n-th row drifts: changed, deleted or added, in turn.
    every = round(1 / drift) if drift else 0
    with db.engine.begin() as connection:
        for start in range(0, rows, _CHUNK):
            batch = []
            for i in range(start, min(rows, start + _CHUNK)):
                row = _expect(_source(i)).rows[0]
                kind = (i // every) % 3 if every and i % every == 0 else None
                if kind == 0:
                    row = {**row, "name": "CHANGED"}
                elif kind == 1:
                    continue
                elif kind == 2:
                    batch.append(
                        {**row, "key": str(uuid.uuid4()), "name": "ADDED"}
                    )
                batch.append(row)
            connection.execute(table.insert(), batch)


class _Counting(relational.RelationalTableReader):
    """Counts the rows it reads."""

    rows = 0

    def read(self, query: contract.TableQuery) -> Any:
        for row in super().read(query):
            self.rows += 1
            yield row


def _child(job: dict[str, Any]) -> int:
    """Runs one diff; prints its measure as JSON."""
    source = _Counting(relational.Database(job["source"]))
    target = _Counting(relational.Database(job["target"]))
    reconciliation = contract.Reconciliation(
        entity="customer",
        source=source,
        source_query=contract.TableQuery(SOURCE, order_by=("id",)),
        expect=_expect,
        target=target,
        target_query=contract.TableQuery(job["table"], order_by=("key",)),
        key=("key",),
        columns=("key", "name", "amount", "at"),
    )
    base = _rss_mb()
    started = time.monotonic()
    report = cutover.reconcile(reconciliation, ranges=not job["hash"])
    seconds = time.monotonic() - started
    measure = Measure(
        drift=job["drift"],
        rows=job["rows"],
        seconds=round(seconds, 2),
        peak_mb=round(_rss_mb(), 1),
        base_mb=round(base, 1),
        source_read=source.rows,
        target_read=target.rows,
        partitioning=report.partitioning,
        status=report.status,
        differences=report.missing + report.extra + report.different,
    )
    print(json.dumps(dataclasses.asdict(measure)))
    return 0


def _rss_mb() -> float:
    """The peak resident memory of this process so far, in MB."""
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # Kilobytes on Linux, bytes on macOS.
    return peak / (1024 * 1024 if sys.platform == "darwin" else 1024)


if __name__ == "__main__":
    sys.exit(main())
