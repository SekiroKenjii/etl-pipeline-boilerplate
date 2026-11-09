"""`etl calibrate <database>`: measures how a database takes its batches, on
a scratch table, and prints a `[tuning]` snippet.

    etl calibrate crm                     # TOML, the measurements as comments.
    etl calibrate crm --format json       # For scripts.
    etl calibrate crm --batch-size 2000   # Not the modules' batch size.

On a table of its own in the named database (created, filled, dropped;
nothing else is written): a batch staged as parameters against the driver's
bulk loader (bulk copy on SQL Server, COPY on Postgres), IN lists of 250,
500 and 1,000 values, and the key registry's statement with new and with
known keys. The batch is the `batch_size` of the modules using the database
(1,000 if none does). Exit 0; 2 when the database is not in the settings, or
is MongoDB.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Sequence
import dataclasses
import datetime
import decimal
import functools
import json
import statistics
import time
from typing import TextIO
import uuid

import sqlalchemy as sa

from app import bootstrap
from cli import exit_codes
from core import contract
from core import mapping_pipelines
from core.runtime import names
from infra.database import relational
from infra.database.relational import base_dialect
from infra.database.relational import bulk_loading
from infra.database.relational import dialects
from infra.database.relational import staging
from infra.database.relational.bookkeeping import definitions

IN_LISTS = (250, 500, 1000)
REPEATS = 3
DEFAULT_BATCH_SIZE = 1000
_FORMATS = ("toml", "json")
_T0 = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)


@dataclasses.dataclass(frozen=True, slots=True)
class Calibration:
    """What `etl calibrate` measured, in seconds, and what it recommends.

    Attributes:
        database: The database, as the settings name it.
        dialect: Its SQLAlchemy dialect name.
        driver: Its driver.
        server: Its server version.
        batch_size: The rows of the batch measured.
        staging: Seconds to stage one batch, by strategy (`parameters`, and
            `bulk copy` where the driver has a bulk loader).
        in_lists: Seconds to read every key of the batch back by IN lists,
            by values per list.
        key_registry: Seconds to register the batch as keys (the key
            registry's statement), by `new` and `known` keys, then by
            strategy.
        bulk_copy_rows: The `tuning.bulk_copy_rows` recommended: a count,
            `"auto"`, or None where nothing is to set.
        in_chunk: The `tuning.in_chunk` recommended.
        measured_at: When.
        scratch_prefix: The prefix of the scratch tables the measurements
            made and dropped; tests check nothing of it is left.
    """

    database: str
    dialect: str
    driver: str
    server: str
    batch_size: int
    staging: dict[str, float]
    in_lists: dict[int, float]
    key_registry: dict[str, dict[str, float]]
    bulk_copy_rows: int | str | None
    in_chunk: int
    measured_at: datetime.datetime
    scratch_prefix: str = ""

    def as_dict(self) -> dict[str, object]:
        """The calibration as `--format json` prints it."""
        return {
            "database": self.database,
            "dialect": self.dialect,
            "driver": self.driver,
            "server": self.server,
            "batch_size": self.batch_size,
            "measured_at": self.measured_at.isoformat(),
            "staging": dict(self.staging),
            "in_lists": {str(size): s for size, s in self.in_lists.items()},
            "key_registry": {
                kind: dict(by_strategy)
                for kind, by_strategy in self.key_registry.items()
            },
            "recommended": {
                "bulk_copy_rows": self.bulk_copy_rows,
                "in_chunk": self.in_chunk,
            },
        }

    def toml(self) -> str:
        """The `[tuning]` snippet, the measurements as comments."""
        rows = self.batch_size
        lines = [
            (
                f"# etl calibrate {self.database}: {self.dialect} {self.server}"
                f" ({self.driver}), batch_size {rows},"
                f" {self.measured_at:%Y-%m-%dT%H:%M:%SZ}"
            ),
            f"# staging {rows} rows: "
            + ", ".join(
                f"{name} {_ms(seconds)} ({_ms(seconds / rows)} a row)"
                for name, seconds in self.staging.items()
            ),
            f"# reading {rows} keys back by IN lists: "
            + ", ".join(
                f"{size} a list {_ms(seconds)}"
                for size, seconds in self.in_lists.items()
            ),
        ]
        for kind, by_strategy in self.key_registry.items():
            lines.append(
                f"# key registry, {rows} {kind} keys: "
                + ", ".join(
                    f"{_ms(seconds)} by {name}"
                    for name, seconds in by_strategy.items()
                )
            )
        lines.append("[tuning]")
        if self.bulk_copy_rows is not None:
            value = json.dumps(self.bulk_copy_rows)
            lines.append(
                f"{f'bulk_copy_rows = {value}':<32}# {self._why_bulk()}"
            )
        else:
            lines.append(f"# bulk_copy_rows: {self._why_bulk()}")
        lines.append(
            f"{f'in_chunk = {self.in_chunk}':<32}# the fastest IN list measured"
        )
        return "\n".join(lines) + "\n"

    def _why_bulk(self) -> str:
        """Why `bulk_copy_rows` is recommended as it is."""
        parameters = self.staging.get(staging.PARAMETERS)
        bulk = self.staging.get(staging.BULK_COPY)
        if parameters is None or bulk is None:
            return "no bulk loader apart from the statements: parameters"
        if bulk < parameters:
            faster = (
                f"bulk copy {parameters / bulk:.1f}x faster than parameters at"
                f" {self.batch_size} rows"
            )
        else:
            faster = (
                f"parameters {bulk / parameters:.1f}x faster than bulk copy at"
                f" {self.batch_size} rows: leave the online choice to find the"
                " crossing"
            )
        if self.dialect == "postgresql":
            return f"COPY stages every batch on Postgres ({faster})"
        return faster


def add_parser(
    commands: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """Adds the `calibrate` command to the subcommands of `etl`."""
    parser = commands.add_parser(
        "calibrate",
        help=(
            "measure how a database takes its batches, on a scratch table,"
            " and print a [tuning] snippet; writes nothing else"
        ),
    )
    parser.add_argument("database", help="a database of the settings")
    parser.add_argument(
        "--batch-size",
        type=int,
        help="the rows of the batch (default: the modules' batch_size)",
    )
    parser.add_argument(
        "--format",
        choices=_FORMATS,
        default="toml",
        help=(
            "toml, a [tuning] snippet with the measurements as comments; or"
            " json: {database, dialect, driver, server, batch_size,"
            " measured_at, staging, in_lists, key_registry, recommended}"
        ),
    )


def command(
    args: argparse.Namespace,
    application: bootstrap.Application,
    stream: TextIO,
) -> int:
    """Runs `etl calibrate`; returns the exit code."""
    settings = application.settings
    database = settings.databases.get(args.database)
    if database is None:
        stream.write(
            f"FAILED no database {args.database!r}:"
            f" {sorted(settings.databases)}\n"
        )
        return exit_codes.ExitCode.INVALID
    if database.is_mongodb:
        stream.write(
            f"FAILED {args.database} is MongoDB: calibrate measures the ways"
            " a SQL database takes its batches\n"
        )
        return exit_codes.ExitCode.INVALID
    if args.batch_size is not None and args.batch_size < 1:
        stream.write("FAILED --batch-size must be >= 1\n")
        return exit_codes.ExitCode.INVALID
    db = application.resources.get(
        names.database(args.database), relational.Database
    )
    batch_size = args.batch_size or _batch_size(application, args.database)
    found = calibrate(db, args.database, batch_size=batch_size)
    if args.format == "json":
        stream.write(json.dumps(found.as_dict()) + "\n")
    else:
        stream.write(found.toml())
    return exit_codes.ExitCode.SUCCESS


def _batch_size(application: bootstrap.Application, database: str) -> int:
    """The batch size of a mapping module reading or writing `database`;
    `DEFAULT_BATCH_SIZE` when none does."""
    for module in application.kernel.modules.values():
        if isinstance(module, mapping_pipelines.MappingModule) and database in (
            module.settings.source,
            module.settings.target,
        ):
            return module.settings.batch_size
    return DEFAULT_BATCH_SIZE


def calibrate(
    db: relational.Database,
    database: str,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    repeats: int = REPEATS,
) -> Calibration:
    """Measures `db` on a scratch table of `batch_size` rows, dropped at
    the end; each timing is the median of `repeats`.

    Raises:
        ValueError: If `batch_size` or `repeats` is below 1.
    """
    if batch_size < 1 or repeats < 1:
        raise ValueError("batch_size and repeats must be >= 1")
    tag = uuid.uuid4().hex[:8]
    metadata = sa.MetaData()
    table = _scratch_table(metadata, f"etl_calibrate_{tag}")
    registry = definitions.etl_tables(
        None, f"etl_calibrate_{tag}_"
    ).key_registry
    rows = [_row(i) for i in range(batch_size)]
    columns = list(rows[0])
    engine = db.engine
    table.create(engine)
    registry.create(engine)
    try:
        timings = _Timings(db, repeats)
        staged = timings.staging(table, columns, rows)
        with engine.begin() as connection:
            connection.execute(table.insert(), rows)
        in_lists = timings.in_lists(table, [str(row["key"]) for row in rows])
        registered = timings.key_registry(registry, batch_size)
    finally:
        registry.drop(engine, checkfirst=True)
        table.drop(engine, checkfirst=True)
    with engine.connect():
        version = engine.dialect.server_version_info or ()
    return Calibration(
        database=database,
        dialect=db.name,
        driver=engine.dialect.driver,
        server=".".join(map(str, version)),
        batch_size=batch_size,
        staging=staged,
        in_lists=in_lists,
        key_registry=registered,
        bulk_copy_rows=_recommended_bulk_copy(db.name, staged, batch_size),
        scratch_prefix=f"etl_calibrate_{tag}",
        in_chunk=_recommended_in_chunk(in_lists),
        measured_at=datetime.datetime.now(datetime.UTC),
    )


def _recommended_bulk_copy(
    dialect: str, staged: dict[str, float], batch_size: int
) -> int | str | None:
    bulk = staged.get(staging.BULK_COPY)
    parameters = staged.get(staging.PARAMETERS)
    if bulk is None or parameters is None or dialect != "mssql":
        return None
    if bulk < parameters * (1 - staging.HYSTERESIS):
        return batch_size
    return "auto"


def _recommended_in_chunk(in_lists: dict[int, float]) -> int:
    """The IN list that read the keys back fastest; the larger one when
    within 5%: fewer round trips."""
    best = min(in_lists, key=lambda size: in_lists[size])
    for size in sorted(in_lists, reverse=True):
        if in_lists[size] <= in_lists[best] * 1.05:
            return size
    return best


class _Timings:
    """The measurements, on one database."""

    def __init__(self, db: relational.Database, repeats: int) -> None:
        self.db = db
        self.repeats = repeats

    def staging(
        self,
        table: sa.Table,
        columns: Sequence[str],
        rows: Sequence[contract.Row],
    ) -> dict[str, float]:
        """Seconds to stage the batch, by parameters and by the bulk
        loader where the driver has one."""
        dialect = self.db.dialect
        engine = self.db.engine

        def parameters() -> float:
            stage = dialect.stage(table, columns)
            with engine.begin() as connection:
                dialect.create_stage(connection, stage)
                started = time.perf_counter()
                base_dialect.Dialect.fill_stage(
                    dialect, connection, stage, rows
                )
                seconds = time.perf_counter() - started
                dialect.drop_stage(connection, stage)
            return seconds

        found: dict[str, float] = {staging.PARAMETERS: self._median(parameters)}
        if engine.dialect.driver == "mssqlpython":
            found[staging.BULK_COPY] = self._median(
                lambda: self._bulk_copy(table, columns, rows)
            )
        elif engine.dialect.driver == "psycopg":
            found[staging.BULK_COPY] = self._median(
                lambda: self._copy(table, columns, rows)
            )
        return found

    def _bulk_copy(
        self,
        table: sa.Table,
        columns: Sequence[str],
        rows: Sequence[contract.Row],
    ) -> float:
        """Seconds of one bulk copy of the batch into a `##` table."""
        stage = sa.Table(
            f"##{base_dialect.STAGE}_{uuid.uuid4().hex}",
            sa.MetaData(),
            *(sa.Column(name, table.c[name].type) for name in columns),
        )
        started = time.perf_counter()
        with bulk_loading.bulk_copied(
            self.db.engine, stage, bulk_loading.coerce(stage, rows)
        ):
            return time.perf_counter() - started

    def _copy(
        self,
        table: sa.Table,
        columns: Sequence[str],
        rows: Sequence[contract.Row],
    ) -> float:
        """Seconds of one COPY of the batch into a temporary table."""
        dialect = self.db.dialect
        stage = dialect.stage(table, columns)
        with self.db.engine.begin() as connection:
            dialect.create_stage(connection, stage)
            started = time.perf_counter()
            bulk_loading.copy_rows(connection, stage, rows)
            seconds = time.perf_counter() - started
            dialect.drop_stage(connection, stage)
        return seconds

    def in_lists(
        self, table: sa.Table, keys: Sequence[str]
    ) -> dict[int, float]:
        """Seconds to read every key back, by values per IN list, for the
        sizes the database's parameter limit allows."""
        limit = self.db.dialect.parameter_limit
        found: dict[int, float] = {}
        for size in IN_LISTS:
            if limit is not None and size > limit - 1:
                continue
            found[size] = self._median(
                functools.partial(self._read_by_in, table, keys, size)
            )
        return found

    def _read_by_in(
        self, table: sa.Table, keys: Sequence[str], size: int
    ) -> float:
        with self.db.engine.connect() as connection:
            started = time.perf_counter()
            for start in range(0, len(keys), size):
                connection.execute(
                    sa.select(table.c.key).where(
                        table.c.key.in_(keys[start : start + size])
                    )
                ).all()
            return time.perf_counter() - started

    def key_registry(
        self, table: sa.Table, batch_size: int
    ) -> dict[str, dict[str, float]]:
        """Seconds of the key registry's statement for a batch of new keys,
        then of known ones; on SQL Server by each staging strategy."""
        key = [c.name for c in table.primary_key.columns]
        strategies: dict[str, dialects.Dialect] = {}
        if self.db.engine.dialect.driver == "mssqlpython":
            strategies[staging.BULK_COPY] = dialects.for_name(
                self.db.name, bulk_copy_rows=1
            )
            strategies[staging.PARAMETERS] = dialects.for_name(
                self.db.name, bulk_copy_rows=batch_size + 1
            )
        else:
            strategies["one statement"] = self.db.dialect
        found: dict[str, dict[str, float]] = {"new": {}, "known": {}}
        for name, dialect in strategies.items():
            batches = [
                _key_rows(f"{name}-{i}", batch_size)
                for i in range(self.repeats)
            ]
            register = functools.partial(self._register, dialect, table, key)
            found["new"][name] = statistics.median(map(register, batches))
            found["known"][name] = self._median(
                functools.partial(register, batches[0])
            )
        return found

    def _register(
        self,
        dialect: dialects.Dialect,
        table: sa.Table,
        key: Sequence[str],
        rows: Sequence[contract.Row],
    ) -> float:
        """Seconds of the key registry's statement for `rows`."""
        started = time.perf_counter()
        dialect.insert_missing(self.db.engine, table, rows, key)
        return time.perf_counter() - started

    def _median(self, measure: Callable[[], float]) -> float:
        return statistics.median(measure() for _ in range(self.repeats))


def _scratch_table(metadata: sa.MetaData, name: str) -> sa.Table:
    """A table shaped like a CRM row: a text key, texts, a number, a time."""
    return sa.Table(
        name,
        metadata,
        sa.Column("key", sa.Unicode(64), primary_key=True),
        sa.Column("name", sa.Unicode(100)),
        sa.Column("status", sa.Unicode(10)),
        sa.Column("amount", sa.Numeric(12, 2)),
        sa.Column("updated_at", sa.DateTime),
    )


def _row(i: int) -> contract.Row:
    return {
        "key": f"K-{i:08d}",
        "name": f"Customer {i}",
        "status": "AI"[i % 2],
        "amount": decimal.Decimal(f"{i % 1000}.50"),
        "updated_at": _T0 + datetime.timedelta(seconds=i),
    }


def _key_rows(tag: str, count: int) -> list[contract.Row]:
    now = datetime.datetime.now(datetime.UTC)
    return [
        {
            "entity": "etl.calibrate",
            "surrogate_key": f"{tag}-{i}",
            "natural_key": json.dumps([f"K-{i:08d}"]),
            "registered_at": now,
        }
        for i in range(count)
    ]


def _ms(seconds: float) -> str:
    milliseconds = seconds * 1000
    return (
        f"{milliseconds:.3f} ms"
        if milliseconds < 1
        else f"{milliseconds:.1f} ms"
    )
