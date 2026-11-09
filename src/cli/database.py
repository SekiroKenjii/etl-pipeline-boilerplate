"""`etl-db`: the database tooling, for DBAs, BAs and devs. Needs `sql`.

    etl-db upgrade                # Create the ETL tables, or upgrade them.
    etl-db check                  # Exit 5 if they need `upgrade`.
    etl-db current                # Tell their revision.
    etl-db ddl --dialect mssql    # The SQL creating them, for a DBA...
    etl-db ddl --dialect mssql --from 0002   # ...or upgrading them.
    etl-db discover legacy        # Suggest the keys and foreign keys.
    etl-db url "Server=...;..."   # Show how a connection string translates.

The database is a SQLAlchemy URL, the connection string of a .NET (ADO),
ODBC, OLE DB or Java (JDBC) application as it is, or a MongoDB URI
(`upgrade` and `discover`; the schema is the database), given by `--url`,
else `$ETL_DATABASE_URL`: keep passwords out of the command line.

`check` exits 0 when the ETL tables are at the latest revision, 5
(NOT_READY) when `upgrade` would bring them there (none yet, an older
revision, or tables created before the version table), 1 when it cannot:
their revision is unknown to this release, or cannot be told.
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
import json
import os
import pathlib
import sys
from typing import TextIO

from cli import exit_codes
from core import contract
from core import keys
from infra.database import mongodb
from infra.database import relational
from infra.database.relational import migrations

_DESCRIPTION = "The database tooling: ETL tables, relationship discovery."
_URL_VARIABLE = "ETL_DATABASE_URL"


def main(argv: Sequence[str] | None = None, out: TextIO | None = None) -> int:
    """Runs the command line.

    Args:
        argv: The arguments; those of the process by default.
        out: Where to write the output; stdout by default.

    Returns:
        The exit code (`exit_codes.ExitCode`): 0 on success, 1 when the
        database fails, 2 for bad arguments; `check`: 5 when the ETL tables
        need `upgrade`.
    """
    stream = out if out is not None else sys.stdout
    args = _parser().parse_args(argv)

    if args.command == "ddl":
        return _ddl(args, stream)
    if args.command == "url":
        return _url(args.connection, args.dialect, stream)

    url = args.url if args.url is not None else os.environ.get(_URL_VARIABLE)
    if not url:
        stream.write(f"FAILED give --url, or set ${_URL_VARIABLE}\n")
        return exit_codes.ExitCode.INVALID
    if url.startswith(("mongodb://", "mongodb+srv://")):
        return _mongodb(args, url, stream)
    try:
        db = relational.Database(url, dialect=args.dialect)
    except relational.ConnectionStringError as exc:
        stream.write(f"FAILED {exc}\n")
        return exit_codes.ExitCode.INVALID
    try:
        if args.command in _TABLES:
            return _tables(args, db, stream)
        return _discover(args, relational.RelationalSchemaInspector(db), stream)
    except contract.PipelineConfigError as exc:
        stream.write(f"FAILED {exc}\n")
        return exit_codes.ExitCode.FAILED
    except contract.PipelineError as exc:
        stream.write(f"FAILED {exc}\n")
        return exit_codes.ExitCode.FAILED
    finally:
        db.engine.dispose()


def _parser() -> argparse.ArgumentParser:
    """Returns the parser of the command line and its subcommands."""
    parser = argparse.ArgumentParser(prog="etl-db", description=_DESCRIPTION)
    commands = parser.add_subparsers(dest="command", required=True)

    for name, help_text in (
        ("ddl", "print the SQL creating or upgrading the ETL tables"),
        ("upgrade", "create the ETL tables, or bring them to the latest"),
        ("init", "the same as upgrade"),
        ("current", "tell the revision of the ETL tables"),
        ("check", "exit 5 if the ETL tables need upgrade"),
    ):
        command = commands.add_parser(name, help=help_text)
        command.add_argument(
            "--schema", help="the schema of the ETL tables (default: none)"
        )
        command.add_argument(
            "--prefix",
            default=relational.DEFAULT_PREFIX,
            help="the prefix of their names (default: %(default)s)",
        )
        if name == "ddl":
            command.add_argument(
                "--dialect",
                required=True,
                help="the database: mssql, oracle, postgresql, mysql...",
            )
            command.add_argument(
                "--from",
                dest="start",
                metavar="REVISION",
                choices=migrations.REVISIONS,
                help=(
                    "upgrade tables at this revision (`etl-db current`"
                    " tells it), rather than create them"
                ),
            )
            command.add_argument(
                "--legacy",
                action="store_true",
                help=(
                    "the tables have no version table (created before it):"
                    " record the revision of --from first"
                ),
            )
        else:
            _add_url(command)

    url = commands.add_parser(
        "url", help="show how a connection string translates (no connection)"
    )
    url.add_argument("connection", help="the connection string")
    _add_dialect(url)

    discover = commands.add_parser(
        "discover",
        help="suggest the keys and foreign keys of a schema, for review",
    )
    discover.add_argument("schema", help="the schema to inspect")
    _add_url(discover)
    discover.add_argument(
        "--tables",
        metavar="GLOB",
        help="only the tables whose name matches, e.g. 'CUST*'",
    )
    discover.add_argument(
        "--min-coverage",
        type=float,
        default=0.9,
        help=(
            "the share of values found in the parent to suggest a foreign"
            " key (default: %(default)s)"
        ),
    )
    discover.add_argument(
        "--sample-rows",
        type=int,
        help="profile candidate keys on that many rows per table",
    )
    discover.add_argument(
        "--all-pairs",
        action="store_true",
        help="check every pair of columns, not only matching names (slow)",
    )
    discover.add_argument(
        "--json",
        type=pathlib.Path,
        metavar="PATH",
        help="also write the report as JSON",
    )
    return parser


def _add_url(command: argparse.ArgumentParser) -> None:
    command.add_argument(
        "--url",
        help=(
            "a SQLAlchemy URL, or an ADO.NET, ODBC, JDBC or libpq connection"
            f" string (default: ${_URL_VARIABLE})"
        ),
    )
    _add_dialect(command)


def _add_dialect(command: argparse.ArgumentParser) -> None:
    command.add_argument(
        "--dialect",
        help="the database, when the connection string does not tell it",
    )


_TABLES = ("upgrade", "init", "current", "check")


def _ddl(args: argparse.Namespace, stream: TextIO) -> int:
    """Prints the SQL creating the ETL tables, or upgrading them."""
    try:
        sql = migrations.ddl(
            args.dialect,
            args.schema,
            args.prefix,
            start=args.start,
            legacy=args.legacy,
        )
    except (ValueError, contract.PipelineConfigError) as exc:
        stream.write(f"FAILED {exc}\n")
        return exit_codes.ExitCode.INVALID
    stream.write(
        sql or "-- The ETL tables are at the latest revision: nothing to run.\n"
    )
    return exit_codes.ExitCode.SUCCESS


def _tables(
    args: argparse.Namespace, db: relational.Database, stream: TextIO
) -> int:
    """Upgrades the ETL tables, or tells where they stand."""
    if args.command in ("upgrade", "init"):
        before = migrations.upgrade(db, args.schema, args.prefix)
        if before.up_to_date:
            done = "nothing to do"
        elif before.empty:
            done = "created"
        elif before.revision is None:
            done = f"upgraded from {before.detected} (no version table)"
        else:
            done = f"upgraded from {before.revision}"
        stream.write(f"ETL tables at revision {migrations.HEAD}: {done}\n")
        return exit_codes.ExitCode.SUCCESS
    found = migrations.status(db, args.schema, args.prefix)
    stream.write(f"{found.revision or 'none'}\n{found.message()}\n")
    if args.command == "current" or found.up_to_date:
        return exit_codes.ExitCode.SUCCESS
    if found.known:
        return exit_codes.ExitCode.NOT_READY
    return exit_codes.ExitCode.FAILED


def _url(text: str, dialect: str | None, stream: TextIO) -> int:
    """Shows how a connection string translates; passwords stay hidden."""
    try:
        spec = relational.parse_connection_string(text, dialect=dialect)
    except relational.ConnectionStringError as exc:
        stream.write(f"FAILED {exc}\n")
        return exit_codes.ExitCode.INVALID
    stream.write(f"{spec.url}\n")
    stream.write(f"  from: {spec.source}\n")
    if spec.connect_args:
        stream.write(f"  connect_args: {spec.connect_args}\n")
    if spec.guessed:
        stream.write(
            "  database: assumed SQL Server, nothing told it; pass --dialect"
            " if wrong\n"
        )
    if spec.ignored:
        stream.write(f"  ignored: {', '.join(spec.ignored)}\n")
    return exit_codes.ExitCode.SUCCESS


def _mongodb(args: argparse.Namespace, url: str, stream: TextIO) -> int:
    """Runs `upgrade` or `discover` on MongoDB; the schema is the database.

    Its collections have no revision: `upgrade` creates what is missing,
    indexes included.
    """
    try:
        db = mongodb.MongoDatabase(
            url, database=getattr(args, "schema", None) or None
        )
    except contract.PipelineError as exc:
        stream.write(f"FAILED {exc}\n")
        return exit_codes.ExitCode.INVALID
    try:
        if args.command in ("upgrade", "init"):
            mongodb.create_etl_collections(db, args.prefix)
            stream.write("ETL collections ready\n")
            return exit_codes.ExitCode.SUCCESS
        if args.command in ("current", "check"):
            stream.write(
                "MongoDB: no revision; `etl-db upgrade` creates the missing"
                " collections and indexes\n"
            )
            return exit_codes.ExitCode.SUCCESS
        return _discover(args, mongodb.MongoSchemaInspector(db), stream)
    except contract.PipelineError as exc:
        stream.write(f"FAILED {exc}\n")
        return exit_codes.ExitCode.FAILED
    finally:
        db.client.close()


def _discover(
    args: argparse.Namespace,
    inspector: contract.ISchemaInspector,
    stream: TextIO,
) -> int:
    """Prints the keys and relationships of a schema; writes them as JSON."""
    report = keys.discover_relationships(
        inspector,
        args.schema,
        min_coverage=args.min_coverage,
        sample_rows=args.sample_rows,
        match_names=not args.all_pairs,
        tables=args.tables,
    )

    stream.write(report.to_text())
    if args.json is not None:
        args.json.write_text(
            json.dumps(report.as_dict(), indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        stream.write(f"Wrote {args.json}\n")
    return exit_codes.ExitCode.SUCCESS
