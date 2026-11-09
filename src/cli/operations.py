"""The commands of day-to-day operations: rejected, parked and saved data.

etl quarantine list --pipeline crm.full_load      # What was rejected, why.
etl quarantine export -o rejected.csv             # For the BAs to fix.
etl run crm.replay --param corrections=rejected.csv
etl quarantine close 12 13                        # Given up on.
etl parked list order                             # Orphans waiting.
etl parked release order 'O-3|1009'               # Given up on.
etl state show crm.full_load full_load:customer   # A watermark.
etl state reset crm.full_load full_load:customer  # Read all again (asks).
etl keys lookup customer 2f1c...                  # A key's natural key.
etl keys forget crm.customer --key-file -         # Right to erasure.
etl housekeeping                                  # Purge old records.

`state reset` and `keys forget` are recorded in the run ledger, with the
operator (`$ETL_OPERATOR`, else the user of the process): `cli.audit`.
"""

from __future__ import annotations

import argparse
from collections.abc import Iterable, Sequence
import csv
import datetime
import json
import pathlib
import sys
from typing import TextIO

from app import bootstrap
from cli import audit
from cli import erasure
from cli import exit_codes
from core import contract
from core import runtime
from core.operators import corrections
from core.runtime import names

_STATUSES = [status.value for status in contract.QuarantineStatus] + ["all"]
_FIELDS_EXPORTED_AS_COLUMNS = ("mapping", "source_key", "entity", "key")


def add_parsers(
    commands: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """Adds the operations commands to the subcommands of `etl`."""
    quarantine = commands.add_parser(
        "quarantine", help="the records rejected by quality rules"
    ).add_subparsers(dest="action", required=True)
    for action, text in (
        ("list", "list the records"),
        ("export", "write the records to a CSV file"),
    ):
        parser = quarantine.add_parser(action, help=text)
        parser.add_argument("--pipeline", help="only those of this pipeline")
        parser.add_argument("--status", choices=_STATUSES, default="open")
        if action == "export":
            parser.add_argument(
                "-o", "--output", type=pathlib.Path, required=True
            )
        else:
            parser.add_argument("--limit", type=int, default=50)
    close = quarantine.add_parser("close", help="give up on records")
    close.add_argument("ids", nargs="+")
    purge = quarantine.add_parser("purge", help="delete old records")
    purge.add_argument("--older-than-days", type=int, required=True)

    parked = commands.add_parser(
        "parked", help="the orphans waiting for their parent"
    ).add_subparsers(dest="action", required=True)
    parked.add_parser("list", help="list a queue").add_argument("queue")
    release = parked.add_parser("release", help="give up on parked records")
    release.add_argument("queue")
    release.add_argument("identities", nargs="+")

    state = commands.add_parser(
        "state", help="what pipelines saved between runs"
    ).add_subparsers(dest="action", required=True)
    for action, text in (
        ("show", "show a saved value"),
        ("reset", "forget a saved value: the next run starts over"),
    ):
        parser = state.add_parser(action, help=text)
        parser.add_argument("pipeline")
        parser.add_argument("key")
        if action == "reset":
            parser.add_argument(
                "--yes", action="store_true", help="do not ask: for scripts"
            )

    keys = commands.add_parser("keys", help="the key crosswalk").add_subparsers(
        dest="action", required=True
    )
    lookup = keys.add_parser("lookup", help="the natural key of a key")
    lookup.add_argument("entity")
    lookup.add_argument("key")
    erasure.add_arguments(
        keys.add_parser(
            "forget", help="erase the ETL's records of a natural key"
        )
    )

    housekeeping = commands.add_parser(
        "housekeeping",
        help="purge the quarantined, parked records and issues past retention",
    )
    housekeeping.add_argument(
        "--days", type=int, help="the retention (default: the settings')"
    )
    housekeeping.add_argument(
        "--queue",
        action="append",
        default=[],
        help=(
            "another parking queue to purge (repeatable); those of the"
            " modules' mappings are purged anyway"
        ),
    )


COMMANDS = ("quarantine", "parked", "state", "keys", "housekeeping")


def command(
    args: argparse.Namespace,
    application: bootstrap.Application,
    stream: TextIO,
) -> int:
    """Runs one operations command; returns the exit code."""
    with application.resources.run_scope() as resources:
        match args.command:
            case "quarantine":
                return _quarantine(args, resources, stream)
            case "parked":
                known = _queues(_catalogs(application), ())
                return _parked(args, resources, known, stream)
            case "state":
                return _state(args, resources, stream)
            case "keys" if args.action == "forget":
                return erasure.forget(
                    args, _catalogs(application), resources, stream
                )
            case "keys":
                registry = resources.get(names.KEYS, contract.IKeyRegistry)
                found = registry.natural_key(args.entity, args.key)
                if found is None:
                    stream.write(f"{args.entity} {args.key}: not registered\n")
                    return exit_codes.ExitCode.NOT_FOUND
                stream.write(f"{args.entity} {args.key}: {list(found)}\n")
            case _:
                days = (
                    args.days
                    if args.days is not None
                    else application.settings.bookkeeping.retention_days
                )
                return _housekeeping(
                    resources,
                    days,
                    _queues(_catalogs(application), args.queue),
                    stream,
                )
    return exit_codes.ExitCode.SUCCESS


def _catalogs(
    application: bootstrap.Application,
) -> list[contract.MappingCatalog]:
    """The mapping catalogs of the modules."""
    return [
        catalog
        for module in application.kernel.modules.values()
        for catalog in module.catalogs()
    ]


def _queues(
    catalogs: Iterable[contract.MappingCatalog], more: Sequence[str]
) -> list[str]:
    """The parking queues: one per mapping (its name), and `more`."""
    found = [m.name for catalog in catalogs for m in catalog.mappings]
    return list(dict.fromkeys([*found, *more]))


def _quarantine(
    args: argparse.Namespace,
    resources: runtime.Resources,
    stream: TextIO,
) -> int:
    """The `quarantine` commands."""
    quarantine = resources.get(names.QUARANTINE, contract.IQuarantine)
    if args.action == "close":
        changed = quarantine.resolve(args.ids, contract.QuarantineStatus.CLOSED)
        stream.write(f"Closed {changed} record(s)\n")
        return exit_codes.ExitCode.SUCCESS
    if args.action == "purge":
        return _purge(resources, args.older_than_days, stream)
    status = (
        None if args.status == "all" else contract.QuarantineStatus(args.status)
    )
    entries = quarantine.entries(args.pipeline, status)
    if args.action == "export":
        count = _export(entries, args.output)
        stream.write(f"Wrote {count} record(s) to {args.output}\n")
        return exit_codes.ExitCode.SUCCESS
    shown = 0
    for entry in entries:
        if shown == args.limit:
            stream.write(f"... more: --limit {args.limit * 2}\n")
            break
        fields = _record_fields(entry.record)
        stream.write(
            f"{entry.id} {entry.pipeline}"
            f" {entry.rejected_at:%Y-%m-%d %H:%M} {entry.status.value}"
            f" {fields.get('mapping') or ''}"
            f" {fields.get('source_key') or ''}  {_issues(entry)}\n"
        )
        shown += 1
    return exit_codes.ExitCode.SUCCESS


def _export(
    entries: Iterable[contract.QuarantineEntry], output: pathlib.Path
) -> int:
    """Writes the entries to a CSV file Excel opens; returns their count."""
    output.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with output.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.writer(file)
        # Prefixed so that a corrections file made from it skips them.
        prefix = corrections.EXPORT_PREFIX
        writer.writerow([
            f"{prefix}id",
            f"{prefix}pipeline",
            f"{prefix}run_id",
            f"{prefix}rejected_at",
            f"{prefix}status",
            *(
                name if name in ("mapping", "source_key") else prefix + name
                for name in _FIELDS_EXPORTED_AS_COLUMNS
            ),
            f"{prefix}issues",
            f"{prefix}record",
        ])
        for entry in entries:
            fields = _record_fields(entry.record)
            # Formula-like cells escaped; corrections read them back as is.
            writer.writerow([
                corrections.escape_cell(value)
                for value in (
                    entry.id,
                    entry.pipeline,
                    entry.run_id,
                    entry.rejected_at.isoformat(),
                    entry.status.value,
                    *(fields.get(name) for name in _FIELDS_EXPORTED_AS_COLUMNS),
                    _issues(entry),
                    json.dumps(entry.record, ensure_ascii=False),
                )
            ])
            count += 1
    return count


def _record_fields(record: object) -> dict[str, object]:
    if not isinstance(record, dict):
        return {}
    return {name: record.get(name) for name in _FIELDS_EXPORTED_AS_COLUMNS}


def _issues(entry: contract.QuarantineEntry) -> str:
    """The issues of an entry, as one line: `field: message; ...`."""
    return "; ".join(
        f"{issue.get('field') or issue.get('rule')}: {issue.get('message')}"
        for issue in entry.issues
    )


def _before(days: int) -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=days)


def _purge(
    resources: runtime.Resources,
    days: int,
    stream: TextIO,
) -> int:
    """Purges the quarantined records older than `days`."""
    before = _before(days)
    quarantine = resources.get(names.QUARANTINE, contract.IQuarantine)
    purged = quarantine.purge(before)
    stream.write(
        f"Purged {purged} quarantined record(s) rejected before"
        f" {before:%Y-%m-%d}\n"
    )
    return exit_codes.ExitCode.SUCCESS


def _housekeeping(
    resources: runtime.Resources,
    days: int,
    queues: Sequence[str],
    stream: TextIO,
) -> int:
    """Purges the quarantined records, the quality issues and the parked
    records older than `days`."""
    _purge(resources, days, stream)
    before = _before(days)
    sink = erasure.optional(resources, names.ISSUES, contract.IIssueSink)
    if sink is not None:
        purged = sink.purge(before)
        stream.write(
            f"Purged {purged} quality issue(s) found before {before:%Y-%m-%d}\n"
        )
    lot = erasure.optional(resources, names.PARKING, contract.IParkingLot)
    if lot is not None:
        released = 0
        for queue in queues:
            # Given up on, as `etl parked release` does.
            old = [
                entry
                for entry in lot.entries(queue)
                if entry.parked_at is not None and entry.parked_at < before
            ]
            lot.release(queue, old)
            released += len(old)
        stream.write(
            f"Purged {released} parked record(s) parked before"
            f" {before:%Y-%m-%d} ({', '.join(queues) or 'no queue'})\n"
        )
    return exit_codes.ExitCode.SUCCESS


def _parked(
    args: argparse.Namespace,
    resources: runtime.Resources,
    known_queues: Sequence[str],
    stream: TextIO,
) -> int:
    """The `parked` commands."""
    lot = resources.get(names.PARKING, contract.IParkingLot)
    entries = list(lot.entries(args.queue))
    if not entries and args.queue not in known_queues:
        stream.write(
            f"{args.queue}: no such queue (the mappings park to:"
            f" {', '.join(known_queues) or 'none'})\n"
        )
        return exit_codes.ExitCode.NOT_FOUND
    if args.action == "release":
        wanted = set(args.identities)
        chosen = [e for e in entries if e.identity in wanted]
        lot.release(args.queue, chosen)
        stream.write(f"Released {len(chosen)} record(s)\n")
        return (
            exit_codes.ExitCode.SUCCESS
            if len(chosen) == len(wanted)
            else exit_codes.ExitCode.NOT_FOUND
        )
    for entry in entries:
        since = f"{entry.parked_at:%Y-%m-%d %H:%M}" if entry.parked_at else "-"
        stream.write(
            f"{entry.identity} attempts={entry.attempts} since {since}"
            f"  {entry.reason}\n"
        )
    return exit_codes.ExitCode.SUCCESS


def _state(
    args: argparse.Namespace,
    resources: runtime.Resources,
    stream: TextIO,
) -> int:
    """The `state` commands: `reset` asks first, unless `--yes`, and is
    recorded in the run ledger."""
    store = resources.get(names.STATE, contract.IStateStore)
    value = store.get(args.pipeline, args.key)
    if args.action != "reset":
        stream.write(f"{json.dumps(value, default=str, indent=2)}\n")
        return exit_codes.ExitCode.SUCCESS
    if value is None:
        stream.write(f"{args.pipeline} {args.key}: nothing saved\n")
        return exit_codes.ExitCode.NOT_FOUND
    shown = json.dumps(value, default=str)
    if not args.yes and not _confirm(
        f"Reset {args.pipeline} {args.key} (now {shown})? Its next run"
        " starts over [y/N] ",
        stream,
    ):
        stream.write("Not reset: answer y, or give --yes\n")
        return exit_codes.ExitCode.INVALID
    store.delete(args.pipeline, args.key)
    audit.record(
        resources,
        "etl.state-reset",
        {"pipeline": args.pipeline, "key": args.key},
        f"reset {args.pipeline} {args.key}",
    )
    stream.write(f"Reset {args.pipeline} {args.key} (was {shown})\n")
    return exit_codes.ExitCode.SUCCESS


def _confirm(question: str, stream: TextIO) -> bool:
    """Asks a yes/no question on stdin; no when it cannot be read."""
    stream.write(question)
    stream.flush()
    try:
        answer = sys.stdin.readline()
    except (OSError, ValueError):  # Closed, or not readable.
        return False
    return answer.strip().lower() in ("y", "yes")
