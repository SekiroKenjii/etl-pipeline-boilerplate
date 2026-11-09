"""`etl cutover`: does the target hold what the source says?

    etl cutover report crm                       # Markdown, to stdout.
    etl cutover report crm --format xlsx -o var/cutover/crm.xlsx
    etl cutover report crm --format html -o var/cutover/crm.html
    etl cutover report crm --format json         # For CI.
    etl cutover report crm --counts-only         # Counts only: fast.
    etl cutover report crm --accept-rejections accepted.csv
    etl cutover report crm --save                # A rehearsal, sealed.
    etl cutover compare BEFORE.json AFTER.json   # Two rehearsals.
    etl cutover verify var/cutover/crm/X.json    # The report saved?
    etl cutover record var/cutover/crm/X.json    # One saved by 0.1.0.

`report` exits by its verdict: 0 PASSED (every row reconciles, every
missing row a parked orphan), 3 PASSED-WITH-REJECTS (rejected rows left
behind, all accepted by `--accept-rejections`), 1 FAILED (an entity empty
on both sides too, unless `--accept-empty`); 0 for a `--counts-only`
report, COUNTED, which is no verdict whatever the counts. The cutover gate
of a runbook or a CI job.

`report --save` records the digest of the sealed report in the run ledger,
as a run of `etl.cutover`; `verify` checks a report against it: an edited
report sealed again has a digest the ledger never recorded. Reports saved
before the ledger kept them (0.1.0: no provenance) are in none: `record`
records them, with who did, and `verify` then says ADOPTED rather than OK.
"""

from __future__ import annotations

import argparse
from collections.abc import Generator, Iterable, Mapping, Sequence
import contextlib
import dataclasses
import json
import pathlib
import signal
import socket
import threading
import types
from typing import TextIO
import urllib.parse

from app import bootstrap
from cli import audit
from cli import exit_codes
import config
from core import contract
from core import cutover
from core import runtime
from core.cutover import acceptance
from core.cutover import checksum
from core.cutover import history
from core.runtime import names
from infra.database import relational
from infra.reports import xlsx

_FORMATS = ("markdown", "json", "xlsx", "html")
REHEARSALS = pathlib.Path("var/cutover")
OFFLINE = ("compare",)
RECORD = "etl.cutover"
_ADOPTED = "adopted"
_EXIT_CODES = {
    "PASSED": exit_codes.ExitCode.SUCCESS,
    "PASSED-WITH-REJECTS": exit_codes.ExitCode.PARTIAL_SUCCESS,
    "COUNTED": exit_codes.ExitCode.SUCCESS,
    "FAILED": exit_codes.ExitCode.FAILED,
}


def add_parser(
    commands: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """Adds the `cutover` command to the subcommands of `etl`."""
    parser = commands.add_parser("cutover", help="reconcile source and target")
    actions = parser.add_subparsers(dest="action", required=True)
    report = actions.add_parser("report", help="report on a module's entities")
    report.add_argument("module", help="the module, e.g. crm")
    report.add_argument("--format", choices=_FORMATS, default="markdown")
    report.add_argument(
        "-o", "--output", type=pathlib.Path, help="the file to write"
    )
    report.add_argument(
        "--counts-only",
        action="store_true",
        help="count the rows in the databases, compare none: no verdict",
    )
    report.add_argument(
        "--samples",
        type=int,
        default=20,
        help="differences shown per entity (default: %(default)s)",
    )
    report.add_argument(
        "--buckets",
        type=int,
        help=(
            "partitions of the keys in the diff (default: one per"
            f" {cutover.ROWS_PER_PARTITION} target rows)"
        ),
    )
    report.add_argument(
        "--accept-rejections",
        type=pathlib.Path,
        metavar="CSV",
        help=(
            "the rejected rows the BAs accept to leave behind: a CSV of"
            " entity, reason, rows (reason * for any); the others fail the"
            " report"
        ),
    )
    report.add_argument(
        "--accept-empty",
        action="append",
        default=[],
        metavar="ENTITY",
        help=(
            "an entity the BAs accept empty on both sides (* for any); the"
            " others fail the report, as a wrong source table or filter"
            " would leave them"
        ),
    )
    report.add_argument(
        "--time-resolution",
        type=int,
        default=checksum.MICROSECONDS,
        metavar="TICKS",
        help=(
            "ticks per second timestamps compare at, when a side stores"
            " them coarser: 300 for SQL Server datetime, 1 for whole seconds"
            " (default: %(default)s, microseconds)"
        ),
    )
    report.add_argument(
        "--save",
        nargs="?",
        const=REHEARSALS,
        type=pathlib.Path,
        metavar="DIR",
        help=(
            "also keep the report, sealed with its SHA-256, as"
            " DIR/<module>/<time>-<digest>.json, and record the digest in"
            " the run ledger (default DIR: %(const)s)"
        ),
    )
    compare = actions.add_parser("compare", help="compare two rehearsals")
    compare.add_argument("before", type=pathlib.Path)
    compare.add_argument("after", type=pathlib.Path)
    verify = actions.add_parser(
        "verify",
        help="check that saved reports are those recorded in the run ledger",
    )
    verify.add_argument("reports", type=pathlib.Path, nargs="+")
    adopt = actions.add_parser(
        "record",
        help=(
            "record in the run ledger reports saved before it kept them"
            " (0.1.0), so that verify knows them"
        ),
    )
    adopt.add_argument("reports", type=pathlib.Path, nargs="+")


def offline(args: argparse.Namespace, stream: TextIO) -> int:
    """Runs `etl cutover compare`, on report files only."""
    reports = _load([args.before, args.after], stream)
    if reports is None:
        return exit_codes.ExitCode.INVALID
    tampered = [p for p, data in reports.items() if not cutover.verify(data)]
    for path in tampered:
        stream.write(f"WARNING {path} changed since it was sealed\n")
    try:
        comparison = cutover.compare(reports[args.before], reports[args.after])
    except ValueError as exc:
        stream.write(f"FAILED {exc}\n")
        return exit_codes.ExitCode.INVALID
    stream.write(cutover.comparison_markdown(comparison))
    stream.write(
        "\nCompared as they are: `etl cutover verify` tells whether each is"
        " the report saved.\n"
    )
    return (
        exit_codes.ExitCode.FAILED if tampered else exit_codes.ExitCode.SUCCESS
    )


def command(
    args: argparse.Namespace,
    application: bootstrap.Application,
    stream: TextIO,
) -> int:
    """Runs `etl cutover report`, `verify` or `record`; returns the exit
    code.

    Raises:
        SystemExit: On SIGTERM or SIGINT (`ExitCode.CANCELLED`).
    """
    with _exit_on_signals():
        if args.action == "verify":
            return _verify(args.reports, application, stream)
        if args.action == "record":
            return _adopt(args.reports, application, stream)
        return _report(args, application, stream)


@contextlib.contextmanager
def _exit_on_signals() -> Generator[None]:
    """Turns SIGTERM and SIGINT into a `SystemExit`, in the main thread.

    The diff spills source rows, personal data included, to a temporary
    directory removed as the stack unwinds: killed by SIGTERM's default
    action, the process would leave it behind. Signals that follow the first
    are ignored until then, as one would cut the removal short.
    """
    if threading.current_thread() is not threading.main_thread():
        yield
        return
    signals = (signal.SIGTERM, signal.SIGINT)

    def stop(signum: int, frame: types.FrameType | None) -> None:
        del signum, frame  # Unused.
        for sig in signals:
            signal.signal(sig, signal.SIG_IGN)
        raise SystemExit(exit_codes.ExitCode.CANCELLED)

    previous = {sig: signal.signal(sig, stop) for sig in signals}
    try:
        yield
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def _report(
    args: argparse.Namespace,
    application: bootstrap.Application,
    stream: TextIO,
) -> int:
    """Runs `etl cutover report`; returns the exit code."""
    module = application.kernel.modules.get(args.module)
    if module is None:
        stream.write(
            f"FAILED no module {args.module!r}:"
            f" {sorted(application.kernel.modules)}\n"
        )
        return exit_codes.ExitCode.INVALID
    if args.format == "xlsx" and args.output is None:
        stream.write("FAILED --format xlsx needs --output\n")
        return exit_codes.ExitCode.INVALID
    try:
        accepted = _accepted(args)
    except (OSError, ValueError) as exc:
        stream.write(f"FAILED {exc}\n")
        return exit_codes.ExitCode.INVALID
    with application.resources.run_scope() as resources:
        ledger = _optional(resources, names.LEDGER, contract.IRunLedger)
        if args.save is not None and ledger is None:
            stream.write(
                "FAILED --save records the report in the run ledger: the"
                " settings give none\n"
            )
            return exit_codes.ExitCode.INVALID
        reconciliations = module.reconciliations(resources)
        if not reconciliations:
            stream.write(f"FAILED {module.name} declares no reconciliation\n")
            return exit_codes.ExitCode.INVALID
        report = cutover.build(
            module.name,
            reconciliations,
            parking=_optional(resources, names.PARKING, contract.IParkingLot),
            ledger=ledger,
            counted_only=args.counts_only,
            buckets=args.buckets,
            samples=args.samples,
            accepted=accepted,
            resolution=args.time_resolution,
            provenance=_provenance(
                application, module, reconciliations, args, accepted
            ),
            spill_rows=application.settings.tuning.effective().spill_rows.value,
        )
        sealed = cutover.seal(report.as_dict())
        if args.save is not None and ledger is not None:
            path = _save(sealed, report, args.save)
            _record(ledger, sealed, path)
            stream.write(
                f"Saved {path} (SHA-256 {sealed['digest']}), recorded in the"
                " run ledger\n"
            )
    _write(report, args.format, args.output, stream, str(sealed["digest"]))
    return _EXIT_CODES[report.verdict]


def _accepted(
    args: argparse.Namespace,
) -> acceptance.AcceptedRejections | None:
    """What the BAs accept: rejected rows (`--accept-rejections`), empty
    entities (`--accept-empty`); None for nothing.

    Raises:
        OSError: If the CSV cannot be read.
        ValueError: If it is not such a CSV.
    """
    accepted = (
        acceptance.AcceptedRejections.from_csv(args.accept_rejections)
        if args.accept_rejections is not None
        else None
    )
    if not args.accept_empty:
        return accepted
    return dataclasses.replace(
        accepted or acceptance.AcceptedRejections({}),
        empty=frozenset(args.accept_empty),
    )


def _save(
    sealed: Mapping[str, object],
    report: cutover.CutoverReport,
    directory: pathlib.Path,
) -> pathlib.Path:
    """Writes the sealed report under `directory`; returns its path."""
    stamp = report.generated_at.strftime("%Y%m%dT%H%M%SZ")
    # Sorted by time; the digest keeps two reports of a second apart.
    path = (
        directory / report.module / f"{stamp}-{str(sealed['digest'])[:8]}.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(sealed, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return path


def _record(
    ledger: contract.IRunLedger,
    sealed: Mapping[str, object],
    path: pathlib.Path,
    *,
    adopted: bool = False,
) -> None:
    """Records a saved report in the run ledger: a run of `etl.cutover`
    whose id is the report's digest; `adopted` when it is recorded after it
    was saved (`etl cutover record`)."""
    run = contract.ETLRunInfo(
        pipeline_name=RECORD, run_id=str(sealed[history.DIGEST])
    )
    result = contract.PipelineResult(pipeline_name=RECORD, run_id=run.run_id)
    found = history.verdict(sealed)
    how = "recorded from" if adopted else "saved as"
    result.complete(
        contract.ETLStatus.SUCCESS,
        f"{sealed.get('module')}: {found}, {how} {path.name}",
    )
    meta = {
        "module": str(sealed.get("module")),
        "verdict": found,
        "generated_at": str(sealed.get("generated_at")),
        "file": path.name,
        "user": audit.operator(),
    }
    if adopted:
        meta[_ADOPTED] = "true"
    ledger.start(run, socket.gethostname(), meta)
    ledger.finish(result)


def _adopt(
    paths: Sequence[pathlib.Path],
    application: bootstrap.Application,
    stream: TextIO,
) -> int:
    """Records reports saved before the run ledger kept them; none when one
    of them cannot be."""
    reports = _load(paths, stream)
    if reports is None:
        return exit_codes.ExitCode.INVALID
    with application.resources.run_scope() as resources:
        ledger = _optional(resources, names.LEDGER, contract.IRunLedger)
        if ledger is None:
            stream.write(
                "FAILED the reports are recorded in the run ledger: the"
                " settings give none\n"
            )
            return exit_codes.ExitCode.INVALID
        checked = {
            path: _adoptable(ledger, data) for path, data in reports.items()
        }
        refused = [
            path
            for path, (status, _) in checked.items()
            if status not in ("OK", "ADOPTED", "RECORDED")
        ]
        for path, (status, detail) in checked.items():
            if status == "RECORDED" and not refused:
                _record(ledger, reports[path], path, adopted=True)
                detail = f": by {audit.operator()}"
            elif status == "RECORDED":
                status, detail = "SKIPPED", ": another report is refused"
            stream.write(f"{status:<10} {path}{detail}\n")
    if refused:
        stream.write(f"FAILED nothing recorded: {len(refused)} refused\n")
        return exit_codes.ExitCode.FAILED
    return exit_codes.ExitCode.SUCCESS


def _adoptable(
    ledger: contract.IRunLedger, report: Mapping[str, object]
) -> tuple[str, str]:
    """Whether a report can be recorded: RECORDED (to be), OK or ADOPTED
    (it is), CHANGED or REFUSED; and why."""
    status, detail = _check(ledger, report)
    if status != "UNRECORDED":
        return status, detail
    # This version records each report it saves: one of its reports that
    # the ledger lacks was edited and sealed again, and is never adopted.
    if "provenance" in report:
        return "REFUSED", (
            ": made by this version, so recorded when saved: this one never"
            " was. Run the report again with --save"
        )
    if ledger.get(str(report.get(history.DIGEST))) is not None:
        return "REFUSED", ": the run ledger has another run of its digest"
    return "RECORDED", ""


def _verify(
    paths: Sequence[pathlib.Path],
    application: bootstrap.Application,
    stream: TextIO,
) -> int:
    """Checks each report: its content has its digest, and the ledger
    recorded that digest when it was saved."""
    reports = _load(paths, stream)
    if reports is None:
        return exit_codes.ExitCode.INVALID
    with application.resources.run_scope() as resources:
        ledger = _optional(resources, names.LEDGER, contract.IRunLedger)
        if ledger is None:
            stream.write(
                "FAILED the reports are checked against the run ledger: the"
                " settings give none\n"
            )
            return exit_codes.ExitCode.INVALID
        failed = False
        for path, data in reports.items():
            status, detail = _check(ledger, data)
            failed = failed or status not in ("OK", "ADOPTED")
            stream.write(f"{status:<10} {path}{detail}\n")
    return exit_codes.ExitCode.FAILED if failed else exit_codes.ExitCode.SUCCESS


def _check(
    ledger: contract.IRunLedger, report: Mapping[str, object]
) -> tuple[str, str]:
    """Whether a report is the one saved: OK, ADOPTED (recorded after it was
    saved), CHANGED, or UNRECORDED; and what produced it."""
    if not cutover.verify(report):
        return "CHANGED", ": its content is not that of its digest"
    digest = str(report.get(history.DIGEST))
    record = ledger.get(digest)
    if (
        record is None
        or record.pipeline != RECORD
        or record.meta.get("module") != report.get("module")
        or record.meta.get("generated_at") != report.get("generated_at")
    ):
        return "UNRECORDED", ": the run ledger never recorded its digest"
    if record.meta.get(_ADOPTED):
        return "ADOPTED", (
            f": {history.verdict(report)} {report.get('module')},"
            f" {report.get('generated_at')}, recorded by"
            f" {record.meta.get('user', '?')} on"
            f" {record.started_at:%Y-%m-%d} (etl cutover record): saved"
            " before the run ledger kept reports, only its seal vouches for it"
        )
    provenance = _mapping(report.get("provenance"))
    module = _mapping(provenance.get("module"))
    produced = ", ".join(
        f"{name} {value}"
        for name, value in (
            ("env", provenance.get("env")),
            ("build", provenance.get("build")),
            ("mappings", module.get("mappings")),
            ("keys", module.get("keys")),
            ("canonical form", provenance.get("canonical_form", 1)),
        )
        if value
    )
    return "OK", (
        f": {history.verdict(report)} {report.get('module')},"
        f" {report.get('generated_at')}, saved by"
        f" {record.meta.get('user', '?')}"
        + (f"; {produced}" if produced else "")
    )


def _mapping(value: object) -> Mapping[str, object]:
    """`value` if it is a JSON object, else an empty one."""
    if isinstance(value, dict):
        return value
    return {}


def _load(
    paths: Iterable[pathlib.Path], stream: TextIO
) -> dict[pathlib.Path, dict[str, object]] | None:
    """The reports of the files; None, said, if one cannot be read."""
    reports: dict[pathlib.Path, dict[str, object]] = {}
    for path in paths:
        try:
            data: object = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            stream.write(f"FAILED {path}: {exc}\n")
            return None
        if not isinstance(data, dict):
            stream.write(f"FAILED {path}: not a cutover report\n")
            return None
        reports[path] = data
    return reports


def _provenance(
    application: bootstrap.Application,
    module: runtime.Module,
    reconciliations: Sequence[contract.Reconciliation],
    args: argparse.Namespace,
    accepted: acceptance.AcceptedRejections | None,
) -> dict[str, object]:
    """What produces the report: never a credential."""
    settings = application.settings
    try:
        fingerprint: dict[str, str] = dict(module.fingerprint())
    # The report says so, rather than not being made.
    except Exception as exc:  # pylint: disable=broad-exception-caught
        fingerprint = {"error": type(exc).__name__}
    return {
        **bootstrap.run_meta(settings),
        "user": audit.operator(),
        "module": fingerprint,
        "databases": {
            "source": _databases(
                settings, (r.source_database for r in reconciliations)
            ),
            "target": _databases(
                settings, (r.target_database for r in reconciliations)
            ),
        },
        "options": {
            "counts_only": args.counts_only,
            "buckets": args.buckets,
            "samples": args.samples,
            "time_resolution": args.time_resolution,
        },
        "accepted_rejections": accepted.as_dict() if accepted else None,
        "canonical_form": checksum.FORM,
    }


def _databases(
    settings: config.AppSettings, found: Iterable[str | None]
) -> list[dict[str, object]]:
    """Where the named databases are: dialect, host, port and database."""
    return [
        _database(name, settings.databases.get(name))
        for name in sorted({name for name in found if name})
    ]


def _database(
    name: str, database: config.DatabaseSettings | None
) -> dict[str, object]:
    """Where a database is, without its credentials."""
    if database is None:
        return {"name": name, "error": "not in the settings"}
    url = database.url.get_secret_value()
    if database.is_mongodb:
        parts = urllib.parse.urlsplit(url)
        return {
            "name": name,
            "dialect": "mongodb",
            "host": parts.hostname,
            "port": parts.port,
            "database": parts.path.lstrip("/") or None,
        }
    try:
        spec = relational.parse_connection_string(url, dialect=database.dialect)
    except relational.ConnectionStringError:
        return {"name": name, "error": "its connection string does not parse"}
    return {
        "name": name,
        "dialect": spec.url.get_backend_name(),
        "host": spec.url.host,
        "port": spec.url.port,
        "database": spec.url.database,
    }


def _write(
    report: cutover.CutoverReport,
    output_format: str,
    output: pathlib.Path | None,
    stream: TextIO,
    digest: str,
) -> None:
    """Writes the report to `output`, or to the stream."""
    if output_format == "xlsx" and output is not None:
        xlsx.write_cutover_report(report.as_dict(), output)
        text = None
    elif output_format == "json":
        text = (
            json.dumps(
                {**report.as_dict(), "digest": digest},
                indent=2,
                ensure_ascii=False,
            )
            + "\n"
        )
    elif output_format == "html":
        text = cutover.to_html(report, digest)
    else:
        text = cutover.to_markdown(report)
    if output is None and text is not None:
        stream.write(text)
        return
    if output is not None and text is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text, encoding="utf-8")
    stream.write(f"{report.verdict} {report.module}: wrote {output}\n")


def _optional[T](
    resources: runtime.Resources, name: str, kind: type[T]
) -> T | None:
    """The resource, or None when the settings provide none."""
    try:
        return resources.get(name, kind)
    except contract.PipelineConfigError:
        return None
