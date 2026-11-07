"""`etl`: runs the application, for operators and schedulers.

    etl run crm.full_load               # Run a pipeline once.
    etl run crm.sync --dry-run          # Read and check, load nothing.
    etl run crm.full_load --resume      # Resume the last run, if it failed.
    etl run crm.full_load --step 'customer*'    # One entity again.
    etl run crm.full_load --label $JOB --resume # A scheduler's retry.
    etl list                            # The pipelines of the modules.
    etl plan crm.full_load              # Its steps, in the order they run.
    etl check                           # Can every resource be reached?
    etl runs crm.full_load              # The last runs, from the ledger.
    etl runs --run 3f2a...              # The steps of a run.
    etl runs crm.full_load --format json        # For scripts.
    etl config show                     # The settings, secrets hidden.
    etl new-module billing              # The skeleton of a new module.
    etl cutover report crm              # Reconcile the target with the source.
    etl quarantine list                 # Rejected records; also parked,
                                        # state, keys, housekeeping
                                        # (`cli.operations`).

The environment is `--env`, else `$ETL_ENV`, else `dev`; settings come from
`config/base.toml`, `config/<env>.toml` and `ETL__*` variables. `run`
exits with what schedulers act on: 0 success, 1 failed (retry), 2 invalid,
3 partial success (0 with `--partial-ok`), 4 already running, 130
cancelled; `check` 5 when a resource is not ready. Every code:
`cli.exit_codes`.
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
import datetime
import io
import json
import pathlib
import sys
from typing import TextIO

from app import bootstrap
from cli import cutover
from cli import exit_codes
from cli import operations
from cli import scaffold
import config
from config import secrets as config_secrets
from core import contract
from core import runtime
from core.runtime import names

_DESCRIPTION = "Runs the ETL application."
_FORMATS = ("text", "json")


def main(argv: Sequence[str] | None = None, out: TextIO | None = None) -> int:
    """Runs the command line.

    Args:
        argv: The arguments; those of the process by default.
        out: Where to write the output; stdout by default.

    Returns:
        The exit code (`exit_codes.ExitCode`).
    """
    stream = out if out is not None else sys.stdout
    args = _parser().parse_args(argv)
    try:
        if args.command == "new-module":
            try:
                written = scaffold.scaffold(args.name, args.root)
            except ValueError as exc:
                stream.write(f"FAILED {exc}\n")
                return exit_codes.ExitCode.INVALID
            for path in written:
                stream.write(f"Wrote {path}\n")
            stream.write(scaffold.SNIPPET.format(name=args.name))
            return exit_codes.ExitCode.SUCCESS
        if args.command == "cutover" and args.action in cutover.OFFLINE:
            return cutover.offline(args, stream)
        if args.command == "config":
            # Placeholders unresolved: showing them needs no secret manager.
            with config_secrets.unresolved():
                settings = config.load_settings(args.env, directory=args.config)
            stream.write(
                json.dumps(settings.redacted(), indent=2, ensure_ascii=False)
            )
            stream.write("\n")
            return exit_codes.ExitCode.SUCCESS
        # Written last, after the run's logs: `... --format json | tail -n 1`.
        last = (
            io.StringIO()
            if args.command == "run" and args.format == "json"
            else None
        )
        with bootstrap.bootstrap(
            args.env, directory=args.config, logs=args.command == "run"
        ) as application:
            code = _command(args, application, stream if last is None else last)
        if last is not None:
            stream.write(last.getvalue())
        return code
    except (config.ConfigError, contract.PipelineConfigError) as exc:
        if args.command == "run" and args.format == "json":
            _write_json(
                stream,
                _summary(args.pipeline, exit_codes.ExitCode.INVALID, str(exc)),
            )
        else:
            stream.write(f"FAILED {exc}\n")
        return exit_codes.ExitCode.INVALID


def _command(
    args: argparse.Namespace,
    application: bootstrap.Application,
    stream: TextIO,
) -> int:
    """Runs one command on the application."""
    kernel = application.kernel
    match args.command:
        case "list":
            for name in kernel.pipelines():
                stream.write(f"{name}\n")
        case "plan":
            _plan(kernel, args.pipeline, stream)
        case "check":
            return _check(kernel, stream)
        case "runs" if args.run:
            return _run_steps(application, args.run, stream, args.format)
        case "runs":
            return _runs(
                application, args.pipeline, args.limit, stream, args.format
            )
        case "cutover":
            return cutover.command(args, application, stream)
        case command if command in operations.COMMANDS:
            return operations.command(args, application, stream)
        case _:
            return _run(kernel, args, stream)
    return exit_codes.ExitCode.SUCCESS


def _plan(kernel: runtime.Kernel, pipeline: str, stream: TextIO) -> None:
    """`etl plan`: the steps, in the order they may run."""
    for step in kernel.plan(pipeline):
        after = (
            f"  after {', '.join(step.dependencies)}"
            if step.dependencies
            else ""
        )
        stream.write(f"{step.name}{after}\n")


def _check(kernel: runtime.Kernel, stream: TextIO) -> int:
    """`etl check`: whether every resource that can be checked works."""
    report = kernel.check()
    for name, status in report.items():
        stream.write(
            f"{status:<4} {name}\n"
            if status == "ok"
            else f"FAIL {name}: {status}\n"
        )
    if any(status != "ok" for status in report.values()):
        return exit_codes.ExitCode.NOT_READY
    return exit_codes.ExitCode.SUCCESS


def _run(
    kernel: runtime.Kernel, args: argparse.Namespace, stream: TextIO
) -> int:
    """`etl run`: runs a pipeline once, and tells how it went."""
    outcome = kernel.run(
        args.pipeline,
        since=args.since,
        until=args.until,
        dry_run=args.dry_run,
        params=dict(args.param),
        steps=args.step,
        resume=args.resume or False,
        label=args.label,
        rekey=args.rekey,
    )
    result = outcome.result
    code = exit_codes.of_run(outcome.exit_code)
    if args.partial_ok and code is exit_codes.ExitCode.PARTIAL_SUCCESS:
        code = exit_codes.ExitCode.SUCCESS
    if args.format == "json":
        _write_json(
            stream,
            _summary(
                args.pipeline,
                code,
                outcome.message,
                result,
                outcome.resumed_from,
            ),
        )
    elif result is None:
        stream.write(f"{args.pipeline}: {outcome.message}\n")
    else:
        stats = result.stats
        stream.write(
            f"{args.pipeline} {result.status.value}: {stats.total}"
            f" read, {stats.created} created, {stats.updated} updated,"
            f" {stats.deleted} deleted, {stats.rejected} rejected (run"
            f" {result.run_id}"
            + (
                f", resuming {outcome.resumed_from}"
                if outcome.resumed_from
                else ""
            )
            + ")\n"
        )
        if result.status is contract.ETLStatus.FAILED and result.message:
            stream.write(f"  {result.message}\n")
    return code


def _summary(
    pipeline: str,
    code: int,
    message: str | None,
    result: contract.PipelineResult | None = None,
    resumed_from: str | None = None,
) -> dict[str, object]:
    """How a run ended, as `etl run --format json` tells it."""
    return {
        "pipeline": pipeline,
        "run_id": result.run_id if result else None,
        "status": result.status.value if result else None,
        "exit_code": int(code),
        "resumed_from": resumed_from,
        "stats": result.stats.as_dict() if result else {},
        "message": result.message if result else message,
    }


def _write_json(stream: TextIO, data: object) -> None:
    """Writes `data` as one line of JSON."""
    stream.write(json.dumps(data, ensure_ascii=False) + "\n")


def _record(record: contract.RunRecord) -> dict[str, object]:
    """A run of the ledger, as `etl runs --format json` tells it."""
    return {
        "run_id": record.run_id,
        "pipeline": record.pipeline,
        "status": record.status.value,
        "started_at": record.started_at.isoformat(),
        "completed_at": (
            record.completed_at.isoformat() if record.completed_at else None
        ),
        "host": record.host,
        "dry_run": record.dry_run,
        "stats": dict(record.stats),
        "message": record.message,
        "meta": dict(record.meta),
    }


def _step(step: contract.StepRecord) -> dict[str, object]:
    """A step of a run, as `etl runs --run --format json` tells it."""
    return {
        "step": step.step,
        "status": step.status.value,
        "completed_at": step.completed_at.isoformat(),
        "stats": dict(step.stats),
        "message": step.message,
        "host": step.host,
        "attempts": step.attempts,
    }


def _runs(
    application: bootstrap.Application,
    pipeline: str | None,
    limit: int,
    stream: TextIO,
    output_format: str = "text",
) -> int:
    """Writes the last runs of the ledger, newest first."""
    ledger = application.resources.get(names.LEDGER, contract.IRunLedger)
    records = ledger.recent(pipeline, limit)
    if output_format == "json":
        _write_json(stream, [_record(record) for record in records])
        return exit_codes.ExitCode.SUCCESS
    for record in records:
        took = (
            f"{(record.completed_at - record.started_at).total_seconds():.1f}s"
            if record.completed_at
            else "-"
        )
        meta = " ".join(
            f"{key}={record.meta[key]}"
            for key in (
                "user",
                "label",
                "build",
                "mappings",
                "keys",
                "anonymized",
            )
            if record.meta.get(key)
        )
        stream.write(
            f"{record.started_at:%Y-%m-%d %H:%M:%S} {record.pipeline}"
            f" {record.status.value} {took} {record.run_id}"
            + (f" {meta}" if meta else "")
            + "\n"
        )
    return exit_codes.ExitCode.SUCCESS


def _run_steps(
    application: bootstrap.Application,
    run_id: str,
    stream: TextIO,
    output_format: str = "text",
) -> int:
    """Writes a run and how each of its steps ended."""
    ledger = application.resources.get(names.LEDGER, contract.IRunLedger)
    record = ledger.get(run_id)
    steps = sorted(
        record.steps.values() if record else (), key=lambda s: s.completed_at
    )
    if output_format == "json":
        _write_json(
            stream,
            None
            if record is None
            else {**_record(record), "steps": [_step(s) for s in steps]},
        )
        if record is None:
            return exit_codes.ExitCode.NOT_FOUND
        return exit_codes.ExitCode.SUCCESS
    if record is None:
        stream.write(f"FAILED no run {run_id!r}\n")
        return exit_codes.ExitCode.NOT_FOUND
    stream.write(
        f"{record.started_at:%Y-%m-%d %H:%M:%S} {record.pipeline}"
        f" {record.status.value} {record.run_id}"
        + "".join(
            f" {key}={record.meta[key]}"
            for key in ("label", "resumed_from", "steps", "rekeyed_from")
            if record.meta.get(key)
        )
        + "\n"
    )
    for step in steps:
        stats = step.stats
        stream.write(
            f"  {step.completed_at:%H:%M:%S} {step.step} {step.status.value}"
            f" {stats.get('total', 0)} read, {stats.get('rejected', 0)}"
            f" rejected, {step.attempts} attempt(s), {step.host}"
            + (f": {step.message}" if step.message else "")
            + "\n"
        )
    return exit_codes.ExitCode.SUCCESS


def _timestamp(text: str) -> datetime.datetime:
    moment = datetime.datetime.fromisoformat(text)
    return moment if moment.tzinfo else moment.replace(tzinfo=datetime.UTC)


def _param(text: str) -> tuple[str, str]:
    name, equals, value = text.partition("=")
    if not equals or not name:
        raise argparse.ArgumentTypeError(f"{text!r} is not NAME=VALUE")
    return name, value


def _parser() -> argparse.ArgumentParser:
    """Returns the parser of the command line and its subcommands."""
    parser = argparse.ArgumentParser(prog="etl", description=_DESCRIPTION)
    parser.add_argument(
        "--env", help=f"the environment (default: ${config.ENV_VARIABLE})"
    )
    parser.add_argument(
        "--config",
        type=pathlib.Path,
        default=pathlib.Path("config"),
        help="the directory of the settings files (default: %(default)s)",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    run = commands.add_parser("run", help="run a pipeline once")
    run.add_argument("pipeline", help="module.pipeline")
    run.add_argument(
        "--since", type=_timestamp, help="the start of the window (ISO 8601)"
    )
    run.add_argument(
        "--until", type=_timestamp, help="the end of the window (ISO 8601)"
    )
    run.add_argument(
        "--dry-run", action="store_true", help="read and check, load nothing"
    )
    run.add_argument(
        "--partial-ok",
        action="store_true",
        help=(
            "exit 0 when some rows were rejected (partial success): for"
            " schedulers that retry any other code; alerts watch the rejects"
        ),
    )
    run.add_argument(
        "--param",
        type=_param,
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help="a parameter of the run, e.g. corrections=fixes.csv",
    )
    run.add_argument(
        "--step",
        action="append",
        default=[],
        metavar="NAME",
        help=(
            "run only this step, a name or a pattern (customer*); again for"
            " more. Their dependencies are taken as done"
        ),
    )
    run.add_argument(
        "--resume",
        nargs="?",
        const=True,
        metavar="RUN_ID",
        help=(
            "do not run again the steps a run finished: that run, or the"
            " last one (of the same --label) unless it succeeded (then every"
            " step runs)"
        ),
    )
    run.add_argument(
        "--label",
        metavar="TEXT",
        help=(
            "the scheduler's name of this job, recorded with the run, e.g."
            " the Kubernetes Job: with --resume, a retry of the job resumes"
            " its run, and takes over the locks it left. Never give two runs"
            " at once the same label"
        ),
    )
    run.add_argument(
        "--rekey",
        action="store_true",
        help=(
            "run even though the surrogate keys changed since the last"
            " successful run (key secret or namespaces): every key changes"
        ),
    )
    run.add_argument(
        "--format",
        choices=_FORMATS,
        default="text",
        help=(
            "how the outcome is told: text, or json, the last line of the"
            " output, after the logs: one object {pipeline, run_id, status,"
            " exit_code, resumed_from, stats, message}; run_id and status"
            " are null for a run that never started (exit 2, 4)"
        ),
    )
    commands.add_parser("list", help="list the pipelines")
    plan = commands.add_parser("plan", help="show the steps of a pipeline")
    plan.add_argument("pipeline", help="module.pipeline")
    commands.add_parser("check", help="check that the resources work")
    runs = commands.add_parser("runs", help="show the last runs")
    runs.add_argument("pipeline", nargs="?", help="only this pipeline")
    runs.add_argument("--limit", type=int, default=20)
    runs.add_argument("--run", metavar="RUN_ID", help="the steps of this run")
    runs.add_argument(
        "--format",
        choices=_FORMATS,
        default="text",
        help=(
            "text, or json: a list of runs, newest first, {run_id, pipeline,"
            " status, started_at, completed_at, host, dry_run, stats,"
            " message, meta}; with --run, that run with its steps, {...,"
            " steps: [{step, status, completed_at, stats, message, host,"
            " attempts}]}, or null if there is none (exit 7)"
        ),
    )
    cutover.add_parser(commands)
    operations.add_parsers(commands)
    new = commands.add_parser("new-module", help="write a module skeleton")
    new.add_argument("name", help="the module, e.g. billing")
    new.add_argument(
        "--root",
        type=pathlib.Path,
        default=pathlib.Path("src"),
        help="where the `modules` package is (default: %(default)s)",
    )
    settings = commands.add_parser("config", help="the settings")
    settings.add_subparsers(dest="action", required=True).add_parser(
        "show", help="show the settings, secrets hidden"
    )
    return parser
