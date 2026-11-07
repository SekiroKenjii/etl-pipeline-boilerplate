"""`etl-mappings`: the tooling around mapping workbooks, for BAs, devs and CI.

    etl-mappings template mappings/crm.xlsx   # Start a workbook.
    etl-mappings validate                     # Check mappings/*.xlsx.
    etl-mappings compile                      # Write mappings/compiled/.
    etl-mappings check                        # CI: compiled files current?
    etl-mappings diff OLD NEW                 # Review: what changed, reaches?

Custom transforms are loaded with `--registry package.module:attribute` (a
`TransformRegistry`), so that validation knows them.
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
import dataclasses
import importlib
import pathlib
import sys
from typing import TextIO

from cli import exit_codes
from core import contract
from core import mapping
from core.mapping import catalog as mapping_catalog
from infra import mapping as infra_mapping

COMPILED_DIR = "compiled"
_DESCRIPTION = "The tooling around mapping workbooks, for BAs, devs and CI."


@dataclasses.dataclass(frozen=True, slots=True)
class _Checked:
    """A workbook, read and validated."""

    workbook: pathlib.Path
    catalog: contract.MappingCatalog | None
    problems: list[contract.MappingProblem]
    warnings: list[contract.MappingProblem] = dataclasses.field(
        default_factory=list
    )


def main(argv: Sequence[str] | None = None, out: TextIO | None = None) -> int:
    """Runs the command line.

    Args:
        argv: The arguments; those of the process by default.
        out: Where to write the report; stdout by default.

    Returns:
        The exit code (`exit_codes.ExitCode`): 0 when everything is fine, 2
        for an invalid workbook or argument, 6 when `diff` finds changes or
        `check` stale compiled files.
    """
    stream = out if out is not None else sys.stdout
    args = _parser().parse_args(argv)
    try:
        registry = _registry(args.registry)
    except (ImportError, AttributeError, ValueError) as exc:
        print(f"FAILED {exc}", file=stream)
        return exit_codes.ExitCode.INVALID

    match args.command:
        case "template":
            return _template(
                args.path,
                args.module or args.path.stem,
                args.force,
                registry,
                stream,
            )
        case "validate":
            return _validate(args.paths, registry, stream)
        case "compile":
            return _compile(args.paths, args.out, registry, stream)
        case "check":
            return _check(args.paths, args.out, registry, stream)
        case "diff":
            return _diff(args.old, args.new, stream)
    # argparse rejects unknown commands.
    return exit_codes.ExitCode.INVALID  # pragma: no cover


def _parser() -> argparse.ArgumentParser:
    """Returns the parser of the command line and its subcommands."""
    parser = argparse.ArgumentParser(
        prog="etl-mappings", description=_DESCRIPTION
    )
    parser.add_argument(
        "--registry",
        action="append",
        default=[],
        metavar="MODULE:ATTR",
        help="a TransformRegistry holding custom transforms (repeatable)",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    template = commands.add_parser(
        "template", help="write a workbook to start from"
    )
    template.add_argument("path", type=pathlib.Path)
    template.add_argument(
        "--force", action="store_true", help="overwrite the file"
    )
    template.add_argument(
        "--module",
        help=(
            "the module the example mappings target, as <module>.customer"
            " (default: the workbook's name, e.g. crm for crm.xlsx)"
        ),
    )

    diff = commands.add_parser(
        "diff", help="what changed between two catalogs (.json or .xlsx)"
    )
    diff.add_argument("old", type=pathlib.Path)
    diff.add_argument("new", type=pathlib.Path)

    for name, help_text in [
        ("validate", "report every problem of the workbooks"),
        ("compile", "validate, then write the compiled JSON of each workbook"),
        ("check", "fail if a compiled JSON is missing, stale or orphaned"),
    ]:
        command = commands.add_parser(name, help=help_text)
        command.add_argument(
            "paths",
            nargs="*",
            type=pathlib.Path,
            default=[pathlib.Path("mappings")],
        )
        if name != "validate":
            command.add_argument(
                "--out",
                type=pathlib.Path,
                default=None,
                help=f"default: <workbook dir>/{COMPILED_DIR}",
            )
    return parser


def _registry(specs: Sequence[str]) -> mapping.TransformRegistry:
    """Merges the registries named `package.module:attribute`.

    Raises:
        ImportError: If a module cannot be imported.
        AttributeError: If it has no such attribute.
        ValueError: If a name does not point to a `TransformRegistry`.
    """
    registry = mapping.TransformRegistry()
    for spec in specs:
        module_name, _, attribute = spec.partition(":")
        if not attribute:
            raise ValueError(
                f"--registry {spec!r}: expected package.module:attribute"
            )
        value = getattr(importlib.import_module(module_name), attribute)
        if not isinstance(value, mapping.TransformRegistry):
            raise ValueError(f"--registry {spec!r} is not a TransformRegistry")
        registry.include(value)
    return registry


def _workbooks(paths: Sequence[pathlib.Path]) -> list[pathlib.Path]:
    """Returns the workbooks of `paths`, directories expanded."""
    found: list[pathlib.Path] = []
    for path in paths:
        if path.is_dir():
            # `~$name.xlsx`: the lock file of a workbook open in Excel.
            found.extend(
                p
                for p in sorted(path.glob("*.xlsx"))
                if not p.name.startswith("~$")
            )
        else:
            found.append(path)
    return found


def _check_workbook(
    workbook: pathlib.Path, registry: mapping.TransformRegistry
) -> _Checked:
    """Reads and validates one workbook."""
    if not workbook.exists():
        return _Checked(
            workbook,
            None,
            [contract.MappingProblem(f"{workbook} does not exist")],
        )
    try:
        catalog = infra_mapping.XlsxMappingReader().read(workbook)
    except contract.MappingDefinitionError as exc:
        return _Checked(workbook, None, exc.problems)
    return _Checked(
        workbook,
        catalog,
        mapping.validate_catalog(catalog, registry, allow_drafts=True),
        mapping.catalog_warnings(catalog),
    )


def _report(checked: _Checked, out: TextIO) -> bool:
    """Prints what is wrong with a workbook, or a summary of it, and what
    is likely wrong (warnings, which do not fail).

    Returns:
        Whether the workbook is valid.
    """
    catalog = checked.catalog
    for warning in checked.warnings:
        print(f"WARN   {warning}", file=out)
    if checked.problems or catalog is None:
        print(
            f"FAILED {checked.workbook}: {len(checked.problems)} problem(s)",
            file=out,
        )
        for problem in checked.problems:
            print(f"  - {problem}", file=out)
        return False

    drafts = sum(
        1 for m in catalog.mappings if m.status is contract.MappingStatus.DRAFT
    )
    print(
        f"OK     {checked.workbook}: {len(catalog.mappings)} mapping(s)"
        f" ({drafts} draft), {len(catalog.lookups)} lookup table(s)",
        file=out,
    )
    return True


def _compiled_path(
    workbook: pathlib.Path, out_dir: pathlib.Path | None
) -> pathlib.Path:
    return (out_dir or workbook.parent / COMPILED_DIR) / f"{workbook.stem}.json"


def _diff(old: pathlib.Path, new: pathlib.Path, out: TextIO) -> int:
    """Prints what changed; exits CHANGED when something did."""
    catalogs = []
    for path in (old, new):
        try:
            catalogs.append(
                infra_mapping.XlsxMappingReader().read(path)
                if path.suffix == ".xlsx"
                else mapping.load_catalog(path)
            )
        except (OSError, ValueError, contract.MappingDefinitionError) as exc:
            print(f"FAILED {path}: {exc}", file=out)
            return exit_codes.ExitCode.INVALID
    changes = mapping.compare_catalogs(catalogs[0], catalogs[1])
    print(mapping.describe_changes(changes), end="", file=out)
    return (
        exit_codes.ExitCode.CHANGED if changes else exit_codes.ExitCode.SUCCESS
    )


def _template(
    path: pathlib.Path,
    module: str,
    force: bool,
    registry: mapping.TransformRegistry,
    out: TextIO,
) -> int:
    """Writes a workbook to start from; refuses to overwrite one."""
    if path.exists() and not force:
        print(f"{path} already exists (use --force to overwrite it)", file=out)
        return exit_codes.ExitCode.INVALID

    infra_mapping.write_template(
        path,
        transforms=[(t.name, t.description) for t in registry.transforms()],
        type_names=mapping.SUPPORTED_TYPE_NAMES,
        row_filters=registry.filter_names(),
        row_splitters=registry.splitter_names(),
        personal_kinds=sorted(mapping.PERSONAL_KINDS),
        module=module,
    )
    print(f"Wrote {path}", file=out)
    return exit_codes.ExitCode.SUCCESS


def _validate(
    paths: Sequence[pathlib.Path],
    registry: mapping.TransformRegistry,
    out: TextIO,
) -> int:
    """Reports every problem of the workbooks."""
    workbooks = _workbooks(paths)
    if not workbooks:
        print("No workbook found", file=out)
        return exit_codes.ExitCode.INVALID
    # `sum`, not `any`: every workbook is reported.
    failing = sum(
        not _report(_check_workbook(workbook, registry), out)
        for workbook in workbooks
    )
    return (
        exit_codes.ExitCode.INVALID if failing else exit_codes.ExitCode.SUCCESS
    )


def _compile(
    paths: Sequence[pathlib.Path],
    out_dir: pathlib.Path | None,
    registry: mapping.TransformRegistry,
    out: TextIO,
) -> int:
    """Writes the compiled JSON of every valid workbook."""
    ok = True
    for workbook in _workbooks(paths):
        checked = _check_workbook(workbook, registry)
        if not _report(checked, out) or checked.catalog is None:
            ok = False
            continue
        target = _compiled_path(workbook, out_dir)
        mapping_catalog.save_catalog(target, checked.catalog)
        print(f"       -> {target}", file=out)
    return exit_codes.ExitCode.SUCCESS if ok else exit_codes.ExitCode.INVALID


def _check(
    paths: Sequence[pathlib.Path],
    out_dir: pathlib.Path | None,
    registry: mapping.TransformRegistry,
    out: TextIO,
) -> int:
    """Fails when a workbook is invalid (INVALID), or a compiled JSON is
    missing, stale, or has no workbook (CHANGED)."""
    ok = valid = True
    expected: set[pathlib.Path] = set()

    for workbook in _workbooks(paths):
        checked = _check_workbook(workbook, registry)
        if not _report(checked, out) or checked.catalog is None:
            valid = False
            continue
        target = _compiled_path(workbook, out_dir)
        expected.add(target.resolve())
        current = (
            target.read_text(encoding="utf-8") if target.exists() else None
        )
        if current != mapping_catalog.dumps(checked.catalog):
            state = "missing" if current is None else "out of date"
            print(
                f"FAILED {target} is {state}: run `etl-mappings compile`",
                file=out,
            )
            ok = False

    for directory in {p if p.is_dir() else p.parent for p in paths}:
        compiled_dir = out_dir or directory / COMPILED_DIR
        for orphan in sorted(compiled_dir.glob("*.json")):
            if (
                orphan.resolve() not in expected
                and not (directory / f"{orphan.stem}.xlsx").exists()
            ):
                print(
                    f"FAILED {orphan} has no workbook anymore: delete it",
                    file=out,
                )
                ok = False

    if not valid:
        return exit_codes.ExitCode.INVALID
    return exit_codes.ExitCode.SUCCESS if ok else exit_codes.ExitCode.CHANGED


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
