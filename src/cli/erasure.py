"""`etl keys forget`: a right to erasure, in the ETL's own records.

    printf '1003\\n' | etl keys forget crm.customer --key-file -
    etl keys forget crm.customer --key-file /run/secrets/erasure
    etl keys forget crm.customer 1003    # On the command line: shell history

For the natural key of an entity, it erases:

- the entity's keys in the crosswalk;
- its quarantined and parked records, and those of other entities that
  reference it: their foreign keys hold its natural key (an order parked,
  waiting for the customer);
- the quality issues of all those records, by their source key.

The key case is that of the entity's mappings (`--case` overrides it). The
parking queues are those of the mappings of the entity and of the mappings
referencing it (`--queue` adds others). Both come from the modules' catalogs
(`Module.catalogs`). The run ledger records the erasure as a run of
`etl.erasure`, with the surrogate keys and the operator (`cli.audit`), never
the natural key.

Not erased (see docs/security.md): the target system's rows, backups, log
and trace backends, Kafka dead letters, exported files, lineage backends.
"""

from __future__ import annotations

import argparse
from collections.abc import Collection, Iterable, Mapping, Sequence
import dataclasses
import pathlib
import sys
from typing import TextIO

from cli import audit
from cli import exit_codes
from core import contract
from core import keys as core_keys
from core import runtime
from core.runtime import names

RECORD = "etl.erasure"


def add_arguments(parser: argparse.ArgumentParser) -> None:
    """Adds the arguments of `etl keys forget`."""
    parser.add_argument("entity", help="e.g. crm.customer")
    parser.add_argument(
        "natural_key",
        nargs="*",
        help=(
            "its components; better --key-file: a command line ends up in"
            " shell histories, process lists and job specs"
        ),
    )
    parser.add_argument(
        "--key-file",
        type=pathlib.Path,
        metavar="PATH",
        help="read the components from a file, one per line; - for stdin",
    )
    parser.add_argument(
        "--case",
        choices=[case.value for case in contract.KeyCase],
        help="the key case (default: that of the entity's mappings)",
    )
    parser.add_argument(
        "--queue",
        action="append",
        default=[],
        help=(
            "another parking queue its records may wait in (repeatable); the"
            " queues of the mappings of the entity and of those referencing it"
            " are searched anyway"
        ),
    )


@dataclasses.dataclass(frozen=True, slots=True)
class Plan:
    """What to look at to erase the records of one entity's natural keys.

    Attributes:
        entity: The entity, e.g. `crm.customer`.
        case: The key case of its natural keys.
        mappings: The mappings loading it.
        references: The foreign keys referencing it: (mapping, field).
        known: Whether a catalog knows the entity; if not, a record
            referencing it is any record with a foreign key holding its
            natural key.
        queues: The parking queues to search.
    """

    entity: str
    case: contract.KeyCase
    mappings: frozenset[str]
    references: frozenset[tuple[str, str]]
    known: bool
    queues: tuple[str, ...]

    @classmethod
    def of(
        cls,
        entity: str,
        catalogs: Iterable[contract.MappingCatalog],
        *,
        case: contract.KeyCase | None = None,
        queues: Sequence[str] = (),
    ) -> Plan:
        """Reads the plan of `entity` from the catalogs.

        Args:
            entity: The entity.
            catalogs: The mapping catalogs of the modules.
            case: The key case, overriding the mappings'.
            queues: More parking queues to search.

        Raises:
            ValueError: If the entity's mappings disagree on the key case,
                and `case` is not given.
        """
        specs = [m for c in catalogs for m in c.mappings]
        own = [m for m in specs if m.target == entity]
        names_ = {m.name for m in own}
        references = {
            (m.name, f.target)
            for m in specs
            for f in m.fields
            if f.references is not None and f.references in names_
        }
        cases = {m.key_case for m in own}
        if case is None and len(cases) > 1:
            raise ValueError(
                f"the mappings of {entity} have different key cases"
                f" ({', '.join(sorted(c.value for c in cases))}): give --case"
            )
        chosen = case or (cases.pop() if cases else contract.KeyCase.PRESERVE)
        found = sorted(names_ | {name for name, _ in references})
        return cls(
            entity=entity,
            case=chosen,
            mappings=frozenset(names_),
            references=frozenset(references),
            known=bool(own),
            queues=tuple(dict.fromkeys([*found, *queues])),
        )

    def about(
        self,
        record: object,
        natural: Sequence[str],
        surrogates: Collection[str],
    ) -> bool:
        """Whether a record (JSON data of a mapped record) is about the
        natural key: one of the entity's, or one referencing it."""
        if not isinstance(record, Mapping):
            return False
        wanted = list(natural)
        if record.get("entity") == self.entity and (
            record.get("key") in surrogates
            or record.get("natural_key") == wanted
        ):
            return True
        foreign = record.get("foreign_keys")
        if not isinstance(foreign, Mapping):
            return False
        if not self.known:
            return wanted in foreign.values()
        return any(
            foreign.get(field) == wanted
            for mapping, field in self.references
            if record.get("mapping") == mapping
        )


@dataclasses.dataclass(slots=True)
class Erased:
    """What an erasure erased.

    Attributes:
        keys: The surrogate keys forgotten by the crosswalk.
        quarantined: The quarantined records.
        parked: The parked records.
        issues: The quality issues.
    """

    keys: list[str] = dataclasses.field(default_factory=list)
    quarantined: int = 0
    parked: int = 0
    issues: int = 0

    def __bool__(self) -> bool:
        return bool(self.keys or self.quarantined or self.parked or self.issues)

    def summary(self) -> str:
        return (
            f"{len(self.keys)} key(s), {self.quarantined} quarantined and"
            f" {self.parked} parked record(s), {self.issues} issue(s) erased"
        )


def natural_key(args: argparse.Namespace, stdin: TextIO) -> list[str]:
    """The components of the natural key: the command line's or the file's.

    Raises:
        ValueError: If neither or both give it, or the file cannot be read.
    """
    if args.key_file is None:
        if not args.natural_key:
            raise ValueError("give the natural key: --key-file, or arguments")
        return list(args.natural_key)
    if args.natural_key:
        raise ValueError("give the natural key once: --key-file or arguments")
    try:
        text = (
            stdin.read()
            if str(args.key_file) == "-"
            else args.key_file.read_text(encoding="utf-8")
        )
    except OSError as exc:
        raise ValueError(f"cannot read {args.key_file}: {exc}") from None
    components = [
        line.rstrip("\r\n") for line in text.splitlines() if line.strip()
    ]
    if not components:
        raise ValueError(f"{args.key_file} holds no natural key")
    return components


def erase(
    plan: Plan,
    components: Sequence[str],
    resources: runtime.Resources,
) -> Erased:
    """Erases the records of a natural key, in every store that has some.

    Args:
        plan: What to look at.
        components: The natural key, as given.
        resources: The resources of the command.

    Returns:
        What was erased.
    """
    natural = core_keys.canonical(components, plan.case)
    erased = Erased()
    registry = resources.get(names.KEYS, contract.IKeyRegistry)
    erased.keys = list(registry.forget(plan.entity, natural))

    # Issues name the entity's own records by `keys.canonical_text`.
    sources = {"|".join(natural)}

    quarantine = resources.get(names.QUARANTINE, contract.IQuarantine)
    found = [
        entry
        for entry in quarantine.entries(status=None)
        if plan.about(entry.record, natural, erased.keys)
    ]
    sources.update(_source_keys(entry.record for entry in found))
    erased.quarantined = quarantine.erase([entry.id for entry in found])

    lot = optional(resources, names.PARKING, contract.IParkingLot)
    if lot is not None and plan.queues:
        for queue in plan.queues:
            parked = [
                entry
                for entry in lot.entries(queue)
                if entry.identity in erased.keys
                or plan.about(entry.payload, natural, erased.keys)
            ]
            sources.update(_source_keys(entry.payload for entry in parked))
            lot.release(queue, parked)
            erased.parked += len(parked)

    sink = optional(resources, names.ISSUES, contract.IIssueSink)
    if sink is not None:
        erased.issues = sink.erase(sources)
    return erased


def forget(
    args: argparse.Namespace,
    catalogs: Iterable[contract.MappingCatalog],
    resources: runtime.Resources,
    stream: TextIO,
    stdin: TextIO | None = None,
) -> int:
    """Runs `etl keys forget`; returns the exit code."""
    try:
        components = natural_key(args, stdin or sys.stdin)
        plan = Plan.of(
            args.entity,
            catalogs,
            case=contract.KeyCase(args.case) if args.case else None,
            queues=args.queue,
        )
        core_keys.canonical(components, plan.case)  # No blank component.
    except ValueError as exc:
        stream.write(f"FAILED {exc}\n")
        return exit_codes.ExitCode.INVALID
    if not plan.known:
        stream.write(
            f"WARNING no mapping loads {args.entity}: key case"
            f" {plan.case.value}, any foreign key holding the key matches\n"
        )

    erased = erase(plan, components, resources)
    summary = erased.summary()
    audit.record(
        resources,
        RECORD,
        {"entity": args.entity, "keys": ",".join(erased.keys)},
        summary,
    )
    stream.write(f"{args.entity}: {summary}\n")
    return (
        exit_codes.ExitCode.SUCCESS if erased else exit_codes.ExitCode.NOT_FOUND
    )


def _source_keys(records: Iterable[object]) -> set[str]:
    return {
        str(record["source_key"])
        for record in records
        if isinstance(record, Mapping) and record.get("source_key") is not None
    }


def optional[T](
    resources: runtime.Resources, name: str, kind: type[T]
) -> T | None:
    """A resource the settings may not give (no parking without a
    database)."""
    try:
        return resources.get(name, kind)
    except contract.PipelineConfigError:
        return None
