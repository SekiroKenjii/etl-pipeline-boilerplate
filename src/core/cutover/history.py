"""Rehearsals of a cutover: reports sealed with their digest, and compared.

A report is saved as JSON with the SHA-256 of its content. The digest alone
proves nothing, as anyone can seal an edited report again: `etl cutover
report --save` records it in the run ledger, where `etl cutover verify`
checks it. Two rehearsals compare entity by entity, with what produced each
(`provenance`).
"""

from __future__ import annotations

from collections.abc import Mapping
import dataclasses
import hashlib
import json
import typing

from shared import utils

DIGEST = "digest"
COUNTERS = (
    "source_rows",
    "rejected",
    "accepted",
    "unaccepted",
    "expected_rows",
    "target_rows",
    "matched",
    "missing",
    "parked_missing",
    "unexplained_missing",
    "extra",
    "different",
    "duplicates",
)


def digest(report: Mapping[str, object]) -> str:
    """The SHA-256 of a report's content, its own digest aside."""
    content = {k: v for k, v in report.items() if k != DIGEST}
    text = json.dumps(
        content, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(text.encode()).hexdigest()


def seal(report: Mapping[str, object]) -> dict[str, object]:
    """The report with its digest."""
    return {**report, DIGEST: digest(report)}


def verify(report: Mapping[str, object]) -> bool:
    """Whether a sealed report is as it was sealed: its content has its
    digest. Whether that digest is the one recorded when it was saved is
    the ledger's to tell."""
    return report.get(DIGEST) == digest(report)


def verdict(report: Mapping[str, object]) -> str:
    """The verdict of a report; of an older one, from `passed`."""
    found = report.get("verdict")
    if isinstance(found, str):
        return found
    return "PASSED" if report.get("passed") else "FAILED"


@dataclasses.dataclass(frozen=True, slots=True)
class EntityChange:
    """How an entity changed between two rehearsals.

    Attributes:
        entity: The entity.
        before: Its status before; None if it was not reconciled.
        after: Its status after; None if it is not any more.
        deltas: The counters that changed: after minus before.
    """

    entity: str
    before: str | None
    after: str | None
    deltas: dict[str, int]


@dataclasses.dataclass(frozen=True, slots=True)
class Comparison:
    """Two rehearsals of a module's cutover, compared.

    Attributes:
        module: The module.
        before: When the first report was made, and its digest.
        after: Likewise for the second.
        passed: Whether each passed, before and after.
        entities: Every entity, changed or not.
        verdicts: The verdicts, before and after.
        provenance: What produced them, where it changed: before and after,
            by entry (`module.mappings`, `env`, `databases.target`...).
    """

    module: str
    before: tuple[str, str]
    after: tuple[str, str]
    passed: tuple[bool, bool]
    entities: list[EntityChange]
    verdicts: tuple[str, str] = ("", "")
    provenance: dict[str, tuple[object, object]] = dataclasses.field(
        default_factory=dict[str, tuple[object, object]]
    )

    @property
    def changed(self) -> list[EntityChange]:
        """The entities whose status or counters changed."""
        return [e for e in self.entities if e.deltas or e.before != e.after]


def compare(
    before: Mapping[str, object], after: Mapping[str, object]
) -> Comparison:
    """Compares two reports of the same module.

    Raises:
        ValueError: If they are reports of different modules.
    """
    if before.get("module") != after.get("module"):
        raise ValueError(
            f"reports of {before.get('module')!r} and {after.get('module')!r}"
        )
    old, new = _entities(before), _entities(after)
    entities: list[EntityChange] = []
    for name in [*old, *(n for n in new if n not in old)]:
        a, b = old.get(name), new.get(name)
        deltas = {
            counter: _count(b, counter) - _count(a, counter)
            for counter in COUNTERS
            if a is not None
            and b is not None
            and _count(b, counter) != _count(a, counter)
        }
        entities.append(
            EntityChange(
                name,
                str(a["status"]) if a else None,
                str(b["status"]) if b else None,
                deltas,
            )
        )
    old_provenance = flat(_mapping(before.get("provenance")))
    new_provenance = flat(_mapping(after.get("provenance")))
    return Comparison(
        module=str(after.get("module")),
        before=(str(before.get("generated_at")), str(before.get(DIGEST, ""))),
        after=(str(after.get("generated_at")), str(after.get(DIGEST, ""))),
        passed=(bool(before.get("passed")), bool(after.get("passed"))),
        entities=entities,
        verdicts=(verdict(before), verdict(after)),
        provenance={
            entry: (old_provenance.get(entry), new_provenance.get(entry))
            for entry in sorted(old_provenance.keys() | new_provenance.keys())
            if old_provenance.get(entry) != new_provenance.get(entry)
        },
    )


def flat(data: Mapping[str, object], prefix: str = "") -> dict[str, object]:
    """Nested mappings as one, by dotted entry: a report's provenance."""
    found: dict[str, object] = {}
    for name, value in data.items():
        if nested := _mapping(value):
            found |= flat(nested, f"{prefix}{name}.")
        else:
            found[f"{prefix}{name}"] = value
    return found


def _mapping(value: object) -> Mapping[str, object]:
    """A JSON object of a report; empty for anything else."""
    if isinstance(value, Mapping):
        return typing.cast(Mapping[str, object], value)
    return {}


def _entities(report: Mapping[str, object]) -> dict[str, Mapping[str, object]]:
    """The entities of a report, by name."""
    found = report.get("entities")
    listed = typing.cast(list[object], found) if isinstance(found, list) else []
    entities = [_mapping(entity) for entity in listed]
    return {str(entity["entity"]): entity for entity in entities}


def _count(entity: Mapping[str, object] | None, counter: str) -> int:
    """A counter of an entity; 0 when it has none."""
    value = None if entity is None else entity.get(counter)
    return value if isinstance(value, int) else 0


def comparison_markdown(comparison: Comparison) -> str:
    """Renders a comparison."""
    lines = [
        f"# Cutover rehearsals: {comparison.module}",
        "",
        (
            f"- Before: {comparison.before[0]}, {comparison.verdicts[0]},"
            f" digest `{comparison.before[1][:16]}`"
        ),
        (
            f"- After: {comparison.after[0]}, {comparison.verdicts[1]},"
            f" digest `{comparison.after[1][:16]}`"
        ),
        "",
    ]
    if comparison.provenance:
        lines += ["Produced otherwise:", ""]
        lines += [
            f"- {entry}: {shown(old)} → {shown(new)}"
            for entry, (old, new) in comparison.provenance.items()
        ]
        lines.append("")
    if not comparison.changed:
        lines.append("Nothing changed.")
        return "\n".join(lines) + "\n"
    lines += [
        "| Entity | Status | Changes |",
        "|---|---|---|",
    ]
    for change in comparison.changed:
        status = (
            f"{change.before or '-'} → {change.after or '-'}"
            if change.before != change.after
            else str(change.after)
        )
        deltas = ", ".join(
            f"{counter} {delta:+d}" for counter, delta in change.deltas.items()
        )
        lines.append(f"| {change.entity} | {status} | {deltas or '-'} |")
    return "\n".join(lines) + "\n"


def shown(value: object) -> str:
    """A provenance value, as Markdown: on one line."""
    if value is None:
        return "-"
    if isinstance(value, str):
        return f"`{utils.one_line(value)}`"
    return f"`{json.dumps(value, ensure_ascii=False, sort_keys=True)}`"
