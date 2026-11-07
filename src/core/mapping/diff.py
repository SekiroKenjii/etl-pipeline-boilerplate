"""What changed between two versions of a mapping catalog, for review.

    etl-mappings diff mappings/compiled/crm.json mappings/crm.xlsx

A change of a mapping reaches the mappings referencing it too: their
foreign keys hold its keys. Locations (sheets and rows) are not changes.
"""

from __future__ import annotations

from collections.abc import Iterable
import dataclasses
import typing

from core import contract
from core.mapping import compiler

_MAPPING_ATTRIBUTES = (
    "source",
    "target",
    "version",
    "status",
    "owner",
    "unmapped_source_fields",
    "ignored_source_fields",
    "row_filter",
    "row_split",
    "key_version",
    "key_hashing",
    "key_case",
    "key_namespace",
)
_FIELD_ATTRIBUTES = (
    "sources",
    "target_type",
    "required",
    "transform",
    "params",
    "format",
    "default",
    "primary_key",
    "references",
    "personal",
)
_REKEYING_ATTRIBUTES = ("key_version", "key_hashing", "key_case")


@dataclasses.dataclass(frozen=True, slots=True)
class CatalogChanges:
    """The changes from one catalog to another.

    Attributes:
        lines: The changes, one per line, by mapping.
        changed: The mappings changed, added or removed.
        affected: The other mappings referencing a changed one.
        rekeyed: The mappings whose surrogate keys change.
        rekey_reasons: Why, by mapping in `rekeyed`.
    """

    lines: list[str]
    changed: list[str]
    affected: list[str]
    rekeyed: list[str]
    rekey_reasons: dict[str, str] = dataclasses.field(
        default_factory=dict[str, str]
    )

    def __bool__(self) -> bool:
        return bool(self.lines)


def compare(
    old: contract.MappingCatalog, new: contract.MappingCatalog
) -> CatalogChanges:
    """Lists what changed from `old` to `new`."""
    before = {spec.name: spec for spec in old.mappings}
    after = {spec.name: spec for spec in new.mappings}
    lines: list[str] = []
    changed: list[str] = []
    rekeyed: list[str] = []
    reasons: dict[str, str] = {}
    for name in [*before, *(n for n in after if n not in before)]:
        a, b = before.get(name), after.get(name)
        if a is None or b is None:
            lines.append(f"{name}: {'added' if a is None else 'removed'}")
            changed.append(name)
            continue
        found = [
            f"{name}: {attribute} {_show(getattr(a, attribute))} →"
            f" {_show(getattr(b, attribute))}"
            for attribute in _MAPPING_ATTRIBUTES
            if getattr(a, attribute) != getattr(b, attribute)
        ]
        found.extend(_fields(name, a.fields, b.fields))
        if found:
            lines.extend(found)
            changed.append(name)
        reason = _rekey_reason(a, b)
        if reason:
            rekeyed.append(name)
            reasons[name] = reason
    for table in sorted(old.lookups.keys() | new.lookups.keys()):
        lines.extend(
            _lookup(table, old.lookups.get(table), new.lookups.get(table))
        )
    dependencies = compiler.mapping_dependencies(new)
    affected = sorted(
        name
        for name, parents in dependencies.items()
        if name not in changed and parents & set(changed)
    )
    return CatalogChanges(lines, changed, affected, rekeyed, reasons)


def describe(changes: CatalogChanges) -> str:
    """The changes as text, with what they reach."""
    if not changes:
        return "No change.\n"
    text = [*changes.lines]
    if changes.rekeyed:
        text.append(
            "Surrogate keys change for: "
            + ", ".join(changes.rekeyed)
            + " (rows loaded already will not match: reload them, and run"
            " with --rekey)"
        )
        text.extend(
            f"  {name}: {changes.rekey_reasons[name]}"
            for name in changes.rekeyed
            if name in changes.rekey_reasons
        )
    if changes.affected:
        text.append(
            "Also check, as they reference changed mappings: "
            + ", ".join(changes.affected)
        )
    return "\n".join(text) + "\n"


def _fields(
    mapping: str,
    old: Iterable[contract.FieldSpec],
    new: Iterable[contract.FieldSpec],
) -> list[str]:
    """The changes of the fields of a mapping."""
    before = {f.target: f for f in old}
    after = {f.target: f for f in new}
    lines: list[str] = []
    for target in [*before, *(t for t in after if t not in before)]:
        a, b = before.get(target), after.get(target)
        if a is None or b is None:
            lines.append(
                f"{mapping}.{target}: {'added' if a is None else 'removed'}"
            )
            continue
        lines.extend(
            f"{mapping}.{target}: {attribute} {_show(getattr(a, attribute))}"
            f" → {_show(getattr(b, attribute))}"
            for attribute in _FIELD_ATTRIBUTES
            if getattr(a, attribute) != getattr(b, attribute)
        )
    return lines


def _lookup(
    table: str, old: dict[str, str] | None, new: dict[str, str] | None
) -> list[str]:
    """The changes of a lookup table."""
    if old is None or new is None:
        return [f"lookup {table}: {'added' if old is None else 'removed'}"]
    lines: list[str] = []
    for key in sorted(old.keys() | new.keys()):
        if old.get(key) != new.get(key):
            lines.append(
                f"lookup {table}: {key!r} {_show(old.get(key))} →"
                f" {_show(new.get(key))}"
            )
    return lines


def _rekey_reason(a: contract.MappingSpec, b: contract.MappingSpec) -> str:
    """Why the keys of a mapping change from `a` to `b`; empty if not."""
    reasons = [
        f"{attribute} {_show(getattr(a, attribute))} →"
        f" {_show(getattr(b, attribute))}"
        for attribute in _REKEYING_ATTRIBUTES
        if getattr(a, attribute) != getattr(b, attribute)
    ]
    if _key_sources(a) != _key_sources(b):
        reasons.append(
            f"natural key {_show(_key_sources(a))} → {_show(_key_sources(b))}"
        )
    old, new = compiler.namespace_name(a), compiler.namespace_name(b)
    if old != new:
        reason = f"key namespace {old!r} → {new!r}"
        if b.key_namespace is None:
            reason += (
                "; to keep the keys across the rename, set its Key namespace"
                f" to {old!r}"
            )
        reasons.append(reason)
    return "; ".join(reasons)


def _key_sources(spec: contract.MappingSpec) -> tuple[str, ...]:
    return next((f.sources for f in spec.fields if f.primary_key), ())


def _show(value: object) -> str:
    if value is None:
        return "-"
    if isinstance(value, tuple):
        parts = typing.cast(tuple[object, ...], value)
        return ", ".join(str(part) for part in parts) or "-"
    if hasattr(value, "value"):
        return str(getattr(value, "value"))
    if isinstance(value, (str, dict)):
        return repr(typing.cast(object, value))
    return str(value)
