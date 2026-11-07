"""Builders of mapping specs, and a registry of project-style transforms."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any

from core import contract
from core import mapping as core_mapping

registry = core_mapping.TransformRegistry()


@registry.register("customer.full_address")
def full_address(
    street: str | None,
    ward: str | None,
    district: str | None,
    *,
    separator: str = ", ",
) -> str | None:
    """Street, ward and district, skipping the empty ones."""
    parts = [
        part.strip()
        for part in (street, ward, district)
        if part and part.strip()
    ]
    return separator.join(parts) or None


@registry.register("customer.explode")
def explode(value: object) -> object:
    """Always fails, like a custom transform choking on bad legacy data."""
    raise ValueError("cannot parse")


def field(
    target: str, *sources: str, row: int = 2, **options: Any
) -> contract.FieldSpec:
    return contract.FieldSpec(
        target=target,
        sources=sources,
        location=f"crm.xlsx › customer › row {row}",
        **options,
    )


def mapping(
    *fields: contract.FieldSpec, **options: Any
) -> contract.MappingSpec:
    options.setdefault("status", contract.MappingStatus.APPROVED)
    return contract.MappingSpec(
        name=options.pop("name", "customer"),
        source=options.pop("source", "legacy.CUST_MASTER"),
        target=options.pop("target", "crm.customer"),
        fields=fields,
        location="crm.xlsx › Mappings › row 2",
        **options,
    )


def catalog(
    *mappings: contract.MappingSpec,
    lookups: dict[str, dict[str, str]] | None = None,
) -> contract.MappingCatalog:
    return contract.MappingCatalog(
        "crm", mappings, lookups or {}, source_file="crm.xlsx"
    )


@registry.register_filter("customer.not_deleted")
def not_deleted(row: Mapping[str, object]) -> bool:
    """Legacy rows soft-deleted with DELETED = 'Y' are not migrated."""
    return row.get("DELETED") != "Y"


@registry.register_filter("customer.broken_filter")
def broken_filter(row: Mapping[str, object]) -> bool:
    raise KeyError("STATUS")


@registry.register_splitter("customer.phones")
def phones(row: Mapping[str, object]) -> Iterator[dict[str, object]]:
    """One target row per non-empty phone column (unpivot)."""
    for kind in ("HOME", "WORK"):
        number = row.get(f"{kind}_PHONE")
        if number:
            yield {
                "CUST_NO": row["CUST_NO"],
                "KIND": kind.lower(),
                "NUMBER": number,
            }


@registry.register_splitter("customer.not_rows")
def not_rows(row: Mapping[str, object]) -> list[object]:
    return ["oops"]
