"""The compiled mappings as their JSON text holds them (`catalog`)."""

from __future__ import annotations

from typing import Required, TypedDict

__all__ = ["CatalogJson", "FieldJson", "LocationJson", "MappingJson"]


class LocationJson(TypedDict, total=False):
    """A location, as files compiled before it was text hold it."""

    file: Required[str]
    sheet: str | None
    row: int | None


class FieldJson(TypedDict, total=False):
    """A field of a mapping, as the JSON text holds it."""

    target: Required[str]
    sources: Required[list[str]]
    target_type: str | None
    required: Required[bool]
    transform: str | None
    params: dict[str, str]
    format: str | None
    default: str | None
    notes: str | None
    primary_key: bool
    references: str | None
    personal: str | None
    location: str | LocationJson | None


class MappingJson(TypedDict, total=False):
    """A mapping, as the JSON text holds it."""

    name: Required[str]
    source: Required[str]
    target: Required[str]
    version: Required[int]
    status: Required[str]
    owner: str | None
    unmapped_source_fields: Required[str]
    ignored_source_fields: Required[list[str]]
    row_filter: str | None
    row_split: str | None
    key_version: int
    key_hashing: str | None
    key_case: str
    key_namespace: str | None
    location: str | LocationJson | None
    fields: Required[list[FieldJson]]


class CatalogJson(TypedDict, total=False):
    """A catalog, as the JSON text holds it."""

    format_version: Required[int]
    name: Required[str]
    source_file: str | None
    labels: dict[str, str]
    lookups: Required[dict[str, dict[str, str]]]
    mappings: Required[list[MappingJson]]
