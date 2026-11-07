"""The compiled form of a mapping workbook: canonical JSON.

Reviewed in pull requests (an xlsx does not diff) and run in production. The
same catalog always gives the same text.
"""

from __future__ import annotations

import json
import pathlib
from typing import Any

from core import contract
from shared import utils

FORMAT_VERSION = 1


def dumps(catalog: contract.MappingCatalog) -> str:
    """Returns the canonical JSON text of a catalog."""
    data = {
        "format_version": FORMAT_VERSION,
        "name": catalog.name,
        "source_file": catalog.source_file,
        "labels": catalog.labels,
        "lookups": catalog.lookups,
        "mappings": [_mapping_to_dict(mapping) for mapping in catalog.mappings],
    }
    return json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def loads(text: str) -> contract.MappingCatalog:
    """Reads a catalog from its JSON text.

    Raises:
        ValueError: If the text is not in the supported format version.
    """
    data = json.loads(text)
    version = data.get("format_version")
    if version != FORMAT_VERSION:
        raise ValueError(
            f"Unsupported compiled mapping format {version!r} (expected"
            f" {FORMAT_VERSION})"
        )

    return contract.MappingCatalog(
        name=data["name"],
        mappings=tuple(_mapping_from_dict(item) for item in data["mappings"]),
        lookups={name: dict(table) for name, table in data["lookups"].items()},
        source_file=data.get("source_file"),
        labels=dict(data.get("labels", {})),
    )


def load_catalog(path: pathlib.Path | str) -> contract.MappingCatalog:
    """Reads a compiled catalog file."""
    return loads(pathlib.Path(path).read_text(encoding="utf-8"))


def save_catalog(
    path: pathlib.Path | str, catalog: contract.MappingCatalog
) -> None:
    utils.atomic_write_text(pathlib.Path(path), dumps(catalog))


def _location_from_json(data: str | dict[str, Any] | None) -> str | None:
    """A location; files compiled before it was text hold its parts."""
    if not isinstance(data, dict):
        return data
    parts = [data["file"], data.get("sheet"), data.get("row")]
    if parts[2] is not None:
        parts[2] = f"row {parts[2]}"
    return " › ".join(str(part) for part in parts if part is not None)


def _mapping_to_dict(mapping: contract.MappingSpec) -> dict[str, Any]:
    return {
        "name": mapping.name,
        "source": mapping.source,
        "target": mapping.target,
        "version": mapping.version,
        "status": mapping.status.value,
        "owner": mapping.owner,
        "unmapped_source_fields": mapping.unmapped_source_fields.value,
        "ignored_source_fields": list(mapping.ignored_source_fields),
        "row_filter": mapping.row_filter,
        "row_split": mapping.row_split,
        "key_version": mapping.key_version,
        "key_hashing": (
            mapping.key_hashing.value if mapping.key_hashing else None
        ),
        "key_case": mapping.key_case.value,
        "key_namespace": mapping.key_namespace,
        "location": mapping.location,
        "fields": [_field_to_dict(field) for field in mapping.fields],
    }


def _mapping_from_dict(data: dict[str, Any]) -> contract.MappingSpec:
    return contract.MappingSpec(
        name=data["name"],
        source=data["source"],
        target=data["target"],
        version=int(data["version"]),
        status=contract.MappingStatus(data["status"]),
        owner=data.get("owner"),
        unmapped_source_fields=contract.UnmappedPolicy(
            data["unmapped_source_fields"]
        ),
        ignored_source_fields=tuple(data["ignored_source_fields"]),
        row_filter=data.get("row_filter"),
        row_split=data.get("row_split"),
        key_version=int(data.get("key_version", 1)),
        key_hashing=_key_hashing(data),
        key_case=contract.KeyCase(
            data.get("key_case", contract.KeyCase.PRESERVE.value)
        ),
        key_namespace=data.get("key_namespace"),
        location=_location_from_json(data.get("location")),
        fields=tuple(_field_from_dict(item) for item in data["fields"]),
    )


def _key_hashing(data: dict[str, Any]) -> contract.KeyHashing | None:
    """The key hashing of a mapping: null for the default; files written
    before it had a default always name it, uuid5 when they do not."""
    value = data.get("key_hashing", contract.KeyHashing.UUID5.value)
    return None if value is None else contract.KeyHashing(value)


def _field_to_dict(field: contract.FieldSpec) -> dict[str, Any]:
    return {
        "target": field.target,
        "sources": list(field.sources),
        "target_type": field.target_type,
        "required": field.required,
        "transform": field.transform,
        "params": field.params,
        "format": field.format,
        "default": field.default,
        "notes": field.notes,
        "primary_key": field.primary_key,
        "references": field.references,
        "personal": field.personal,
        "location": field.location,
    }


def _field_from_dict(data: dict[str, Any]) -> contract.FieldSpec:
    return contract.FieldSpec(
        target=data["target"],
        sources=tuple(data["sources"]),
        target_type=data.get("target_type"),
        required=bool(data["required"]),
        transform=data.get("transform"),
        params=dict(data.get("params", {})),
        format=data.get("format"),
        default=data.get("default"),
        notes=data.get("notes"),
        primary_key=bool(data.get("primary_key", False)),
        references=data.get("references"),
        personal=data.get("personal"),
        location=_location_from_json(data.get("location")),
    )
