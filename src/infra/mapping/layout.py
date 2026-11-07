"""The layout of mapping workbooks, shared by the reader and the template.

- `Mappings`: one row per mapping;
- one sheet per mapping, named after it: one row per target column;
- `lookup_<name>`: lookup tables, key then value;
- `README`, and sheets whose name starts with `_`, are ignored.

Row 1 of every sheet is the header, in any case.
"""

from __future__ import annotations

import unicodedata

from core import contract

INDEX_SHEET = ("Mappings",)
LOOKUP_PREFIX = "lookup_"
IGNORED_SHEETS = ("readme",)

# Canonical key -> (header, other accepted spellings...).
INDEX_COLUMNS: dict[str, tuple[str, ...]] = {
    "mapping": ("Mapping", "mapping name"),
    "source": ("Source table", "source"),
    "target": ("Target table", "target"),
    "version": ("Version",),
    "status": ("Status",),
    "owner": ("Owner",),
    "unmapped": ("Unmapped source fields",),
    "ignored": ("Ignored source fields",),
    "row_filter": ("Row filter",),
    "row_split": ("Row split",),
    "key_version": ("Key version",),
    "key_hashing": ("Key hashing",),
    "key_case": ("Key case",),
    "key_namespace": ("Key namespace",),
}
FIELD_COLUMNS: dict[str, tuple[str, ...]] = {
    "target": ("Target column",),
    "target_type": ("Target type",),
    "required": ("Required",),
    "sources": ("Source columns", "source column"),
    "transform": ("Transform",),
    "params": ("Parameters", "params"),
    "format": ("Format",),
    "default": ("Default",),
    "notes": ("Notes",),
    "key": ("Key",),
    "references": ("References",),
    "personal": ("Personal data",),
}
REQUIRED_INDEX_COLUMNS = ("mapping", "source", "target")
REQUIRED_FIELD_COLUMNS = ("target",)

YES = frozenset({"y", "yes", "true", "1", "x"})
NO = frozenset({"n", "no", "false", "0"})
PRIMARY_KEY = "pk"
STATUSES = {
    "draft": contract.MappingStatus.DRAFT,
    "approved": contract.MappingStatus.APPROVED,
}
POLICIES = {
    "error": contract.UnmappedPolicy.ERROR,
    "ignore": contract.UnmappedPolicy.IGNORE,
}
HASHINGS = {
    "uuid5": contract.KeyHashing.UUID5,
    "hmac": contract.KeyHashing.HMAC,
}
CASES = {
    "preserve": contract.KeyCase.PRESERVE,
    "upper": contract.KeyCase.UPPER,
    "lower": contract.KeyCase.LOWER,
}


def normalize(text: str) -> str:
    """Returns `text` as compared: NFC, case-folded, single spaces."""
    return " ".join(unicodedata.normalize("NFC", text).casefold().split())


def headers(columns: dict[str, tuple[str, ...]]) -> list[str]:
    """Returns the headers of `columns`, as the template writes them."""
    return [names[0] for names in columns.values()]
