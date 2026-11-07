"""What mapping a source row produces: target records, issues, references."""

from __future__ import annotations

from collections.abc import Mapping
import dataclasses

from core import contract

# The `field` of an issue about the whole row, e.g. a failing row hook.
ROW = "*"


@dataclasses.dataclass(frozen=True, slots=True)
class MappingIssue:
    """Why one field of one record could not be mapped: bad data, not a bug.

    Attributes:
        field: The target column, or `ROW` for the whole row.
        message: What is wrong.
        value: The value at fault, if any.
    """

    field: str
    message: str
    value: object = None


@dataclasses.dataclass(frozen=True, slots=True)
class MappedRecord:
    """A target row, the issues met while producing it, and its lineage.

    Attributes:
        data: The target row, by target column.
        issues: What could not be mapped.
        mapping: The mapping that produced it.
        version: The version of that mapping.
        source_key: The source record it comes from, if known.
        part: Tells apart the rows a row splitter made out of one source row
            (0, 1, 2...).
        entity: The target entity.
        key: The surrogate key, when the mapping has a primary key.
        natural_key: The canonical natural key behind `key`.
        foreign_keys: The canonical natural keys of the referenced rows, by
            foreign key column, for the foreign keys that are set.
        deleted: Whether the record is a deletion: `data` then holds the
            columns matching the rows to delete (the key, or a scope).
    """

    data: dict[str, object]
    issues: tuple[MappingIssue, ...]
    mapping: str
    version: int
    source_key: str | None = None
    part: int = 0
    entity: str = ""
    key: str | None = None
    natural_key: tuple[str, ...] | None = None
    foreign_keys: Mapping[str, tuple[str, ...]] = dataclasses.field(
        default_factory=dict[str, tuple[str, ...]]
    )
    deleted: bool = False

    @property
    def ok(self) -> bool:
        return not self.issues

    def key_entry(self) -> contract.KeyEntry | None:
        """Returns what to register in the crosswalk once this is loaded."""
        if self.deleted or self.key is None or self.natural_key is None:
            return None
        return contract.KeyEntry(self.entity, self.key, self.natural_key)


@dataclasses.dataclass(frozen=True, slots=True)
class Reference:
    """A foreign key field, and what it points to.

    Attributes:
        field: The target column holding the foreign key.
        mapping: The mapping it references.
        entity: The target entity of that mapping.
        self_reference: Whether the mapping references itself, e.g.
            `employee.manager`.
    """

    field: str
    mapping: str
    entity: str
    self_reference: bool
