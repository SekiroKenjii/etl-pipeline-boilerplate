"""The port and data types of source-to-target mapping specifications (STM).

A spec is format-neutral: authored in xlsx (`infra.mapping`), compiled to JSON
(`core.mapping.catalog`), turned into a transformer by `core.mapping`. Every
element keeps where it was defined (`crm.xlsx › customer › row 12`), for
errors.
"""

from __future__ import annotations

import dataclasses
import enum
import pathlib
from typing import Protocol

from core.contract import exceptions
from core.contract import keys


class MappingStatus(enum.StrEnum):
    """The review status of a mapping."""

    DRAFT = "draft"
    APPROVED = "approved"


class UnmappedPolicy(enum.StrEnum):
    """What to do with a source column no field uses and none ignores."""

    ERROR = "error"
    IGNORE = "ignore"


@dataclasses.dataclass(frozen=True, slots=True)
class FieldSpec:
    """How one target column is produced: source columns, transform, cast.

    Keys: a `primary_key` field is the surrogate key derived from its source
    columns (the natural key). A field that `references` another mapping is a
    foreign key: its source columns hold that mapping's natural key, and its
    value is that mapping's surrogate key, computed without any lookup.

    Attributes:
        target: The target column.
        sources: The source columns, in the order the transform takes them.
        target_type: The target type, written like a database type
            (`VARCHAR(200)`, `NUMBER(10,2)`, `DATE`...).
        required: Whether an empty result is an issue.
        transform: The transform, by name; None takes the single source.
        params: The parameters of the transform, as text from the sheet.
        format: How dates are written in the source (`dd/MM/yyyy`).
        default: The value used when the result is empty.
        notes: Free text for the authors.
        primary_key: Whether this is the surrogate key of the mapping.
        references: The mapping this foreign key points to.
        personal: The kind of personal data the column holds (`name`,
            `email`, `phone`...), anonymized where personal data must not
            go; None for none.
        location: Where the field is defined, as the reader words it.
    """

    target: str
    sources: tuple[str, ...] = ()
    target_type: str | None = None
    required: bool = False
    transform: str | None = None
    params: dict[str, str] = dataclasses.field(default_factory=dict[str, str])
    format: str | None = None
    default: str | None = None
    notes: str | None = None
    primary_key: bool = False
    references: str | None = None
    personal: str | None = None
    location: str | None = None


@dataclasses.dataclass(frozen=True, slots=True)
class MappingSpec:
    """The mapping of one source entity to one target entity.

    Attributes:
        name: The unique name of the mapping in its catalog.
        source: The source entity, e.g. `legacy.CUST_MASTER`.
        target: The target entity, e.g. `crm.customer`.
        fields: How each target column is produced.
        version: The revision of the mapping, for lineage.
        status: The review status.
        owner: Who maintains the mapping.
        unmapped_source_fields: What to do with source columns nothing uses.
        ignored_source_fields: Source columns knowingly left out.
        row_filter: A registered row filter: drops source rows on purpose
            (counted as filtered).
        row_split: A registered row splitter: turns one source row into
            several (e.g. unpivot).
        key_version: Part of the namespace of the surrogate keys: bump it only
            to change every key of the entity on purpose.
        key_hashing: How keys are derived; HMAC for natural keys holding
            personal data. None: HMAC when a key secret is configured,
            UUIDv5 otherwise.
        key_case: The letter case natural keys are normalized to: UPPER or
            LOWER for a source with a case-insensitive collation.
        key_namespace: Names the namespace of the surrogate keys, so that
            renaming the source or target table keeps them; None derives it
            from both, `<source>:<target>`.
        location: Where the mapping is defined, as the reader words it.
    """

    name: str
    source: str
    target: str
    fields: tuple[FieldSpec, ...]
    version: int = 1
    status: MappingStatus = MappingStatus.DRAFT
    owner: str | None = None
    unmapped_source_fields: UnmappedPolicy = UnmappedPolicy.ERROR
    ignored_source_fields: tuple[str, ...] = ()
    row_filter: str | None = None
    row_split: str | None = None
    key_version: int = 1
    key_hashing: keys.KeyHashing | None = None
    key_case: keys.KeyCase = keys.KeyCase.PRESERVE
    key_namespace: str | None = None
    location: str | None = None


@dataclasses.dataclass(frozen=True, slots=True)
class MappingCatalog:
    """Every mapping of one workbook (one domain), and their lookup tables.

    Attributes:
        name: The domain, i.e. the workbook name.
        mappings: The mappings.
        lookups: The lookup tables, by name: key to value.
        source_file: The workbook the catalog was read from.
        labels: The column headers as the authors wrote them, by field key,
            so that errors name columns the way the spreadsheet does.
    """

    name: str
    mappings: tuple[MappingSpec, ...]
    lookups: dict[str, dict[str, str]] = dataclasses.field(
        default_factory=dict[str, dict[str, str]]
    )
    source_file: str | None = None
    labels: dict[str, str] = dataclasses.field(default_factory=dict[str, str])

    def get(self, name: str) -> MappingSpec:
        """Returns the mapping called `name`.

        Raises:
            KeyError: If there is no such mapping.
        """
        for mapping in self.mappings:
            if mapping.name == name:
                return mapping
        known = ", ".join(mapping.name for mapping in self.mappings) or "none"
        raise KeyError(f"No mapping {name!r} in {self.name!r} (known: {known})")


@dataclasses.dataclass(frozen=True, slots=True)
class MappingProblem:
    """A problem of a mapping definition, and where to fix it.

    Attributes:
        message: What is wrong.
        location: Where, if known.
    """

    message: str
    location: str | None = None

    def __str__(self) -> str:
        if self.location is None:
            return self.message
        return f"{self.location}: {self.message}"


class MappingDefinitionError(exceptions.FatalError):
    """A mapping definition cannot be used.

    Attributes:
        problems: Every problem found, not only the first one.
    """

    def __init__(self, problems: list[MappingProblem]) -> None:
        self.problems = problems
        lines = "\n".join(f"  - {problem}" for problem in problems)
        super().__init__(f"{len(problems)} mapping problem(s):\n{lines}")


class IMappingReader(Protocol):
    """Reads an authored mapping file (xlsx...) into a catalog."""

    def read(self, path: pathlib.Path) -> MappingCatalog:
        """Returns the catalog of `path`.

        Raises:
            MappingDefinitionError: With every problem of the file.
        """
        ...
