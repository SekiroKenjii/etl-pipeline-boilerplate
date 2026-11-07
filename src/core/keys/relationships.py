"""Suggests the keys and relationships of a legacy schema; applies nothing.

Evidence, strongest first:

1. declared constraints: primary keys, unique constraints, foreign keys;
2. candidate keys: key-like columns whose values are unique and never NULL;
3. inferred foreign keys: a column whose name points to a candidate key of
   another table (`customer_id` -> `customer.id`), of a compatible type,
   whose values are (mostly) found there. Values not found are orphans.

BAs and DBAs confirm the report, then write it into the mapping workbooks.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
import dataclasses
import enum
import re

from core import contract

_KEY_LIKE_NAME = re.compile(
    r"(^|_)(id|no|num|nr|code|key|ref|uuid|guid)$|id$", re.I
)
_GENERIC_KEY_NAMES = frozenset(
    {"id", "no", "num", "nr", "code", "key", "ref", "uuid"}
)


class Evidence(enum.StrEnum):
    """Why a key or a relationship is suggested."""

    DECLARED = "declared"
    PROFILED = "profiled"
    INFERRED = "inferred"


@dataclasses.dataclass(frozen=True, slots=True)
class CandidateKey:
    """Columns that identify the rows of a table.

    Attributes:
        table: The table, `schema.table`.
        columns: The columns of the key.
        evidence: DECLARED (primary key, unique constraint) or PROFILED.
        primary: Whether it is the declared primary key.
    """

    table: str
    columns: tuple[str, ...]
    evidence: Evidence
    primary: bool = False


@dataclasses.dataclass(frozen=True, slots=True)
class Relationship:
    """A foreign key, declared or suggested.

    Attributes:
        table: The referencing (child) table.
        columns: Its referencing columns.
        referenced_table: The referenced (parent) table.
        referenced_columns: The referenced columns.
        evidence: DECLARED or INFERRED.
        coverage: The share of the distinct values found in the parent; None
            when not checked (declared foreign keys are enforced).
        orphans: The distinct values not found in the parent.
        name: The constraint name, for a declared foreign key.
    """

    table: str
    columns: tuple[str, ...]
    referenced_table: str
    referenced_columns: tuple[str, ...]
    evidence: Evidence
    coverage: float | None = None
    orphans: int = 0
    name: str | None = None

    @property
    def self_reference(self) -> bool:
        return self.table == self.referenced_table


@dataclasses.dataclass(frozen=True, slots=True)
class RelationshipReport:
    """The keys and relationships found in a schema.

    Attributes:
        schema: The schema inspected.
        keys: The candidate keys, by table.
        relationships: The foreign keys, declared then inferred, by table.
        sampled: Whether profiling ran on a sample of the rows: a column
            unique on a sample may not be unique on the whole table.
    """

    schema: str
    keys: tuple[CandidateKey, ...]
    relationships: tuple[Relationship, ...]
    sampled: bool = False

    def as_dict(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "sampled": self.sampled,
            "keys": [dataclasses.asdict(key) for key in self.keys],
            "relationships": [
                dataclasses.asdict(relationship)
                for relationship in self.relationships
            ],
        }

    def to_text(self) -> str:
        """Returns the report as plain text, one line per key and relation."""
        lines = [f"Schema {self.schema}", "", "Candidate keys:"]
        lines.extend(
            f"  {key.table} ({', '.join(key.columns)})  {key.evidence}"
            + ("  primary key" if key.primary else "")
            for key in self.keys
        )
        if self.sampled:
            lines.append("  (profiled on a sample: confirm on the full table)")
        lines.extend(["", "Relationships:"])
        for relation in self.relationships:
            line = (
                f"  {relation.table} ({', '.join(relation.columns)}) ->"
                f" {relation.referenced_table}"
                f" ({', '.join(relation.referenced_columns)})"
                f"  {relation.evidence}"
            )
            if relation.coverage is not None:
                line += f"  coverage {relation.coverage:.1%}"
                if relation.orphans:
                    line += f", {relation.orphans} orphan value(s)"
            lines.append(line)
        if not self.relationships:
            lines.append("  none found")
        return "\n".join(lines) + "\n"


def discover_relationships(
    inspector: contract.ISchemaInspector,
    schema: str,
    *,
    min_coverage: float = 0.9,
    sample_rows: int | None = None,
    match_names: bool = True,
    tables: str | None = None,
) -> RelationshipReport:
    """Finds the candidate keys and foreign keys of the tables of `schema`.

    Args:
        inspector: Reads the catalog and profiles the data.
        schema: The schema to inspect.
        min_coverage: The share of a column's values that must be found in a
            candidate key to suggest a foreign key; below 1.0, foreign keys
            with orphans (dirty legacy data) are suggested too.
        sample_rows: Profile candidate keys on that many rows per table,
            instead of the whole table.
        match_names: Whether a foreign key needs a name pointing to its
            parent. False checks every pair of columns of compatible types: a
            query per pair, so only for small schemas.
        tables: Only the tables whose name (without schema) matches this
            glob, case-insensitively, e.g. `CUST*`: legacy schemas often hold
            thousands of tables.

    Returns:
        The report, for a human to review.

    Raises:
        ValueError: If `min_coverage` is not in (0, 1].
    """
    if not 0 < min_coverage <= 1:
        raise ValueError("min_coverage must be in (0, 1]")

    included = inspector.tables(schema, tables)
    names = {table.name for table in included}
    keys = list(_declared_keys(included))
    keys.extend(_profiled_keys(inspector, included, keys, sample_rows))

    declared = [
        Relationship(
            table=fk.table,
            columns=fk.columns,
            referenced_table=fk.referenced_table,
            referenced_columns=fk.referenced_columns,
            evidence=Evidence.DECLARED,
            name=fk.name,
        )
        for fk in inspector.foreign_keys(schema, tables)
        if fk.referenced_table in names
    ]
    covered = {(r.table, r.columns) for r in declared}
    inferred = [
        relation
        for relation in _inferred(
            inspector, included, keys, match_names, covered
        )
        if relation.coverage is not None and relation.coverage >= min_coverage
    ]

    return RelationshipReport(
        schema=schema,
        keys=tuple(sorted(keys, key=lambda k: (k.table, not k.primary))),
        relationships=tuple(
            sorted(declared, key=lambda r: (r.table, r.columns))
            + sorted(inferred, key=lambda r: (r.table, r.columns))
        ),
        sampled=sample_rows is not None,
    )


def _declared_keys(
    tables: Iterable[contract.TableInfo],
) -> Iterator[CandidateKey]:
    for table in tables:
        if table.primary_key:
            yield CandidateKey(
                table.name, table.primary_key, Evidence.DECLARED, primary=True
            )
        for columns in table.unique:
            yield CandidateKey(table.name, columns, Evidence.DECLARED)


def _profiled_keys(
    inspector: contract.ISchemaInspector,
    tables: Iterable[contract.TableInfo],
    declared: list[CandidateKey],
    sample_rows: int | None,
) -> Iterator[CandidateKey]:
    """Yields the key-like columns that are unique and never NULL.

    Only single columns whose name looks like a key are profiled, one query
    per table.
    """
    known = {(key.table, key.columns) for key in declared}
    for table in tables:
        columns = [
            column.name
            for column in table.columns
            if _KEY_LIKE_NAME.search(column.name)
            and (table.name, (column.name,)) not in known
        ]
        if not columns:
            continue
        profiles = inspector.profile(table.name, columns, sample_rows)
        for column in columns:
            if profiles[column].is_unique:
                yield CandidateKey(table.name, (column,), Evidence.PROFILED)


def _inferred(
    inspector: contract.ISchemaInspector,
    tables: list[contract.TableInfo],
    keys: list[CandidateKey],
    match_names: bool,
    covered: set[tuple[str, tuple[str, ...]]],
) -> Iterator[Relationship]:
    """Yields single-column foreign keys to single-column candidate keys.

    Columns in `covered` (declared foreign keys) are not checked. Between two
    unique columns, values are included both ways: a declared key is taken
    for the parent, never for the child of a merely profiled key.
    """
    declared = {
        (key.table, key.columns)
        for key in keys
        if key.evidence is Evidence.DECLARED
    }
    described = {
        (table.name, column.name): column
        for table in tables
        for column in table.columns
    }
    # A key of an empty collection has no described column: nothing to match.
    targets = [
        (key, described[(key.table, key.columns[0])])
        for key in keys
        if len(key.columns) == 1 and (key.table, key.columns[0]) in described
    ]

    for table in tables:
        for column in table.columns:
            if (table.name, (column.name,)) in covered:
                continue
            for key, key_column in targets:
                if not _candidate(
                    table.name, column, key, key_column, declared, match_names
                ):
                    continue
                check = inspector.inclusion(
                    table.name, column.name, key.table, key_column.name
                )
                if check.distinct == 0:
                    continue
                yield Relationship(
                    table=table.name,
                    columns=(column.name,),
                    referenced_table=key.table,
                    referenced_columns=key.columns,
                    evidence=Evidence.INFERRED,
                    coverage=check.coverage,
                    orphans=check.unmatched,
                )


def _candidate(
    table: str,
    column: contract.ColumnInfo,
    key: CandidateKey,
    key_column: contract.ColumnInfo,
    declared: set[tuple[str, tuple[str, ...]]],
    match_names: bool,
) -> bool:
    """Whether `table.column` may be a foreign key to `key`: worth checking
    its values."""
    if (key.table, key.columns[0]) == (table, column.name):
        return False  # The key itself.
    if (
        key.evidence is Evidence.PROFILED
        and (table, (column.name,)) in declared
    ):
        return False  # A declared key is a parent, never a child.
    if not column.family.comparable(key_column.family):
        return False
    return not match_names or _names_match(
        column.name, key.table, key_column.name
    )


def _names_match(column: str, table: str, key: str) -> bool:
    """Whether `column` is named after the key `key` of `table`.

    `cust_no` matches `cust_no`, and `customer_id` matches `customer.id`
    or `customers.id`; a generic name (`id`, `code`) only matches with the
    table name in front of it.
    """
    column, key = _plain(column), _plain(key)
    if column == key and key not in _GENERIC_KEY_NAMES:
        return True
    name = _plain(table.rpartition(".")[2])
    singular = name[:-2] if name.endswith("ses") else name.removesuffix("s")
    return column in {name + key, singular + key}


def _plain(name: str) -> str:
    return name.lower().replace("_", "")
