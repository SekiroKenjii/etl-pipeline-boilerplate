"""The ports of database tables: reading, writing, and inspecting them.

No SQL text, driver paramstyle or native type name crosses them; adapters
(`infra.database`) know their dialect and driver.
"""

from __future__ import annotations

import abc
from collections.abc import Collection, Iterator, Sequence
import dataclasses
import datetime
import decimal
import enum
import fnmatch
import itertools
import typing
import uuid

from core.contract import formats


class Operator(enum.StrEnum):
    """How a `Condition` compares a column with its value."""

    EQ = "="
    NE = "!="
    LT = "<"
    LE = "<="
    GT = ">"
    GE = ">="
    IN = "in"
    NOT_IN = "not in"
    IS_NULL = "is null"
    IS_NOT_NULL = "is not null"


@dataclasses.dataclass(frozen=True, slots=True)
class Condition:
    """A condition on one column, which every adapter can translate.

    Conditions are ANDed. For anything more complex (joins, functions, OR),
    read from a view of the source database.

    Attributes:
        column: The column.
        operator: The comparison.
        value: The value compared with, as the database driver gives it
            (any type: a bson ObjectId is a value too); a collection for IN
            and NOT_IN (`values`), nothing for IS_NULL and IS_NOT_NULL.
    """

    column: str
    operator: Operator
    value: object = None

    def __post_init__(self) -> None:
        if self.operator in (Operator.IN, Operator.NOT_IN) and (
            isinstance(self.value, (str, bytes))
            or not isinstance(self.value, Collection)
        ):
            raise ValueError(f"{self.operator} needs a collection of values")

    @property
    def values(self) -> tuple[object, ...]:
        """The values of an IN or NOT_IN condition.

        Raises:
            ValueError: If the operator takes no collection.
        """
        if self.operator not in (Operator.IN, Operator.NOT_IN):
            raise ValueError(f"{self.operator} takes no collection of values")
        # `__post_init__` checked that it is one.
        return tuple(typing.cast(Collection[object], self.value))


@dataclasses.dataclass(frozen=True, slots=True)
class TableQuery:
    """The rows to read from a table, in a total order.

    Rows are read by keyset pagination on `order_by`, so the `order_by` columns
    must be unique together and never NULL: a watermark, then a key as
    tie-breaker.

    Attributes:
        table: The table, `schema.table` or `table`.
        order_by: The columns of the order, unique together.
        columns: The columns to read; every column when empty.
        after: Only rows strictly after these values of the first
            `len(after)` columns of `order_by` (a keyset cursor).
        lower: Only rows whose first `order_by` column is >= this value.
        upper: Only rows whose first `order_by` column is <= this value.
        filters: Only rows matching all these conditions.
    """

    table: str
    order_by: tuple[str, ...]
    columns: tuple[str, ...] = ()
    after: tuple[object, ...] | None = None
    lower: object = None
    upper: object = None
    filters: tuple[Condition, ...] = ()

    def __post_init__(self) -> None:
        if not self.order_by:
            raise ValueError(f"Reading {self.table} needs an order_by")
        if self.after is not None and not (
            0 < len(self.after) <= len(self.order_by)
        ):
            raise ValueError(
                f"after has {len(self.after)} value(s), order_by"
                f" {len(self.order_by)} column(s)"
            )


class ITableReader(abc.ABC):
    """Reads the rows of database tables. Thread-safe."""

    @abc.abstractmethod
    def read(self, query: TableQuery) -> Iterator[formats.Row]:
        """Yields the rows of `query`, in its order.

        Raises:
            FatalError: If a row has NULL in an `order_by` column: it cannot
                be paginated, nor tracked by a watermark.
        """

    @abc.abstractmethod
    def max_value(
        self,
        table: str,
        column: str,
        filters: Sequence[Condition] = (),
    ) -> object:
        """Returns the largest value of `column`, None for an empty table."""

    def column_family(self, table: str, column: str) -> TypeFamily:
        """Returns the family of the type of a column.

        This default guesses it from the largest value (`family_of`), for
        readers without a catalog; adapters with one reflect the column.
        """
        return family_of(self.max_value(table, column))

    @abc.abstractmethod
    def count(self, table: str, filters: Sequence[Condition] = ()) -> int:
        """Returns the number of rows of `table` matching `filters`."""

    def keys_at(
        self,
        table: str,
        order_by: Sequence[str],
        offsets: Sequence[int],
        filters: Sequence[Condition] = (),
    ) -> list[tuple[object, ...]]:
        """Returns the values of `order_by` of the rows at `offsets`.

        Rows matching `filters` are numbered from 0 in `order_by` order (unique
        together, never NULL): quantiles of a key, to split a table into ranges.
        Offsets past the last row are left out. This default reads every row up
        to the last offset; adapters override it to skip through an index.

        Args:
            table: The table.
            order_by: The columns of the order, unique together.
            offsets: The offsets, ascending, from 0.
            filters: Only rows matching all these conditions.

        Raises:
            ValueError: If `offsets` are not ascending, or below 0.
            FatalError: If a row has NULL in an `order_by` column.
        """
        if any(b <= a for a, b in itertools.pairwise([-1, *offsets])):
            raise ValueError(f"offsets must be ascending, from 0: {offsets}")
        wanted = iter(offsets)
        target = next(wanted, None)
        found: list[tuple[object, ...]] = []
        if target is None:
            return found
        query = TableQuery(
            table,
            order_by=tuple(order_by),
            columns=tuple(order_by),
            filters=tuple(filters),
        )
        for index, row in enumerate(self.read(query)):
            if index == target:
                found.append(tuple(row[column] for column in order_by))
                target = next(wanted, None)
                if target is None:
                    break
        return found


class WriteMode(enum.StrEnum):
    """How rows are written to a table."""

    INSERT = "insert"
    UPSERT = "upsert"
    INSERT_MISSING = "insert_missing"
    UPDATE = "update"


@dataclasses.dataclass(frozen=True, slots=True)
class TableTarget:
    """Where and how a loader writes rows.

    Attributes:
        table: The table, `schema.table` or `table`.
        key: The columns of a unique constraint, required by UPSERT and
            INSERT_MISSING: they decide whether a row is new.
        mode: How rows are written.
        update_columns: The columns an UPSERT updates; every column of the rows
            except the key when None. Leave out columns owned by the target,
            such as a creation date.
    """

    table: str
    key: tuple[str, ...] = ()
    mode: WriteMode = WriteMode.UPSERT
    update_columns: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        if self.mode is not WriteMode.INSERT and not self.key:
            raise ValueError(f"{self.mode} into {self.table} needs a key")


@dataclasses.dataclass(slots=True)
class WriteResult:
    """What writing a batch did.

    Attributes:
        created: The rows inserted.
        updated: The existing rows changed.
        unchanged: The rows already in the table as they are (or, for
            INSERT_MISSING, already there), and the duplicates of a key
            within the batch.
    """

    created: int = 0
    updated: int = 0
    unchanged: int = 0


class ITableWriter(abc.ABC):
    """Writes rows to database tables. Thread-safe."""

    @abc.abstractmethod
    def write(
        self, target: TableTarget, rows: Sequence[formats.Row]
    ) -> WriteResult:
        """Writes a batch in one transaction: all of it, or nothing.

        Every row must have the same columns. When a key appears several times
        in the batch, the last row wins.

        Raises:
            ValueError: If the rows do not have the same columns.
        """

    @abc.abstractmethod
    def delete(self, table: str, rows: Sequence[formats.Row]) -> int:
        """Deletes the rows matching any of `rows`, in one transaction.

        Each row gives the values of the columns to match: a key, or a parent
        key to delete all its children. The rows of one call must have the
        same columns.

        Returns:
            The rows deleted.

        Raises:
            ValueError: If the rows do not have the same columns.
        """


class TypeFamily(enum.StrEnum):
    """The family of a column type, whatever the database calls it.

    `NUMBER(10)` (Oracle), `int` (SQL Server) and `integer` (Postgres) are
    all INTEGER: values compare within a family, and numbers across INTEGER
    and DECIMAL.
    """

    INTEGER = "integer"
    DECIMAL = "decimal"
    FLOAT = "float"
    TEXT = "text"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    TIME = "time"
    UUID = "uuid"
    BINARY = "binary"
    JSON = "json"
    OBJECT_ID = "object_id"
    OTHER = "other"

    def comparable(self, other: TypeFamily) -> bool:
        """Whether values of the two families can be compared."""
        numbers = (TypeFamily.INTEGER, TypeFamily.DECIMAL)
        if self in numbers and other in numbers:
            return True
        return self is other and self is not TypeFamily.OTHER


# Subclasses first: a bool is an int, a datetime a date.
_VALUE_FAMILIES: tuple[tuple[type, TypeFamily], ...] = (
    (bool, TypeFamily.BOOLEAN),
    (int, TypeFamily.INTEGER),
    (decimal.Decimal, TypeFamily.DECIMAL),
    (float, TypeFamily.FLOAT),
    (str, TypeFamily.TEXT),
    (bytes, TypeFamily.BINARY),
    (datetime.datetime, TypeFamily.DATETIME),
    (datetime.date, TypeFamily.DATE),
    (datetime.time, TypeFamily.TIME),
    (uuid.UUID, TypeFamily.UUID),
)


def family_of(value: object) -> TypeFamily:
    """Returns the family of a value as a driver gives it; OTHER for None
    or a type of no family."""
    for base, family in _VALUE_FAMILIES:
        if isinstance(value, base):
            return family
    return TypeFamily.OTHER


@dataclasses.dataclass(frozen=True, slots=True)
class ColumnInfo:
    """A column of a table, as the database catalog describes it.

    Attributes:
        name: The column name.
        data_type: Its type, as the database names it, for humans.
        nullable: Whether it accepts NULL.
        family: The family of its type, for comparisons.
    """

    name: str
    data_type: str
    nullable: bool
    family: TypeFamily = TypeFamily.OTHER


@dataclasses.dataclass(frozen=True, slots=True)
class TableInfo:
    """A table, as the database catalog describes it.

    Attributes:
        name: The table, `schema.table`.
        columns: Its columns, in order.
        primary_key: The columns of its primary key; empty if it has none.
        unique: The columns of its other unique constraints and indexes.
        estimated_rows: The row count of the planner statistics, or -1 when
            the table was never analyzed.
    """

    name: str
    columns: tuple[ColumnInfo, ...]
    primary_key: tuple[str, ...] = ()
    unique: tuple[tuple[str, ...], ...] = ()
    estimated_rows: int = -1

    @property
    def bare_name(self) -> str:
        """The name of the table without its schema."""
        return self.name.rpartition(".")[2]

    def column(self, name: str) -> ColumnInfo:
        """Returns the column `name`.

        Raises:
            KeyError: If the table has no such column.
        """
        for column in self.columns:
            if column.name == name:
                return column
        raise KeyError(f"{self.name} has no column {name!r}")


@dataclasses.dataclass(frozen=True, slots=True)
class ForeignKeyInfo:
    """A foreign key declared in the database catalog.

    Attributes:
        name: The constraint name.
        table: The referencing table, `schema.table`.
        columns: Its referencing columns.
        referenced_table: The referenced table, `schema.table`.
        referenced_columns: The referenced columns.
    """

    name: str
    table: str
    columns: tuple[str, ...]
    referenced_table: str
    referenced_columns: tuple[str, ...]


@dataclasses.dataclass(frozen=True, slots=True)
class ColumnProfile:
    """Counts over the values of a column.

    Attributes:
        rows: The rows counted.
        non_null: The rows where the column is not NULL.
        distinct: The distinct non-NULL values.
    """

    rows: int
    non_null: int
    distinct: int

    @property
    def is_unique(self) -> bool:
        """Whether every row has its own, non-NULL, value: a candidate key."""
        return 0 < self.rows == self.non_null == self.distinct


@dataclasses.dataclass(frozen=True, slots=True)
class InclusionCheck:
    """How many values of a column are found in another one.

    Attributes:
        distinct: The distinct non-NULL values of the referencing column.
        unmatched: Those not found in the referenced column.
    """

    distinct: int
    unmatched: int

    @property
    def coverage(self) -> float:
        """The share of values found, 1.0 when there is no value."""
        if self.distinct == 0:
            return 1.0
        return 1 - self.unmatched / self.distinct


def matches(name: str, pattern: str | None) -> bool:
    """Whether a table name matches a glob, case-insensitively."""
    return pattern is None or fnmatch.fnmatchcase(name.lower(), pattern.lower())


class ISchemaInspector(abc.ABC):
    """Reads the catalog of a database, and profiles its columns.

    Profiling queries scan tables: run them against a replica or a copy.
    """

    @abc.abstractmethod
    def tables(
        self, schema: str, pattern: str | None = None
    ) -> list[TableInfo]:
        """Returns the tables of `schema`, by name.

        Args:
            schema: The schema.
            pattern: Only the tables whose name matches this glob,
                case-insensitively (`CUST*`); every table when None.
        """

    @abc.abstractmethod
    def foreign_keys(
        self, schema: str, pattern: str | None = None
    ) -> list[ForeignKeyInfo]:
        """Returns the foreign keys declared on the tables of `schema`.

        Args:
            schema: The schema.
            pattern: Only those of the tables whose name matches this glob.
        """

    @abc.abstractmethod
    def profile(
        self,
        table: str,
        columns: Sequence[str],
        sample_rows: int | None = None,
    ) -> dict[str, ColumnProfile]:
        """Counts the values of `columns`, by column.

        Args:
            table: The table, `schema.table`.
            columns: The columns to profile.
            sample_rows: Profile only that many rows, when set: a column
                unique on a sample may not be unique on the table.
        """

    @abc.abstractmethod
    def inclusion(
        self,
        table: str,
        column: str,
        referenced_table: str,
        referenced_column: str,
    ) -> InclusionCheck:
        """Checks whether the values of a column are in another column."""
