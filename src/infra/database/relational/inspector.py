"""Reading the catalog of any relational database, and profiling columns."""

from __future__ import annotations

from collections.abc import Sequence
import datetime
import decimal
from typing import Any
import uuid

from sqlalchemy import exc as sa_exc
import sqlalchemy as sa

from core import contract
from infra.database.relational import database


class RelationalSchemaInspector(
    database.RelationalAdapter, contract.ISchemaInspector
):
    """Reads the catalog through SQLAlchemy reflection; counts values.

    Catalog reads are cheap; `profile` and `inclusion` scan tables: run them
    against a replica or a copy of production. Row estimates come from the
    database's statistics (`dialects`); -1 when it has none.
    """

    def tables(
        self, schema: str, pattern: str | None = None
    ) -> list[contract.TableInfo]:
        """See `contract.ISchemaInspector.tables`.

        A table dropped while the catalog is read is left out.
        """
        found = []
        with self.db.connect(f"reading the tables of {schema}") as connection:
            inspector = sa.inspect(connection)
            for name in self._names(inspector, schema, pattern):
                try:
                    found.append(
                        self._table(connection, inspector, schema, name)
                    )
                except sa_exc.NoSuchTableError:
                    continue
        return found

    def foreign_keys(
        self, schema: str, pattern: str | None = None
    ) -> list[contract.ForeignKeyInfo]:
        found = []
        with self.db.connect(f"reading the foreign keys of {schema}") as conn:
            inspector = sa.inspect(conn)
            for table in self._names(inspector, schema, pattern):
                try:
                    keys = inspector.get_foreign_keys(table, schema=schema)
                except sa_exc.NoSuchTableError:
                    continue
                for fk in keys:
                    referred = fk["referred_schema"] or schema
                    found.append(
                        contract.ForeignKeyInfo(
                            name=fk.get("name") or "",
                            table=f"{schema}.{table}",
                            columns=tuple(fk["constrained_columns"]),
                            referenced_table=(
                                f"{referred}.{fk['referred_table']}"
                            ),
                            referenced_columns=tuple(fk["referred_columns"]),
                        )
                    )
        return found

    def _names(
        self, inspector: sa.Inspector, schema: str, pattern: str | None
    ) -> list[str]:
        """Returns the names of the tables to read, before reading them."""
        return sorted(
            name
            for name in inspector.get_table_names(schema=schema)
            if contract.matches(name, pattern)
        )

    def profile(
        self,
        table: str,
        columns: Sequence[str],
        sample_rows: int | None = None,
    ) -> dict[str, contract.ColumnProfile]:
        if not columns:
            return {}
        reflected = self.db.table(table)
        source: sa.Select[Any] = sa.select(
            *(database.column(reflected, name) for name in columns)
        )
        if sample_rows is not None:
            source = source.limit(sample_rows)
        rows = source.subquery("etl_profiled")
        counts: list[sa.ColumnElement[Any]] = [sa.func.count()]
        for name in columns:
            counts.append(sa.func.count(rows.c[name]))
            counts.append(sa.func.count(sa.distinct(rows.c[name])))

        with self.db.connect(f"profiling {table}") as connection:
            row = connection.execute(sa.select(*counts)).one()
        return {
            name: contract.ColumnProfile(
                rows=row[0], non_null=row[1 + 2 * i], distinct=row[2 + 2 * i]
            )
            for i, name in enumerate(columns)
        }

    def inclusion(
        self,
        table: str,
        column: str,
        referenced_table: str,
        referenced_column: str,
    ) -> contract.InclusionCheck:
        child = database.column(self.db.table(table), column)
        parent = database.column(
            self.db.table(referenced_table), referenced_column
        )
        values = (
            sa.select(child.label("value"))
            .where(child.is_not(None))
            .distinct()
            .subquery("etl_values")
        )
        parents = (
            sa.select(parent.label("value")).distinct().subquery("etl_parents")
        )
        # A join, not NOT EXISTS: SQL Server takes no subquery in COUNT.
        statement = sa.select(
            sa.func.count(values.c.value), sa.func.count(parents.c.value)
        ).select_from(
            values.outerjoin(parents, parents.c.value == values.c.value)
        )

        with self.db.connect(
            f"checking {table}.{column} against"
            f" {referenced_table}.{referenced_column}"
        ) as connection:
            distinct, matched = connection.execute(statement).one()
        return contract.InclusionCheck(
            distinct=distinct, unmatched=distinct - matched
        )

    def _table(
        self,
        connection: sa.Connection,
        inspector: sa.Inspector,
        schema: str,
        name: str,
    ) -> contract.TableInfo:
        """Returns a table: its columns and its declared keys."""
        columns = tuple(
            contract.ColumnInfo(
                name=column["name"],
                data_type=str(column["type"]),
                nullable=bool(column["nullable"]),
                family=family(column["type"]),
            )
            for column in inspector.get_columns(name, schema=schema)
        )
        primary = tuple(
            inspector.get_pk_constraint(name, schema=schema)[
                "constrained_columns"
            ]
        )
        unique: list[tuple[str, ...]] = []
        try:
            constraints = inspector.get_unique_constraints(name, schema=schema)
        except NotImplementedError:
            # SQL Server: its unique constraints show as unique indexes.
            constraints = []
        for constraint in constraints:
            unique.append(tuple(constraint["column_names"]))
        for index in inspector.get_indexes(name, schema=schema):
            names = tuple(n for n in index["column_names"] if n is not None)
            if (
                index["unique"]
                and len(names) == len(index["column_names"])
                and names not in unique
                and names != primary
            ):
                unique.append(names)
        return contract.TableInfo(
            name=f"{schema}.{name}",
            columns=columns,
            primary_key=primary,
            unique=tuple(unique),
            estimated_rows=self.db.dialect.estimated_rows(
                connection, schema, name
            ),
        )


def family(type_: sa.types.TypeEngine[Any]) -> contract.TypeFamily:
    """Returns the family of a SQLAlchemy type, as reflected.

    Oracle's `NUMBER(p, 0)` is an INTEGER, `NUMBER` a DECIMAL.
    """
    if isinstance(type_, sa.Boolean):
        return contract.TypeFamily.BOOLEAN
    if isinstance(type_, sa.JSON):
        return contract.TypeFamily.JSON
    if isinstance(type_, sa.Numeric) and not isinstance(type_, sa.Float):
        if getattr(type_, "scale", None) == 0:
            return contract.TypeFamily.INTEGER
        return contract.TypeFamily.DECIMAL
    try:
        python_type = type_.python_type
    except NotImplementedError:
        return contract.TypeFamily.OTHER
    # Subclasses first: a datetime is a date.
    for base, found in _PYTHON_FAMILIES:
        if issubclass(python_type, base):
            return found
    return contract.TypeFamily.OTHER


_PYTHON_FAMILIES: list[tuple[type[Any], contract.TypeFamily]] = [
    (bool, contract.TypeFamily.BOOLEAN),
    (int, contract.TypeFamily.INTEGER),
    (decimal.Decimal, contract.TypeFamily.DECIMAL),
    (float, contract.TypeFamily.FLOAT),
    (str, contract.TypeFamily.TEXT),
    (bytes, contract.TypeFamily.BINARY),
    (datetime.datetime, contract.TypeFamily.DATETIME),
    (datetime.date, contract.TypeFamily.DATE),
    (datetime.time, contract.TypeFamily.TIME),
    (uuid.UUID, contract.TypeFamily.UUID),
    (dict, contract.TypeFamily.JSON),
]
