"""Reading and writing tables of any relational database SQLAlchemy supports."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
import threading
from typing import Any

import sqlalchemy as sa

from core import contract
from infra.database import batches
from infra.database.relational import database
from infra.database.relational import dialects
from shared import utils


class RelationalTableReader(database.RelationalAdapter, contract.ITableReader):
    """Reads tables by keyset pagination.

    Each page is one short query, `WHERE (order_by) > (last row) ORDER BY
    order_by` with the database's LIMIT: no transaction stays open while the
    pipeline works, and an index on `order_by` makes every page as fast as the
    first. Column names are those SQLAlchemy reflects: Oracle's `CUST_NO` is
    `cust_no`.

    Before the first read of a table, by its order and filters, the reader
    checks that no row has NULL in an `order_by` column, once per reader (a
    reader serves one run).

    Attributes:
        page_size: The rows fetched per query.
    """

    def __init__(
        self, db: database.Database | str, *, page_size: int = 10_000
    ) -> None:
        """Initializes the reader.

        Args:
            db: A shared database, or a SQLAlchemy URL.
            page_size: The rows fetched per query.

        Raises:
            ValueError: If `page_size` is below 1.
        """
        if page_size < 1:
            raise ValueError("page_size must be >= 1")
        super().__init__(db)
        self.page_size = page_size
        self._checked: set[tuple[str, tuple[str, ...], str]] = set()
        self._checked_lock = threading.Lock()

    def read(self, query: contract.TableQuery) -> Iterator[contract.Row]:
        """See `contract.ITableReader.read`."""
        table = self.db.table(query.table)
        order = [database.column(table, name) for name in query.order_by]
        self._check_nulls(query, table, order)

        selected = (
            [database.column(table, name) for name in query.columns]
            if query.columns
            else list(table.c)
        )
        base = sa.select(*selected).where(*_conditions(table, query.filters))
        if query.lower is not None:
            base = base.where(order[0] >= query.lower)
        if query.upper is not None:
            base = base.where(order[0] <= query.upper)
        base = base.order_by(*order).limit(self.page_size)

        after = query.after
        while True:
            statement = base
            if after is not None:
                statement = statement.where(
                    self.db.dialect.after(order[: len(after)], after)
                )
            with self.db.connect(f"reading {query.table}") as connection:
                page = [
                    dict(row)
                    for row in connection.execute(statement).mappings()
                ]

            yield from page
            if len(page) < self.page_size:
                return
            after = tuple(page[-1][name] for name in query.order_by)

    def max_value(
        self,
        table: str,
        column: str,
        filters: Sequence[contract.Condition] = (),
    ) -> Any:
        reflected = self.db.table(table)
        statement = sa.select(
            sa.func.max(database.column(reflected, column))
        ).where(*_conditions(reflected, filters))
        with self.db.connect(f"reading the max of {table}.{column}") as conn:
            return conn.execute(statement).scalar()

    def count(
        self, table: str, filters: Sequence[contract.Condition] = ()
    ) -> int:
        reflected = self.db.table(table)
        statement = (
            sa.select(sa.func.count())
            .select_from(reflected)
            .where(*_conditions(reflected, filters))
        )
        with self.db.connect(f"counting the rows of {table}") as connection:
            return int(connection.execute(statement).scalar_one())

    def keys_at(
        self,
        table: str,
        order_by: Sequence[str],
        offsets: Sequence[int],
        filters: Sequence[contract.Condition] = (),
    ) -> list[tuple[Any, ...]]:
        """See `contract.ITableReader.keys_at`.

        One query per offset, each after the row of the previous one (keyset)
        skipping the rows in between (`OFFSET n ... FETCH FIRST 1 ROW ONLY`,
        `LIMIT 1 OFFSET n`): with an index on `order_by`, the database walks
        it once up to the last offset, and only the rows wanted cross the
        network.
        """
        skips = _skips(offsets)
        reflected = self.db.table(table)
        order = [database.column(reflected, name) for name in order_by]
        self._check_nulls(
            contract.TableQuery(
                table, order_by=tuple(order_by), filters=tuple(filters)
            ),
            reflected,
            order,
        )
        base = (
            sa.select(*order)
            .where(*_conditions(reflected, filters))
            .order_by(*order)
            .limit(1)
        )
        found: list[tuple[Any, ...]] = []
        with self.db.connect(f"reading the quantiles of {table}") as conn:
            for skipped in skips:
                statement = base.offset(skipped)
                if found:
                    statement = statement.where(
                        self.db.dialect.after(order, found[-1])
                    )
                row = conn.execute(statement).first()
                if row is None:
                    break
                found.append(tuple(row))
        return found

    def _check_nulls(
        self,
        query: contract.TableQuery,
        table: sa.Table,
        order: list[sa.Column[Any]],
    ) -> None:
        """Fails if a row has NULL in an `order_by` column.

        Keyset conditions never match such a row: it would be skipped
        silently, at every run. A table is checked once per reader for an
        order and filters, since the check may scan it: the composition root
        gives each run its own reader.

        Raises:
            FatalError: If a row has NULL in an `order_by` column.
        """
        checked = (query.table, tuple(query.order_by), repr(query.filters))
        with self._checked_lock:
            if checked in self._checked:
                return
        statement = (
            sa.select(sa.literal(1))
            .select_from(table)
            .where(
                sa.or_(*(c.is_(None) for c in order)),
                *_conditions(table, query.filters),
            )
            .limit(1)
        )
        with self.db.connect(f"reading {query.table}") as connection:
            found = connection.execute(statement).first()
        if found is not None:
            raise contract.FatalError(
                f"{query.table} has rows with NULL in {list(query.order_by)}:"
                " they cannot be read by keyset, and would be skipped. Filter"
                " them out, or order by NOT NULL columns."
            )
        with self._checked_lock:
            self._checked.add(checked)


class RelationalTableWriter(database.RelationalAdapter, contract.ITableWriter):
    """Writes batches into tables, each in one transaction.

    The batch goes into a temporary staging table shaped like the target,
    then is merged into it set-based, the way the database does it best
    (`dialects`):

    - INSERT: `INSERT ... SELECT`;
    - INSERT_MISSING: inserts the keys not in the target;
    - UPSERT: updates the rows that changed (`IS DISTINCT FROM`, so that
      unchanged rows are not rewritten), then inserts the new keys;
    - UPDATE: updates the rows that changed, inserts nothing.

    The key must be the columns of a unique constraint or index of the target.
    Writers racing on the same keys are safe: the one inserting a key second
    fails on that constraint, and writes its batch again, updating what the
    first one inserted. Database errors fail the whole batch: connection
    losses and deadlocks with a `RetryableError`, anything else with a
    `FatalError`.

    Of rows with the same key, the last one is written. Keys the target
    compares as equal though they differ in Python (`'ABC'` and `'abc'`
    under a case-insensitive collation, `'a'` and `'a '` on SQL Server) fail
    the batch: which one is the last is the pipeline's to decide. So do NULLs
    in the key, which would match no row, and be inserted again at every
    run.
    """

    def write(
        self, target: contract.TableTarget, rows: Sequence[contract.Row]
    ) -> contract.WriteResult:
        """See `contract.ITableWriter.write`."""
        if not rows:
            return contract.WriteResult()

        columns = batches.columns(target, rows)
        table = self.db.table(target.table)
        for name in columns:
            database.column(table, name)
        unique = batches.last_by_key(target, rows)
        updates = (
            list(target.update_columns)
            if target.update_columns is not None
            else [c for c in columns if c not in target.key]
        )

        dialect = self.db.dialect
        action = f"writing {len(rows)} row(s) to {target.table}"
        with (
            database.errors(action, dialect),
            dialect.bulk_stage(self.db.engine, table, columns, unique) as bulk,
        ):
            stage = bulk if bulk is not None else dialect.stage(table, columns)
            created, updated = utils.retry(
                lambda: self._merge(
                    action,
                    target,
                    table,
                    stage,
                    unique,
                    updates=updates,
                    bulk=bulk,
                ),
                when=lambda exc: (
                    target.mode is not contract.WriteMode.INSERT
                    and dialects.is_unique_violation(exc.__cause__)
                ),
            )
        return contract.WriteResult(
            created=created,
            updated=updated,
            # An UPDATE by a non-unique key may change more rows than given.
            unchanged=max(0, len(rows) - created - updated),
        )

    def delete(self, table: str, rows: Sequence[contract.Row]) -> int:
        """See `contract.ITableWriter.delete`.

        The rows to match go into a staging table, then one DELETE ... WHERE
        EXISTS removes their matches.
        """
        if not rows:
            return 0
        columns = batches.match_columns(table, rows)
        reflected = self.db.table(table)
        for name in columns:
            database.column(reflected, name)
        unique = list(
            {tuple(row[c] for c in columns): row for row in rows}.values()
        )
        dialect = self.db.dialect
        stage = dialect.stage(reflected, columns)
        with self.db.begin(f"deleting {len(rows)} row(s) of {table}") as conn:
            dialect.create_stage(conn, stage)
            dialect.fill_stage(conn, stage, unique)
            deleted = dialect.delete(conn, reflected, stage)
            dialect.drop_stage(conn, stage)
        return deleted

    def _merge(
        self,
        action: str,
        target: contract.TableTarget,
        table: sa.Table,
        stage: sa.Table,
        rows: Sequence[contract.Row],
        *,
        updates: Sequence[str],
        bulk: sa.Table | None,
    ) -> tuple[int, int]:
        """Writes the batch in one transaction; returns (created, updated).

        A batch already in a bulk staging table is only merged.

        Raises:
            FatalError: If rows have the same key to the target.
        """
        dialect = self.db.dialect
        with self.db.begin(action) as connection:
            if bulk is None:
                dialect.create_stage(connection, stage)
                dialect.fill_stage(connection, stage, rows)
            if target.mode is not contract.WriteMode.INSERT and target.key:
                _check_unique(connection, dialect, target, stage)
            counts = dialect.merge(connection, target, table, stage, updates)
            if bulk is None:
                dialect.drop_stage(connection, stage)
        return counts


def _check_unique(
    connection: sa.Connection,
    dialect: dialects.Dialect,
    target: contract.TableTarget,
    stage: sa.Table,
) -> None:
    """Fails if rows of the staging table have the same key to the target.

    Raises:
        FatalError: If they have; the message has no value of the batch.
    """
    found = dialect.duplicate_keys(connection, stage, target.key)
    if found:
        raise contract.FatalError(
            f"{found} key(s) of the batch for {target.table} have more than"
            f" one row, compared as the target compares {list(target.key)}"
            " (its collation: case-insensitive, or ignoring trailing blanks)."
            " Make the keys unique the target's way before writing them."
        )


def _skips(offsets: Sequence[int]) -> list[int]:
    """Returns the rows between each offset and the one before it.

    Raises:
        ValueError: If the offsets are not ascending, or below 0.
    """
    found = []
    previous = -1
    for offset in offsets:
        if offset <= previous:
            raise ValueError(f"offsets must be ascending, from 0: {offsets}")
        found.append(offset - previous - 1)
        previous = offset
    return found


def _conditions(
    table: sa.Table, filters: Sequence[contract.Condition]
) -> list[sa.ColumnElement[bool]]:
    """Translates the conditions of the port into SQLAlchemy expressions."""
    expressions = []
    for condition in filters:
        col = database.column(table, condition.column)
        value = condition.value
        match condition.operator:
            case contract.Operator.EQ:
                expressions.append(col == value)
            case contract.Operator.NE:
                expressions.append(col != value)
            case contract.Operator.LT:
                expressions.append(col < value)
            case contract.Operator.LE:
                expressions.append(col <= value)
            case contract.Operator.GT:
                expressions.append(col > value)
            case contract.Operator.GE:
                expressions.append(col >= value)
            case contract.Operator.IN:
                expressions.append(col.in_(condition.values))
            case contract.Operator.NOT_IN:
                expressions.append(col.not_in(condition.values))
            case contract.Operator.IS_NULL:
                expressions.append(col.is_(None))
            case contract.Operator.IS_NOT_NULL:
                expressions.append(col.is_not(None))
    return expressions
