"""The base behavior of a relational database, for any one with TEMPORARY
tables and UPDATE ... FROM, like SQLite and MySQL: staging tables and their
merge, the keyset condition, change detection, inserting what is missing,
transient errors. What a database does differently is in `dialects`.
"""

from __future__ import annotations

from collections.abc import Generator, Mapping, Sequence
import contextlib
import decimal

from sqlalchemy import exc as sa_exc
import sqlalchemy as sa

from core import contract
from infra.database.relational import bulk_loading
from infra.database.relational import driver_errors
from infra.database.relational import staging
from infra.database.relational import types as relational_types

STAGE = "etl_stage"
_UPDATING = (contract.WriteMode.UPSERT, contract.WriteMode.UPDATE)
# Rows from which `insert_missing` bulk copies its staging table when
# `bulk_copy_rows` is set (the online choice decides otherwise): the copy
# costs about 20 ms a batch (a table created and dropped on a connection of
# its own), parameters about 0.12 ms a row (SQL Server 2022, 4 columns).
BULK_INSERT_MISSING = 300
# Values per IN list where the database allows more: longer lists gain
# little and plan worse (`tuning.in_chunk`).
IN_CHUNK_CAP = 1000
# Parameters a statement binds besides its IN list: the other conditions.
_RESERVED_PARAMETERS = 32


def error_codes(text: str) -> frozenset[str]:
    """Returns the error codes written in `text`, separated by blanks."""
    return frozenset(text.split())


class Dialect:
    """The base behavior, for any database with TEMPORARY tables and
    UPDATE ... FROM, like SQLite and MySQL.

    Attributes:
        session_setup: Statements run on each new connection, before any
            use. Never an isolation level: SQLAlchemy resets it whenever a
            connection given back to the pool ran in another one
            (AUTOCOMMIT).
        isolation_level: The isolation level of the transactions, when not
            the server's: given to the engine, which keeps it on every
            connection of its pool.
        retryable_codes: Error codes of the database or its drivers that
            are transient (`error_code`).
        fatal_codes: Error codes retrying cannot fix.
        retryable_messages: Parts of the message of transient errors with
            neither code nor SQLSTATE.
        fatal_messages: Parts of the message of errors retrying cannot fix,
            with neither code nor SQLSTATE.
        parameter_limit: The most parameters one statement binds, by the
            database or its drivers; None where the driver writes the
            values into the text (no bound limit).
        in_list_limit: The most values of one IN list, where the database
            caps the list itself (Oracle); None otherwise.
        in_chunk_cap: The most values per IN list the dialect derives,
            where the database allows more (`in_chunk`).
        bulk_copy_rows: The rows from which a batch is bulk copied into
            its staging table, where the database has a bulk loader apart
            from its statements (SQL Server); None chooses online per
            table (`staging.StagingChooser`).
    """

    name = "generic"
    session_setup: tuple[str, ...] = ()
    isolation_level: str | None = None
    parameter_limit: int | None = None
    in_list_limit: int | None = None
    in_chunk_cap: int = IN_CHUNK_CAP
    # SQLite (the base dialect): busy, locked or unreadable files are transient;
    # a missing table or a read-only database are not.
    retryable_codes: frozenset[str] = error_codes(
        "SQLITE_BUSY SQLITE_LOCKED SQLITE_CANTOPEN SQLITE_IOERR SQLITE_PROTOCOL"
    )
    fatal_codes: frozenset[str] = error_codes(
        "SQLITE_ERROR SQLITE_AUTH SQLITE_CONSTRAINT SQLITE_CORRUPT "
        "SQLITE_MISMATCH SQLITE_NOTADB SQLITE_PERM SQLITE_RANGE "
        "SQLITE_READONLY SQLITE_TOOBIG"
    )
    retryable_messages: tuple[str, ...] = ()
    fatal_messages: tuple[str, ...] = ("no such table", "no such column")

    def __init__(
        self,
        *,
        bulk_copy_rows: int | None = None,
        in_chunk: int | None = None,
        chooser: staging.StagingChooser | None = None,
    ) -> None:
        """Initializes the dialect.

        Args:
            bulk_copy_rows: The rows from which a batch is bulk copied,
                where the database has a bulk loader; None chooses online.
            in_chunk: The values per IN list; None derives it from the
                parameter limit (`in_chunk`).
            chooser: The online choice of the staging, when
                `bulk_copy_rows` is None; one of its own by default.

        Raises:
            ValueError: If `bulk_copy_rows` or `in_chunk` is below 1.
        """
        if (bulk_copy_rows is not None and bulk_copy_rows < 1) or (
            in_chunk is not None and in_chunk < 1
        ):
            raise ValueError("bulk_copy_rows and in_chunk must be >= 1")
        self.bulk_copy_rows = bulk_copy_rows
        self._in_chunk = in_chunk
        if chooser is not None:
            self.chooser: staging.StagingChooser | None = chooser
        elif bulk_copy_rows is None:
            self.chooser = staging.StagingChooser()
        else:
            self.chooser = None

    def in_chunk(self) -> int:
        """The values per IN list of a statement: the one given, else from
        the parameter limit, with room for the statement's other
        parameters, at most `in_chunk_cap` (`IN_CHUNK_CAP`; SQL Server's
        `MSSQL_IN_CHUNK`).

        SQLite binds 999 parameters before 3.32, 32,766 since; SQL Server
        2,100; Postgres 32,767 through psycopg; MySQL 65,535; Oracle 65,535,
        and 1,000 values per IN list (ORA-01795).
        """
        if self._in_chunk is not None:
            return self._in_chunk
        found = self.in_chunk_cap
        if self.parameter_limit is not None:
            found = min(
                found, max(1, self.parameter_limit - _RESERVED_PARAMETERS)
            )
        if self.in_list_limit is not None:
            found = min(found, self.in_list_limit)
        return found

    def transient(self, exc: sa_exc.DBAPIError) -> bool:
        """Whether a database error may not happen again: worth a retry.

        In order: a connection SQLAlchemy found lost; the database's error
        code; the message; the SQLSTATE; at last the kind of error, an
        operational or interface error being transient (a lost connection,
        a timeout) and any other not.
        """
        if exc.connection_invalidated:
            return True
        orig = exc.orig
        code = driver_errors.error_code(orig)
        if code in self.retryable_codes:
            return True
        if code in self.fatal_codes:
            return False
        text = str(orig)
        if any(part in text for part in self.retryable_messages):
            return True
        if any(part in text for part in self.fatal_messages):
            return False
        transient = driver_errors.state_is_transient(
            driver_errors.sqlstate(orig)
        )
        if transient is not None:
            return transient
        return isinstance(exc, (sa_exc.OperationalError, sa_exc.InterfaceError))

    def session_warnings(self, connection: sa.Connection) -> list[str]:
        """What a session of the database does that the ETL does not expect,
        for `etl check`: nothing by default."""
        del connection  # Unused.
        return []

    def bind(self, column: sa.ColumnElement[object], value: object) -> object:
        """Returns `value` as the operand of a comparison with `column`,
        which SQLAlchemy binds as a parameter: itself, or a `Decimal` for an
        integer compared with an exact number (`DECIMAL(20, 0)`), which the
        drivers would bind as a BIGINT, refused past its range."""
        if (
            isinstance(column.type, sa.Numeric)
            and not isinstance(column.type, sa.Float)
            and isinstance(value, int)
            and not isinstance(value, bool)
        ):
            return decimal.Decimal(value)
        return value

    def after(
        self,
        columns: Sequence[sa.ColumnElement[object]],
        values: Sequence[object],
    ) -> sa.ColumnElement[bool]:
        """Returns `(columns) > (values)`, the keyset condition.

        Written `c1 >= v1 AND (c1 > v1 OR (c2 >= v2 AND (c2 > v2 OR ...)))`:
        the leading `c1 >= v1` lets an index on the columns serve it, where
        row value comparisons are not supported.
        """
        last = self.bind(columns[-1], values[-1])
        condition: sa.ColumnElement[bool] = columns[-1] > last
        for col, value in zip(reversed(columns[:-1]), reversed(values[:-1])):
            bound = self.bind(col, value)
            condition = sa.and_(col >= bound, sa.or_(col > bound, condition))
        return condition

    def stage(self, target: sa.Table, columns: Sequence[str]) -> sa.Table:
        """Returns the staging table of a batch: the columns of the target."""
        return sa.Table(
            STAGE,
            sa.MetaData(),
            *(sa.Column(name, target.c[name].type) for name in columns),
            prefixes=["TEMPORARY"],
        )

    @contextlib.contextmanager
    def bulk_stage(
        self,
        engine: sa.Engine,
        target: sa.Table,
        columns: Sequence[str],
        rows: Sequence[contract.Row],
        minimum: int | None = None,
    ) -> Generator[sa.Table | None]:
        """Yields a staging table filled before the batch's transaction.

        For bulk loaders that cannot join the transaction, from `minimum`
        rows (`bulk_copy_rows` if None; the online choice when that is None
        too); None, by default, stages the batch in the transaction
        (`stage`, `fill_stage`).
        """
        del engine, target, columns, rows, minimum  # Unused.
        yield None

    def create_stage(self, connection: sa.Connection, stage: sa.Table) -> None:
        stage.create(connection)

    def drop_stage(self, connection: sa.Connection, stage: sa.Table) -> None:
        """Drops the staging table, in the batch's transaction."""
        stage.drop(connection)

    def estimated_rows(
        self, connection: sa.Connection, schema: str, table: str
    ) -> int:
        """Returns the row count of the statistics, -1 when unknown."""
        del connection, schema, table  # Unused.
        return -1

    def _estimate(
        self, connection: sa.Connection, statement: str, **params: object
    ) -> int:
        """Runs a statistics query; -1 when it has no answer."""
        found = connection.execute(sa.text(statement), params).scalar()
        return -1 if found is None or found < 0 else int(found)

    def fill_stage(
        self,
        connection: sa.Connection,
        stage: sa.Table,
        rows: Sequence[contract.Row],
    ) -> None:
        """Inserts the batch into the staging table, in one round trip when
        the driver can."""
        connection.execute(stage.insert(), bulk_loading.coerce(stage, rows))

    def prepare_join(
        self, engine: sa.Engine, table: sa.Table, columns: Sequence[str]
    ) -> None:
        """Learns how to join `table` with a staging table on `columns`.

        Called before a batch's transaction; nothing to learn by default.
        """
        del engine, table, columns  # Unused.

    def delete(
        self, connection: sa.Connection, table: sa.Table, stage: sa.Table
    ) -> int:
        """Deletes the rows of `table` matching a row of the staging table."""
        match = sa.and_(*(table.c[c.name] == c for c in stage.c))
        return self.run(
            connection,
            sa.delete(table).where(sa.exists().where(match)),
            table=table,
            on=[c.name for c in stage.c],
        )

    def duplicate_keys(
        self, connection: sa.Connection, stage: sa.Table, key: Sequence[str]
    ) -> int:
        """Returns how many keys have more than one row in the staging table.

        Compared by the database, in the collation of the staging columns,
        which is the target's: keys that differ in Python may be one key to
        the target, like `'ABC'` and `'abc'` under a case-insensitive
        collation, or `'a'` and `'a '` on SQL Server.
        """
        columns = [stage.c[name] for name in key]
        duplicates = (
            sa.select(*columns)
            .group_by(*columns)
            .having(sa.func.count() > 1)
            .subquery()
        )
        return int(
            connection.execute(
                sa.select(sa.func.count()).select_from(duplicates)
            ).scalar_one()
        )

    def changed[T](
        self, current: sa.ColumnElement[T], new: sa.ColumnElement[T]
    ) -> sa.ColumnElement[bool]:
        """Returns whether a value changed, NULLs included."""
        return current.is_distinct_from(new)

    def merge(
        self,
        connection: sa.Connection,
        target: contract.TableTarget,
        table: sa.Table,
        stage: sa.Table,
        updates: Sequence[str],
    ) -> tuple[int, int]:
        """Merges the staging table into the target.

        Returns:
            The rows created, and the rows updated.
        """
        columns = [c.name for c in stage.c]
        if target.mode is contract.WriteMode.INSERT:
            select = sa.select(*stage.c)
            created = connection.execute(
                sa.insert(table).from_select(columns, select)
            ).rowcount
            return created, 0

        updated = 0
        if target.mode in _UPDATING and updates:
            updated = self.update(connection, target, table, stage, updates)
        if target.mode is contract.WriteMode.UPDATE:
            return 0, updated
        missing = ~sa.exists().where(_same_key(target, table, stage))
        created = self.run(
            connection,
            sa.insert(table).from_select(
                columns, sa.select(*stage.c).where(missing)
            ),
            table=table,
            on=target.key,
        )
        return created, updated

    def update(
        self,
        connection: sa.Connection,
        target: contract.TableTarget,
        table: sa.Table,
        stage: sa.Table,
        updates: Sequence[str],
    ) -> int:
        """Updates the target rows that changed; returns how many."""
        statement = (
            sa.update(table)
            .values({name: stage.c[name] for name in updates})
            .where(
                _same_key(target, table, stage),
                sa.or_(*(
                    self.changed(table.c[name], stage.c[name])
                    for name in updates
                )),
            )
        )
        return self.run(connection, statement, table=table, on=target.key)

    def run(
        self,
        connection: sa.Connection,
        statement: relational_types.Joining,
        *,
        table: sa.Table,
        on: Sequence[str],
    ) -> int:
        """Runs a statement joining `table` with the staging table on the
        columns `on`; returns the rows it changed."""
        del table, on  # Unused.
        return connection.execute(statement).rowcount

    def insert_missing(
        self,
        engine: sa.Engine,
        table: sa.Table,
        rows: Sequence[contract.Row],
        key: Sequence[str],
    ) -> None:
        """Inserts the rows whose `key` is not in `table`, without reading
        first.

        One INSERT, autocommitted, where the database skips the rows there
        (`insert_skipping`): one round trip. Through a staging table in a
        transaction otherwise, as `merge` does: two writers inserting the
        same new key at once both find it missing, the second one fails on
        the primary key, and its caller tries again (`bookkeeping`).
        """
        if not rows:
            return
        # pylint: disable-next=assignment-from-none
        statement = self.insert_skipping(table, key)
        if statement is not None:
            autocommitted(engine, statement, bulk_loading.coerce(table, rows))
            return
        columns = list(rows[0])
        target = contract.TableTarget(
            table.fullname, tuple(key), contract.WriteMode.INSERT_MISSING
        )
        with (
            self.bulk_stage(
                engine, table, columns, rows, minimum=BULK_INSERT_MISSING
            ) as bulk,
            engine.begin() as connection,
        ):
            stage = bulk if bulk is not None else self.stage(table, columns)
            if bulk is None:
                self.create_stage(connection, stage)
                self.fill_stage(connection, stage, rows)
            self.merge(connection, target, table, stage, [])
            if bulk is None:
                self.drop_stage(connection, stage)

    def insert_skipping(
        self, table: sa.Table, key: Sequence[str]
    ) -> sa.Insert | None:
        """Returns the INSERT into `table` that skips the rows whose `key`
        is there already; None where the database has no such INSERT."""
        del table, key  # Unused.
        return None


def autocommitted(
    engine: sa.Engine,
    statement: sa.Executable,
    parameters: Sequence[Mapping[str, object]] | Mapping[str, object],
) -> None:
    """Runs one statement on a connection of its own, autocommitted."""
    with engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT").execute(
            statement, parameters
        )


def _same_key(
    target: contract.TableTarget, table: sa.Table, stage: sa.Table
) -> sa.ColumnElement[bool]:
    return sa.and_(*(table.c[k] == stage.c[k] for k in target.key))
