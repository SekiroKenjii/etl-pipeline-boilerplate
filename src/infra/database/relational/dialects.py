"""What the relational adapters do differently from one database to another.

Where SQLAlchemy Core does not, or not best:

- keyset `(a, b) > (x, y)`: row values on Postgres, expanded elsewhere;
- staging tables and their merge: COPY on Postgres, bulk copy on SQL Server,
  private or global temporary tables and MERGE on Oracle;
- change detection: IS DISTINCT FROM, compared in binary on SQL Server and
  MySQL (case-insensitive collations; SQL Server also ignores trailing
  blanks), with DBMS_LOB for Oracle LOBs;
- row estimates, the session time zone, and transient errors (`transient`).

A naive datetime is UTC: every session runs in UTC, so a naive value written
into a column with a time zone means the same instant whatever the server's,
the client's or `PGTZ`'s zone. SQL Server and SQLite have no session time
zone (a naive value written into a `datetimeoffset` gets +00:00).

Other databases SQLAlchemy supports get the base `Dialect`.
"""

from __future__ import annotations

from collections.abc import Generator, Sequence
import contextlib
import hashlib
import re
import threading
from typing import Any
import uuid

from sqlalchemy import exc as sa_exc
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pg_dialect

from core import contract

STAGE = "etl_stage"
_ROW_LOCKS = "WITH (ROWLOCK)"
type _Joining = sa.Insert | sa.Update | sa.Delete
_UPDATING = (contract.WriteMode.UPSERT, contract.WriteMode.UPDATE)

# SQLSTATEs (ISO SQL, ODBC, Postgres) of transient failures: connection
# errors (08xxx), serialization failures and deadlocks, insufficient
# resources (53xxx: too many connections...), lock timeouts, cancelled
# statements (a statement or lock timeout), servers shutting down or
# starting, ODBC timeouts. Any other SQLSTATE is fatal: authorization (28),
# bad SQL or a missing table or column (42), a missing database (3D) or
# schema (3F), bad data (22), a constraint (23)...
_RETRYABLE_STATES = (
    "01002",
    "08",
    "40001",
    "40003",
    "40P01",
    "53",
    "55P03",
    "57014",
    "57P01",
    "57P02",
    "57P03",
    "HYT00",
    "HYT01",
)
# "General error": the SQLSTATE of anything else, which tells nothing.
_UNTOLD_STATES = frozenset({"HY000"})
_SQLSTATE = re.compile(r"^[0-9A-Z]{5}$")
# The native error number in pyodbc messages: `... (1205) (SQLExecDirectW)`.
_ODBC_NATIVE = re.compile(r"\((-?\d+)\) \(SQL\w*\)")
_BULK_NATIVE = re.compile(r"Sql Error: (\d+)")


def _codes(text: str) -> frozenset[str]:
    """Returns the error codes written in `text`, separated by blanks."""
    return frozenset(text.split())


class Dialect:
    """The base behavior, for any database with TEMPORARY tables and
    UPDATE ... FROM, like SQLite and MySQL.

    Attributes:
        session_setup: Statements run on each new connection, before any
            use.
        retryable_codes: Error codes of the database or its drivers that
            are transient (`error_code`).
        fatal_codes: Error codes retrying cannot fix.
        retryable_messages: Parts of the message of transient errors with
            neither code nor SQLSTATE.
        fatal_messages: Parts of the message of errors retrying cannot fix,
            with neither code nor SQLSTATE.
    """

    name = "generic"
    session_setup: tuple[str, ...] = ()
    # SQLite (the base dialect): busy, locked or unreadable files are transient;
    # a missing table or a read-only database are not.
    retryable_codes: frozenset[str] = _codes(
        "SQLITE_BUSY SQLITE_LOCKED SQLITE_CANTOPEN SQLITE_IOERR SQLITE_PROTOCOL"
    )
    fatal_codes: frozenset[str] = _codes(
        "SQLITE_ERROR SQLITE_AUTH SQLITE_CONSTRAINT SQLITE_CORRUPT "
        "SQLITE_MISMATCH SQLITE_NOTADB SQLITE_PERM SQLITE_RANGE "
        "SQLITE_READONLY SQLITE_TOOBIG"
    )
    retryable_messages: tuple[str, ...] = ()
    fatal_messages: tuple[str, ...] = ("no such table", "no such column")

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
        code = error_code(orig)
        if code in self.retryable_codes:
            return True
        if code in self.fatal_codes:
            return False
        text = str(orig)
        if any(part in text for part in self.retryable_messages):
            return True
        if any(part in text for part in self.fatal_messages):
            return False
        state = sqlstate(orig)
        if state is not None and state not in _UNTOLD_STATES:
            return state.startswith(_RETRYABLE_STATES)
        return isinstance(exc, (sa_exc.OperationalError, sa_exc.InterfaceError))

    def after(
        self, columns: Sequence[sa.ColumnElement[Any]], values: Sequence[Any]
    ) -> sa.ColumnElement[bool]:
        """Returns `(columns) > (values)`, the keyset condition.

        Written `c1 >= v1 AND (c1 > v1 OR (c2 >= v2 AND (c2 > v2 OR ...)))`:
        the leading `c1 >= v1` lets an index on the columns serve it, where
        row value comparisons are not supported.
        """
        condition: sa.ColumnElement[bool] = columns[-1] > values[-1]
        for col, value in zip(reversed(columns[:-1]), reversed(values[:-1])):
            condition = sa.and_(col >= value, sa.or_(col > value, condition))
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
    ) -> Generator[sa.Table | None]:
        """Yields a staging table filled before the batch's transaction.

        For bulk loaders that cannot join the transaction; None, by default,
        stages the batch in the transaction (`stage`, `fill_stage`).
        """
        del engine, target, columns, rows  # Unused.
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
        self, connection: sa.Connection, statement: str, **params: Any
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
        connection.execute(stage.insert(), coerce(stage, rows))

    def delete(
        self, connection: sa.Connection, table: sa.Table, stage: sa.Table
    ) -> int:
        """Deletes the rows of `table` matching a row of the staging table."""
        match = sa.and_(*(table.c[c.name] == c for c in stage.c))
        return self.run(
            connection, sa.delete(table).where(sa.exists().where(match))
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

    def changed(
        self, current: sa.ColumnElement[Any], new: sa.ColumnElement[Any]
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
        return self.run(connection, statement)

    def run(self, connection: sa.Connection, statement: _Joining) -> int:
        """Runs a statement joining the target with the staging table;
        returns the rows it changed."""
        return connection.execute(statement).rowcount


class MssqlDialect(Dialect):
    """SQL Server: `#` temporary tables, and case-sensitive change detection.

    Most SQL Server databases use a case-insensitive collation, where `'b'`
    equals `'B'`: a fix of the case only would never be written. Under any
    collation, binary ones included, `'a'` equals `'a '`: their byte lengths
    tell them apart.
    """

    name = "mssql"
    _BINARY_COLLATION = "Latin1_General_100_BIN2"
    bulk_copy_rows = 2000
    # Error numbers (pyodbc, pymssql, bulk copy): deadlock victim (1205),
    # lock timeout (1222), Azure SQL's database unavailable or busy (40613,
    # 40501, 40197, 49918-49920), network errors (233, 10053, 10054, 10060,
    # 64), timeout (-2); login failed (18456), cannot open the database
    # (4060), no such object or column (208, 207), permission (229, 230),
    # syntax (102, 156).
    retryable_codes = _codes(
        "1205 1222 40613 40501 40197 49918 49919 49920 233 10053 10054 "
        "10060 64 -2"
    )
    fatal_codes = _codes("18456 4060 208 207 229 230 102 156")
    # mssql-python gives no error number: match the messages.
    retryable_messages = (
        "was deadlocked on lock",
        "Lock request time out period exceeded",
        "Connection reset",
        "Broken pipe",
        "Io error",
    )
    fatal_messages = ("Login failed for user", "Cannot open database")

    @contextlib.contextmanager
    def bulk_stage(
        self,
        engine: sa.Engine,
        target: sa.Table,
        columns: Sequence[str],
        rows: Sequence[contract.Row],
    ) -> Generator[sa.Table | None]:
        """See `Dialect.bulk_stage`; bulk copy (BCP) with mssql-python.

        Bulk copy runs on a connection of its own, which cannot see the
        session's `#` tables: the batch goes into a global `##` table, created
        on a connection held until the batch is merged. SQL Server drops it
        with that session, should the process die.

        Raises:
            DBAPIError: If the bulk copy fails: its error, wrapped for
                `transient` to tell whether to try again.
        """
        if engine.dialect.driver != "mssqlpython" or (
            len(rows) < self.bulk_copy_rows
        ):
            yield None
            return

        stage = sa.Table(
            f"##{STAGE}_{uuid.uuid4().hex}",
            sa.MetaData(),
            *(sa.Column(name, target.c[name].type) for name in columns),
        )
        with engine.connect() as holder:
            stage.create(holder)
            holder.commit()
            try:
                driver = holder.connection.driver_connection
                if driver is None:
                    raise contract.FatalError("bulk copy: no driver connection")
                cursor = driver.cursor()
                try:
                    cursor.bulkcopy(
                        stage.name,
                        [
                            tuple(row[name] for name in columns)
                            for row in coerce(stage, rows)
                        ],
                        column_mappings=list(columns),
                        keep_nulls=True,
                        timeout=0,
                    )
                except (RuntimeError, ValueError, TypeError) as exc:
                    # Server or network errors, or data the copy cannot convert.
                    raise sa_exc.DBAPIError(
                        f"bulk copy into {stage.name}",
                        None,
                        exc,
                        hide_parameters=True,
                    ) from exc
                finally:
                    cursor.close()
                yield stage
            finally:
                stage.drop(holder)
                holder.commit()

    def estimated_rows(
        self, connection: sa.Connection, schema: str, table: str
    ) -> int:
        return self._estimate(
            connection,
            "SELECT SUM(p.rows) FROM sys.partitions p"
            " JOIN sys.tables t ON t.object_id = p.object_id"
            " JOIN sys.schemas s ON s.schema_id = t.schema_id"
            " WHERE s.name = :schema AND t.name = :table"
            " AND p.index_id IN (0, 1)",
            schema=schema,
            table=table,
        )

    def changed(
        self, current: sa.ColumnElement[Any], new: sa.ColumnElement[Any]
    ) -> sa.ColumnElement[bool]:
        """See `Dialect.changed`; text in binary, and by its byte length."""
        if not isinstance(current.type, sa.String):
            return super().changed(current, new)
        return sa.or_(
            super().changed(
                current.collate(self._BINARY_COLLATION),
                new.collate(self._BINARY_COLLATION),
            ),
            super().changed(
                sa.func.datalength(current), sa.func.datalength(new)
            ),
        )

    def stage(self, target: sa.Table, columns: Sequence[str]) -> sa.Table:
        return sa.Table(
            f"#{STAGE}",
            sa.MetaData(),
            *(sa.Column(name, target.c[name].type) for name in columns),
        )

    def run(self, connection: sa.Connection, statement: _Joining) -> int:
        """See `Dialect.run`; seeks the target's rows by their key.

        Left to itself, SQL Server scans the whole target (a plan made while
        the table was small stays cached), locking every row and page it
        reads: batches written at once, by threads or processes, then wait
        for each other's new rows and deadlock, and every batch costs a scan.
        A nested loop from the staging table, with row locks, touches only
        the batch's own keys. Without an index on the key, each of its rows
        scans the target: slow, still correct.
        """
        if isinstance(statement, (sa.Update, sa.Delete)):
            statement = statement.with_hint(_ROW_LOCKS, dialect_name=self.name)
        compiled = statement.compile(
            dialect=connection.dialect, compile_kwargs={"literal_binds": True}
        )
        # Compiled by SQLAlchemy with literal binds: no raw data.
        return connection.exec_driver_sql(
            f"{compiled} OPTION (LOOP JOIN)"
        ).rowcount


class OracleDialect(Dialect):
    """Oracle: temporary staging tables, MERGE, LOB comparisons."""

    name = "oracle"
    session_setup = ("ALTER SESSION SET TIME_ZONE = '+00:00'",)
    # Deadlock (60), resource busy or timeout (51, 54, 30006), cannot
    # serialize (8177), lost or closed connections (3113, 3114, 3135,
    # 12537, 12547, DPY-4011), no listener or connect timeout (12541, 12170,
    # DPY-6005), the database starting or stopping (1033, 1034, 1089, 1092,
    # 12514, 12528); no table or column (942, 904), bad login or locked
    # account (1017, 28000), privileges (1031), a missing user (1918).
    retryable_codes = _codes(
        "ORA-00060 ORA-00051 ORA-00054 ORA-30006 ORA-08177 ORA-03113 "
        "ORA-03114 ORA-03135 ORA-12537 ORA-12547 ORA-12541 ORA-12170 "
        "ORA-01033 ORA-01034 ORA-01089 ORA-01092 ORA-12514 ORA-12528 "
        "DPY-4011 DPY-6005"
    )
    fatal_codes = _codes(
        "ORA-00942 ORA-00904 ORA-01017 ORA-28000 ORA-01031 ORA-01918"
    )

    def __init__(self) -> None:
        self._created: set[str] = set()
        self._lock = threading.Lock()

    _PRIVATE_TEMP_TABLE_PREFIX = "ORA$PTT_ETL_STAGE"

    def stage(self, target: sa.Table, columns: Sequence[str]) -> sa.Table:
        """See `Dialect.stage`.

        A private temporary table, created in the transaction and gone with
        it (Oracle 18c and later). Private temporary tables cannot hold LOBs:
        a batch with LOB columns goes into a global temporary table instead,
        one per target and set of columns (`etl_stage_<hash>`), created once
        and kept: its rows are private to the session and deleted at commit.
        """
        cols = [sa.Column(name, target.c[name].type) for name in columns]
        if not any(_is_lob(column.type) for column in cols):
            return sa.Table(
                self._PRIVATE_TEMP_TABLE_PREFIX,
                sa.MetaData(),
                *cols,
                prefixes=["PRIVATE TEMPORARY"],
                oracle_on_commit="DROP DEFINITION",
            )
        definition = [(c.name, str(c.type)) for c in cols]
        digest = hashlib.sha256(
            repr((target.fullname, definition)).encode()
        ).hexdigest()[:16]
        return sa.Table(
            f"etl_stage_{digest}",
            sa.MetaData(),
            *cols,
            prefixes=["GLOBAL TEMPORARY"],
            oracle_on_commit="DELETE ROWS",
        )

    def transient(self, exc: sa_exc.DBAPIError) -> bool:
        """See `Dialect.transient`; python-oracledb also tells it."""
        args = getattr(exc.orig, "args", ())
        if args and getattr(args[0], "isrecoverable", False):
            return True
        return super().transient(exc)

    def create_stage(self, connection: sa.Connection, stage: sa.Table) -> None:
        """See `Dialect.create_stage`.

        A global temporary table is created once, on a connection of its own:
        Oracle commits before and after any other DDL.
        """
        if stage.name == self._PRIVATE_TEMP_TABLE_PREFIX:
            stage.create(connection)
            return
        with self._lock:
            if stage.name in self._created:
                return
            with connection.engine.connect() as own:
                stage.create(own, checkfirst=True)
                own.commit()
            self._created.add(stage.name)

    def drop_stage(self, connection: sa.Connection, stage: sa.Table) -> None:
        del connection, stage  # Dropped, or emptied, at commit.

    def estimated_rows(
        self, connection: sa.Connection, schema: str, table: str
    ) -> int:
        denormalize = connection.dialect.denormalize_name
        return self._estimate(
            connection,
            "SELECT num_rows FROM all_tables"
            " WHERE owner = :schema AND table_name = :table",
            schema=denormalize(schema),
            table=denormalize(table),
        )

    def changed(
        self, current: sa.ColumnElement[Any], new: sa.ColumnElement[Any]
    ) -> sa.ColumnElement[bool]:
        """See `Dialect.changed`; LOBs only compare with DBMS_LOB."""
        if not _is_lob(current.type):
            return super().changed(current, new)
        differ = sa.func.dbms_lob.compare(current, new) != 0
        return sa.or_(
            differ,
            sa.and_(current.is_(None), new.is_not(None)),
            sa.and_(current.is_not(None), new.is_(None)),
        )

    def update(
        self,
        connection: sa.Connection,
        target: contract.TableTarget,
        table: sa.Table,
        stage: sa.Table,
        updates: Sequence[str],
    ) -> int:
        """See `Dialect.update`; Oracle has no UPDATE ... FROM, but MERGE."""
        current = table.alias("etl_target")
        new = stage.alias("etl_new")
        dialect = connection.dialect

        def sql(expression: sa.ColumnElement[Any]) -> str:
            return str(
                expression.compile(
                    dialect=dialect, compile_kwargs={"literal_binds": True}
                )
            )

        quote = dialect.identifier_preparer.quote
        on = sa.and_(*(current.c[k] == new.c[k] for k in target.key))
        changed = sa.or_(
            *(self.changed(current.c[name], new.c[name]) for name in updates)
        )
        # Quoted identifiers, values compiled by SQLAlchemy: no raw data.
        name = dialect.identifier_preparer.format_table(table)
        statement = (
            f"MERGE INTO {name}"  # nosec B608
            f" etl_target USING {quote(stage.name)} etl_new ON ({sql(on)})"
            " WHEN MATCHED THEN UPDATE SET "
            + ", ".join(
                f"etl_target.{quote(name)} = etl_new.{quote(name)}"
                for name in updates
            )
            + f" WHERE {sql(changed)}"
        )
        return connection.exec_driver_sql(statement).rowcount


class MysqlDialect(Dialect):
    """MySQL: temporary tables, case-sensitive change detection.

    MySQL's default collations are case-insensitive, like SQL Server's.
    """

    name = "mysql"
    # Under InnoDB's default REPEATABLE READ, the merge's `NOT EXISTS` read of
    # the target takes shared next-key locks on its gaps, where batches of
    # other key ranges insert: writes made at once deadlock. READ COMMITTED,
    # the default of the other databases, reads the target unlocked.
    session_setup = (
        "SET time_zone = '+00:00'",
        "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
    )
    # Lock wait timeout (1205), deadlock (1213), too many connections
    # (1040), server shutting down (1053), network errors (1158-1161),
    # cannot connect (2002, 2003), lost connections (2006, 2013, 2055),
    # closed for inactivity (4031); access denied (1044, 1045, 1142),
    # unknown database (1049), no such table or column (1146, 1054), syntax
    # (1064).
    retryable_codes = _codes(
        "1205 1213 1040 1053 1158 1159 1160 1161 2002 2003 2006 2013 2055 4031"
    )
    fatal_codes = _codes("1044 1045 1142 1049 1146 1054 1064")

    def create_stage(self, connection: sa.Connection, stage: sa.Table) -> None:
        """See `Dialect.create_stage`.

        A rollback does not drop a temporary table: one left by a failed
        batch on this pooled connection is dropped first.
        """
        self.drop_stage(connection, stage, if_exists=True)
        super().create_stage(connection, stage)

    def drop_stage(
        self,
        connection: sa.Connection,
        stage: sa.Table,
        if_exists: bool = False,
    ) -> None:
        """See `Dialect.drop_stage`; DROP TABLE alone would commit."""
        name = connection.dialect.identifier_preparer.quote(stage.name)
        exists = " IF EXISTS" if if_exists else ""
        connection.exec_driver_sql(f"DROP TEMPORARY TABLE{exists} {name}")

    def changed(
        self, current: sa.ColumnElement[Any], new: sa.ColumnElement[Any]
    ) -> sa.ColumnElement[bool]:
        if isinstance(current.type, sa.String):
            current = sa.cast(current, sa.LargeBinary)
            new = sa.cast(new, sa.LargeBinary)
        return super().changed(current, new)

    def estimated_rows(
        self, connection: sa.Connection, schema: str, table: str
    ) -> int:
        return self._estimate(
            connection,
            "SELECT table_rows FROM information_schema.tables"
            " WHERE table_schema = :schema AND table_name = :table",
            schema=schema,
            table=table,
        )


class PostgresDialect(Dialect):
    """Postgres: row values, COPY into the staging table, ON CONFLICT."""

    name = "postgresql"
    # A SET, not a startup option: `PGTZ` would override that.
    session_setup = ("SET TIME ZONE 'UTC'",)
    # Connection errors have no SQLSTATE: bad login, missing database or role.
    fatal_messages = (
        "password authentication failed",
        "no pg_hba.conf entry",
        "does not exist",
    )

    def estimated_rows(
        self, connection: sa.Connection, schema: str, table: str
    ) -> int:
        # -1 until the table is first analyzed.
        return self._estimate(
            connection,
            "SELECT c.reltuples::bigint FROM pg_class c"
            " JOIN pg_namespace n ON n.oid = c.relnamespace"
            " WHERE n.nspname = :schema AND c.relname = :table",
            schema=schema,
            table=table,
        )

    def after(
        self, columns: Sequence[sa.ColumnElement[Any]], values: Sequence[Any]
    ) -> sa.ColumnElement[bool]:
        return sa.tuple_(*columns) > sa.tuple_(*values)

    def stage(self, target: sa.Table, columns: Sequence[str]) -> sa.Table:
        return sa.Table(
            STAGE,
            sa.MetaData(),
            *(sa.Column(name, target.c[name].type) for name in columns),
            prefixes=["TEMPORARY"],
            postgresql_on_commit="DROP",
        )

    def drop_stage(self, connection: sa.Connection, stage: sa.Table) -> None:
        del connection, stage  # Dropped at commit.

    def fill_stage(
        self,
        connection: sa.Connection,
        stage: sa.Table,
        rows: Sequence[contract.Row],
    ) -> None:
        """See `Dialect.fill_stage`; COPY with psycopg, the fastest way in.

        Raises:
            DBAPIError: If COPY fails: the driver's error, wrapped as
                SQLAlchemy does for its statements.
        """
        driver = connection.connection.driver_connection
        if connection.dialect.driver != "psycopg" or driver is None:
            super().fill_stage(connection, stage, rows)
            return
        # Imported here: only Postgres through psycopg needs them.
        # pylint: disable=import-outside-toplevel
        import psycopg
        from psycopg import sql as pg_sql
        from psycopg.types import json as pg_json

        # pylint: enable=import-outside-toplevel

        json_columns = [isinstance(c.type, sa.JSON) for c in stage.c]
        statement = pg_sql.SQL("COPY {} ({}) FROM STDIN").format(
            pg_sql.Identifier(STAGE),
            pg_sql.SQL(", ").join(pg_sql.Identifier(c.name) for c in stage.c),
        )
        try:
            with driver.cursor() as cursor, cursor.copy(statement) as copy:
                for row in rows:
                    copy.write_row([
                        pg_json.Jsonb(row[c.name])
                        if is_json and row[c.name] is not None
                        else row[c.name]
                        for c, is_json in zip(stage.c, json_columns)
                    ])
        except psycopg.Error as exc:
            error: Any = exc  # A DBAPI error, which SQLAlchemy cannot type.
            lost = connection.dialect.is_disconnect(error, driver, None)
            if lost:
                connection.invalidate(exc)
            raise sa_exc.DBAPIError.instance(
                f"COPY {STAGE} FROM STDIN",
                None,
                exc,
                psycopg.Error,
                hide_parameters=True,
                connection_invalidated=lost,
                dialect=connection.dialect,
            ) from exc

    def merge(
        self,
        connection: sa.Connection,
        target: contract.TableTarget,
        table: sa.Table,
        stage: sa.Table,
        updates: Sequence[str],
    ) -> tuple[int, int]:
        """See `Dialect.merge`; one INSERT ... ON CONFLICT statement."""
        if target.mode in (
            contract.WriteMode.INSERT,
            contract.WriteMode.UPDATE,
        ):
            return super().merge(connection, target, table, stage, updates)

        insert = pg_dialect.insert(table).from_select(
            [c.name for c in stage.c], sa.select(*stage.c)
        )
        if target.mode is contract.WriteMode.INSERT_MISSING or not updates:
            statement = insert.on_conflict_do_nothing(index_elements=target.key)
        else:
            statement = insert.on_conflict_do_update(
                index_elements=target.key,
                set_={name: insert.excluded[name] for name in updates},
                where=sa.or_(*(
                    table.c[name].is_distinct_from(insert.excluded[name])
                    for name in updates
                )),
            )
        # `xmax = 0` tells an inserted row from an updated one.
        flags: list[bool] = list(
            connection.execute(
                statement.returning(sa.literal_column("xmax = 0"))
            ).scalars()
        )
        created = sum(1 for flag in flags if flag)
        return created, len(flags) - created


_DIALECTS: dict[str, type[Dialect]] = {
    "mssql": MssqlDialect,
    "mysql": MysqlDialect,
    "mariadb": MysqlDialect,
    "oracle": OracleDialect,
    "postgresql": PostgresDialect,
}


_DIALECT_BY_DRIVER: dict[str, type[Dialect]] = {
    "psycopg": PostgresDialect,
    "psycopg2": PostgresDialect,
    "pg8000": PostgresDialect,
    "mssql_python": MssqlDialect,
    "pyodbc": MssqlDialect,
    "pymssql": MssqlDialect,
    "oracledb": OracleDialect,
    "cx_Oracle": OracleDialect,
    "pymysql": MysqlDialect,
    "MySQLdb": MysqlDialect,
    "mysql": MysqlDialect,
    "sqlite3": Dialect,
}

# mssql-python gives the SQLSTATE of an error only as its description.
_MSSQL_PYTHON_STATES = {
    "Disconnect error": "01002",
    "Client unable to establish connection": "08001",
    "Connection not open": "08003",
    "Server rejected the connection": "08004",
    "Connection failure during transaction": "08007",
    "Communication link failure": "08S01",
    "Invalid authorization specification": "28000",
    "Invalid catalog name": "3D000",
    "Serialization failure": "40001",
    "Statement completion unknown": "40003",
    "Syntax error or access violation": "42000",
    "Base table or view not found": "42S02",
    "Column not found": "42S22",
    "Operation canceled": "HY008",
    "Timeout expired": "HYT00",
    "Connection timeout expired": "HYT01",
}


def for_engine(engine: sa.Engine) -> Dialect:
    """Returns the behavior of the engine's database; the base by default."""
    return _DIALECTS.get(engine.dialect.name, Dialect)()


def is_transient(
    exc: sa_exc.DBAPIError, dialect: Dialect | None = None
) -> bool:
    """Whether a database error is transient: worth trying again.

    Args:
        exc: The error.
        dialect: The database's; by default, told by the driver that raised
            the error.
    """
    if dialect is None:
        driver = type(exc.orig).__module__.split(".", 1)[0]
        dialect = _DIALECT_BY_DRIVER.get(driver, Dialect)()
    return dialect.transient(exc)


def driver_error(exc: BaseException) -> sa_exc.DBAPIError | None:
    """Wraps an error a driver raised past SQLAlchemy, as SQLAlchemy would.

    SQLAlchemy wraps the errors of statements, not all the others: a dead
    connection given back to the pool fails on SQL Server when its isolation
    level is reset, with the error of mssql-python.

    Returns:
        The error, wrapped; None if not a known driver's.
    """
    if type(exc).__module__.split(".", 1)[0] not in _DIALECT_BY_DRIVER:
        return None
    return sa_exc.DBAPIError(None, None, exc, hide_parameters=True)


def error_code(orig: BaseException | None) -> str | None:
    """Returns the database's own code of a driver error, if it has one.

    `ORA-00060` or `DPY-4011` (python-oracledb), `1213` (MySQL drivers,
    pymssql, the number in pyodbc's and bulk copy's messages),
    `SQLITE_BUSY` (sqlite3, without the extended code).
    """
    if orig is None:
        return None
    name = getattr(orig, "sqlite_errorname", None)
    if isinstance(name, str):
        return "_".join(name.split("_")[:2])
    args = getattr(orig, "args", ())
    first = args[0] if args else None
    full_code = getattr(first, "full_code", None)
    if isinstance(full_code, str):
        return full_code
    if isinstance(first, int) and not isinstance(first, bool):
        return str(first)
    found = _ODBC_NATIVE.search(str(orig)) or _BULK_NATIVE.search(str(orig))
    return found.group(1) if found else None


def sqlstate(orig: BaseException | None) -> str | None:
    """Returns the SQLSTATE of a driver error, if it tells it.

    psycopg and PyMySQL give it, pyodbc first in its arguments, mssql-python
    as its description.
    """
    state = getattr(orig, "sqlstate", None)
    if isinstance(state, str) and _SQLSTATE.match(state):
        return state
    args = getattr(orig, "args", ())
    if args and isinstance(args[0], str) and _SQLSTATE.match(args[0]):
        return args[0]
    description = getattr(orig, "driver_error", None)
    if isinstance(description, str):
        return _MSSQL_PYTHON_STATES.get(description)
    return None


def coerce(
    table: sa.Table, rows: Sequence[contract.Row]
) -> list[dict[str, Any]]:
    """Adapts values to the column types where drivers are strict.

    A UUID may come as text (mappings produce text): UUID columns get
    `uuid.UUID` objects.
    """
    uuids = [
        c.name
        for c in table.c
        if isinstance(c.type, sa.Uuid) and c.type.as_uuid
    ]
    if not uuids:
        return [dict(row) for row in rows]
    coerced = []
    for row in rows:
        values = dict(row)
        for name in uuids:
            value = values.get(name)
            if isinstance(value, str):
                values[name] = uuid.UUID(value)
        coerced.append(values)
    return coerced


# How each database reports a duplicate key: SQLSTATE 23505 (Postgres),
# errors 2627 and 2601 (SQL Server, whose drivers do not all give the number),
# ORA-00001, MySQL 1062, SQLite.
_UNIQUE_VIOLATIONS = (
    "23505",
    "2627",
    "2601",
    "Cannot insert duplicate key",
    "ORA-00001",
    "1062",
    "Duplicate entry",
    "UNIQUE constraint failed",
)


def is_unique_violation(exc: BaseException | None) -> bool:
    """Whether a database error is a duplicate key, whatever the database.

    A writer racing another one on the same key gets one: trying again then
    finds the row, and updates it. Any other constraint (a foreign key, NOT
    NULL) is not.
    """
    if not isinstance(exc, sa_exc.IntegrityError):
        return False
    if getattr(exc.orig, "sqlstate", None) == "23505":
        return True
    text = str(exc.orig)
    return any(marker in text for marker in _UNIQUE_VIOLATIONS)


def _is_lob(type_: sa.types.TypeEngine[Any]) -> bool:
    """Whether values of the type are LOBs: CLOB, NCLOB, BLOB..."""
    return isinstance(type_, (sa.LargeBinary, sa.Text))


def _same_key(
    target: contract.TableTarget, table: sa.Table, stage: sa.Table
) -> sa.ColumnElement[bool]:
    return sa.and_(*(table.c[k] == stage.c[k] for k in target.key))
