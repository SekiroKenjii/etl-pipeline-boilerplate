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

Other databases SQLAlchemy supports get the base `Dialect`
(`base_dialect`).
"""

from __future__ import annotations

from collections.abc import Generator, Sequence
import contextlib
import hashlib
import logging
import sqlite3
import threading
import uuid

from sqlalchemy import exc as sa_exc
import sqlalchemy as sa
from sqlalchemy.dialects import mysql as mysql_dialect
from sqlalchemy.dialects import postgresql as pg_dialect
from sqlalchemy.dialects import sqlite as sqlite_dialect

from core import contract
from infra.database.relational import base_dialect
from infra.database.relational import bulk_loading
from infra.database.relational import driver_errors
from infra.database.relational import mssql_datetimes
from infra.database.relational import mssql_indexes
from infra.database.relational import staging
from infra.database.relational import types as relational_types

_LOG = logging.getLogger(__name__)

STAGE = base_dialect.STAGE
_ROW_LOCKS = "WITH (ROWLOCK)"
# SQL Server compiles a plan per IN list length: 1,000 keys read back in
# lists of 500 took 13.3 ms, in one list of 1,000 15.6 ms (SQL Server 16,
# `etl calibrate`, docs/tuning.md).
MSSQL_IN_CHUNK = 500

Dialect = base_dialect.Dialect
error_code = driver_errors.error_code
sqlstate = driver_errors.sqlstate
is_unique_violation = driver_errors.is_unique_violation


class SqliteDialect(Dialect):
    """SQLite: the base behavior, and INSERT ... ON CONFLICT DO NOTHING."""

    name = "sqlite"
    # SQLITE_MAX_VARIABLE_NUMBER: 999 before 3.32.0, 32,766 since.
    parameter_limit = 32_766 if sqlite3.sqlite_version_info >= (3, 32) else 999

    def bind(self, column: sa.ColumnElement[object], value: object) -> object:
        """`value` as it is: SQLite would bind a `Decimal` as a float, whose
        digits stop at 2**53."""
        del column  # Unused.
        return value

    def insert_skipping(
        self, table: sa.Table, key: Sequence[str]
    ) -> sa.Insert | None:
        # RETURNING makes SQLAlchemy send one multi-row INSERT; without it,
        # sqlite3 runs one statement a row (177 ms a batch of 1,000, not 13).
        return (
            sqlite_dialect.insert(table)
            .on_conflict_do_nothing(index_elements=list(key))
            .returning(*(table.c[name] for name in key))
        )


class MssqlDialect(Dialect):
    """SQL Server: `#` temporary tables, and case-sensitive change detection.

    Most SQL Server databases use a case-insensitive collation, where `'b'`
    equals `'B'`: a fix of the case only would never be written. Under any
    collation, binary ones included, `'a'` equals `'a '`: their byte lengths
    tell them apart.
    """

    name = "mssql"
    _BINARY_COLLATION = "Latin1_General_100_BIN2"
    # Error 8003: a statement binds 2,100 parameters at most.
    parameter_limit = 2100
    in_chunk_cap = MSSQL_IN_CHUNK

    def __init__(
        self,
        *,
        bulk_copy_rows: int | None = None,
        in_chunk: int | None = None,
        chooser: staging.StagingChooser | None = None,
    ) -> None:
        super().__init__(
            bulk_copy_rows=bulk_copy_rows, in_chunk=in_chunk, chooser=chooser
        )
        self._seeks: dict[tuple[str, frozenset[str]], bool] = {}
        self._seeks_lock = threading.Lock()
        # Tables whose next parameter staging the chooser is to time.
        self._timing: dict[str, int] = {}

    # Error numbers (pyodbc, pymssql, bulk copy): deadlock victim (1205),
    # lock timeout (1222), no memory to run the query yet (8645), Azure SQL's
    # database unavailable or busy (40613, 40501, 40197, 49918-49920),
    # network errors (233, 10053, 10054, 10060, 64), timeout (-2); login
    # failed (18456), cannot open the database (4060), no such object or
    # column (208, 207), permission (229, 230), syntax (102, 156).
    retryable_codes = base_dialect.error_codes(
        "1205 1222 8645 40613 40501 40197 49918 49919 49920 233 10053 10054 "
        "10060 64 -2"
    )
    fatal_codes = base_dialect.error_codes("18456 4060 208 207 229 230 102 156")
    # mssql-python gives no error number: match the messages.
    retryable_messages = (
        "was deadlocked on lock",
        "Lock request time out period exceeded",
        "A timeout occurred while waiting for memory resources",
        "Connection reset",
        "Broken pipe",
        "Io error",
    )
    fatal_messages = ("Login failed for user", "Cannot open database")

    def bind(self, column: sa.ColumnElement[object], value: object) -> object:
        """See `Dialect.bind`; `datetime` and `smalldatetime` columns compare
        with a parameter cast to their type (`mssql_datetimes`)."""
        return mssql_datetimes.bind(column, super().bind(column, value))

    @contextlib.contextmanager
    def bulk_stage(
        self,
        engine: sa.Engine,
        target: sa.Table,
        columns: Sequence[str],
        rows: Sequence[contract.Row],
        minimum: int | None = None,
    ) -> Generator[sa.Table | None]:
        """See `Dialect.bulk_stage`; bulk copy (BCP) with mssql-python, into
        a global `##` table (`bulk_loading.bulk_copied`), from
        `bulk_copy_rows` rows, or when the online choice says so
        (`staging.StagingChooser`: the first batches of a table go both
        ways, timed; the faster way is kept).

        Raises:
            DBAPIError: If the bulk copy fails: its error, wrapped for
                `transient` to tell whether to try again.
        """
        if engine.dialect.driver != "mssqlpython" or not self._bulk_copies(
            target.fullname, len(rows), minimum
        ):
            yield None
            return
        stage = sa.Table(
            f"##{STAGE}_{uuid.uuid4().hex}",
            sa.MetaData(),
            *(sa.Column(name, target.c[name].type) for name in columns),
        )
        with contextlib.ExitStack() as stack:
            started = self._now()
            filled = stack.enter_context(
                bulk_loading.bulk_copied(
                    engine, stage, bulk_loading.coerce(stage, rows)
                )
            )
            self._timed(target.fullname, staging.BULK_COPY, len(rows), started)
            yield filled

    def _bulk_copies(self, table: str, rows: int, minimum: int | None) -> bool:
        """Whether a batch of `rows` rows of `table` is bulk copied: by the
        threshold when one is set, else as the chooser says, which then
        times the staging either way."""
        if self.bulk_copy_rows is not None or self.chooser is None:
            threshold = self.bulk_copy_rows if minimum is None else minimum
            return threshold is not None and rows >= threshold
        if self.chooser.pick(table, rows) == staging.BULK_COPY:
            return True
        with self._seeks_lock:
            self._timing[table] = self._timing.get(table, 0) + 1
        return False

    def _now(self) -> float:
        return self.chooser.clock() if self.chooser is not None else 0.0

    def _timed(
        self,
        table: str,
        strategy: relational_types.Staging,
        rows: int,
        started: float,
    ) -> None:
        if self.chooser is not None:
            self.chooser.record(table, strategy, rows, self._now() - started)

    def fill_stage(
        self,
        connection: sa.Connection,
        stage: sa.Table,
        rows: Sequence[contract.Row],
    ) -> None:
        """See `Dialect.fill_stage`; timed for the chooser when it picked
        parameters for this batch (`stage` knows its target)."""
        target = stage.info.get("target")
        timing = False
        if isinstance(target, str):
            with self._seeks_lock:
                pending = self._timing.get(target, 0)
                if pending:
                    self._timing[target] = pending - 1
                    timing = True
        started = self._now()
        super().fill_stage(connection, stage, rows)
        if timing and isinstance(target, str):
            self._timed(target, staging.PARAMETERS, len(rows), started)

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

    def changed[T](
        self, current: sa.ColumnElement[T], new: sa.ColumnElement[T]
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
            info={"target": target.fullname},
        )

    def prepare_join(
        self, engine: sa.Engine, table: sa.Table, columns: Sequence[str]
    ) -> None:
        """See `Dialect.prepare_join`; whether an index seeks `columns`."""
        if self._seeks_known(engine.dialect, table, columns) is None:
            with engine.connect() as connection:
                self._learn_seeks(connection, table, columns)

    def _seeks_known(
        self, dialect: sa.Dialect, table: sa.Table, columns: Sequence[str]
    ) -> bool | None:
        with self._seeks_lock:
            return self._seeks.get(_join_of(dialect, table, columns))

    def _learn_seeks(
        self, connection: sa.Connection, table: sa.Table, columns: Sequence[str]
    ) -> bool:
        """Reads whether an index seeks `columns`, once per table (with or
        without its schema) and columns; a table without one is logged once.

        Once per `Database`, which a process keeps: an index created while
        it runs is seen by the next run.
        """
        learnt = _join_of(connection.dialect, table, columns)
        seeks = mssql_indexes.seekable(connection, table, learnt[1])
        with self._seeks_lock:
            if learnt in self._seeks:
                return self._seeks[learnt]
            self._seeks[learnt] = seeks
        if not seeks:
            _LOG.warning(
                "%s has no index on %s: each batch scans it. Add one (a"
                " primary key, a unique or any index led by these columns);"
                " until then batches written at once into it may deadlock.",
                table.fullname,
                sorted(columns),
            )
        return seeks

    def run(
        self,
        connection: sa.Connection,
        statement: relational_types.Joining,
        *,
        table: sa.Table,
        on: Sequence[str],
    ) -> int:
        """See `Dialect.run`; seeks the target's rows by `on`, if indexed.

        Left to itself, SQL Server scans the whole target (a plan made while
        the table was small stays cached), locking every row and page it
        reads: batches written at once, by threads or processes, then wait
        for each other's new rows and deadlock, and every batch costs a scan.
        A nested loop from the staging table, with row locks, touches only
        the batch's own keys. Without an index on `on`, that nested loop
        would scan the target once per row of the batch: the statement then
        runs without hints, one scan per batch.
        """
        seeks = self._seeks_known(connection.dialect, table, on)
        if seeks is None:
            seeks = self._learn_seeks(connection, table, on)
        if not seeks:
            return connection.execute(statement).rowcount
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
    # 65,535 bind variables a statement; ORA-01795: 1,000 values an IN list.
    parameter_limit = 65_535
    in_list_limit = 1000
    # Deadlock (60), resource busy or timeout (51, 54, 30006), cannot
    # serialize (8177), snapshot too old (1555: a new query reads a new
    # snapshot), shared memory short (4031), lost or closed connections
    # (3113, 3114, 3135, 12537, 12547, DPY-4011), no listener or connect
    # timeout (12541, 12170, DPY-6005), the database starting or stopping
    # (1033, 1034, 1089, 1092, 12514, 12528); no table or column (942, 904),
    # bad login or locked account (1017, 28000), privileges (1031), a
    # missing user (1918).
    retryable_codes = base_dialect.error_codes(
        "ORA-00060 ORA-00051 ORA-00054 ORA-30006 ORA-08177 ORA-01555 "
        "ORA-04031 ORA-03113 ORA-03114 ORA-03135 ORA-12537 ORA-12547 "
        "ORA-12541 ORA-12170 ORA-01033 ORA-01034 ORA-01089 ORA-01092 "
        "ORA-12514 ORA-12528 DPY-4011 DPY-6005"
    )
    fatal_codes = base_dialect.error_codes(
        "ORA-00942 ORA-00904 ORA-01017 ORA-28000 ORA-01031 ORA-01918"
    )

    def __init__(
        self,
        *,
        bulk_copy_rows: int | None = None,
        in_chunk: int | None = None,
        chooser: staging.StagingChooser | None = None,
    ) -> None:
        super().__init__(
            bulk_copy_rows=bulk_copy_rows, in_chunk=in_chunk, chooser=chooser
        )
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

    def changed[T](
        self, current: sa.ColumnElement[T], new: sa.ColumnElement[T]
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

        def sql(expression: sa.ClauseElement) -> str:
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
    session_setup = ("SET time_zone = '+00:00'",)
    # Error 1390: a prepared statement takes 65,535 placeholders at most.
    parameter_limit = 65_535
    # Under InnoDB's default REPEATABLE READ, the merge's `NOT EXISTS` read of
    # the target takes shared next-key locks on its gaps, where batches of
    # other key ranges insert: writes made at once deadlock. READ COMMITTED,
    # the default of the other databases, reads the target unlocked.
    isolation_level = "READ COMMITTED"
    # Lock wait timeout (1205), deadlock (1213), too many connections
    # (1040), server shutting down (1053), network errors (1158-1161),
    # cannot connect (2002, 2003), lost connections (2006, 2013, 2055),
    # closed for inactivity (4031); access denied (1044, 1045, 1142),
    # unknown database (1049), no such table or column (1146, 1054), syntax
    # (1064).
    retryable_codes = base_dialect.error_codes(
        "1205 1213 1040 1053 1158 1159 1160 1161 2002 2003 2006 2013 2055 4031"
    )
    fatal_codes = base_dialect.error_codes("1044 1045 1142 1049 1146 1054 1064")

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

    def changed[T](
        self, current: sa.ColumnElement[T], new: sa.ColumnElement[T]
    ) -> sa.ColumnElement[bool]:
        if isinstance(current.type, sa.String):
            return super().changed(
                sa.cast(current, sa.LargeBinary), sa.cast(new, sa.LargeBinary)
            )
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

    def insert_skipping(
        self, table: sa.Table, key: Sequence[str]
    ) -> sa.Insert | None:
        """See `Dialect.insert_skipping`; `ON DUPLICATE KEY UPDATE k = k`
        leaves a row there as it is. INSERT IGNORE would also hide other
        errors: a value truncated, a bad date."""
        kept = key[0]
        return mysql_dialect.insert(table).on_duplicate_key_update(
            {kept: table.c[kept]}
        )


class PostgresDialect(Dialect):
    """Postgres: row values, COPY into the staging table, ON CONFLICT."""

    name = "postgresql"
    # A SET, not a startup option: `PGTZ` would override that.
    session_setup = ("SET TIME ZONE 'UTC'",)
    # The Bind message counts its parameters in 16 bits (psycopg).
    parameter_limit = 32_767
    # Connection errors have no SQLSTATE: bad login, missing database or role.
    # A statement past its `statement_timeout` (57014, like a cancel) would
    # time out again: keyset pages are short, so it is slow for a reason a
    # retry cannot fix. Waits on locks are `lock_timeout`'s (55P03), retried.
    fatal_messages = (
        "password authentication failed",
        "no pg_hba.conf entry",
        "does not exist",
        "canceling statement due to statement timeout",
    )

    def session_warnings(self, connection: sa.Connection) -> list[str]:
        """See `Dialect.session_warnings`: messages in a language other than
        English, which `fatal_messages` cannot tell."""
        locale = str(
            connection.execute(sa.text("SHOW lc_messages")).scalar_one()
        )
        if _in_english(locale):
            return []
        return [
            f"lc_messages is {locale!r}: a statement past its"
            " statement_timeout is told by its English message, else retried"
            " as a cancel is; set the role's (ALTER ROLE <user> SET"
            " lc_messages = 'C')"
        ]

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
        self,
        columns: Sequence[sa.ColumnElement[object]],
        values: Sequence[object],
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
        """See `Dialect.fill_stage`; COPY with psycopg, the fastest way in
        (`bulk_loading.copy_rows`).

        Raises:
            DBAPIError: If COPY fails: the driver's error, wrapped as
                SQLAlchemy does for its statements.
        """
        if connection.dialect.driver != "psycopg":
            super().fill_stage(connection, stage, rows)
            return
        bulk_loading.copy_rows(connection, stage, rows)

    def insert_missing(
        self,
        engine: sa.Engine,
        table: sa.Table,
        rows: Sequence[contract.Row],
        key: Sequence[str],
    ) -> None:
        """See `Dialect.insert_missing`; one INSERT ... SELECT FROM unnest()
        ... ON CONFLICT DO NOTHING, the batch bound as one array a column.

        Few, large parameters: next to a thread busy in Python, a multi-row
        VALUES of 1,000 rows took twice as long and COPY into a staging
        table three times (every round trip waits for the GIL).
        """
        if not rows:
            return
        columns = list(rows[0])
        arrays = sa.func.unnest(*(
            sa.bindparam(f"p_{name}", type_=sa.ARRAY(table.c[name].type))
            for name in columns
        ))
        statement = (
            pg_dialect.insert(table)
            .from_select(
                columns,
                sa.select(arrays.table_valued(*columns).render_derived()),
            )
            .on_conflict_do_nothing(index_elements=list(key))
        )
        values = bulk_loading.coerce(table, rows)
        base_dialect.autocommitted(
            engine,
            statement,
            {f"p_{name}": [row[name] for row in values] for name in columns},
        )

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
    "sqlite": SqliteDialect,
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
    "sqlite3": SqliteDialect,
}


def for_engine(
    engine: sa.Engine,
    *,
    bulk_copy_rows: int | None = None,
    in_chunk: int | None = None,
) -> Dialect:
    """Returns the behavior of the engine's database; the base by default.

    Args:
        engine: The engine.
        bulk_copy_rows: See `Dialect`; None chooses online.
        in_chunk: See `Dialect.in_chunk`; None derives it.
    """
    return for_name(
        engine.dialect.name, bulk_copy_rows=bulk_copy_rows, in_chunk=in_chunk
    )


def for_name(
    name: str,
    *,
    bulk_copy_rows: int | None = None,
    in_chunk: int | None = None,
) -> Dialect:
    """Returns the behavior of a database by its SQLAlchemy dialect name.

    Args:
        name: The SQLAlchemy dialect name (`mssql`, `postgresql`...).
        bulk_copy_rows: See `Dialect`; None chooses online.
        in_chunk: See `Dialect.in_chunk`; None derives it.
    """
    return _DIALECTS.get(name, Dialect)(
        bulk_copy_rows=bulk_copy_rows, in_chunk=in_chunk
    )


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


def _in_english(locale: str) -> bool:
    """Whether Postgres messages in `locale` are English: an empty one is
    the server's environment, which a session cannot tell."""
    language = locale.split(".", 1)[0].split("_", 1)[0]
    return language in ("C", "POSIX", "en")


def _join_of(
    dialect: sa.Dialect, table: sa.Table, columns: Sequence[str]
) -> tuple[str, frozenset[str]]:
    """The table, in its schema (the default one if none), and the columns
    joined on: `dbo.t` and `t` are one table."""
    schema = table.schema or dialect.default_schema_name or ""
    return f"{schema}.{table.name}", frozenset(c.lower() for c in columns)


def _is_lob[T](type_: sa.types.TypeEngine[T]) -> bool:
    """Whether values of the type are LOBs: CLOB, NCLOB, BLOB..."""
    return isinstance(type_, (sa.LargeBinary, sa.Text))
