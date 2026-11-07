"""A relational database, through a SQLAlchemy engine shared by the adapters."""

from __future__ import annotations

from collections.abc import Generator, Sequence
import contextlib
import threading
from typing import Any

from sqlalchemy import event as sa_event
from sqlalchemy import exc as sa_exc
import sqlalchemy as sa

from core import contract
from infra.database.relational import connection_strings
from infra.database.relational import dialects
from shared import utils


class Database:
    """A database: an engine (and its connection pool), and its dialect.

    Adapters given the same `Database` share its pool; whoever created the
    database disposes of it (`close`) once every adapter is done. An engine
    passed in belongs to the caller.

    An engine created here keeps statement parameters (the rows of a batch) out
    of its errors and runs its sessions in UTC (a naive datetime is UTC). Give
    an engine passed in `hide_parameters=True` and UTC sessions yourself.

        Database("mssql+mssqlpython://etl@db/crm?Encrypt=yes")
        Database("postgresql+psycopg://etl@db/crm")
        Database("Server=db;Initial Catalog=crm;User ID=etl;Password=...")
        Database("jdbc:oracle:thin:@//db:1521/CRM", dialect="oracle")

    Attributes:
        engine: The SQLAlchemy engine.
        dialect: What differs from one database to another.
        connection: How the connection string was translated, if one was
            given.
    """

    def __init__(
        self,
        engine: sa.Engine | str,
        *,
        dialect: str | None = None,
        **options: Any,
    ) -> None:
        """Initializes the database; nothing connects before the first use.

        Args:
            engine: An engine, or a connection string to create one: a
                SQLAlchemy URL, or an ADO.NET, ODBC, OLE DB, JDBC or libpq
                string (`connection_strings`). Keep passwords out of the code:
                read it from the environment or a secret store.
            dialect: The database of a connection string that does not tell
                it: `mssql`, `oracle`, `postgresql`, `mysql`.
            **options: Options of `sqlalchemy.create_engine`, with a
                connection string.

        Raises:
            ConnectionStringError: If the connection string cannot be
                translated.
        """
        self._owned = isinstance(engine, str)
        self.connection: connection_strings.ConnectionSpec | None = None
        if isinstance(engine, str):
            spec = connection_strings.parse(engine, dialect=dialect)
            self.connection = spec
            options["connect_args"] = {
                **spec.connect_args,
                **options.get("connect_args", {}),
            }
            if _sqlite_file(spec.url):
                # Pooled, a connection out at `dispose()` would never close.
                options.setdefault("poolclass", sa.pool.NullPool)
            # Batches, their personal data included, stay out of errors.
            options["hide_parameters"] = True
            self.engine = sa.create_engine(spec.url, **options)
        else:
            self.engine = engine
        self.dialect = dialects.for_engine(self.engine)
        if self._owned and self.dialect.session_setup:
            sa_event.listen(self.engine, "connect", self._set_up_session)
        self._tables: dict[str, sa.Table] = {}
        self._lock = threading.Lock()

    @property
    def name(self) -> str:
        return self.engine.dialect.name

    def _set_up_session(self, connection: Any, record: Any) -> None:
        """Runs the dialect's `session_setup` on a new DBAPI connection."""
        del record  # Unused.
        cursor = connection.cursor()
        try:
            for statement in self.dialect.session_setup:
                cursor.execute(statement)
        finally:
            cursor.close()
        # Kept by the session: a transaction rolled back would undo a SET.
        connection.commit()

    def close(self) -> None:
        """Disposes of an engine created here, and forgets the tables read;
        an engine passed in is the caller's."""
        with self._lock:
            self._tables.clear()
        if self._owned:
            self.engine.dispose()

    @contextlib.contextmanager
    def connect(self, action: str) -> Generator[sa.Connection]:
        """Yields a connection in autocommit mode, translating errors.

        Args:
            action: What is being done, for the error messages.
        """
        with errors(action, self.dialect), self.engine.connect() as connection:
            yield connection.execution_options(
                isolation_level="AUTOCOMMIT", preserve_rowcount=True
            )

    @contextlib.contextmanager
    def begin(self, action: str) -> Generator[sa.Connection]:
        """Yields a connection in a transaction, committed on success.

        Args:
            action: What is being done, for the error messages.
        """
        with errors(action, self.dialect), self.engine.begin() as connection:
            # Row counts of INSERTs are only kept on request.
            yield connection.execution_options(preserve_rowcount=True)

    def table(self, name: str) -> sa.Table:
        """Returns the table `schema.table` or `table`, reflected once.

        Raises:
            FatalError: If the table does not exist.
        """
        with self._lock:
            table = self._tables.get(name)
        if table is not None:
            return table

        schema, table_name = split_name(name)

        def reflect() -> sa.Table:
            with errors(f"reading the definition of {name}", self.dialect):
                try:
                    return sa.Table(
                        table_name,
                        sa.MetaData(),
                        schema=schema,
                        autoload_with=self.engine,
                    )
                except sa_exc.NoSuchTableError as exc:
                    raise contract.FatalError(f"No table {name}") from exc

        # SQL Server may pick catalog reads as deadlock victims of DDL.
        table = utils.retry(
            reflect,
            when=lambda exc: isinstance(exc, contract.RetryableError),
            attempts=3,
        )
        with self._lock:
            self._tables[name] = table
        return table


def split_name(name: str) -> tuple[str | None, str]:
    """Splits `schema.table` into its schema (None if absent) and table.

    Raises:
        ValueError: If `name` has more than two parts.
    """
    parts = name.split(".")
    if len(parts) > 2 or not all(parts):
        raise ValueError(f"Not a table name: {name!r}")
    return (None, parts[0]) if len(parts) == 1 else (parts[0], parts[1])


def column(table: sa.Table, name: str) -> sa.Column[Any]:
    """Returns the column `name` of `table`.

    Raises:
        FatalError: If there is no such column; the message lists them.
    """
    try:
        return table.c[name]
    except KeyError as exc:
        raise contract.FatalError(
            f"{table.fullname} has no column {name!r}; it has"
            f" {[c.name for c in table.c]}"
        ) from exc


def insert_many(
    connection: sa.Connection,
    table: sa.Table,
    rows: Sequence[contract.Row],
) -> None:
    """Inserts rows in as few statements as the database takes.

    SQLAlchemy's "insertmanyvalues" batches them into multi-row INSERTs,
    within each database's limit of parameters; on some databases (Postgres
    with psycopg, SQLite) only for an INSERT ... RETURNING, which it is then.
    Elsewhere the driver's `executemany` does as much (MySQL rewrites it into
    multi-row INSERTs, Oracle binds arrays).
    """
    if not rows:
        return
    statement = table.insert()
    dialect = connection.dialect
    if (
        dialect.use_insertmanyvalues
        and not dialect.use_insertmanyvalues_wo_returning
        and dialect.insert_executemany_returning
    ):
        statement = statement.returning(*table.primary_key.columns)
    connection.execute(statement, list(rows))


def _sqlite_file(url: str | sa.URL) -> bool:
    """Whether `url` is a SQLite database in a file, not in memory."""
    parsed = sa.make_url(url)
    return (
        parsed.get_backend_name() == "sqlite"
        and parsed.database not in (None, "", ":memory:")
        and parsed.query.get("mode") != "memory"
    )


@contextlib.contextmanager
def errors(
    action: str, dialect: dialects.Dialect | None = None
) -> Generator[None]:
    """Translates database errors into the exceptions of the ETL core.

    Lost connections, timeouts, deadlocks, an exhausted connection pool are
    retryable; anything retrying cannot fix (a bad login, a missing table,
    bad SQL, a constraint, bad data) is fatal: `dialects.is_transient`
    tells them apart.

    Args:
        action: What was being done, for the message.
        dialect: The database's; by default, told by the driver.

    Raises:
        RetryableError: For a transient failure.
        FatalError: For any other database error.
    """
    try:
        yield
    except sa_exc.DBAPIError as exc:
        transient = dialects.is_transient(exc, dialect)
        error = contract.RetryableError if transient else contract.FatalError
        raise error(f"{action}: {_first_line(exc.orig or exc)}") from exc
    except (sa_exc.TimeoutError, sa_exc.DisconnectionError) as exc:
        # Pool exhausted in time, or a pooled connection found dead.
        raise contract.RetryableError(f"{action}: {_first_line(exc)}") from exc
    except sa_exc.SQLAlchemyError as exc:
        raise contract.FatalError(f"{action}: {exc}") from exc
    except Exception as exc:
        wrapped = dialects.driver_error(exc)
        if wrapped is None:
            raise
        transient = dialects.is_transient(wrapped, dialect)
        error = contract.RetryableError if transient else contract.FatalError
        raise error(f"{action}: {_first_line(exc)}") from exc


def _first_line(exc: BaseException) -> str:
    text = str(exc).strip()
    return text.splitlines()[0] if text else type(exc).__name__


class RelationalAdapter:
    """The base of the adapters, which may share their `Database`.

    Attributes:
        db: The database of the adapter.
    """

    def __init__(self, db: Database | str) -> None:
        """Initializes the adapter.

        Args:
            db: A shared database, or a SQLAlchemy URL for one of its own.
        """
        self.db = Database(db) if isinstance(db, str) else db
