"""The revisions of the ETL tables, applied by Alembic: `etl-db upgrade`.

    from infra.database.relational import migrations

    migrations.upgrade(db)                  # Create them, or upgrade them.
    migrations.status(db).up_to_date        # What `etl-db check` tells.
    migrations.ddl("mssql", start="0002")   # The SQL of the upgrade, for DBAs.

Each revision is a hand-written script of `versions/`. The revision is kept
in `<prefix>alembic_version`, in the schema of the ETL tables: each prefix
has its own. Tables created before the version table existed get their
revision detected from their columns and indexes; when none matches,
`upgrade` refuses rather than guess. There is no downgrade: restore the
backup taken before the upgrade.
"""

from __future__ import annotations

from collections.abc import Generator
import contextlib
import dataclasses
import hashlib
import io
import pathlib
import threading
import time

from alembic import command
from alembic import config as alembic_config
from alembic import context
from alembic import util as alembic_util
from alembic.runtime import migration
from sqlalchemy import exc as sa_exc
import sqlalchemy as sa

from core import contract
from infra.database.relational import bookkeeping
from infra.database.relational import database
from infra.database.relational import types as relational_types
from shared import utils

REVISIONS = ("0001", "0002", "0003", "0004")
HEAD = REVISIONS[-1]
DESCRIPTIONS = {
    "0001": "the tables of 0.1.0",
    "0002": "the steps of the runs; the status of quarantined records",
    "0003": "the keys of quarantined records; indexes of the issues",
    "0004": "one open quarantine entry per record",
}
_DIRECTORY = pathlib.Path(__file__).parent
# Alembic's `context` and `op` are process globals: one command at once.
_ALEMBIC = threading.Lock()
_RACING_UPGRADE_ATTEMPTS = 5
_UPGRADE_LOCK_WAIT_SECONDS = 600
_VERSION_TABLE = "alembic_version"
# Not on every database: `bookkeeping.OPEN_ENTRY_INDEX_DIALECTS`.
_OPEN_ENTRY_INDEX = "quarantine_open"

# Detects the revision of unversioned tables. Whole new tables are not
# markers: `create_all` also added them to older tables.
_MARKERS: dict[str, tuple[tuple[str, str, str], ...]] = {
    "0002": (
        ("quarantine", "column", "status"),
        ("quarantine", "column", "resolved_at"),
        ("quarantine", "index", "quarantine_status"),
    ),
    "0003": (
        ("quarantine", "column", "record_key"),
        ("quarantine", "index", "quarantine_record"),
        ("quality_issues", "index", "quality_issues_record"),
        ("quality_issues", "index", "quality_issues_found"),
    ),
    "0004": (("quarantine", "index", _OPEN_ENTRY_INDEX),),
}
_NEW_TABLES_BY_REVISION = {"0002": ("run_steps",)}


@dataclasses.dataclass(frozen=True)
class Status:
    """Where the ETL tables of a database stand.

    Attributes:
        dialect: The database: `mssql`, `oracle`, `postgresql`...
        revision: The revision recorded; None if there is no version table.
        detected: Without version table: the revision the tables look like;
            None if there are no ETL tables at all.
        problem: Without version table: why their revision cannot be told.
        schema: The schema of the tables.
        prefix: The prefix of their names.
    """

    dialect: str
    revision: str | None
    detected: str | None = None
    problem: str | None = None
    schema: str | None = None
    prefix: str = bookkeeping.DEFAULT_PREFIX

    @property
    def up_to_date(self) -> bool:
        """Whether the tables are at the latest revision."""
        return self.revision == HEAD

    @property
    def known(self) -> bool:
        """Whether `upgrade` can bring the tables to the latest revision."""
        if self.revision is not None:
            return self.revision in REVISIONS
        return self.problem is None

    @property
    def empty(self) -> bool:
        """Whether there are no ETL tables at all."""
        return self.revision is None and self.detected is None

    def message(self) -> str:
        """Where the tables stand, and what to run if they are behind."""
        if self.up_to_date:
            return f"The ETL tables are at revision {HEAD}, the latest."
        if self.revision is not None and not self.known:
            return (
                f"The ETL tables are at revision {self.revision}, unknown to"
                " this release: a later release upgraded them. Run that"
                " release; there is no downgrade."
            )
        if self.problem is not None:
            return (
                "The ETL tables have no version table, and match no revision:"
                f" {self.problem}. Compare them with `etl-db ddl"
                f"{self._options()}` and fix them by hand; nothing was changed."
            )
        if self.empty:
            return (
                "There are no ETL tables: run `etl-db upgrade`, or have a DBA"
                f" run the SQL of `etl-db ddl{self._options()}`."
            )
        if self.revision is None:
            found = self.detected
            return (
                "The ETL tables have no version table (they were created"
                f" before it); they look like revision {found}"
                f" ({DESCRIPTIONS[str(found)]}). Run `etl-db upgrade`, or have"
                " a DBA run the SQL of `etl-db ddl"
                f"{self._options()} --from {found} --legacy`."
            )
        return (
            f"The ETL tables are at revision {self.revision}"
            f" ({DESCRIPTIONS[self.revision]}); this release needs {HEAD}"
            f" ({DESCRIPTIONS[HEAD]}). Run `etl-db upgrade`, or have a DBA run"
            f" the SQL of `etl-db ddl{self._options()} --from {self.revision}`."
        )

    def _options(self) -> str:
        options = f" --dialect {self.dialect}"
        if self.schema is not None:
            options += f" --schema {self.schema}"
        if self.prefix != bookkeeping.DEFAULT_PREFIX:
            options += f" --prefix {self.prefix}"
        return options


def status(
    db: database.Database,
    schema: str | None = None,
    prefix: str = bookkeeping.DEFAULT_PREFIX,
) -> Status:
    """Tells where the ETL tables of a database stand; changes nothing.

    Raises:
        RetryableError: If the server cannot be reached.
        FatalError: If the tables cannot be read.
    """
    with (
        database.errors("reading the revision of the ETL tables", db.dialect),
        db.engine.connect() as connection,
    ):
        return _status(connection, schema, prefix)


def check(
    db: database.Database,
    schema: str | None = None,
    prefix: str = bookkeeping.DEFAULT_PREFIX,
) -> None:
    """Checks that the ETL tables are at the latest revision.

    Raises:
        PipelineConfigError: If they are not, with what to run.
        RetryableError: If the server cannot be reached, after a few
            attempts.
        FatalError: If the tables cannot be read.
    """
    # Reading the catalog deadlocks with another process's DDL on SQL
    # Server (processes starting together, one upgrading): look again.
    found = utils.retry(
        lambda: status(db, schema, prefix),
        when=lambda exc: isinstance(exc, contract.RetryableError),
        attempts=_RACING_UPGRADE_ATTEMPTS,
        delay=0.2,
    )
    if not found.up_to_date:
        raise contract.PipelineConfigError(found.message())


def upgrade(
    db: database.Database,
    schema: str | None = None,
    prefix: str = bookkeeping.DEFAULT_PREFIX,
) -> Status:
    """Creates the ETL tables, or brings them to the latest revision.

    Tables without version table get the revision they look like recorded
    first. Safe for processes starting together: one failing on what another
    just did looks again, a few times. Prefer one `etl-db upgrade` per release,
    before the pipelines start.

    Returns:
        Where the tables stood before.

    Raises:
        PipelineConfigError: If the revision of the tables cannot be told,
            or is unknown to this release.
        RetryableError: If the server cannot be reached.
        FatalError: If the DDL fails, e.g. for lack of privileges.
    """

    def attempt() -> Status:
        with (
            database.errors("upgrading the ETL tables", db.dialect),
            db.engine.connect() as connection,
            _exclusive(connection, schema, prefix),
        ):
            before = _status(connection, schema, prefix)
            connection.commit()
            if not before.known:
                raise contract.PipelineConfigError(before.message())
            if before.up_to_date:
                return before
            if before.revision is None and before.detected in (None, HEAD):
                # Nothing yet, or an interrupted creation of the latest tables.
                with connection.begin():
                    bookkeeping.etl_tables(schema, prefix).metadata.create_all(
                        connection
                    )
                _run(connection, "stamp", HEAD, schema, prefix)
                return before
            if before.revision is None:
                _run(connection, "stamp", str(before.detected), schema, prefix)
            _run(connection, "upgrade", HEAD, schema, prefix)
            return before

    # A concurrent upgrade fails this one in database-specific ways, or shows
    # half-done tables: detect the revision again and retry.
    return utils.retry(
        attempt,
        when=lambda exc: isinstance(exc, contract.PipelineError),
        attempts=_RACING_UPGRADE_ATTEMPTS,
        delay=0.2,
    )


def ddl(
    dialect: str,
    schema: str | None = None,
    prefix: str = bookkeeping.DEFAULT_PREFIX,
    *,
    start: str | None = None,
    legacy: bool = False,
) -> str:
    """Returns the SQL creating the ETL tables, or upgrading them.

    Args:
        dialect: The SQLAlchemy dialect: `mssql`, `oracle`, `postgresql`...
        schema: The schema of the tables.
        prefix: Prefixes the table names.
        start: The revision of the tables to upgrade; None to create them.
        legacy: Whether the tables have no version table yet (created
            before it): the SQL records `start` first.

    Raises:
        ValueError: If the dialect is unknown, `start` is not a revision, or
            `legacy` has no `start`.
    """
    try:
        sa.make_url(f"{dialect}://").get_dialect()
    except sa_exc.NoSuchModuleError as exc:
        raise ValueError(f"No database {dialect!r}") from exc
    if start is None:
        if legacy:
            raise ValueError(
                "--legacy needs the revision of the tables: --from"
            )
        return _create_sql(dialect, schema, prefix) + _sql(
            dialect, schema, prefix, ("stamp", HEAD)
        )
    if start not in REVISIONS:
        raise ValueError(
            f"No revision {start!r}: one of {', '.join(REVISIONS)}"
        )
    steps = [("upgrade", f"{start}:{HEAD}")]
    if legacy:
        steps.insert(0, ("stamp", start))
    return _sql(dialect, schema, prefix, *steps)


def drop(
    db: database.Database,
    schema: str | None = None,
    prefix: str = bookkeeping.DEFAULT_PREFIX,
) -> None:
    """Drops the ETL tables and their version table, for tests."""
    with database.errors("dropping the ETL tables", db.dialect):
        bookkeeping.etl_tables(schema, prefix).metadata.drop_all(db.engine)
        version = sa.Table(
            prefix + _VERSION_TABLE,
            sa.MetaData(schema=schema),
            sa.Column("version_num", sa.String(32), primary_key=True),
        )
        version.drop(db.engine, checkfirst=True)


def run_environment() -> None:
    """Runs the revisions Alembic chose; called by `env.py` only."""
    config = context.config
    schema = config.attributes["schema"]
    prefix = config.attributes["prefix"]
    options: relational_types.MigrationOptions = {
        "version_table": prefix + _VERSION_TABLE,
        "version_table_schema": schema,
        # Where DDL is transactional, a failed revision leaves the one before.
        "transaction_per_migration": True,
    }
    if context.is_offline_mode():
        context.configure(
            dialect_name=config.attributes["dialect"],
            literal_binds=True,
            **options,
        )
        _for_dbas(context.get_context().dialect)
    else:
        context.configure(connection=config.attributes["connection"], **options)
    with context.begin_transaction():
        context.run_migrations(schema=schema, prefix=prefix)


def _status(
    connection: sa.Connection, schema: str | None, prefix: str
) -> Status:
    """Where the tables stand: the revision recorded, else told."""
    dialect = connection.dialect.name
    recorded = migration.MigrationContext.configure(
        connection,
        opts={
            "version_table": prefix + _VERSION_TABLE,
            "version_table_schema": schema,
        },
    ).get_current_revision()
    if recorded is not None:
        return Status(dialect, recorded, schema=schema, prefix=prefix)
    try:
        detected = _detect(connection, schema, prefix)
    except _Unknown as exc:
        return Status(
            dialect, None, problem=str(exc), schema=schema, prefix=prefix
        )
    return Status(dialect, None, detected, schema=schema, prefix=prefix)


class _Unknown(Exception):
    """The ETL tables match no revision."""


def _detect(
    connection: sa.Connection, schema: str | None, prefix: str
) -> str | None:
    """Tells the revision of ETL tables without version table.

    Returns:
        The revision; None if there are no ETL tables.

    Raises:
        _Unknown: If the tables match no revision.
    """
    inspector = sa.inspect(connection)
    existing = {name.lower() for name in inspector.get_table_names(schema)}
    tables = bookkeeping.etl_tables(schema, prefix).metadata.sorted_tables
    present = {t.name: t for t in tables if t.name.lower() in existing}
    if not present:
        return None
    columns = {
        name: {c["name"].lower() for c in inspector.get_columns(name, schema)}
        for name in present
    }
    indexes = {
        name: {
            str(i["name"]).lower()
            for i in inspector.get_indexes(name, schema)
            if i["name"]
        }
        for name in present
    }
    dialect = connection.dialect.name
    if all(
        _as_declared(table, columns[name], indexes[name], dialect)
        for name, table in present.items()
    ):
        # A creation of the latest tables, maybe interrupted.
        return HEAD

    def found(table: str, kind: str, name: str) -> bool:
        table = prefix + table
        if table not in present:
            return False
        if kind == "column":
            return name in columns[table]
        return (prefix + name).lower() in indexes[table]

    revision = REVISIONS[0]
    gap = None
    for later in REVISIONS[1:]:
        markers = _markers(later, dialect)
        seen = [marker for marker in markers if found(*marker)]
        if seen and len(seen) < len(markers):
            missing = [m for m in markers if m not in seen]
            raise _Unknown(
                f"some of revision {later} is there, not all: missing"
                f" {_names(prefix, missing)}"
            )
        if len(seen) < len(markers):
            gap = gap or later
        elif gap is None:
            revision = later
        elif seen:
            raise _Unknown(f"revision {later} is there, {gap} is not")
    needed = {t.name for t in tables} - {
        prefix + table
        for later in REVISIONS[REVISIONS.index(revision) + 1 :]
        for table in _NEW_TABLES_BY_REVISION.get(later, ())
    }
    if missing_tables := sorted(needed - set(present)):
        raise _Unknown(
            f"they look like revision {revision}, without"
            f" {', '.join(missing_tables)}"
        )
    return revision


def _markers(revision: str, dialect: str) -> tuple[tuple[str, str, str], ...]:
    if (
        revision == "0004"
        and dialect not in bookkeeping.OPEN_ENTRY_INDEX_DIALECTS
    ):
        return ()
    return _MARKERS.get(revision, ())


def _names(prefix: str, markers: list[tuple[str, str, str]]) -> str:
    return ", ".join(
        f"the {kind} {prefix}{table}.{name}"
        if kind == "column"
        else f"the {kind} {prefix}{name}"
        for table, kind, name in markers
    )


def _as_declared(
    table: sa.Table, columns: set[str], indexes: set[str], dialect: str
) -> bool:
    """Whether a table has the columns and indexes of the latest revision."""
    expected = {
        str(index.name).lower()
        for index in table.indexes
        if dialect in bookkeeping.OPEN_ENTRY_INDEX_DIALECTS
        or not str(index.name).endswith(_OPEN_ENTRY_INDEX)
    }
    return {c.name.lower() for c in table.columns} <= columns and (
        expected <= indexes
    )


def _create_sql(dialect: str, schema: str | None, prefix: str) -> str:
    """The CREATE statements of the latest tables."""
    statements: list[str] = []

    def collect(statement: sa.schema.ExecutableDDLElement, *_: object) -> None:
        compiled = str(statement.compile(dialect=engine.dialect)).strip()
        statements.append(compiled.replace("\t", "    ") + ";")

    engine = sa.create_mock_engine(sa.make_url(f"{dialect}://"), collect)
    _for_dbas(engine.dialect)
    bookkeeping.etl_tables(schema, prefix).metadata.create_all(
        engine, checkfirst=False
    )
    return "\n\n".join(statements) + "\n\n"


def _for_dbas(dialect: sa.Dialect) -> None:
    """Sets up a dialect writing SQL for a database it never connected to,
    as it would after connecting to a supported version."""
    # No Oracle `RETURNING ... INTO :ret_0`: the script is for a DBA's tool.
    dialect.insert_returning = False
    if dialect.name == "mssql":
        # NVARCHAR(max), not NTEXT: SQL Server 2005 and later.
        setattr(dialect, "deprecate_large_types", True)


def _config(
    schema: str | None,
    prefix: str,
    *,
    connection: sa.Connection | None = None,
    dialect: str | None = None,
    output: io.StringIO | None = None,
) -> alembic_config.Config:
    """Alembic's configuration, without file: what `env.py` reads."""
    config = alembic_config.Config(output_buffer=output)
    config.set_main_option(
        "script_location", str(_DIRECTORY).replace("%", "%%")
    )
    config.attributes.update(
        connection=connection, dialect=dialect, schema=schema, prefix=prefix
    )
    return config


@contextlib.contextmanager
def _exclusive(
    connection: sa.Connection, schema: str | None, prefix: str
) -> Generator[None]:
    """Holds the database's lock on upgrading these tables, where the session
    has one (Postgres, SQL Server, MySQL): another process upgrading waits.

    Oracle's `DBMS_LOCK` needs a grant and SQLite has one writer: there, a
    racing process fails and `upgrade` tries again.

    Raises:
        RetryableError: If another process holds it for
            `_UPGRADE_LOCK_WAIT_SECONDS`.
    """
    name = hashlib.sha256(f"etl:{schema}.{prefix}".encode()).hexdigest()[:32]
    dialect = connection.dialect.name
    statements = {
        "postgresql": (
            "SELECT pg_try_advisory_lock(:key)",
            "SELECT pg_advisory_unlock(:key)",
        ),
        "mssql": (
            (
                "DECLARE @got INT; EXEC @got = sp_getapplock @Resource = :name,"
                " @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout"
                " = 0; SELECT CASE WHEN @got >= 0 THEN 1 ELSE 0 END"
            ),
            "EXEC sp_releaseapplock @Resource = :name, @LockOwner = 'Session'",
        ),
        "mysql": ("SELECT GET_LOCK(:name, 0)", "SELECT RELEASE_LOCK(:name)"),
    }.get(dialect)
    if statements is None:
        yield
        return
    acquire, release = (sa.text(s) for s in statements)
    params = {"name": name, "key": int(name[:15], 16)}

    # Polled: a lock wait the database ends leaves the session unclear.
    deadline = time.monotonic() + _UPGRADE_LOCK_WAIT_SECONDS
    while True:
        got = connection.execute(acquire, params).scalar()
        connection.commit()
        if got:
            break
        if time.monotonic() > deadline:
            raise contract.RetryableError(
                "Another process has been upgrading the ETL tables for"
                f" {_UPGRADE_LOCK_WAIT_SECONDS} s"
            )
        time.sleep(0.5)
    try:
        yield
    finally:
        connection.rollback()
        connection.execute(release, params)
        connection.commit()


def _run(
    connection: sa.Connection,
    action: str,
    revision: str,
    schema: str | None,
    prefix: str,
) -> None:
    """Runs `alembic stamp` or `alembic upgrade` on the database."""
    with _ALEMBIC, _alembic_errors():
        config = _config(schema, prefix, connection=connection)
        if action == "stamp":
            command.stamp(config, revision)
        else:
            command.upgrade(config, revision)


def _sql(
    dialect: str,
    schema: str | None,
    prefix: str,
    *steps: tuple[str, str],
) -> str:
    """The SQL of Alembic's steps, offline: `stamp` or `upgrade`."""
    output = io.StringIO()
    config = _config(schema, prefix, dialect=dialect, output=output)
    with _ALEMBIC, _alembic_errors():
        for action, revision in steps:
            if action == "stamp":
                command.stamp(config, revision, sql=True)
            else:
                command.upgrade(config, revision, sql=True)
    return output.getvalue()


@contextlib.contextmanager
def _alembic_errors() -> Generator[None]:
    """Alembic's own errors (a revision unknown...) are the user's."""
    try:
        yield
    except alembic_util.CommandError as exc:
        raise contract.PipelineConfigError(str(exc)) from exc
