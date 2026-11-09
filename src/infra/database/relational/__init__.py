"""Relational databases through SQLAlchemy Core: SQL Server, Oracle,
Postgres, MySQL, SQLite... Postgres and SQLite need nothing more; SQL
Server, Oracle and MySQL need their driver's extra (`mssql`, `oracle`,
`mysql`).

    from infra.database import relational

    # A SQLAlchemy URL, or the application's ADO.NET / JDBC string as it is.
    db = relational.Database(os.environ["CRM_DATABASE_URL"])
    reader = relational.RelationalTableReader(db)
    writer = relational.RelationalTableWriter(db)
    registry = relational.RelationalKeyRegistry(db)

Adapters given the same `Database` share its connection pool. What differs
between databases is in `dialects`.
"""

from infra.database.relational.bookkeeping import DEFAULT_PREFIX
from infra.database.relational.bookkeeping import etl_tables
from infra.database.relational.bookkeeping import EtlTables
from infra.database.relational.bookkeeping import RelationalIssueSink
from infra.database.relational.bookkeeping import RelationalKeyRegistry
from infra.database.relational.bookkeeping import RelationalParkingLot
from infra.database.relational.bookkeeping import RelationalQuarantine
from infra.database.relational.bookkeeping import RelationalRunLedger
from infra.database.relational.bookkeeping import RelationalRunLock
from infra.database.relational.bookkeeping import RelationalStateStore
from infra.database.relational.changes import create_replication
from infra.database.relational.changes import drop_replication
from infra.database.relational.changes import enable_change_tracking
from infra.database.relational.changes import PostgresLogicalReplication
from infra.database.relational.changes import SqlServerChangeTracking
from infra.database.relational.connection_strings import ConnectionSpec
from infra.database.relational.connection_strings import ConnectionStringError
from infra.database.relational.connection_strings import (
    parse as parse_connection_string,
)
from infra.database.relational.database import Database
from infra.database.relational.database import RelationalAdapter
from infra.database.relational.dialects import Dialect
from infra.database.relational.inspector import family
from infra.database.relational.inspector import RelationalSchemaInspector
from infra.database.relational.migrations import check as check_etl_tables
from infra.database.relational.migrations import ddl
from infra.database.relational.migrations import drop as drop_etl_tables
from infra.database.relational.migrations import upgrade as create_etl_tables
from infra.database.relational.tables import RelationalTableReader
from infra.database.relational.tables import RelationalTableWriter

__all__ = [
    "DEFAULT_PREFIX",
    "ConnectionSpec",
    "ConnectionStringError",
    "Database",
    "Dialect",
    "EtlTables",
    "PostgresLogicalReplication",
    "RelationalAdapter",
    "RelationalIssueSink",
    "RelationalKeyRegistry",
    "RelationalParkingLot",
    "RelationalQuarantine",
    "RelationalSchemaInspector",
    "RelationalRunLedger",
    "RelationalRunLock",
    "RelationalStateStore",
    "RelationalTableReader",
    "RelationalTableWriter",
    "SqlServerChangeTracking",
    "check_etl_tables",
    "create_etl_tables",
    "create_replication",
    "ddl",
    "drop_etl_tables",
    "drop_replication",
    "enable_change_tracking",
    "etl_tables",
    "family",
    "parse_connection_string",
]
