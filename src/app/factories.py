"""The resources of the settings: which adapter each setting means.

Adapters are imported when their resource is first used: a deployment using
SQL databases only needs no MongoDB driver.
"""

from __future__ import annotations

from collections.abc import Callable
import socket
from typing import Any
import urllib.parse

import config
from core import contract
from core import keys
from core import quality
from core import runtime
from core import state as core_state
from core.runtime import names

# Adapters need their family's extra: imported when a resource is created.
# pylint: disable=import-outside-toplevel

# Creating it creates or upgrades the ETL tables (`bookkeeping.create`).
ETL_TABLES = "etl.tables"
LINEAGE = "endpoint.lineage"
OTLP = "endpoint.otlp"


def resource_specs(
    settings: config.AppSettings,
) -> list[runtime.ResourceSpec[Any]]:
    """Returns every resource the settings describe."""
    specs: list[runtime.ResourceSpec[Any]] = []
    for name, database in settings.databases.items():
        specs.extend(
            _mongodb_specs(name, database)
            if database.is_mongodb
            else _relational_specs(name, database)
        )
    for name, store in settings.stores.items():
        specs.append(_store_spec(name, store))
    for name, cluster in settings.kafka.items():
        specs.append(_kafka_spec(name, cluster))
    specs.extend(_bookkeeping_specs(settings))
    if settings.lineage is not None:
        specs.append(
            _endpoint_spec(
                LINEAGE,
                settings.lineage.endpoint,
                settings.lineage.timeout_seconds,
                "the OpenLineage endpoint",
            )
        )
    if settings.logging.otlp is not None:
        specs.append(
            _endpoint_spec(
                OTLP,
                settings.logging.otlp.endpoint,
                settings.logging.otlp.timeout_seconds,
                "the OTLP endpoint",
            )
        )
    return specs


def _endpoint_spec(
    name: str, endpoint: str, timeout_seconds: float, description: str
) -> runtime.ResourceSpec[str]:
    """An HTTP endpoint events are sent to, checked by connecting to it."""

    def ping(url: str) -> None:
        parts = urllib.parse.urlsplit(url if "//" in url else f"//{url}")
        if not parts.hostname:
            raise ValueError(f"{url!r} names no host")
        port = parts.port or (443 if parts.scheme == "https" else 80)
        # A connection only: sending an event would record one.
        socket.create_connection(
            (parts.hostname, port), timeout=timeout_seconds
        ).close()

    return runtime.ResourceSpec(
        name, lambda _: endpoint, health=ping, description=description
    )


def _relational_specs(
    name: str, settings: config.DatabaseSettings
) -> list[runtime.ResourceSpec[Any]]:
    """A SQL database, and its table reader, writer and inspector."""
    from infra.database import relational

    def create(_: runtime.Resources) -> relational.Database:
        db = relational.Database(
            settings.url.get_secret_value(),
            dialect=settings.dialect,
            **settings.options.arguments(),
        )
        return db

    def close(db: relational.Database) -> None:
        db.close()

    def ping(db: relational.Database) -> None:
        import sqlalchemy as sa

        with db.connect("checking the database") as connection:
            connection.execute(sa.select(1))

    database = names.database(name)
    return [
        runtime.ResourceSpec(
            database,
            create,
            close=close,
            health=ping,
            description=f"SQL database {name}",
        ),
        # A reader per run: it checks each read order once (NULLs).
        runtime.ResourceSpec(
            names.reader(name),
            lambda r: relational.RelationalTableReader(
                r.get(database, relational.Database),
                page_size=settings.page_size,
            ),
            scope=runtime.Scope.RUN,
        ),
        runtime.ResourceSpec(
            names.writer(name),
            lambda r: relational.RelationalTableWriter(
                r.get(database, relational.Database)
            ),
        ),
        runtime.ResourceSpec(
            names.inspector(name),
            lambda r: relational.RelationalSchemaInspector(
                r.get(database, relational.Database)
            ),
        ),
        _changes_spec(
            name,
            settings,
            lambda r: _RelationalChangeLog(
                r.get(database, relational.Database), settings
            ),
        ),
    ]


def _mongodb_specs(
    name: str, settings: config.DatabaseSettings
) -> list[runtime.ResourceSpec[Any]]:
    """A MongoDB database, and its collection reader, writer and inspector."""
    from infra.database import mongodb

    def create(_: runtime.Resources) -> mongodb.MongoDatabase:
        db = mongodb.MongoDatabase(
            settings.url.get_secret_value(), **settings.options.arguments()
        )
        return db

    def close(db: mongodb.MongoDatabase) -> None:
        db.close()

    def ping(db: mongodb.MongoDatabase) -> None:
        db.client.admin.command("ping")

    database = names.database(name)
    return [
        runtime.ResourceSpec(
            database,
            create,
            close=close,
            health=ping,
            description=f"MongoDB database {name}",
        ),
        # A reader per run: it checks each read order once (NULLs).
        runtime.ResourceSpec(
            names.reader(name),
            lambda r: mongodb.MongoTableReader(
                r.get(database, mongodb.MongoDatabase),
                page_size=settings.page_size,
            ),
            scope=runtime.Scope.RUN,
        ),
        runtime.ResourceSpec(
            names.writer(name),
            lambda r: mongodb.MongoTableWriter(
                r.get(database, mongodb.MongoDatabase)
            ),
        ),
        runtime.ResourceSpec(
            names.inspector(name),
            lambda r: mongodb.MongoSchemaInspector(
                r.get(database, mongodb.MongoDatabase)
            ),
        ),
        _changes_spec(
            name,
            settings,
            lambda r: _MongoChangeLog(r.get(database, mongodb.MongoDatabase)),
        ),
    ]


def _changes_spec(
    name: str,
    settings: config.DatabaseSettings,
    native: Callable[[runtime.Resources], contract.IChangeLog],
) -> runtime.ResourceSpec[contract.IChangeLog]:
    """The change log of a database: its own, or Debezium's topics."""
    changes = settings.changes
    if changes.kind == "native" or changes.kafka is None:
        return runtime.ResourceSpec(names.changes(name), native)
    cluster = names.kafka(changes.kafka)

    def debezium(resources: runtime.Resources) -> contract.IChangeLog:
        from infra import messaging
        from infra.messaging import kafka

        found = resources.get(cluster, kafka.KafkaCluster)
        return messaging.DebeziumChangeLog(
            found,
            prefix=changes.topic_prefix,
            topics=changes.topics,
            dead_letters=(
                found.dead_letters(changes.dead_letter_topic)
                if changes.dead_letter_topic
                else None
            ),
        )

    return runtime.ResourceSpec(
        names.changes(name),
        debezium,
        description=f"Debezium changes of {name}, on Kafka {changes.kafka}",
    )


def _kafka_spec(
    name: str, settings: config.KafkaSettings
) -> runtime.ResourceSpec[Any]:
    """A Kafka cluster: its topics."""

    def create(_: runtime.Resources) -> contract.IMessageLog:
        from infra.messaging import kafka

        return kafka.KafkaCluster(
            settings.librdkafka(),
            group_id=settings.group_id,
            timeout_seconds=settings.timeout_seconds,
        )

    def ping(cluster: Any) -> None:
        cluster.ping()

    return runtime.ResourceSpec(
        names.kafka(name),
        create,
        health=ping,
        description=f"Kafka cluster {name}",
    )


def _store_spec(
    name: str, settings: config.StoreSettings
) -> runtime.ResourceSpec[Any]:
    """A file or blob store."""

    def create(_: runtime.Resources) -> contract.IObjectStore:
        if settings.kind == "gcs" and settings.bucket is not None:
            from infra.storage import gcs

            return gcs.GcsObjectStore(settings.bucket, project=settings.project)
        if settings.kind == "local" and settings.path is not None:
            from infra import storage

            return storage.LocalObjectStore(settings.path)
        raise contract.PipelineConfigError(f"Store {name!r} is incomplete")

    def ping(store: contract.IObjectStore) -> None:
        if settings.kind == "local":
            # Listing would walk the whole tree: that it exists is enough.
            if settings.path is None or not settings.path.is_dir():
                raise FileNotFoundError(f"{settings.path} is not a directory")
            return
        next(iter(store.list()), None)

    return runtime.ResourceSpec(
        names.store(name),
        create,
        close=lambda store: store.close(),
        health=ping,
        description=f"{settings.kind} store {name}",
    )


def _bookkeeping_specs(
    settings: config.AppSettings,
) -> list[runtime.ResourceSpec[Any]]:
    """The ETL's records: in a database if named, else in files."""
    book = settings.bookkeeping
    if book.database is None:
        directory = book.directory

        def no_parking(_: runtime.Resources) -> object:
            raise contract.PipelineConfigError(
                "Parking records needs a database: set bookkeeping.database"
            )

        return [
            runtime.ResourceSpec(
                names.STATE,
                lambda _: core_state.JsonFileStateStore(directory / "state"),
            ),
            runtime.ResourceSpec(
                names.KEYS,
                lambda _: keys.CachedKeyRegistry(
                    keys.JsonLinesKeyRegistry(directory / "keys")
                ),
            ),
            runtime.ResourceSpec(
                names.QUARANTINE,
                lambda _: quality.JsonLinesQuarantine(directory / "quarantine"),
            ),
            runtime.ResourceSpec(
                names.ISSUES,
                lambda _: quality.JsonLinesIssueSink(directory / "issues"),
            ),
            runtime.ResourceSpec(names.PARKING, no_parking),
            runtime.ResourceSpec(
                names.LEDGER, lambda _: runtime.JsonLinesRunLedger(directory)
            ),
            runtime.ResourceSpec(
                names.LOCK, lambda _: runtime.FileRunLock(directory / "locks")
            ),
        ]

    database = settings.databases[book.database]
    if database.is_mongodb:
        return _mongodb_bookkeeping(names.database(book.database), book)
    return _relational_bookkeeping(names.database(book.database), book)


def _relational_bookkeeping(
    database: str, book: config.BookkeepingSettings
) -> list[runtime.ResourceSpec[Any]]:
    """The ETL's records in the `etl_*` tables of a SQL database."""
    from infra.database import relational

    options: dict[str, Any] = {
        "schema": book.schema_name,
        "prefix": book.prefix,
    }

    def tables(resources: runtime.Resources) -> relational.Database:
        found = resources.get(database, relational.Database)
        if book.create:
            relational.create_etl_tables(found, book.schema_name, book.prefix)
        else:
            check(found)
        return found

    def check(found: relational.Database) -> None:
        try:
            relational.check_etl_tables(found, book.schema_name, book.prefix)
        except contract.PipelineConfigError as exc:
            raise contract.PipelineConfigError(
                f"bookkeeping.database {book.database!r}: {exc}"
            ) from exc

    def db(resources: runtime.Resources) -> relational.Database:
        return resources.get(ETL_TABLES, relational.Database)

    return [
        runtime.ResourceSpec(
            ETL_TABLES,
            tables,
            health=check,
            description="the ETL tables, at the latest revision",
        ),
        runtime.ResourceSpec(
            names.STATE,
            lambda r: relational.RelationalStateStore(db(r), **options),
        ),
        runtime.ResourceSpec(
            names.KEYS,
            lambda r: keys.CachedKeyRegistry(
                relational.RelationalKeyRegistry(db(r), **options)
            ),
        ),
        runtime.ResourceSpec(
            names.QUARANTINE,
            lambda r: relational.RelationalQuarantine(db(r), **options),
        ),
        runtime.ResourceSpec(
            names.ISSUES,
            lambda r: relational.RelationalIssueSink(db(r), **options),
        ),
        runtime.ResourceSpec(
            names.PARKING,
            lambda r: relational.RelationalParkingLot(db(r), **options),
        ),
        runtime.ResourceSpec(
            names.LEDGER,
            lambda r: relational.RelationalRunLedger(db(r), **options),
        ),
        runtime.ResourceSpec(
            names.LOCK, lambda r: relational.RelationalRunLock(db(r), **options)
        ),
    ]


def _mongodb_bookkeeping(
    database: str, book: config.BookkeepingSettings
) -> list[runtime.ResourceSpec[Any]]:
    """The ETL's records in the `etl_*` collections of MongoDB."""
    from infra.database import mongodb

    def collections(resources: runtime.Resources) -> mongodb.MongoDatabase:
        found = resources.get(database, mongodb.MongoDatabase)
        if book.create:
            mongodb.create_etl_collections(found, book.prefix)
        return found

    def db(resources: runtime.Resources) -> mongodb.MongoDatabase:
        return resources.get(ETL_TABLES, mongodb.MongoDatabase)

    prefix = book.prefix
    return [
        runtime.ResourceSpec(
            ETL_TABLES,
            collections,
            description="the ETL collections, created once",
        ),
        runtime.ResourceSpec(
            names.STATE, lambda r: mongodb.MongoStateStore(db(r), prefix=prefix)
        ),
        runtime.ResourceSpec(
            names.KEYS,
            lambda r: keys.CachedKeyRegistry(
                mongodb.MongoKeyRegistry(db(r), prefix=prefix)
            ),
        ),
        runtime.ResourceSpec(
            names.QUARANTINE,
            lambda r: mongodb.MongoQuarantine(db(r), prefix=prefix),
        ),
        runtime.ResourceSpec(
            names.ISSUES, lambda r: mongodb.MongoIssueSink(db(r), prefix=prefix)
        ),
        runtime.ResourceSpec(
            names.PARKING,
            lambda r: mongodb.MongoParkingLot(db(r), prefix=prefix),
        ),
        runtime.ResourceSpec(
            names.LEDGER, lambda r: mongodb.MongoRunLedger(db(r), prefix=prefix)
        ),
        runtime.ResourceSpec(
            names.LOCK, lambda r: mongodb.MongoRunLock(db(r), prefix=prefix)
        ),
    ]


class _RelationalChangeLog(contract.IChangeLog):
    """The change log a SQL database has: Change Tracking, or logical
    replication."""

    def __init__(self, db: Any, settings: config.DatabaseSettings) -> None:
        self._db = db
        self._prefix = settings.replication_prefix

    def source(self, table: str) -> contract.IChangeSource:
        from infra.database import relational

        dialect = self._db.name
        if dialect == "mssql":
            return relational.SqlServerChangeTracking(self._db, table)
        if dialect == "postgresql":
            name = self._prefix + table.replace(".", "_").lower()
            return relational.PostgresLogicalReplication(
                self._db, table, slot=name, publication=name
            )
        raise contract.PipelineConfigError(
            f"No change log read for {dialect} databases: extract {table} by"
            " watermark, and find its deletes with MissingSourceRows (or feed"
            " Debezium changes through Kafka)"
        )


class _MongoChangeLog(contract.IChangeLog):
    """MongoDB change streams."""

    def __init__(self, db: Any) -> None:
        self._db = db

    def source(self, table: str) -> contract.IChangeSource:
        from infra.database import mongodb

        return mongodb.MongoChangeStream(self._db, table)
