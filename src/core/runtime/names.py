"""The names of the resources the composition root provides.

resources.get(names.STATE, contract.IStateStore)
resources.get(names.reader("crm"), contract.ITableReader)
"""

from __future__ import annotations

STATE = "etl.state"
KEYS = "etl.keys"
QUARANTINE = "etl.quarantine"
ISSUES = "etl.issues"
PARKING = "etl.parking"
LEDGER = "etl.runs"
LOCK = "etl.locks"


def database(name: str) -> str:
    """The database `name` of the settings: a `Database`, a `MongoDatabase`."""
    return f"database.{name}"


def reader(name: str) -> str:
    """The `ITableReader` of database `name`."""
    return f"reader.{name}"


def writer(name: str) -> str:
    """The `ITableWriter` of database `name`."""
    return f"writer.{name}"


def inspector(name: str) -> str:
    """The `ISchemaInspector` of database `name`."""
    return f"inspector.{name}"


def changes(name: str) -> str:
    """The `IChangeLog` of database `name`: CDC, where it has one."""
    return f"changes.{name}"


def store(name: str) -> str:
    """The `IObjectStore` of store `name` of the settings."""
    return f"store.{name}"


def kafka(name: str) -> str:
    """The `IMessageLog` of Kafka cluster `name` of the settings."""
    return f"kafka.{name}"
