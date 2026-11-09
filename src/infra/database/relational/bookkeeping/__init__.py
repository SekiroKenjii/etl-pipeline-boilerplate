"""The ETL's own records in a relational database: key crosswalk, state,
parked records, quarantine, quality issues, runs and their locks.

The tables are declared in `definitions`, as the latest revision of
`migrations`, which creates or upgrades them (`etl-db upgrade`, `etl-db
ddl`). JSON is stored as text. The records are shared by every host running
the pipelines.
"""

from infra.database.relational.bookkeeping.definitions import DEFAULT_PREFIX
from infra.database.relational.bookkeeping.definitions import etl_tables
from infra.database.relational.bookkeeping.definitions import EtlTables
from infra.database.relational.bookkeeping.definitions import (
    OPEN_ENTRY_INDEX_DIALECTS,
)
from infra.database.relational.bookkeeping.quarantine import RelationalIssueSink
from infra.database.relational.bookkeeping.quarantine import (
    RelationalQuarantine,
)
from infra.database.relational.bookkeeping.runs import RelationalRunLedger
from infra.database.relational.bookkeeping.runs import RelationalRunLock
from infra.database.relational.bookkeeping.stores import RelationalKeyRegistry
from infra.database.relational.bookkeeping.stores import RelationalParkingLot
from infra.database.relational.bookkeeping.stores import RelationalStateStore

__all__ = [
    "DEFAULT_PREFIX",
    "OPEN_ENTRY_INDEX_DIALECTS",
    "EtlTables",
    "RelationalIssueSink",
    "RelationalKeyRegistry",
    "RelationalParkingLot",
    "RelationalQuarantine",
    "RelationalRunLedger",
    "RelationalRunLock",
    "RelationalStateStore",
    "etl_tables",
]
