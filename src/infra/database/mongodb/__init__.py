"""MongoDB, through PyMongo.

    from infra.database import mongodb

    db = mongodb.MongoDatabase(os.environ["LEGACY_MONGODB_URI"])
    reader = mongodb.MongoTableReader(db)   # collections as tables
    writer = mongodb.MongoTableWriter(db)

Collections implement the table ports, so the table operators work on them.
Adapters given the same `MongoDatabase` share its client.
"""

from infra.database.mongodb.bookkeeping import create_etl_collections
from infra.database.mongodb.bookkeeping import DEFAULT_PREFIX
from infra.database.mongodb.bookkeeping import drop_etl_collections
from infra.database.mongodb.bookkeeping import MongoIssueSink
from infra.database.mongodb.bookkeeping import MongoKeyRegistry
from infra.database.mongodb.bookkeeping import MongoParkingLot
from infra.database.mongodb.bookkeeping import MongoQuarantine
from infra.database.mongodb.bookkeeping import MongoRunLedger
from infra.database.mongodb.bookkeeping import MongoRunLock
from infra.database.mongodb.bookkeeping import MongoStateStore
from infra.database.mongodb.changes import MongoChangeStream
from infra.database.mongodb.client import CODEC_OPTIONS
from infra.database.mongodb.client import MongoAdapter
from infra.database.mongodb.client import MongoDatabase
from infra.database.mongodb.inspector import MongoSchemaInspector
from infra.database.mongodb.tables import conditions
from infra.database.mongodb.tables import MongoTableReader
from infra.database.mongodb.tables import MongoTableWriter

__all__ = [
    "CODEC_OPTIONS",
    "DEFAULT_PREFIX",
    "MongoAdapter",
    "MongoChangeStream",
    "MongoDatabase",
    "MongoIssueSink",
    "MongoKeyRegistry",
    "MongoParkingLot",
    "MongoQuarantine",
    "MongoSchemaInspector",
    "MongoRunLedger",
    "MongoRunLock",
    "MongoStateStore",
    "MongoTableReader",
    "MongoTableWriter",
    "conditions",
    "create_etl_collections",
    "drop_etl_collections",
]
