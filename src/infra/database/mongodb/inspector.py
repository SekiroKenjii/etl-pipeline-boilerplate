"""The schema of MongoDB collections, inferred from samples, and profiles."""

from __future__ import annotations

import collections
from collections.abc import Iterator, Sequence
import datetime
import decimal
from typing import cast
import uuid

from bson import objectid
from pymongo import collection as mongo_collection
from pymongo import database as mongo_database

from core import contract
from infra.database.mongodb import client
from infra.database.mongodb import documents

_MAX_FIELD_PATH_DEPTH = 3

_COUNT: documents.Document = {"$count": "n"}
_MATCH = "$match"


class MongoSchemaInspector(client.MongoAdapter, contract.ISchemaInspector):
    """Describes collections as tables, from a sample of their documents.

    A collection has no declared schema: its columns are the field paths
    found in `sample_size` random documents (`$sample`), nested documents
    included (`address.city`), arrays as one column. A column is nullable when
    some sampled documents lack it. Keys are `_id` and the unique indexes;
    MongoDB declares no foreign keys, so every relationship found is
    inferred. The schema is the database.

    Attributes:
        sample_size: The documents sampled per collection.
    """

    def __init__(
        self, db: client.MongoDatabase | str, *, sample_size: int = 1000
    ) -> None:
        """Initializes the inspector.

        Args:
            db: A shared database, or a MongoDB URI.
            sample_size: The documents sampled per collection.
        """
        super().__init__(db)
        self.sample_size = sample_size

    def tables(
        self, schema: str, pattern: str | None = None
    ) -> list[contract.TableInfo]:
        database = self.db.client.get_database(
            schema, codec_options=client.CODEC_OPTIONS
        )
        found = []
        with client.errors(f"reading the collections of {schema}"):
            for name in sorted(database.list_collection_names()):
                if name.startswith("system.") or not contract.matches(
                    name, pattern
                ):
                    continue
                found.append(self._table(database, schema, name))
        return found

    def foreign_keys(
        self, schema: str, pattern: str | None = None
    ) -> list[contract.ForeignKeyInfo]:
        del schema, pattern  # MongoDB declares none.
        return []

    def profile(
        self,
        table: str,
        columns: Sequence[str],
        sample_rows: int | None = None,
    ) -> dict[str, contract.ColumnProfile]:
        collection = self._collection(table)
        head: list[documents.Document] = (
            [{"$limit": sample_rows}] if sample_rows is not None else []
        )
        profiles = {}
        with client.errors(f"profiling {table}"):
            rows = _count(collection, [*head, _COUNT])
            for column in columns:
                path = f"${column}"
                non_null = _count(
                    collection,
                    [
                        *head,
                        _present(column),
                        _COUNT,
                    ],
                )
                distinct = _count(
                    collection,
                    [
                        *head,
                        _present(column),
                        {"$group": {"_id": path}},
                        _COUNT,
                    ],
                )
                profiles[column] = contract.ColumnProfile(
                    rows, non_null, distinct
                )
        return profiles

    def inclusion(
        self,
        table: str,
        column: str,
        referenced_table: str,
        referenced_column: str,
    ) -> contract.InclusionCheck:
        collection = self._collection(table)
        values: list[documents.Document] = [
            _present(column),
            {"$group": {"_id": f"${column}"}},
        ]
        with client.errors(
            f"checking {table}.{column} against"
            f" {referenced_table}.{referenced_column}"
        ):
            distinct = _count(collection, [*values, _COUNT])
            unmatched = _count(
                collection,
                [
                    *values,
                    {
                        "$lookup": {
                            "from": _bare(referenced_table),
                            "localField": "_id",
                            "foreignField": referenced_column,
                            "as": "found",
                            "pipeline": [
                                {"$limit": 1},
                                {"$project": {"_id": 1}},
                            ],
                        }
                    },
                    {_MATCH: {"found": {"$size": 0}}},
                    _COUNT,
                ],
            )
        return contract.InclusionCheck(distinct=distinct, unmatched=unmatched)

    def _table(
        self,
        database: mongo_database.Database[documents.Document],
        schema: str,
        name: str,
    ) -> contract.TableInfo:
        """Returns a collection as a table: sampled columns, unique indexes."""
        collection = database[name]
        sample = list(
            collection.aggregate([{"$sample": {"size": self.sample_size}}])
        )
        seen: dict[str, collections.Counter[str]] = {}
        for document in sample:
            for path, value in _paths(document):
                seen.setdefault(path, collections.Counter())[_type(value)] += 1

        columns = tuple(
            contract.ColumnInfo(
                name=path,
                data_type="|".join(sorted(types)),
                nullable=(sum(types.values()) < len(sample) or "null" in types),
                family=_family(types),
            )
            for path, types in seen.items()
        )
        unique = tuple(
            tuple(field for field, _ in index["key"])
            for index_name, index in collection.index_information().items()
            if index.get("unique") and index_name != "_id_"
        )
        return contract.TableInfo(
            name=f"{schema}.{name}",
            columns=columns,
            primary_key=("_id",),
            unique=unique,
            estimated_rows=collection.estimated_document_count(),
        )

    def _collection(
        self, table: str
    ) -> mongo_collection.Collection[documents.Document]:
        schema, _, name = table.partition(".")
        database = self.db.client.get_database(
            schema, codec_options=client.CODEC_OPTIONS
        )
        return database[name]


def _bare(table: str) -> str:
    return table.partition(".")[2] or table


def _count(
    collection: mongo_collection.Collection[documents.Document],
    pipeline: list[documents.Document],
) -> int:
    found = list(collection.aggregate(pipeline, allowDiskUse=True))
    return int(cast(documents.CountedDocument, found[0])["n"]) if found else 0


def _paths(
    document: documents.Document, prefix: str = "", depth: int = 1
) -> Iterator[tuple[str, object]]:
    """Yields the field paths of a document, nested documents included."""
    for key, value in document.items():
        path = f"{prefix}{key}"
        if isinstance(value, dict) and value and depth < _MAX_FIELD_PATH_DEPTH:
            yield from _paths(value, f"{path}.", depth + 1)
        else:
            yield path, value


# First match wins: a bool is an int too.
_BSON_TYPES: tuple[tuple[type, str], ...] = (
    (bool, "bool"),
    (float, "double"),
    (decimal.Decimal, "decimal"),
    (str, "string"),
    (objectid.ObjectId, "objectId"),
    (datetime.datetime, "date"),
    (uuid.UUID, "uuid"),
    (bytes, "binData"),
    (list, "array"),
    (dict, "object"),
)


def _type(value: object) -> str:
    """Names the BSON type of a value, as MongoDB's `$type` does."""
    if value is None:
        return "null"
    if isinstance(value, int) and not isinstance(value, bool):
        return "long" if abs(value) >= 2**31 else "int"
    for kind, name in _BSON_TYPES:
        if isinstance(value, kind):
            return name
    return type(value).__name__


_FAMILIES = {
    "bool": contract.TypeFamily.BOOLEAN,
    "int": contract.TypeFamily.INTEGER,
    "long": contract.TypeFamily.INTEGER,
    "double": contract.TypeFamily.FLOAT,
    "decimal": contract.TypeFamily.DECIMAL,
    "string": contract.TypeFamily.TEXT,
    "objectId": contract.TypeFamily.OBJECT_ID,
    "date": contract.TypeFamily.DATETIME,
    "uuid": contract.TypeFamily.UUID,
    "binData": contract.TypeFamily.BINARY,
    "object": contract.TypeFamily.JSON,
}


def _family(types: collections.Counter[str]) -> contract.TypeFamily:
    """Returns the family of a field, ignoring nulls; OTHER if it varies."""
    families = {
        _FAMILIES.get(name, contract.TypeFamily.OTHER)
        for name in types
        if name != "null"
    }
    if families == {contract.TypeFamily.INTEGER, contract.TypeFamily.DECIMAL}:
        return contract.TypeFamily.DECIMAL
    return families.pop() if len(families) == 1 else contract.TypeFamily.OTHER


def _present(column: str) -> documents.Document:
    """The stage keeping the documents where `column` is set."""
    return {_MATCH: {column: {"$ne": None}}}
