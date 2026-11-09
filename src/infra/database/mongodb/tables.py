"""Reading and writing MongoDB collections as tables of documents."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
import threading

import pymongo
from pymongo import collection as mongo_collection
from pymongo import errors as mongo_errors

from core import contract
from infra.database import batches
from infra.database.mongodb import client
from infra.database.mongodb import documents
from shared import utils

_OPERATORS = {
    contract.Operator.EQ: "$eq",
    contract.Operator.NE: "$ne",
    contract.Operator.LT: "$lt",
    contract.Operator.LE: "$lte",
    contract.Operator.GT: "$gt",
    contract.Operator.GE: "$gte",
    contract.Operator.IN: "$in",
    contract.Operator.NOT_IN: "$nin",
}


def conditions(filters: Sequence[contract.Condition]) -> documents.Document:
    """Translates the conditions of the port into a MongoDB filter.

    Columns are field paths: `address.city`. IS_NULL matches a missing field
    too, as a document database has no other way to say "no value".
    """
    clauses: list[documents.Document] = []
    for condition in filters:
        if condition.operator is contract.Operator.IS_NULL:
            clauses.append({condition.column: None})
        elif condition.operator is contract.Operator.IS_NOT_NULL:
            clauses.append({condition.column: {"$ne": None}})
        else:
            value = condition.value
            if condition.operator in (
                contract.Operator.IN,
                contract.Operator.NOT_IN,
            ):
                value = list(condition.values)
            clauses.append(
                {condition.column: {_OPERATORS[condition.operator]: value}}
            )
    return {"$and": clauses} if clauses else {}


class MongoTableReader(client.MongoAdapter, contract.ITableReader):
    """Reads collections by keyset pagination, like tables.

    Each page is one query sorted by `order_by`, after the last document of the
    previous page: with an index `(watermark, _id)` every page is an index range
    scan. Nested documents stay nested; field paths (`address.city`) work in
    `order_by`, `columns` and filters. Name the `columns` for a stable shape: a
    field a document lacks is then None.

    Before the first read of a collection, by its order and filters, the reader
    checks that every document has the `order_by` fields, once per reader (a
    reader serves one run).

    Attributes:
        page_size: The documents fetched per query.
    """

    def __init__(
        self, db: client.MongoDatabase | str, *, page_size: int = 10_000
    ) -> None:
        """Initializes the reader.

        Args:
            db: A shared database, or a MongoDB URI.
            page_size: The documents fetched per query.

        Raises:
            ValueError: If `page_size` is below 1.
        """
        if page_size < 1:
            raise ValueError("page_size must be >= 1")
        super().__init__(db)
        self.page_size = page_size
        self._checked: set[tuple[str, tuple[str, ...], str]] = set()
        self._checked_lock = threading.Lock()

    def read(self, query: contract.TableQuery) -> Iterator[contract.Row]:
        """See `contract.ITableReader.read`."""
        collection = self.db.collection(query.table)
        base = conditions(query.filters)
        self._check_nulls(collection, query, base)

        window: list[documents.Document] = [base] if base else []
        first = query.order_by[0]
        if query.lower is not None:
            window.append({first: {"$gte": query.lower}})
        if query.upper is not None:
            window.append({first: {"$lte": query.upper}})
        projection: documents.Document | None = None
        if query.columns:
            projection = dict.fromkeys(query.columns, 1)
            if "_id" not in query.columns:
                projection["_id"] = 0
        sort = [(column, pymongo.ASCENDING) for column in query.order_by]

        after = query.after
        while True:
            clauses = list(window)
            if after is not None:
                clauses.append(_after(query.order_by[: len(after)], after))
            with client.errors(f"reading {query.table}"):
                page = list(
                    collection.find(
                        {"$and": clauses} if clauses else {},
                        projection,
                        sort=sort,
                        limit=self.page_size,
                    )
                )

            for document in page:
                yield _shaped(document, query.columns)
            if len(page) < self.page_size:
                return
            after = tuple(
                utils.get_path(page[-1], column) for column in query.order_by
            )

    def max_value(
        self,
        table: str,
        column: str,
        filters: Sequence[contract.Condition] = (),
    ) -> object:
        clauses = [conditions(filters), {column: {"$ne": None}}]
        with client.errors(f"reading the max of {table}.{column}"):
            found = self.db.collection(table).find_one(
                {"$and": [c for c in clauses if c]},
                {column: 1},
                sort=[(column, pymongo.DESCENDING)],
            )
        return None if found is None else utils.get_path(found, column)

    def count(
        self, table: str, filters: Sequence[contract.Condition] = ()
    ) -> int:
        with client.errors(f"counting the documents of {table}"):
            return int(
                self.db.collection(table).count_documents(conditions(filters))
            )

    def keys_at(
        self,
        table: str,
        order_by: Sequence[str],
        offsets: Sequence[int],
        filters: Sequence[contract.Condition] = (),
    ) -> list[tuple[object, ...]]:
        """See `contract.ITableReader.keys_at`.

        One query per offset, each after the document of the previous one,
        skipping those in between (`skip`): with an index on `order_by`, the
        server walks it once up to the last offset, a covered query.
        """
        skips = _skips(offsets)
        collection = self.db.collection(table)
        base = conditions(filters)
        self._check_nulls(
            collection,
            contract.TableQuery(
                table, order_by=tuple(order_by), filters=tuple(filters)
            ),
            base,
        )
        projection: documents.Document = dict.fromkeys(order_by, 1)
        if "_id" not in order_by:
            projection["_id"] = 0
        sort = [(column, pymongo.ASCENDING) for column in order_by]
        found: list[tuple[object, ...]] = []
        for skipped in skips:
            clauses = [base] if base else []
            if found:
                clauses.append(_after(order_by, found[-1]))
            with client.errors(f"reading the quantiles of {table}"):
                document = collection.find_one(
                    {"$and": clauses} if clauses else {},
                    projection,
                    sort=sort,
                    skip=skipped,
                )
            if document is None:
                break
            found.append(
                tuple(utils.get_path(document, column) for column in order_by)
            )
        return found

    def _check_nulls(
        self,
        collection: mongo_collection.Collection[documents.Document],
        query: contract.TableQuery,
        base: documents.Document,
    ) -> None:
        """Fails if a document lacks an `order_by` field, or has it null.

        A collection is checked once per reader for an order and filters,
        since the check may scan it: the composition root gives each run its
        own reader.

        Raises:
            FatalError: If one does: keyset conditions would skip it.
        """
        checked = (query.table, tuple(query.order_by), repr(query.filters))
        with self._checked_lock:
            if checked in self._checked:
                return
        missing: documents.Document = {
            "$or": [{column: None} for column in query.order_by]
        }
        with client.errors(f"reading {query.table}"):
            found = collection.find_one(
                {"$and": [base, missing]} if base else missing, {"_id": 1}
            )
        if found is not None:
            raise contract.FatalError(
                f"{query.table} has documents without {list(query.order_by)}:"
                " they cannot be read by keyset, and would be skipped. Filter"
                " them out, or order by fields every document has."
            )
        with self._checked_lock:
            self._checked.add(checked)


def _skips(offsets: Sequence[int]) -> list[int]:
    """Returns the documents between each offset and the one before it.

    Raises:
        ValueError: If the offsets are not ascending, or below 0.
    """
    found = []
    previous = -1
    for offset in offsets:
        if offset <= previous:
            raise ValueError(f"offsets must be ascending, from 0: {offsets}")
        found.append(offset - previous - 1)
        previous = offset
    return found


def _after(
    columns: Sequence[str], values: Sequence[object]
) -> documents.Document:
    """Returns `(columns) > (values)`: `c1 >= v1` and one of `c1 > v1`,
    `c1 = v1 and c2 > v2`..."""
    alternatives: list[documents.Document] = []
    for i, column in enumerate(columns):
        clause: documents.Document = dict(zip(columns[:i], values[:i]))
        clause[column] = {"$gt": values[i]}
        alternatives.append(clause)
    return {columns[0]: {"$gte": values[0]}, "$or": alternatives}


def _shaped(
    document: documents.Document, columns: Sequence[str]
) -> contract.Row:
    """Returns a document with every requested top-level field, None if
    missing."""
    for column in columns:
        root = column.split(".")[0]
        document.setdefault(root, None)
    return document


class MongoTableWriter(client.MongoAdapter, contract.ITableWriter):
    """Writes batches of rows into collections, in one bulk write.

    - UPSERT: `$set` of the update columns, `$setOnInsert` of the others;
    - INSERT_MISSING: `$setOnInsert` only: existing documents are untouched;
    - UPDATE: `$set` on every document matching the key, no insert;
    - INSERT: plain inserts; a duplicate key fails the batch.

    Columns may be field paths (`address.city`): flat rows become nested
    documents. The key fields need a unique index (`_id` has one), or racing
    writers duplicate documents. On a replica set or a sharded cluster each
    batch is one transaction; on a standalone server a failed batch may be half
    written, and writing it again (upserts) completes it.

    Attributes:
        transactions: Whether to use transactions where the server can.
    """

    def __init__(
        self, db: client.MongoDatabase | str, *, transactions: bool = True
    ) -> None:
        """Initializes the writer.

        Args:
            db: A shared database, or a MongoDB URI.
            transactions: Whether to write each batch in a transaction where
                the server can.
        """
        super().__init__(db)
        self.transactions = transactions

    def write(
        self, target: contract.TableTarget, rows: Sequence[contract.Row]
    ) -> contract.WriteResult:
        """See `contract.ITableWriter.write`."""
        if not rows:
            return contract.WriteResult()
        columns = batches.columns(target, rows)
        unique = batches.last_by_key(target, rows)
        updates = (
            list(target.update_columns)
            if target.update_columns is not None
            else [c for c in columns if c not in target.key]
        )
        operations = [_operation(target, row, updates) for row in unique]
        action = f"writing {len(rows)} row(s) to {target.table}"

        def attempt() -> contract.WriteResult:
            with self.db.transaction(action, self.transactions) as session:
                result = self.db.collection(target.table).bulk_write(
                    operations, ordered=True, session=session
                )
            if target.mode is contract.WriteMode.INSERT:
                created, updated = result.inserted_count, 0
            else:
                created, updated = result.upserted_count, result.modified_count
            return contract.WriteResult(
                created=created,
                updated=updated,
                # An UPDATE by a non-unique key may change more than given.
                unchanged=max(0, len(rows) - created - updated),
            )

        return utils.retry(attempt, when=lambda exc: _racing(target, exc))

    def delete(self, table: str, rows: Sequence[contract.Row]) -> int:
        """See `contract.ITableWriter.delete`; one bulk of `DeleteMany`."""
        if not rows:
            return 0
        batches.match_columns(table, rows)
        operations = [pymongo.DeleteMany(dict(row)) for row in rows]
        action = f"deleting {len(rows)} row(s) of {table}"
        with self.db.transaction(action, self.transactions) as session:
            result = self.db.collection(table).bulk_write(
                operations, ordered=True, session=session
            )
        return int(result.deleted_count)


def _operation(
    target: contract.TableTarget, row: contract.Row, updates: Sequence[str]
) -> (
    pymongo.InsertOne[documents.Document]
    | pymongo.UpdateMany
    | pymongo.UpdateOne
):
    """Returns the bulk operation writing one row, in the target's mode."""
    if target.mode is contract.WriteMode.INSERT:
        return pymongo.InsertOne(_nested(row))
    key = {name: row[name] for name in target.key}
    if target.mode is contract.WriteMode.UPDATE:
        changed = {name: row[name] for name in updates}
        return pymongo.UpdateMany(key, {"$set": changed})
    others = {
        name: value for name, value in row.items() if name not in target.key
    }
    changes: documents.Document = {}
    if target.mode is contract.WriteMode.UPSERT:
        changed = {name: others.pop(name) for name in updates if name in others}
        if changed:
            changes["$set"] = changed
    if others:
        changes["$setOnInsert"] = others
    if not changes:
        # Only key fields: the filter alone makes the document.
        changes["$setOnInsert"] = {}
    return pymongo.UpdateOne(key, changes, upsert=True)


def _nested(row: contract.Row) -> documents.Document:
    """Turns field paths (`address.city`) into nested documents."""
    document: documents.Document = {}
    for path, value in row.items():
        parts = path.split(".")
        current = document
        for part in parts[:-1]:
            found = current.setdefault(part, {})
            if not isinstance(found, dict):
                raise TypeError(f"{path}: {part!r} holds a value, not fields")
            current = found
        current[parts[-1]] = value
    return document


def _racing(target: contract.TableTarget, exc: BaseException) -> bool:
    """Whether a batch lost a race to another writer, and can try again."""
    cause = exc.__cause__
    if (
        target.mode is not contract.WriteMode.INSERT
        and client.is_duplicate_key(cause)
    ):
        return True
    return isinstance(
        cause, mongo_errors.PyMongoError
    ) and cause.has_error_label("TransientTransactionError")
