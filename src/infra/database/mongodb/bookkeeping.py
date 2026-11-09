"""The ETL's own records in MongoDB: key crosswalk, state, parked records,
quarantine, quality issues, runs and their locks.

One collection each, `etl_<name>` (`DEFAULT_PREFIX`), for the teams whose
database is MongoDB. Upserts on `_id` make them safe for concurrent steps and
hosts; `create_etl_collections` adds the secondary indexes.
"""

from __future__ import annotations

from collections.abc import Callable, Collection, Iterator, Mapping, Sequence
import dataclasses
import datetime
import itertools
import json
import re
import time
from typing import cast

from bson import objectid
import pymongo
from pymongo import collection as mongo_collection
from pymongo import errors as mongo_errors

from core import contract
from infra.database.mongodb import client
from infra.database.mongodb import documents as mongo_documents
from shared import utils

DEFAULT_PREFIX = "etl_"
# Ids per `$in` where nothing else bounds them (`tuning.in_chunk`).
DEFAULT_IN_CHUNK = 1000
_PAGE_SIZE = 1000

_ENTITY = "_id.entity"
_OPENED = contract.QuarantineStatus.OPEN.value
_SET_ON_INSERT = "$setOnInsert"
_READING_RUNS = "reading the runs"


def create_etl_collections(
    db: client.MongoDatabase, prefix: str = DEFAULT_PREFIX
) -> None:
    """Creates the secondary indexes of the ETL collections; idempotent.

    Raises:
        RetryableError: If the server cannot be reached.
        FatalError: If an index cannot be created.
    """
    with client.errors("creating the ETL collections"):
        db.collection(f"{prefix}key_registry").create_index(
            [(_ENTITY, 1), ("natural_key", 1)]
        )
        for name in ("quarantine", "quality_issues"):
            db.collection(f"{prefix}{name}").create_index(
                [("pipeline", 1), ("run_id", 1)]
            )
        db.collection(f"{prefix}quarantine").create_index(
            [("status", 1), ("pipeline", 1)]
        )
        _one_open_entry_per_record(db.collection(f"{prefix}quarantine"))
        db.collection(f"{prefix}quality_issues").create_index(
            [("record_id", 1)]
        )
        db.collection(f"{prefix}quality_issues").create_index([("found_at", 1)])
        db.collection(f"{prefix}runs").create_index(
            [("pipeline", 1), ("started_at", -1)]
        )
        db.collection(f"{prefix}run_steps").create_index([("run_id", 1)])


_OPEN_ENTRY_INDEX = "one_open_entry_per_record"
# The index of `record_key` of release 0.1.0, not unique.
_RECORD_KEY_INDEX = "pipeline_1_record_key_1"
_INDEX_NOT_FOUND = 27


def _one_open_entry_per_record(
    quarantine: mongo_collection.Collection[mongo_documents.Document],
) -> None:
    """Makes the open entries of a record unique, once.

    Of the open entries of a record an older release left, the latest keeps
    the key; the others, still open, get one of their own, `legacy-<id>`.
    Nothing is deleted. The unique index replaces the index of older
    releases.

    Raises:
        FatalError: If writers of an older release keep adding open entries
            of a record while the index is created.
    """
    indexes = quarantine.index_information()
    if _OPEN_ENTRY_INDEX not in indexes:

        def attempt() -> None:
            with client.errors("making the open quarantine entries unique"):
                _rekey_older_open_entries(quarantine)
                quarantine.create_index(
                    [("pipeline", 1), ("record_key", 1)],
                    name=_OPEN_ENTRY_INDEX,
                    unique=True,
                    partialFilterExpression={"status": _OPENED},
                )

        try:
            utils.retry(
                attempt,
                when=lambda exc: client.is_duplicate_key(exc.__cause__),
                attempts=3,
            )
        except contract.FatalError as exc:
            if not client.is_duplicate_key(exc.__cause__):
                raise
            raise contract.FatalError(
                f"{quarantine.name}: records keep getting a second open entry"
                " while their open entries are made unique. Stop the ETL"
                " processes of release 0.1.0, then run `etl-db upgrade`"
                " again."
            ) from exc
    if _RECORD_KEY_INDEX in indexes:
        with client.errors("dropping the index of 0.1.0 of the quarantine"):
            try:
                quarantine.drop_index(_RECORD_KEY_INDEX)
            except mongo_errors.OperationFailure as exc:
                # Another process upgrading at once dropped it first.
                if exc.code != _INDEX_NOT_FOUND:
                    raise


def _rekey_older_open_entries(
    quarantine: mongo_collection.Collection[mongo_documents.Document],
) -> None:
    """Gives every open entry but the latest of a record a key of its own."""
    doubled = quarantine.aggregate(
        [
            {"$match": {"status": _OPENED}},
            {"$sort": {"rejected_at": 1, "_id": 1}},
            {
                "$group": {
                    "_id": {"p": "$pipeline", "k": "$record_key"},
                    "ids": {"$push": "$_id"},
                }
            },
            {"$match": {"ids.1": {"$exists": True}}},
        ],
        allowDiskUse=True,
    )
    older = [
        id_
        for group in doubled
        for id_ in cast(mongo_documents.DuplicatesDocument, group)["ids"][:-1]
    ]
    for chunk in itertools.batched(older, DEFAULT_IN_CHUNK):
        quarantine.bulk_write(
            [
                pymongo.UpdateOne(
                    {"_id": id_, "status": _OPENED},
                    {"$set": {"record_key": f"legacy-{id_}"}},
                )
                for id_ in chunk
            ],
            ordered=False,
        )


def drop_etl_collections(
    db: client.MongoDatabase, prefix: str = DEFAULT_PREFIX
) -> None:
    """Drops the ETL collections, for tests."""
    with client.errors("dropping the ETL collections"):
        for name in (
            "key_registry",
            "state",
            "parked",
            "quarantine",
            "quality_issues",
            "runs",
            "run_steps",
            "locks",
        ):
            db.collection(f"{prefix}{name}").drop()


def _jsonable(value: object) -> contract.StateValue:
    """Turns any value into BSON-friendly data, through `to_jsonable`."""
    loaded: contract.StateValue = json.loads(
        json.dumps(value, default=utils.to_jsonable)
    )
    return loaded


def _jsonable_fields(
    fields: Mapping[str, object],
) -> dict[str, contract.StateValue]:
    """`_jsonable`, of fields: a dict."""
    loaded: dict[str, contract.StateValue] = json.loads(
        json.dumps(fields, default=utils.to_jsonable)
    )
    return loaded


class _Bookkeeper(client.MongoAdapter):
    """An adapter writing to one ETL collection, `<prefix><suffix>`."""

    suffix: str

    def __init__(
        self, db: client.MongoDatabase | str, *, prefix: str = DEFAULT_PREFIX
    ) -> None:
        """Initializes the adapter.

        Args:
            db: A shared database, or a MongoDB URI.
            prefix: The prefix of the collection names.
        """
        super().__init__(db)
        self.collection_name = f"{prefix}{self.suffix}"

    @property
    def collection(
        self,
    ) -> mongo_collection.Collection[mongo_documents.Document]:
        return self.db.collection(self.collection_name)

    def _upsert(
        self, action: str, operations: Sequence[pymongo.UpdateOne]
    ) -> None:
        """Runs upserts, again when a concurrent writer inserted first."""

        def attempt() -> None:
            with client.errors(action):
                self.collection.bulk_write(operations, ordered=False)

        utils.retry(
            attempt, when=lambda exc: client.is_duplicate_key(exc.__cause__)
        )


class MongoKeyRegistry(_Bookkeeper, contract.IKeyRegistry):
    """The crosswalk of loaded keys, in `etl_key_registry`.

    Put a `keys.TieredKeyCache` in front of it to save round trips.

    Attributes:
        in_chunk: The keys looked up per `$in` query.
    """

    suffix = "key_registry"

    def __init__(
        self,
        db: client.MongoDatabase | str,
        *,
        prefix: str = DEFAULT_PREFIX,
        in_chunk: int | None = None,
    ) -> None:
        """Initializes the registry.

        Args:
            db: A shared database, or a MongoDB URI.
            prefix: The prefix of the collection names.
            in_chunk: The keys looked up per `$in` query; 1,000 if None.

        Raises:
            ValueError: If `in_chunk` is below 1.
        """
        if in_chunk is not None and in_chunk < 1:
            raise ValueError("in_chunk must be >= 1")
        super().__init__(db, prefix=prefix)
        self.in_chunk = in_chunk or DEFAULT_IN_CHUNK

    def register(self, entries: Sequence[contract.KeyEntry]) -> None:
        unique = {(e.entity, e.surrogate_key): e for e in entries}
        if not unique:
            return
        now = utils.utc_now()
        self._upsert(
            f"registering {len(unique)} key(s)",
            [
                pymongo.UpdateOne(
                    {"_id": _key_id(entry.entity, entry.surrogate_key)},
                    {
                        _SET_ON_INSERT: {
                            "natural_key": list(entry.natural_key),
                            "registered_at": now,
                        }
                    },
                    upsert=True,
                )
                for entry in unique.values()
            ],
        )

    def known(self, entity: str, surrogate_keys: Collection[str]) -> set[str]:
        keys = list(surrogate_keys)
        found: set[str] = set()
        with client.errors(f"looking up {entity} keys"):
            for start in range(0, len(keys), self.in_chunk):
                ids = [
                    _key_id(entity, k)
                    for k in keys[start : start + self.in_chunk]
                ]
                found.update(
                    cast(mongo_documents.KeyDocument, document)["_id"]["key"]
                    for document in self.collection.find(
                        {"_id": {"$in": ids}}, {"_id": 1}
                    )
                )
        return found

    def entries(self, entity: str) -> Iterator[contract.KeyEntry]:
        after: str | None = None
        while True:
            query: mongo_documents.Document = {_ENTITY: entity}
            if after is not None:
                query["_id.key"] = {"$gt": after}
            with client.errors(f"reading the {entity} keys"):
                page = list(
                    self.collection.find(
                        query,
                        sort=[("_id.key", pymongo.ASCENDING)],
                        limit=_PAGE_SIZE,
                    )
                )
            keys = [
                cast(mongo_documents.KeyDocument, document) for document in page
            ]
            for document in keys:
                yield contract.KeyEntry(
                    entity,
                    document["_id"]["key"],
                    tuple(document["natural_key"]),
                )
            if len(keys) < _PAGE_SIZE:
                return
            after = keys[-1]["_id"]["key"]

    def natural_key(
        self, entity: str, surrogate_key: str
    ) -> tuple[str, ...] | None:
        with client.errors(f"looking up {entity} {surrogate_key}"):
            found = self.collection.find_one(
                {"_id": _key_id(entity, surrogate_key)}
            )
        return (
            None
            if found is None
            else tuple(cast(mongo_documents.KeyDocument, found)["natural_key"])
        )

    def forget(self, entity: str, natural_key: Sequence[str]) -> list[str]:
        query = {_ENTITY: entity, "natural_key": list(natural_key)}
        with client.errors(f"forgetting a key of {entity}"):
            gone = [
                cast(mongo_documents.KeyDocument, d)["_id"]["key"]
                for d in self.collection.find(query, {"_id": 1})
            ]
            self.collection.delete_many(query)
        return gone


def _key_id(entity: str, key: str) -> dict[str, str]:
    # Field order matters in an `_id` document: always built here.
    return {"entity": entity, "key": key}


class MongoStateStore(_Bookkeeper, contract.IStateStore):
    """The state kept between runs, in `etl_state`."""

    suffix = "state"

    def get(self, namespace: str, key: str) -> contract.StateValue:
        with client.errors(f"reading the state {namespace}/{key}"):
            found = self.collection.find_one({"_id": _state_id(namespace, key)})
        return (
            None
            if found is None
            else cast(mongo_documents.StateDocument, found)["value"]
        )

    def set(self, namespace: str, key: str, value: contract.StateValue) -> None:
        with client.errors(f"saving the state {namespace}/{key}"):
            self.collection.replace_one(
                {"_id": _state_id(namespace, key)},
                {"value": value, "updated_at": utils.utc_now()},
                upsert=True,
            )

    def delete(self, namespace: str, key: str) -> None:
        with client.errors(f"deleting the state {namespace}/{key}"):
            self.collection.delete_one({"_id": _state_id(namespace, key)})


def _state_id(namespace: str, key: str) -> dict[str, str]:
    return {"namespace": namespace, "key": key}


class MongoParkingLot(_Bookkeeper, contract.IParkingLot):
    """Parked records, in `etl_parked`; `attempts` counts atomically."""

    suffix = "parked"

    def park(self, queue: str, entries: Sequence[contract.ParkedEntry]) -> None:
        latest = {entry.identity: entry for entry in entries}
        if not latest:
            return
        now = utils.utc_now()
        self._upsert(
            f"parking records in {queue}",
            [
                pymongo.UpdateOne(
                    {"_id": _parked_id(queue, entry.identity)},
                    {
                        "$set": {
                            "payload": entry.payload,
                            "reason": entry.reason,
                            "last_parked_at": now,
                        },
                        "$inc": {"attempts": 1},
                        _SET_ON_INSERT: {"parked_at": now},
                    },
                    upsert=True,
                )
                for entry in latest.values()
            ],
        )

    def entries(self, queue: str) -> Iterator[contract.ParkedEntry]:
        after: str | None = None
        while True:
            query: mongo_documents.Document = {"_id.queue": queue}
            if after is not None:
                query["_id.identity"] = {"$gt": after}
            with client.errors(f"reading the parked {queue}"):
                page = list(
                    self.collection.find(
                        query,
                        sort=[("_id.identity", pymongo.ASCENDING)],
                        limit=_PAGE_SIZE,
                    )
                )
            parked = [
                cast(mongo_documents.ParkedDocument, document)
                for document in page
            ]
            for document in parked:
                yield contract.ParkedEntry(
                    document["_id"]["identity"],
                    document["payload"],
                    document["reason"],
                    document["attempts"],
                    document["parked_at"],
                )
            if len(parked) < _PAGE_SIZE:
                return
            after = parked[-1]["_id"]["identity"]

    def release(
        self, queue: str, entries: Sequence[contract.ParkedEntry]
    ) -> None:
        if not entries:
            return
        with client.errors(f"releasing parked records of {queue}"):
            self.collection.bulk_write(
                [
                    pymongo.DeleteOne({
                        "_id": _parked_id(queue, entry.identity),
                        "attempts": entry.attempts,
                    })
                    for entry in entries
                ],
                ordered=False,
            )


def _parked_id(queue: str, identity: str) -> dict[str, str]:
    return {"queue": queue, "identity": identity}


class MongoQuarantine(_Bookkeeper, contract.IQuarantine):
    """Rejected records, in `etl_quarantine`, with the issues that rejected
    them."""

    suffix = "quarantine"

    def __init__(
        self,
        db: client.MongoDatabase | str,
        *,
        prefix: str = DEFAULT_PREFIX,
        serialize: Callable[[object], object] | None = None,
    ) -> None:
        """Initializes the quarantine.

        Args:
            db: A shared database, or a MongoDB URI.
            prefix: The prefix of the collection names.
            serialize: Turns a record into JSON-friendly data, e.g. to hide
                sensitive fields; `utils.to_jsonable` by default.
        """
        super().__init__(db, prefix=prefix)
        self._serialize = serialize

    def put(
        self,
        records: Sequence[contract.RejectedRecord[object]],
        ctx: contract.ETLContext,
    ) -> None:
        """See `contract.IQuarantine.put`: a record rejected again updates
        its open entry, keeping its id.

        Safe for steps rejecting the same record at once once the open
        entries are unique (`create_etl_collections`): a writer whose upsert
        collides with another's tries again, then updates its entry.
        """
        if not records:
            return
        pipeline = ctx.run.pipeline_name
        now = utils.utc_now()
        documents: dict[str, dict[str, object]] = {}
        for rejected in records:
            record = _jsonable(
                self._serialize(rejected.record)
                if self._serialize
                else rejected.record
            )
            key = contract.QuarantineEntry.key_of(
                pipeline, rejected.checkpoint.value, record
            )
            # The last one, if twice in the batch.
            documents[key] = {
                "run_id": ctx.run.run_id,
                "checkpoint": rejected.checkpoint.value,
                "record": record,
                "issues": _jsonable(
                    [issue.as_dict() for issue in rejected.issues]
                ),
                "rejected_at": now,
            }
        self._upsert(
            f"quarantining {len(documents)} record(s)",
            [
                pymongo.UpdateOne(
                    {
                        "pipeline": pipeline,
                        "record_key": key,
                        "status": _OPENED,
                    },
                    {"$set": document},
                    upsert=True,
                )
                for key, document in documents.items()
            ],
        )

    def entries(
        self,
        pipeline: str | None = None,
        status: contract.QuarantineStatus | None = (
            contract.QuarantineStatus.OPEN
        ),
    ) -> Iterator[contract.QuarantineEntry]:
        query: mongo_documents.Document = {}
        if pipeline is not None:
            query["pipeline"] = pipeline
        if status is not None:
            query["status"] = status.value
        with client.errors("reading the quarantine"):
            for found in self.collection.find(query).sort("_id", 1):
                document = cast(mongo_documents.QuarantineDocument, found)
                yield contract.QuarantineEntry(
                    id=str(document["_id"]),
                    pipeline=document["pipeline"],
                    run_id=document["run_id"],
                    checkpoint=document["checkpoint"],
                    record=document["record"],
                    issues=tuple(document["issues"]),
                    rejected_at=document["rejected_at"],
                    status=contract.QuarantineStatus(document["status"]),
                    resolved_at=document.get("resolved_at"),
                )

    def resolve(
        self, ids: Collection[str], status: contract.QuarantineStatus
    ) -> int:
        keys = [objectid.ObjectId(id_) for id_ in dict.fromkeys(ids)]
        if not keys:
            return 0
        with client.errors(f"resolving {len(keys)} record(s)"):
            result = self.collection.update_many(
                {"_id": {"$in": keys}, "status": {"$ne": status.value}},
                {
                    "$set": {
                        "status": status.value,
                        "resolved_at": utils.utc_now(),
                    }
                },
            )
        return int(result.modified_count)

    def erase(self, ids: Collection[str]) -> int:
        keys = [objectid.ObjectId(id_) for id_ in dict.fromkeys(ids)]
        if not keys:
            return 0
        with client.errors(f"erasing {len(keys)} record(s)"):
            result = self.collection.delete_many({"_id": {"$in": keys}})
        return int(result.deleted_count)

    def purge(self, before: datetime.datetime) -> int:
        with client.errors("purging the quarantine"):
            result = self.collection.delete_many(
                {"rejected_at": {"$lt": before}}
            )
        return int(result.deleted_count)


class MongoIssueSink(_Bookkeeper, contract.IIssueSink):
    """Every quality issue, in `etl_quality_issues`."""

    suffix = "quality_issues"

    def write(
        self, issues: Sequence[contract.QualityIssue], ctx: contract.ETLContext
    ) -> None:
        if not issues:
            return
        now = utils.utc_now()
        with client.errors(f"writing {len(issues)} quality issue(s)"):
            self.collection.insert_many([
                {
                    "pipeline": ctx.run.pipeline_name,
                    "run_id": ctx.run.run_id,
                    **_jsonable_fields(issue.as_dict()),
                    "found_at": now,
                }
                for issue in issues
            ])

    def erase(self, record_ids: Collection[str]) -> int:
        wanted = sorted(set(record_ids))
        if not wanted:
            return 0
        with client.errors(f"erasing the issues of {len(wanted)} record(s)"):
            result = self.collection.delete_many({"record_id": {"$in": wanted}})
        return int(result.deleted_count)

    def purge(self, before: datetime.datetime) -> int:
        with client.errors("purging the quality issues"):
            result = self.collection.delete_many({"found_at": {"$lt": before}})
        return int(result.deleted_count)


class MongoRunLedger(_Bookkeeper, contract.IRunLedger):
    """The history of the runs, in `etl_runs`, and of their steps, in
    `etl_run_steps`."""

    suffix = "runs"

    def __init__(
        self, db: client.MongoDatabase | str, *, prefix: str = DEFAULT_PREFIX
    ) -> None:
        super().__init__(db, prefix=prefix)
        self.steps_name = f"{prefix}run_steps"

    def start(
        self,
        run: contract.ETLRunInfo,
        host: str,
        meta: Mapping[str, str] | None = None,
    ) -> None:
        with client.errors(f"recording the run {run.run_id}"):
            # Idempotent: a start retried after a lost reply changes nothing.
            self.collection.update_one(
                {"_id": run.run_id},
                {
                    _SET_ON_INSERT: {
                        "pipeline": run.pipeline_name,
                        "status": contract.ETLStatus.IN_PROGRESS.value,
                        "started_at": run.triggered_at,
                        "host": host,
                        "dry_run": run.dry_run,
                        "meta": dict(meta or {}),
                    }
                },
                upsert=True,
            )

    def record_step(
        self, run_id: str, step: str, result: contract.ETLResult, host: str
    ) -> None:
        record = contract.StepRecord.of(step, result, host)
        with client.errors(f"recording the step {step}"):
            self.db.collection(self.steps_name).replace_one(
                {"_id": f"{run_id}/{step}"},
                {
                    "run_id": run_id,
                    "step": step,
                    "status": record.status.value,
                    "completed_at": record.completed_at,
                    "stats": record.stats,
                    "message": record.message,
                    "host": record.host,
                    "attempts": record.attempts,
                },
                upsert=True,
            )

    def finish(self, result: contract.PipelineResult) -> None:
        with client.errors(f"recording the end of {result.run_id}"):
            self.collection.update_one(
                {"_id": result.run_id},
                {
                    "$set": {
                        "status": result.status.value,
                        "completed_at": result.completed_at,
                        "stats": result.stats.as_dict(),
                        "message": result.message,
                    }
                },
            )

    def recent(
        self, pipeline_name: str | None, limit: int = 20
    ) -> list[contract.RunRecord]:
        query = {} if pipeline_name is None else {"pipeline": pipeline_name}
        with client.errors(_READING_RUNS):
            documents = list(
                self.collection.find(
                    query,
                    sort=[("started_at", pymongo.DESCENDING)],
                    limit=limit,
                )
            )
        return [_run_record(document) for document in documents]

    def get(self, run_id: str) -> contract.RunRecord | None:
        with client.errors(f"reading the run {run_id}"):
            document = self.collection.find_one({"_id": run_id})
            if document is None:
                return None
            steps = list(
                self.db.collection(self.steps_name).find({"run_id": run_id})
            )
        return dataclasses.replace(
            _run_record(document),
            steps={
                step["step"]: contract.StepRecord(
                    step=step["step"],
                    status=contract.ETLStatus(step["status"]),
                    completed_at=step["completed_at"],
                    stats=dict(step.get("stats") or {}),
                    message=step.get("message"),
                    host=step.get("host", ""),
                    attempts=int(step.get("attempts", 1)),
                )
                for step in (
                    cast(mongo_documents.StepDocument, found) for found in steps
                )
            },
        )

    def latest(
        self,
        pipeline_prefix: str,
        statuses: Collection[contract.ETLStatus],
        meta_key: str,
    ) -> contract.RunRecord | None:
        query = {
            "pipeline": {"$regex": f"^{re.escape(pipeline_prefix)}"},
            "status": {"$in": [status.value for status in statuses]},
            "dry_run": {"$ne": True},
            f"meta.{meta_key}": {"$exists": True},
        }
        with client.errors(_READING_RUNS):
            document = self.collection.find_one(
                query, sort=[("started_at", pymongo.DESCENDING)]
            )
        return None if document is None else _run_record(document)

    def last_run(
        self, pipeline_name: str, label: str | None = None
    ) -> contract.RunRecord | None:
        query: dict[str, object] = {
            "pipeline": pipeline_name,
            "dry_run": {"$ne": True},
        }
        if label is not None:
            query["meta.label"] = label
        with client.errors(_READING_RUNS):
            document = self.collection.find_one(
                query, sort=[("started_at", pymongo.DESCENDING)]
            )
        return None if document is None else _run_record(document)


def _run_record(found: mongo_documents.Document) -> contract.RunRecord:
    document = cast(mongo_documents.RunDocument, found)
    return contract.RunRecord(
        run_id=document["_id"],
        pipeline=document["pipeline"],
        status=contract.ETLStatus(document["status"]),
        started_at=document["started_at"],
        completed_at=document.get("completed_at"),
        stats=dict(document.get("stats") or {}),
        message=document.get("message"),
        host=document.get("host", ""),
        dry_run=bool(document.get("dry_run", False)),
        meta=dict(document.get("meta") or {}),
    )


class MongoRunLock(_Bookkeeper, contract.IRunLock):
    """The locks of the steps running, in `etl_locks`: leases, shared by
    every host."""

    suffix = "locks"

    def acquire(self, name: str, owner: str, lease_seconds: float) -> bool:
        now = time.time()
        with client.errors(f"taking the lock {name}"):
            taken = self.collection.update_one(
                {
                    "_id": name,
                    "$or": [{"expires_at": {"$lt": now}}, {"owner": owner}],
                },
                {"$set": {"owner": owner, "expires_at": now + lease_seconds}},
            ).matched_count
            if taken:
                return True
            try:
                self.collection.insert_one({
                    "_id": name,
                    "owner": owner,
                    "expires_at": now + lease_seconds,
                })
            except mongo_errors.DuplicateKeyError:
                return False  # Held by another owner.
        return True

    def renew(self, name: str, owner: str, lease_seconds: float) -> bool:
        with client.errors(f"renewing the lock {name}"):
            return bool(
                self.collection.update_one(
                    {"_id": name, "owner": owner},
                    {"$set": {"expires_at": time.time() + lease_seconds}},
                ).matched_count
            )

    def release(self, name: str, owner: str) -> None:
        with client.errors(f"releasing the lock {name}"):
            self.collection.delete_one({"_id": name, "owner": owner})

    def holder(self, name: str) -> str | None:
        with client.errors(f"reading the lock {name}"):
            found = self.collection.find_one(
                {"_id": name, "expires_at": {"$gte": time.time()}}
            )
        return None if found is None else str(found["owner"])
