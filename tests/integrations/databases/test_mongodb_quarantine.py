"""One open quarantine entry per record in MongoDB, whatever the writers at
once and the entries an older release left."""

from __future__ import annotations

from collections.abc import Callable, Generator
import concurrent.futures
import os
import threading
import uuid

from bson import objectid
from pymongo import monitoring
import pytest

from core import contract
from infra.database import mongodb
from infra.database.mongodb import bookkeeping
from shared import utils
from tests.factories import fakes
from tests.factories import quarantines

_PIPELINE = "crm.full_load"


class _BeforeWriting(monitoring.CommandListener):
    """Runs `then` once, before the first write to the quarantine."""

    def __init__(self) -> None:
        self.then: list[Callable[[], object]] = []
        self.ran = threading.Event()

    def started(self, event: monitoring.CommandStartedEvent) -> None:
        if (
            event.command_name in ("insert", "update")
            and event.command.get(event.command_name) == "etl_quarantine"
            and self.then
            and not self.ran.is_set()
        ):
            self.ran.set()
            with concurrent.futures.ThreadPoolExecutor(1) as thread:
                thread.submit(self.then.pop()).result()

    def succeeded(self, event: monitoring.CommandSucceededEvent) -> None:
        del event  # Unused.

    def failed(self, event: monitoring.CommandFailedEvent) -> None:
        del event  # Unused.


@pytest.fixture
def url() -> str:
    found = os.environ.get("ETL_TEST_MONGODB_URL")
    if not found:
        pytest.skip("set ETL_TEST_MONGODB_URL to run the MongoDB tests")
    return found


@pytest.fixture
def listener() -> _BeforeWriting:
    return _BeforeWriting()


@pytest.fixture
def mongo(
    url: str, listener: _BeforeWriting
) -> Generator[mongodb.MongoDatabase]:
    db = mongodb.MongoDatabase(
        url,
        database=f"t{uuid.uuid4().hex[:10]}",
        event_listeners=[listener],
    )
    try:
        yield db
    finally:
        db.client.drop_database(db.name)
        db.close()


@pytest.fixture
def other(
    url: str, mongo: mongodb.MongoDatabase
) -> Generator[mongodb.MongoQuarantine]:
    """A quarantine of another process, on a client of its own."""
    db = mongodb.MongoDatabase(url, database=mongo.name)
    yield mongodb.MongoQuarantine(db)
    db.close()


def _rejected(key: str) -> contract.RejectedRecord[object]:
    return contract.RejectedRecord(
        {"entity": "crm.customer", "source_key": [key], "data": {"n": key}},
        contract.Checkpoint.AFTER_TRANSFORM,
        (),
    )


def _open(mongo: mongodb.MongoDatabase) -> list[str]:
    return sorted(
        quarantines.fields(e)["source_key"][0]
        for e in mongodb.MongoQuarantine(mongo).entries(_PIPELINE)
    )


def test_a_record_rejected_by_two_writers_at_once_has_one_entry(
    mongo: mongodb.MongoDatabase,
    listener: _BeforeWriting,
    other: mongodb.MongoQuarantine,
) -> None:
    """Between the first writer's read and its write, the other one writes
    the same record."""
    mongodb.create_etl_collections(mongo)
    quarantine = mongodb.MongoQuarantine(mongo)
    listener.then.append(
        lambda: other.put([_rejected("1003")], fakes.make_context(_PIPELINE))
    )

    quarantine.put([_rejected("1003")], fakes.make_context(_PIPELINE))

    assert listener.ran.is_set()
    assert _open(mongo) == ["1003"]


def test_many_writers_rejecting_the_same_records_at_once(
    mongo: mongodb.MongoDatabase, url: str
) -> None:
    mongodb.create_etl_collections(mongo)
    keys = [str(i) for i in range(50)]
    writers = [
        mongodb.MongoQuarantine(mongodb.MongoDatabase(url, database=mongo.name))
        for _ in range(6)
    ]
    start = threading.Barrier(len(writers))

    def put(quarantine: mongodb.MongoQuarantine) -> None:
        start.wait()
        for _ in range(5):
            quarantine.put(
                [_rejected(k) for k in keys], fakes.make_context(_PIPELINE)
            )

    try:
        with concurrent.futures.ThreadPoolExecutor(len(writers)) as threads:
            list(threads.map(put, writers))
    finally:
        for writer in writers:
            writer.db.close()

    assert _open(mongo) == sorted(keys)


def test_a_record_rejected_after_its_replay_gets_a_new_entry(
    mongo: mongodb.MongoDatabase,
) -> None:
    mongodb.create_etl_collections(mongo)
    quarantine = mongodb.MongoQuarantine(mongo)
    quarantine.put([_rejected("1")], fakes.make_context(_PIPELINE))
    (first,) = quarantine.entries(_PIPELINE)
    quarantine.resolve([first.id], contract.QuarantineStatus.REPLAYED)

    quarantine.put([_rejected("1")], fakes.make_context(_PIPELINE))
    quarantine.put([_rejected("1")], fakes.make_context(_PIPELINE))

    (again,) = quarantine.entries(_PIPELINE)
    assert again.id != first.id
    assert len(list(quarantine.entries(_PIPELINE, status=None))) == 2


class TestOpenEntriesOfAnOlderRelease:
    """Before the unique index, a record could have several open entries."""

    @pytest.fixture
    def doubled(self, mongo: mongodb.MongoDatabase) -> list[objectid.ObjectId]:
        collection = mongo.collection("etl_quarantine")
        collection.create_index([("pipeline", 1), ("record_key", 1)])
        key = contract.QuarantineEntry.key_of(
            _PIPELINE,
            contract.Checkpoint.AFTER_TRANSFORM.value,
            _rejected("7").record,
        )
        ids = [objectid.ObjectId() for _ in range(3)]
        collection.insert_many([
            {
                "_id": id_,
                "pipeline": _PIPELINE,
                "record_key": key,
                "status": "open",
                "run_id": f"r{n}",
                "checkpoint": contract.Checkpoint.AFTER_TRANSFORM.value,
                "record": _rejected("7").record,
                "issues": [],
                "rejected_at": utils.utc_now(),
            }
            for n, id_ in enumerate(ids)
        ])
        return ids

    def test_the_latest_keeps_the_key_and_none_is_lost(
        self, mongo: mongodb.MongoDatabase, doubled: list[objectid.ObjectId]
    ) -> None:
        mongodb.create_etl_collections(mongo)
        quarantine = mongodb.MongoQuarantine(mongo)
        quarantine.put([_rejected("7")], fakes.make_context(_PIPELINE))

        entries = {e.id: e for e in quarantine.entries(_PIPELINE)}
        assert set(entries) == {str(id_) for id_ in doubled}
        stored = {
            d["_id"]: d["record_key"]
            for d in mongo.collection("etl_quarantine").find()
        }
        assert stored[doubled[0]] == f"legacy-{doubled[0]}"
        assert stored[doubled[1]] == f"legacy-{doubled[1]}"
        assert not str(stored[doubled[2]]).startswith("legacy-")
        assert entries[str(doubled[2])].run_id != "r2"

    def test_creating_the_collections_again_changes_nothing(
        self, mongo: mongodb.MongoDatabase, doubled: list[objectid.ObjectId]
    ) -> None:
        mongodb.create_etl_collections(mongo)
        before = list(mongo.collection("etl_quarantine").find())

        mongodb.create_etl_collections(mongo)

        assert list(mongo.collection("etl_quarantine").find()) == before
        assert len(doubled) == len(before)
        indexes = mongo.collection("etl_quarantine").index_information()
        (open_index,) = [i for i in indexes.values() if i.get("unique") is True]
        assert open_index["partialFilterExpression"] == {"status": "open"}

    def test_entries_doubled_again_meanwhile_are_reported(
        self,
        mongo: mongodb.MongoDatabase,
        doubled: list[objectid.ObjectId],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A writer of the older release doubles an entry again after they
        were made unique, each time: the index cannot be created."""
        before = list(mongo.collection("etl_quarantine").find())
        monkeypatch.setattr(
            bookkeeping, "_rekey_older_open_entries", lambda quarantine: None
        )

        with pytest.raises(contract.FatalError, match="etl-db upgrade"):
            mongodb.create_etl_collections(mongo)

        assert list(mongo.collection("etl_quarantine").find()) == before
        assert len(before) == len(doubled)
        indexes = mongo.collection("etl_quarantine").index_information()
        assert "pipeline_1_record_key_1" in indexes
