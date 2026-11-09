"""MongoDB errors, provoked on a real server: none shows a value of the
documents written, and each still tells its code and the key it hit.

pymongo prints the whole error document in its messages: the failed
operation, the duplicate key's value, the document that failed validation.
"""

from __future__ import annotations

from collections.abc import Generator
import os
import traceback
import uuid

import pytest

from core import contract
from infra.database import mongodb
from infra.database.mongodb import client
from tests.integrations.databases import test_mongodb_quarantine as quarantine

PII = ("jane.doe@bank.example", "123-45-6789")
_LIMIT = 300


@pytest.fixture
def mongo() -> Generator[mongodb.MongoDatabase]:
    url = os.environ.get("ETL_TEST_MONGODB_URL")
    if not url:
        pytest.skip("set ETL_TEST_MONGODB_URL to run the MongoDB tests")
    db = mongodb.MongoDatabase(url, database=f"t{uuid.uuid4().hex[:10]}")
    try:
        yield db
    finally:
        db.client.drop_database(db.name)
        db.close()


def _record(key: int) -> contract.Row:
    return {"_id": key, "email": PII[0], "ssn": PII[1]}


def _text(error: BaseException) -> str:
    """Everything a log of the error shows: the chained causes included."""
    return "".join(traceback.format_exception(error))


def _check(caught: BaseException, action: str, *told: str) -> str:
    """The message starts with the action, tells `told`, stays short, and
    neither it nor the chained causes show a value of the records."""
    message = str(caught)
    assert message.startswith(f"{action}: "), message
    for part in told:
        assert part in message, message
    assert len(message) < _LIMIT, message
    text = _text(caught)
    assert all(value not in text for value in PII), text
    return message


def _insert(
    mongo: mongodb.MongoDatabase, *rows: contract.Row
) -> contract.WriteResult:
    writer = mongodb.MongoTableWriter(mongo)
    target = contract.TableTarget(
        "customers", key=(), mode=contract.WriteMode.INSERT
    )
    return writer.write(target, list(rows))


class TestErrorsNeverShowTheRowsOfABatch:

    def test_a_duplicate_id_in_an_insert(
        self, mongo: mongodb.MongoDatabase
    ) -> None:
        _insert(mongo, _record(1))
        rows = (_record(2), _record(1))

        with pytest.raises(contract.FatalError) as caught:
            _insert(mongo, *rows)

        _check(caught.value, "writing 2 row(s) to customers", "11000", "_id")
        assert client.is_duplicate_key(caught.value.__cause__)

    def test_a_duplicate_on_a_unique_index(
        self, mongo: mongodb.MongoDatabase
    ) -> None:
        collection = mongo.collection("customers")
        collection.create_index([("email", 1)], unique=True)
        collection.insert_one(_record(1))
        duplicate = _record(2)

        def insert_the_duplicate() -> None:
            with client.errors("inserting a customer"):
                collection.insert_one(duplicate)

        with pytest.raises(contract.FatalError) as caught:
            insert_the_duplicate()

        _check(caught.value, "inserting a customer", "11000", "email")
        assert client.is_duplicate_key(caught.value.__cause__)

    def test_the_quarantine_giving_up_on_a_duplicate_forever(
        self, mongo: mongodb.MongoDatabase
    ) -> None:
        """R2-09's bounded retry: a unique index on another field makes
        every upsert of a new entry collide, until the retry gives up."""
        mongodb.create_etl_collections(mongo)
        collection = mongo.collection("etl_quarantine")
        collection.create_index([("run_id", 1)], unique=True, name="trap")
        ctx = contract.ETLContext(
            contract.ETLRunInfo(quarantine._PIPELINE, run_id="RUNX")
        )
        mongodb.MongoQuarantine(mongo).put([quarantine._rejected("first")], ctx)
        rejected = contract.RejectedRecord[object](
            {
                "entity": "crm.customer",
                "source_key": ["second"],
                "data": {"email": PII[0], "ssn": PII[1]},
            },
            contract.Checkpoint.AFTER_TRANSFORM,
            (),
        )
        store = mongodb.MongoQuarantine(mongo)

        with pytest.raises(contract.FatalError) as caught:
            store.put([rejected], ctx)

        _check(caught.value, "quarantining 1 record(s)", "11000", "run_id")

    def test_a_document_failing_validation(
        self, mongo: mongodb.MongoDatabase
    ) -> None:
        """The server's `errInfo` holds the failing document."""
        mongo.database.create_collection(
            "customers",
            validator={
                "$jsonSchema": {
                    "required": ["name"],
                    "properties": {"email": {"bsonType": "int"}},
                }
            },
        )
        record = _record(1)

        with pytest.raises(contract.FatalError) as caught:
            _insert(mongo, record)

        _check(caught.value, "writing 1 row(s) to customers", "121")

    def test_an_upsert_failing_validation(
        self, mongo: mongodb.MongoDatabase
    ) -> None:
        mongo.database.create_collection(
            "customers",
            validator={
                "$jsonSchema": {"properties": {"ssn": {"maxLength": 3}}}
            },
        )
        writer = mongodb.MongoTableWriter(mongo)
        target = contract.TableTarget("customers", key=("_id",))
        rows = [_record(1)]

        with pytest.raises(contract.FatalError) as caught:
            writer.write(target, rows)

        _check(caught.value, "writing 1 row(s) to customers", "121")
