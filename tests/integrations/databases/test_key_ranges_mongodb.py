"""Splitting a collection into key ranges and reading them, on MongoDB.

Runs when `ETL_TEST_MONGODB_URL` is set (`make db-up` starts one).
"""

from __future__ import annotations

from collections.abc import Generator, Iterator, Sequence
import datetime
import os
from typing import Any
import uuid

import pytest

from core import contract
from core import operators
from core import pipeline
from core import state as core_state
from infra.database import mongodb
from tests.factories import fakes
from tests.integrations.databases import test_key_ranges

T0 = datetime.datetime(2026, 10, 1, 8, 0, tzinfo=datetime.UTC)


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


class _Spy(mongodb.MongoTableReader):
    """Remembers the queries read document by document."""

    def __init__(self, db: mongodb.MongoDatabase) -> None:
        super().__init__(db, page_size=7)
        self.queries: list[contract.TableQuery] = []

    def read(self, query: contract.TableQuery) -> Iterator[contract.Row]:
        self.queries.append(query)
        return super().read(query)


class _Streaming(_Spy):
    """Reads the keys at offsets one by one, as the port's default does."""

    def keys_at(
        self,
        table: str,
        order_by: Sequence[str],
        offsets: Sequence[int],
        filters: Sequence[contract.Condition] = (),
    ) -> list[tuple[Any, ...]]:
        return contract.ITableReader.keys_at(
            self, table, order_by, offsets, filters
        )


class TestKeysAt:

    @pytest.fixture
    def items(self, mongo: mongodb.MongoDatabase) -> str:
        """A composite key, its first field repeated: `(a, b)`."""
        mongo.collection("items").insert_many([
            {
                "a": (i * 7) % 23 // 3,
                "b": f"k{i:02}",
                "tenant": "x" if i % 5 else "y",
            }
            for i in range(41)
        ])
        return "items"

    @pytest.mark.parametrize(
        "offsets", [[0], [0, 1, 2], [3, 17, 25, 40], [10, 41, 50], []]
    )
    def test_the_database_skips_to_the_documents_read_in_order(
        self, mongo: mongodb.MongoDatabase, items: str, offsets: list[int]
    ) -> None:
        reader = _Spy(mongo)
        streaming = _Streaming(mongo)

        found = reader.keys_at(items, ["a", "b"], offsets)
        expected = streaming.keys_at(items, ["a", "b"], offsets)

        assert found == expected
        assert len(found) == len([o for o in offsets if o < 41])
        assert not reader.queries

    @pytest.mark.parametrize("count", [2, 3, 4, 7, 16])
    def test_split_finds_the_bounds_streaming_found(
        self, mongo: mongodb.MongoDatabase, items: str, count: int
    ) -> None:
        reader = _Spy(mongo)
        only = [contract.Condition("tenant", contract.Operator.EQ, "x")]

        for filters in ([], only):
            found = operators.split(reader, items, ["a", "b"], count, filters)
            assert found == test_key_ranges.split_by_reading(
                reader, items, ["a", "b"], count, filters
            )
            assert len(found) > 1

    def test_missing_keys_are_refused(
        self, mongo: mongodb.MongoDatabase
    ) -> None:
        mongo.collection("loose").insert_many([{"a": 1}, {"b": 2}, {"a": 3}])
        reader = mongodb.MongoTableReader(mongo)

        with (pytest.raises(contract.FatalError, match="without"),):
            reader.keys_at("loose", ["a"], [1])


def test_ranges_read_by_key_then_by_watermark(
    mongo: mongodb.MongoDatabase,
) -> None:
    events = mongo.collection("events")
    events.insert_many([
        {"_id": i, "ts": T0 + datetime.timedelta(minutes=(i * 7) % 10)}
        for i in range(10, 70)
    ])
    reader = _Spy(mongo)
    state = core_state.InMemoryStateStore()

    def run() -> list[Any]:
        partitions = operators.KeyPartitions(
            reader, "events", key=["_id"], count=3, state=state
        )
        ctx = contract.ETLContext(contract.ETLRunInfo("p"))
        partitions.prepare(ctx, {})
        loaded: list[Any] = []
        for extractor in partitions.extractors(watermark="ts"):
            loader = fakes.CollectingLoader[contract.Row]()
            result = pipeline.ETLJob(
                extractor,
                operators.Passthrough[contract.Row](),
                loader,
                batch_size=4,
            )(ctx, {})
            assert result.status is contract.ETLStatus.SUCCESS, result.message
            loaded += [row["_id"] for row in loader.rows]
        return loaded

    first = run()
    orders = {q.order_by for q in reader.queries}
    # A late commit at the largest watermark, in the first range.
    events.insert_one({"_id": 1, "ts": T0 + datetime.timedelta(minutes=9)})
    again = run()

    assert sorted(first) == list(range(10, 70))
    assert orders == {("_id",)}
    assert sorted(again) == [1, *(i for i in range(10, 70) if i * 7 % 10 == 9)]
