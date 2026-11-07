"""The MongoDB table reader, on a fake database: what it asks the server."""

from __future__ import annotations

import concurrent.futures
from typing import Any, cast

import pytest

from core import contract
from infra.database import mongodb


class _Collection:
    """Documents `{"at": i, "_id": i}`; counts the checks for nulls."""

    def __init__(self, size: int, *, holes: bool = False) -> None:
        self.documents = [{"_id": i, "at": i} for i in range(size)]
        if holes:
            self.documents.append({"_id": size})
        self.null_checks = 0

    def find_one(self, *args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        self.null_checks += 1
        return next((d for d in self.documents if "at" not in d), None)

    def find(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        del args, kwargs
        return [dict(d) for d in self.documents]


class _Database:
    """A `MongoDatabase` of one collection."""

    def __init__(self, collection: _Collection) -> None:
        self._collection = collection

    def acquire(self) -> None:
        pass

    def release(self) -> None:
        pass

    def collection(self, name: str) -> _Collection:
        del name
        return self._collection


def _reader(collection: _Collection) -> mongodb.MongoTableReader:
    return mongodb.MongoTableReader(
        cast(mongodb.MongoDatabase, _Database(collection)), page_size=1000
    )


def test_missing_order_fields_are_looked_for_once_per_reader() -> None:
    collection = _Collection(9)
    reader = _reader(collection)

    for lower, upper in [(0, 2), (3, 5), (6, 8), (0, 8)]:
        list(
            reader.read(
                contract.TableQuery(
                    "event",
                    order_by=("at", "_id"),
                    lower=lower,
                    upper=upper,
                )
            )
        )
    assert collection.null_checks == 1
    # Other filters, another order: checked again.
    list(
        reader.read(
            contract.TableQuery(
                "event",
                order_by=("at", "_id"),
                filters=(contract.Condition("at", contract.Operator.GE, 3),),
            )
        )
    )
    list(reader.read(contract.TableQuery("event", order_by=("_id",))))
    assert collection.null_checks == 3
    next_run = _reader(collection)
    list(next_run.read(contract.TableQuery("event", order_by=("at", "_id"))))
    assert collection.null_checks == 4


def test_parallel_reads_check_once_and_a_failed_check_again() -> None:
    collection = _Collection(9)
    reader = _reader(collection)
    query = contract.TableQuery("event", order_by=("at", "_id"))

    list(reader.read(query))
    with concurrent.futures.ThreadPoolExecutor(4) as pool:
        for _ in pool.map(lambda _: list(reader.read(query)), range(8)):
            pass
    assert collection.null_checks == 1

    holed = _Collection(3, holes=True)
    reader = _reader(holed)
    for _ in range(2):
        with pytest.raises(contract.FatalError, match="without"):
            list(reader.read(query))
    assert holed.null_checks == 2
