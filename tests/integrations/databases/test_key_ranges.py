"""Splitting a table into key ranges and reading them, on every database."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
import datetime
from typing import Any

import pytest
import sqlalchemy as sa

from core import contract
from core import operators
from core import pipeline
from core import state as core_state
from infra.database import relational
from tests.factories import fakes
from tests.integrations.databases import conftest

T0 = datetime.datetime(2026, 10, 1, 8, 0, 0)


class _Spy(relational.RelationalTableReader):
    """Remembers the queries read row by row."""

    def __init__(self, db: relational.Database) -> None:
        super().__init__(db, page_size=7)
        self.queries: list[contract.TableQuery] = []

    def read(self, query: contract.TableQuery) -> Iterator[contract.Row]:
        self.queries.append(query)
        return super().read(query)


class _Streaming(_Spy):
    """Reads the keys at offsets row by row, as the port's default does."""

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


def split_by_reading(
    reader: contract.ITableReader,
    table: str,
    key: Sequence[str],
    count: int,
    filters: Sequence[contract.Condition] = (),
) -> list[operators.KeyRange]:
    """`operators.split` as it was: the keys streamed up to the last mark."""
    total = reader.count(table, filters)
    if count == 1 or total < 2 * count:
        return [operators.KeyRange()]
    marks = {round(total * i / count) for i in range(1, count)}
    query = contract.TableQuery(
        table, order_by=tuple(key), columns=tuple(key), filters=tuple(filters)
    )
    bounds: list[Any] = []
    for index, row in enumerate(reader.read(query)):
        if index in marks:
            value = row[key[0]]
            if not bounds or value != bounds[-1]:
                bounds.append(value)
            if len(bounds) == len(marks):
                break
    edges = [None, *bounds, None]
    return [
        operators.KeyRange(edges[i], edges[i + 1])
        for i in range(len(edges) - 1)
    ]


def _context(**options: Any) -> contract.ETLContext:
    return contract.ETLContext(contract.ETLRunInfo("p", **options))


class TestKeysAt:

    @pytest.fixture
    def items(self, backend: conftest.Backend) -> str:
        """A composite key, its first column repeated: `(a, b)`."""
        table = backend.table(
            "item",
            sa.Column("a", sa.Integer, primary_key=True, autoincrement=False),
            sa.Column("b", sa.Unicode(10), primary_key=True),
            sa.Column("tenant", sa.Unicode(10), nullable=False),
        )
        backend.create()
        backend.insert(
            table,
            *(
                {
                    "a": (i * 7) % 23 // 3,
                    "b": f"k{i:02}",
                    "tenant": "x" if i % 5 else "y",
                }
                for i in range(41)
            ),
        )
        return table.name

    @pytest.mark.parametrize(
        "offsets", [[0], [0, 1, 2], [3, 17, 25, 40], [10, 41, 50], []]
    )
    def test_the_database_skips_to_the_rows_read_in_order(
        self, backend: conftest.Backend, items: str, offsets: list[int]
    ) -> None:
        reader = _Spy(backend.db)
        streaming = _Streaming(backend.db)
        order = ["a", "b"]

        found = reader.keys_at(items, order, offsets)
        expected = streaming.keys_at(items, order, offsets)

        assert found == expected
        assert len(found) == len([o for o in offsets if o < 41])
        assert reader.queries == []

    def test_filters_apply(self, backend: conftest.Backend, items: str) -> None:
        reader = relational.RelationalTableReader(backend.db)
        streaming = _Streaming(backend.db)
        only = [contract.Condition("tenant", contract.Operator.EQ, "x")]

        found = reader.keys_at(items, ["a", "b"], [2, 9, 31], only)
        expected = streaming.keys_at(items, ["a", "b"], [2, 9, 31], only)

        assert found == expected
        assert len(found) == 3

    @pytest.mark.parametrize("count", [2, 3, 4, 7, 16])
    def test_split_finds_the_bounds_streaming_found(
        self, backend: conftest.Backend, items: str, count: int
    ) -> None:
        reader = _Spy(backend.db)
        only = [contract.Condition("tenant", contract.Operator.EQ, "x")]

        for filters in ([], only):
            found = operators.split(reader, items, ["a", "b"], count, filters)
            assert found == split_by_reading(
                reader, items, ["a", "b"], count, filters
            )
            assert len(found) > 1

    def test_text_keys_split_in_the_order_of_the_database(
        self, backend: conftest.Backend
    ) -> None:
        table = backend.table(
            "code",
            sa.Column("code", sa.Unicode(10), primary_key=True),
        )
        backend.create()
        backend.insert(
            table,
            *({"code": f"{c}{i}"} for c in "aBcDe" for i in range(8)),
        )
        reader = relational.RelationalTableReader(backend.db)

        found = operators.split(reader, table.name, ["code"], 4)
        expected = split_by_reading(reader, table.name, ["code"], 4)

        assert found == expected
        assert len(found) == 4

    def test_null_keys_are_refused(self, backend: conftest.Backend) -> None:
        table = backend.table("loose", sa.Column("a", sa.Integer))
        backend.create()
        backend.insert(table, {"a": 1}, {"a": None}, {"a": 2})
        reader = relational.RelationalTableReader(backend.db)

        with (pytest.raises(contract.FatalError, match="NULL"),):
            reader.keys_at(table.name, ["a"], [1])


class TestPartitionedLoad:

    @pytest.fixture
    def events(self, backend: conftest.Backend) -> sa.Table:
        table = backend.table(
            "event",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
            sa.Column("ts", sa.DateTime, nullable=True),
            sa.Column("v", sa.Unicode(10)),
        )
        backend.create()
        # The watermark says nothing of the key; ties on it.
        backend.insert(
            table,
            *(
                {
                    "id": i,
                    "ts": T0 + datetime.timedelta(minutes=(i * 7) % 10),
                    "v": f"v{i}",
                }
                for i in range(10, 70)
            ),
        )
        return table

    def _run(
        self,
        reader: contract.ITableReader,
        table: str,
        state: contract.IStateStore,
    ) -> list[Any]:
        """Splits `table` in 3, reads each range; the ids loaded."""
        partitions = operators.KeyPartitions(
            reader, table, key=["id"], count=3, state=state
        )
        ctx = _context()
        partitions.prepare(ctx, {})
        loaded: list[Any] = []
        for extractor in partitions.extractors(watermark="ts"):
            loader = fakes.CollectingLoader[contract.Row]()
            job = pipeline.ETLJob(
                extractor,
                operators.Passthrough[contract.Row](),
                loader,
                batch_size=4,
            )
            result = job(ctx, {})
            assert result.status is contract.ETLStatus.SUCCESS, result.message
            loaded += [row["id"] for row in loader.rows]
        return loaded

    def test_ranges_read_by_key_then_by_watermark(
        self, backend: conftest.Backend, events: sa.Table
    ) -> None:
        reader = _Spy(backend.db)
        state = core_state.InMemoryStateStore()

        first = self._run(reader, events.name, state)
        orders = {q.order_by for q in reader.queries}
        reader.queries.clear()
        # Then: the rows at the largest watermark, once; nothing more.
        again = self._run(reader, events.name, state)
        after = self._run(reader, events.name, state)

        assert sorted(first) == list(range(10, 70))
        assert orders == {("id",)}
        assert sorted(again) == [i for i in range(10, 70) if i * 7 % 10 == 9]
        assert {q.order_by for q in reader.queries} == {("ts", "id")}
        assert after == []

    def test_a_late_commit_at_the_largest_watermark_is_read(
        self, backend: conftest.Backend, events: sa.Table
    ) -> None:
        reader = relational.RelationalTableReader(backend.db)
        state = core_state.InMemoryStateStore()

        self._run(reader, events.name, state)
        # A transaction commits a row at the largest watermark, under the
        # key of the first range.
        backend.insert(
            events,
            {"id": 1, "ts": T0 + datetime.timedelta(minutes=9), "v": "l"},
        )
        again = self._run(reader, events.name, state)

        assert 1 in again

    def test_rows_without_a_watermark_are_refused(
        self, backend: conftest.Backend, events: sa.Table
    ) -> None:
        backend.insert(events, {"id": 5, "ts": None, "v": "none"})
        reader = relational.RelationalTableReader(backend.db)
        partitions = operators.KeyPartitions(
            reader,
            events.name,
            key=["id"],
            count=1,
            state=core_state.InMemoryStateStore(),
        )
        extractor = partitions.extractors(watermark="ts")[0]
        context = _context()

        with pytest.raises(contract.FatalError, match="NULL in ts"):
            extractor.query(context)
