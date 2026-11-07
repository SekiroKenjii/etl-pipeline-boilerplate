"""The first read of a range goes by key, up to a watermark; the next ones
read the changes by watermark."""

from __future__ import annotations

from collections.abc import Sequence
import datetime
from typing import Any, cast

import pytest

from core import contract
from core import operators
from core import pipeline
from core import state as core_state
from shared import utils
from tests.factories import fake_ports
from tests.factories import fakes

T0 = datetime.datetime(2026, 10, 1, tzinfo=datetime.UTC)
KEY = "p", "table:t:0"


def _at(minutes: int) -> datetime.datetime:
    return T0 + datetime.timedelta(minutes=minutes)


def _rows(*items: tuple[int, int]) -> list[dict[str, Any]]:
    """Rows of `(id, minutes)`: `ts` is T0 plus the minutes."""
    return [{"id": i, "ts": _at(m), "v": f"v{i}"} for i, m in items]


class _Range:
    """Range 0 of table `t` split in two by `id`, read by watermark `ts`."""

    def __init__(
        self,
        table: list[dict[str, Any]],
        state: contract.IStateStore | None = None,
    ) -> None:
        self.reader = fake_ports.InMemoryTableReader({"t": table})
        self.state = state or core_state.InMemoryStateStore()
        # Range 0 holds the ids below 100.
        self.state.set(
            "p",
            "table:t:ranges",
            {"key": ["id"], "count": 2, "bounds": [100]},
        )

    def run(
        self,
        *,
        batch_size: int = 2,
        fail_on_batch: int | None = None,
        ctx: contract.ETLContext | None = None,
    ) -> list[Any]:
        """Runs the extraction of range 0; the ids loaded."""
        partitions = operators.KeyPartitions(
            self.reader, "t", key=["id"], count=2, state=self.state
        )
        loader = fakes.CollectingLoader[contract.Row](fail_on_batch)
        job = pipeline.ETLJob(
            partitions.extractors(watermark="ts")[0],
            operators.Passthrough[contract.Row](),
            loader,
            batch_size=batch_size,
        )
        try:
            job(ctx or fakes.make_context("p"), {})
        except contract.RetryableError:
            pass
        return [row["id"] for row in loader.rows]

    @property
    def last_query(self) -> contract.TableQuery:
        return self.reader.queries[-1]

    @property
    def saved(self) -> Any:
        return self.state.get(*KEY)


class TestFirstRead:

    def test_goes_by_key_up_to_the_watermark_of_its_start(self) -> None:
        table = _rows((7, 3), (1, 2), (150, 9), (4, 1))
        source = _Range(table)

        assert source.run() == [1, 4, 7]
        query = source.last_query
        assert query.order_by == ("id",)
        assert query.lower is None
        assert query.upper is None
        assert contract.Condition("ts", contract.Operator.LE, _at(9)) in (
            query.filters
        )
        # Done: up to that watermark, whose rows the next run reads again.
        assert source.saved == {
            "columns": ["ts", "id"],
            "position": utils.to_tagged([_at(9), 7]),
            "recheck": True,
        }

    def test_the_runs_after_it_read_the_changes_by_watermark(self) -> None:
        table = _rows((1, 1), (2, 2), (3, 3))
        source = _Range(table)
        source.run()

        assert source.run() == [3]  # The rows at its watermark, once.
        assert source.last_query.order_by == ("ts", "id")
        assert source.run() == []
        table[0]["ts"] = _at(4)
        assert source.run() == [1]

    def test_a_failed_one_resumes_after_its_last_key(self) -> None:
        table = _rows((1, 1), (2, 1), (3, 1), (4, 1), (5, 2), (6, 1))
        source = _Range(table)

        assert source.run(fail_on_batch=2) == [1, 2]
        assert source.saved == {
            "columns": ["ts", "id"],
            "first": {
                "until": utils.to_tagged(_at(2)),
                "after": utils.to_tagged([2]),
            },
        }
        # Meanwhile: 1 changes behind the position, 4 ahead of it, 0 is new.
        table[0]["ts"] = table[3]["ts"] = _at(5)
        table.append({"id": 0, "ts": _at(5), "v": "v0"})

        # The first read ends up to its own watermark: 4 waits.
        assert source.run() == [3, 5, 6]
        assert source.last_query.after == (2,)
        assert source.saved["position"] == utils.to_tagged([_at(2), 6])
        # Then the changes, the rows at that watermark again.
        assert source.run() == [5, 0, 1, 4]

    def test_a_failure_on_its_last_full_batch_is_resumed(self) -> None:
        source = _Range(_rows((1, 1), (2, 1), (3, 1), (4, 1)))

        assert source.run(fail_on_batch=2) == [1, 2]
        assert source.run() == [3, 4]
        # Done, though the last batch was full: no row after it.
        assert source.saved["recheck"] is True
        assert source.run() == [1, 2, 3, 4]
        assert source.run() == []

    def test_a_late_commit_at_its_watermark_is_not_lost(self) -> None:
        # Rows 5 and 9 at the same minute, read; 3 commits after the run,
        # with that minute too.
        table = _rows((5, 1), (9, 1))
        source = _Range(table)

        assert source.run() == [5, 9]
        table.append({"id": 3, "ts": _at(1), "v": "late"})

        assert source.run() == [3, 5, 9]
        assert source.run() == []

    def test_a_dry_run_saves_nothing(self) -> None:
        source = _Range(_rows((1, 1), (2, 1), (3, 1)))

        source.run(ctx=fakes.make_context("p", dry_run=True))

        assert source.saved is None

    def test_an_empty_range_starts_afresh_next_time(self) -> None:
        table = _rows((150, 1))
        source = _Range(table)

        assert source.run() == []
        assert source.saved is None
        table.append({"id": 1, "ts": _at(2), "v": "v1"})
        assert source.run() == [1]
        assert source.last_query.order_by == ("id",)


class TestSavedState:

    def test_a_position_by_watermark_goes_on_by_watermark(self) -> None:
        # Saved by an earlier version, which read every range by watermark.
        state = core_state.InMemoryStateStore()
        state.set(
            *KEY,
            {
                "columns": ["ts", "id"],
                "position": cast(
                    contract.StateValue, utils.to_tagged([_at(1), 2])
                ),
                "recheck": False,
            },
        )
        source = _Range(_rows((1, 1), (2, 1), (3, 1), (4, 2)), state)

        assert source.run() == [3, 4]
        assert source.last_query.order_by == ("ts", "id")
        assert source.last_query.after == (_at(1), 2)

    def test_a_backfill_during_a_first_read_leaves_it_as_it_is(self) -> None:
        source = _Range(_rows((1, 1), (2, 1), (3, 2), (4, 3)))
        source.run(fail_on_batch=2)
        before = source.saved

        backfill = contract.ETLContext(
            contract.ETLRunInfo("p", since=_at(2), until=_at(2))
        )
        assert source.run(ctx=backfill) == [3]
        assert source.last_query.order_by == ("ts", "id")
        assert source.saved == before
        assert source.run() == [3, 4]

    def test_a_first_read_on_other_columns_is_refused(self) -> None:
        state = core_state.InMemoryStateStore()
        state.set(
            *KEY,
            {"columns": ["at", "id"], "first": {"until": 1, "after": [1]}},
        )
        source = _Range(_rows((1, 1)), state)

        with pytest.raises(contract.FatalError, match="Delete it"):
            source.run()

    def test_rows_without_a_watermark_are_refused(self) -> None:
        reader = _NoWatermark({"t": _rows((1, 1), (2, 1))})
        partitions = operators.KeyPartitions(
            reader,
            "t",
            key=["id"],
            count=1,
            state=core_state.InMemoryStateStore(),
        )
        extractor = partitions.extractors(watermark="ts")[0]
        context = fakes.make_context("p")

        with pytest.raises(contract.FatalError, match="NULL in ts"):
            extractor.query(context)


class _NoWatermark(fake_ports.InMemoryTableReader):
    """Counts one row with NULL as its watermark."""

    def count(
        self, table: str, filters: Sequence[contract.Condition] = ()
    ) -> int:
        nulls = contract.Condition("ts", contract.Operator.IS_NULL)
        return 1 if nulls in filters else super().count(table, filters)


class TestKeysAt:

    def test_the_default_reads_up_to_the_last_offset(self) -> None:
        reader = fake_ports.InMemoryTableReader(
            {"t": [{"a": i // 3, "b": i % 3} for i in range(10)]}
        )

        found = reader.keys_at("t", ["a", "b"], [0, 4, 8, 20])

        assert found == [(0, 0), (1, 1), (2, 2)]
        assert reader.keys_at("t", ["a", "b"], []) == []

    @pytest.mark.parametrize("offsets", [[2, 2], [3, 1], [-1]])
    def test_offsets_must_ascend_from_0(self, offsets: list[int]) -> None:
        reader = fake_ports.InMemoryTableReader({"t": []})

        with pytest.raises(ValueError, match="ascending"):
            reader.keys_at("t", ["a"], offsets)

    def test_split_asks_the_reader_for_the_keys_at_the_quantiles(
        self,
    ) -> None:
        reader = _Quantiles({"t": [{"id": i} for i in range(1, 101)]})

        ranges = operators.split(reader, "t", ["id"], 4)

        assert reader.asked == [[25, 50, 75]]
        assert reader.queries == []  # Nothing read row by row.
        assert [r.lower for r in ranges[1:]] == [26, 51, 76]


class _Quantiles(fake_ports.InMemoryTableReader):
    """Answers `keys_at` from its rows, without `read`."""

    def __init__(self, tables: dict[str, list[dict[str, Any]]]) -> None:
        super().__init__(tables)
        self.asked: list[list[int]] = []

    def keys_at(
        self,
        table: str,
        order_by: Sequence[str],
        offsets: Sequence[int],
        filters: Sequence[contract.Condition] = (),
    ) -> list[tuple[Any, ...]]:
        self.asked.append(list(offsets))
        rows = sorted(
            tuple(row[c] for c in order_by) for row in self.tables[table]
        )
        return [rows[offset] for offset in offsets if offset < len(rows)]
