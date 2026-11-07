"""Splitting a table by ranges of its key, and extracting them in parallel."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest

from core import contract
from core import operators
from core import pipeline
from core import state as core_state
from tests.factories import fake_ports
from tests.factories import fakes


def _reader(keys: Sequence[Any]) -> fake_ports.InMemoryTableReader:
    return fake_ports.InMemoryTableReader(
        {"t": [{"id": key, "ts": 1, "v": str(key)} for key in keys]}
    )


class TestSplit:

    def test_ranges_hold_as_many_rows_each(self) -> None:
        reader = _reader(range(1, 101))

        ranges = operators.split(reader, "t", ["id"], 4)

        assert ranges == [
            operators.KeyRange(None, 26),
            operators.KeyRange(26, 51),
            operators.KeyRange(51, 76),
            operators.KeyRange(76, None),
        ]

    def test_text_keys_split_too(self) -> None:
        reader = _reader([f"K{i:03}" for i in range(40)])

        ranges = operators.split(reader, "t", ["id"], 2)

        assert ranges == [
            operators.KeyRange(None, "K020"),
            operators.KeyRange("K020", None),
        ]

    def test_small_tables_stay_whole(self) -> None:
        assert operators.split(_reader([1, 2, 3]), "t", ["id"], 4) == [
            operators.KeyRange()
        ]
        assert operators.split(_reader([]), "t", ["id"], 4) == [
            operators.KeyRange()
        ]

    def test_conditions(self) -> None:
        assert operators.KeyRange(1, 5).conditions("id") == (
            contract.Condition("id", contract.Operator.GE, 1),
            contract.Condition("id", contract.Operator.LT, 5),
        )
        assert operators.KeyRange().conditions("id") == ()


class _Parts(pipeline.BaseOrchestrator):

    def __init__(self, jobs: Sequence[contract.StepHandler]) -> None:
        super().__init__(max_workers=len(jobs))
        self._jobs = jobs

    def steps(self) -> Sequence[contract.PipelineStep]:
        return [
            contract.PipelineStep(f"p{i}", job)
            for i, job in enumerate(self._jobs)
        ]


def _load(
    reader: contract.ITableReader,
    state: contract.IStateStore,
    count: int = 4,
) -> list[dict[str, Any]]:
    partitions = operators.KeyPartitions(
        reader, "t", key=["id"], count=count, state=state
    )
    loaders = []
    for extractor in partitions.extractors(watermark="ts"):
        loader = fakes.CollectingLoader[contract.Row]()
        loaders.append(
            pipeline.ETLJob(
                extractor, operators.Passthrough[contract.Row](), loader
            )
        )
    result = _Parts(loaders).run(fakes.make_context("p"))
    assert result.status is contract.ETLStatus.SUCCESS, result.message
    return [
        row
        for job in loaders
        for row in job.loader.rows  # type: ignore[attr-defined]
    ]


class TestKeyPartitions:

    def test_partitions_read_every_row_once_in_parallel(self) -> None:
        reader = _reader(range(1, 1001))

        rows = _load(reader, core_state.InMemoryStateStore())

        assert sorted(row["id"] for row in rows) == list(range(1, 1001))

    def test_ranges_are_kept_rows_never_move(self) -> None:
        reader = _reader(range(1, 101))
        state = core_state.InMemoryStateStore()
        _load(reader, state)
        _load(reader, state)  # Reads the rows at the last watermark again.

        # New rows, before and after every range, and changed ones.
        reader.tables["t"] += [
            {"id": key, "ts": 2, "v": "new"} for key in (-5, 50, 500)
        ]
        reader.tables["t"][0]["ts"] = 2
        rows = _load(reader, state)

        assert sorted(row["id"] for row in rows) == [-5, 1, 50, 500]
        saved = state.get("p", "table:t:ranges")
        assert isinstance(saved, dict)
        assert saved["count"] == 4

    def test_other_partitions_need_a_reset(self) -> None:
        reader = _reader(range(1, 101))
        state = core_state.InMemoryStateStore()
        _load(reader, state)

        partitions = operators.KeyPartitions(
            reader, "t", key=["id"], count=8, state=state
        )
        ctx = fakes.make_context("p")
        with pytest.raises(contract.FatalError, match="reset that state key"):
            partitions.ranges(ctx)

    def test_a_table_split_in_fewer_ranges_leaves_steps_idle(self) -> None:
        rows = _load(_reader([1, 2, 3]), core_state.InMemoryStateStore())

        assert sorted(row["id"] for row in rows) == [1, 2, 3]
