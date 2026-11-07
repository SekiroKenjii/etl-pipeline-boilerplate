"""The ranges of a partitioned table in runs of some steps (`--step`): the
ranges step must have run first, so that every job reads the same ranges."""

from __future__ import annotations

from collections.abc import Sequence

from core import contract
from core import operators
from core import pipeline
from core import state as core_state
from tests.factories import fake_ports
from tests.factories import fakes


class _Table(pipeline.BaseOrchestrator):
    """`t:ranges`, then `t:0` and `t:1`, as the CRM module declares them."""

    def __init__(
        self, reader: contract.ITableReader, state: contract.IStateStore
    ) -> None:
        super().__init__()
        self.partitions = operators.KeyPartitions(
            reader, "t", key=["id"], count=2, state=state
        )
        self.loaders = [
            fakes.CollectingLoader[contract.Row]() for _ in range(2)
        ]

    def steps(self) -> Sequence[contract.PipelineStep]:
        return [
            contract.PipelineStep("t:ranges", self.partitions.prepare),
            *(
                contract.PipelineStep(
                    f"t:{index}",
                    pipeline.ETLJob(
                        extractor, operators.Passthrough[contract.Row](), loader
                    ),
                    dependencies=("t:ranges",),
                )
                for index, (extractor, loader) in enumerate(
                    zip(
                        self.partitions.extractors(watermark="ts"),
                        self.loaders,
                        strict=True,
                    )
                )
            ),
        ]


def _run(
    reader: contract.ITableReader,
    state: contract.IStateStore,
    *steps: str,
) -> contract.PipelineResult:
    return _Table(reader, state).run(
        contract.ETLContext(contract.ETLRunInfo("p", steps=frozenset(steps)))
    )


def test_a_range_run_alone_needs_the_ranges_saved_first() -> None:
    reader = fake_ports.InMemoryTableReader(
        {"t": [{"id": key, "ts": 1} for key in range(100)]}
    )
    state = core_state.InMemoryStateStore()

    alone = _run(reader, state, "t:1")
    ranges = _run(reader, state, "t:ranges")
    after = _run(reader, state, "t:0", "t:1")

    assert alone.status is contract.ETLStatus.FAILED
    assert "not saved yet" in alone.step_results["t:1"].errors[0].message
    # It saved nothing: the ranges step splits the table.
    assert ranges.status is contract.ETLStatus.SUCCESS
    assert after.status is contract.ETLStatus.SUCCESS
    assert after.stats.total == 100


def test_a_range_run_with_its_ranges_step_or_a_whole_run_splits() -> None:
    reader = fake_ports.InMemoryTableReader(
        {"t": [{"id": key, "ts": 1} for key in range(100)]}
    )

    with_ranges = _run(
        reader, core_state.InMemoryStateStore(), "t:ranges", "t:0"
    )
    whole = _run(reader, core_state.InMemoryStateStore())

    assert with_ranges.status is contract.ETLStatus.SUCCESS
    assert with_ranges.stats.total == 50
    assert whole.status is contract.ETLStatus.SUCCESS
    assert whole.stats.total == 100
