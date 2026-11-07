from __future__ import annotations

from collections.abc import Iterable, Iterator

import pytest

from core import contract
from core import operators
from core import pipeline
from tests.factories import fakes


class Step(pipeline.IDataTransformer[int, int]):

    def __init__(
        self, name: str, events: list[str], fail_setup: bool = False
    ) -> None:
        self.label = name
        self.events = events
        self.fail_setup = fail_setup

    @property
    def name(self) -> str:
        return self.label

    def setup(self, ctx: contract.ETLContext) -> None:
        if self.fail_setup:
            raise RuntimeError(f"{self.label} cannot start")
        self.events.append(f"setup:{self.label}")

    def teardown(self, ctx: contract.ETLContext) -> None:
        self.events.append(f"teardown:{self.label}")

    def transform(
        self, records: Iterable[int], ctx: contract.ETLContext
    ) -> Iterator[int]:
        for record in records:
            self.events.append(f"{self.label}:{record}")
            yield record * 10

    def collect_stats(self) -> contract.ETLStats:
        return contract.ETLStats(filtered=1)


class ToText(pipeline.IDataTransformer[int, str]):

    def transform(
        self, records: Iterable[int], ctx: contract.ETLContext
    ) -> Iterator[str]:
        for record in records:
            yield f"#{record}"


def test_then_runs_lazily_in_order_and_keeps_the_types() -> None:
    events: list[str] = []
    chained: pipeline.IDataTransformer[int, str] = (
        Step("a", events).then(Step("b", events)).then(ToText())
    )

    assert list(chained.transform([1, 2], fakes.make_context())) == [
        "#100",
        "#200",
    ]
    assert events == [
        "a:1",
        "b:10",
        "a:2",
        "b:20",
    ]  # record by record, not stage by stage
    assert chained.name == "a -> b -> ToText"


def test_lifecycles_open_in_order_and_close_in_reverse() -> None:
    events: list[str] = []
    chained = pipeline.chain(
        Step("a", events), Step("b", events), Step("c", events)
    )

    with chained.lifecycle(fakes.make_context()):
        pass

    assert events == [
        "setup:a",
        "setup:b",
        "setup:c",
        "teardown:c",
        "teardown:b",
        "teardown:a",
    ]


def test_a_failing_setup_closes_what_was_opened() -> None:
    events: list[str] = []
    chained = Step("a", events).then(Step("b", events, fail_setup=True))
    ctx = fakes.make_context()

    with pytest.raises(RuntimeError, match="b cannot start"):
        chained.setup(ctx)

    assert events == ["setup:a", "teardown:a"]


def test_stats_of_every_link_are_collected() -> None:
    events: list[str] = []

    assert pipeline.chain(
        Step("a", events), Step("b", events)
    ).collect_stats() == contract.ETLStats(filtered=2)


class TestDropDuplicates:

    def test_duplicates_are_dropped_across_batches_and_counted(self) -> None:
        users = [
            fakes.RawUser("1", "a@x.io", 30),
            fakes.RawUser("2", "b@x.io", 30),
            fakes.RawUser("1", "dup@x.io", 30),
        ]
        loader = fakes.CollectingLoader[fakes.RawUser]()
        job = pipeline.ETLJob(
            fakes.InMemoryExtractor(users),
            operators.DropDuplicates[fakes.RawUser](lambda user: user.id),
            loader,
            batch_size=2,  # the duplicate is in the second batch
        )

        result = job(fakes.make_context(), {})

        assert [user.email for user in loader.rows] == ["a@x.io", "b@x.io"]
        assert (
            result.stats.total,
            result.stats.successful,
            result.stats.filtered,
        ) == (
            3,
            2,
            1,
        )
        assert (
            result.status is contract.ETLStatus.SUCCESS
        )  # dropping on purpose is not an error

    def test_each_run_starts_afresh(self) -> None:
        dedupe = operators.DropDuplicates[int](lambda value: value)
        ctx = fakes.make_context()

        for _ in range(2):
            with dedupe.lifecycle(ctx):
                assert list(dedupe.transform([1, 1, 2], ctx)) == [1, 2]
                assert dedupe.collect_stats().filtered == 1
