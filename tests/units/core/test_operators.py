from __future__ import annotations

from typing import Any

import pytest

from core import contract
from core import operators
from core import pipeline
from core import state as core_state
from tests.factories import fake_ports
from tests.factories import fakes

PIPELINE = "orders"


def _run(
    extractor: operators.ObjectStoreExtractor,
    loader: fakes.CollectingLoader[operators.ObjectRecord],
    batch_size: int = 2,
) -> contract.ETLStatus:
    job = pipeline.ETLJob(
        extractor,
        fakes.Passthrough[operators.ObjectRecord](),
        loader,
        batch_size=batch_size,
    )
    return job(fakes.make_context(PIPELINE), {}).status


def _keys(
    loader: fakes.CollectingLoader[operators.ObjectRecord],
) -> list[tuple[str, int]]:
    return [(record.key, record.index) for record in loader.rows]


class TestObjectStoreExtractor:

    def test_yields_rows_with_their_lineage(self) -> None:
        store = fake_ports.InMemoryObjectStore()
        store.put("a.csv", "1\n2\n")
        store.put("notes.txt", "ignored")
        extractor = operators.ObjectStoreExtractor(
            store, fake_ports.LinesFormat(), pattern="*.csv"
        )

        records = list(extractor.extract(fakes.make_context()))

        assert [(r.key, r.version, r.index, r.data) for r in records] == [
            ("a.csv", "1", 0, {"value": "1"}),
            ("a.csv", "1", 1, {"value": "2"}),
        ]

    def test_incremental_runs_skip_processed_objects(self) -> None:
        store = fake_ports.InMemoryObjectStore()
        store.put("a", "1\n2\n3\n")
        store.put("b", "4\n")
        state = core_state.InMemoryStateStore()

        first = fakes.CollectingLoader[operators.ObjectRecord]()
        assert _run(
            operators.ObjectStoreExtractor(
                store, fake_ports.LinesFormat(), state=state
            ),
            first,
        ) is (contract.ETLStatus.SUCCESS)
        assert {key for key, _ in _keys(first)} == {"a", "b"}

        second = fakes.CollectingLoader[operators.ObjectRecord]()
        _run(
            operators.ObjectStoreExtractor(
                store, fake_ports.LinesFormat(), state=state
            ),
            second,
        )
        assert second.rows == []

        store.put("a", "1\n2\n3\n5\n")  # new version
        third = fakes.CollectingLoader[operators.ObjectRecord]()
        _run(
            operators.ObjectStoreExtractor(
                store, fake_ports.LinesFormat(), state=state
            ),
            third,
        )
        assert {key for key, _ in _keys(third)} == {"a"}

    def test_an_object_interrupted_by_a_failure_is_processed_again(
        self,
    ) -> None:
        store = fake_ports.InMemoryObjectStore()
        store.put("a", "1\n2\n")
        store.put("b", "3\n4\n5\n")
        state = core_state.InMemoryStateStore()
        extractor = operators.ObjectStoreExtractor(
            store, fake_ports.LinesFormat(), state=state
        )
        failing = fakes.CollectingLoader[operators.ObjectRecord](
            fail_on_batch=2
        )

        # batch 1 = all of `a`, batch 2 = the first rows of `b`: it fails
        with pytest.raises(contract.RetryableError):
            _run(extractor, failing)

        retry = fakes.CollectingLoader[operators.ObjectRecord]()
        _run(
            operators.ObjectStoreExtractor(
                store, fake_ports.LinesFormat(), state=state
            ),
            retry,
        )

        assert _keys(retry) == [("b", 0), ("b", 1), ("b", 2)]

    def test_empty_objects_are_marked_processed(self) -> None:
        store = fake_ports.InMemoryObjectStore()
        store.put("empty", "")
        state = core_state.InMemoryStateStore()

        _run(
            operators.ObjectStoreExtractor(
                store, fake_ports.LinesFormat(), state=state
            ),
            fakes.CollectingLoader(),
        )

        rerun = operators.ObjectStoreExtractor(
            store, fake_ports.LinesFormat(), state=state
        )
        assert list(rerun.pending_objects(fakes.make_context(PIPELINE))) == []

    def test_concurrent_reads_keep_the_key_order(self) -> None:
        store = fake_ports.InMemoryObjectStore()
        for name in "abcdef":
            store.put(name, name)
        extractor = operators.ObjectStoreExtractor(
            store, fake_ports.LinesFormat(), concurrency=3
        )

        records = list(extractor.extract(fakes.make_context()))

        assert [r.data["value"] for r in records] == list("abcdef")

    def test_rejects_invalid_concurrency(self) -> None:
        store = fake_ports.InMemoryObjectStore()
        lines = fake_ports.LinesFormat()

        with pytest.raises(ValueError):
            operators.ObjectStoreExtractor(store, lines, concurrency=0)


def _extract(
    extractor: operators.ApiExtractor[dict[str, Any]], ctx: Any = None
) -> list[Any]:
    adapter = pipeline.AsyncExtractorAdapter(extractor)
    ctx = ctx or fakes.make_context()
    with adapter.lifecycle(ctx):
        return list(adapter.extract(ctx))


class TestApiExtractor:

    def test_yields_the_source_records(self) -> None:
        source = fake_ports.StaticSource([{"id": 1}, {"id": 2}])

        assert _extract(operators.ApiExtractor(source)) == [
            {"id": 1},
            {"id": 2},
        ]
        assert source.params == [{}]

    def test_params_can_depend_on_the_run(self) -> None:
        source = fake_ports.StaticSource([])
        extractor = operators.ApiExtractor(
            source, params=lambda ctx: {"pipeline": ctx.run.pipeline_name}
        )

        _extract(extractor, fakes.make_context("orders"))

        assert source.params == [{"pipeline": "orders"}]

    def test_source_lifecycle_runs_on_the_extractor_loop(self) -> None:
        source = fake_ports.StaticSource([{"id": 1}])

        _extract(operators.ApiExtractor(source, params={"status": "open"}))

        assert source.events == ["asetup", "ateardown"]
        assert source.params == [{"status": "open"}]

    def test_name_mentions_the_source(self) -> None:
        assert (
            operators.ApiExtractor(fake_ports.StaticSource([])).name
            == "ApiExtractor(StaticSource)"
        )
