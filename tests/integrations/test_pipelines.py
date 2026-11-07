"""
Real adapters (`infra`) + reusable operators (`core.operators`) + the engine,
composed the way a business module (`modules/<name>`) would.
"""

from __future__ import annotations

from collections.abc import Sequence
import pathlib
from typing import Any

import httpx

from core import contract
from core import operators
from core import pipeline
from core import state as core_state
from infra import formats
from infra import http
from infra import storage
from tests.factories import fakes


def test_local_csv_files_are_loaded_once(tmp_path: pathlib.Path) -> None:
    landing = tmp_path / "landing"
    (landing / "2026-10").mkdir(parents=True)
    (landing / "2026-10" / "orders.csv").write_text("id,total\n1,10\n2,25\n")
    state = core_state.JsonFileStateStore(tmp_path / "state")

    def run() -> fakes.CollectingLoader[operators.ObjectRecord]:
        loader = fakes.CollectingLoader[operators.ObjectRecord]()
        extractor = operators.ObjectStoreExtractor(
            storage.LocalObjectStore(landing),
            formats.CsvFormat(),
            pattern="*.csv",
            state=state,
        )
        result = pipeline.ETLJob(
            extractor, fakes.Passthrough[operators.ObjectRecord](), loader
        )(fakes.make_context("orders"), {})
        assert result.status is contract.ETLStatus.SUCCESS
        return loader

    first = run()
    assert [(r.key, r.data) for r in first.rows] == [
        ("2026-10/orders.csv", {"id": "1", "total": "10"}),
        ("2026-10/orders.csv", {"id": "2", "total": "25"}),
    ]
    assert state.path_for("orders").exists()
    assert run().rows == []  # already processed


def test_two_endpoints_of_one_api_run_as_parallel_steps() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params["page"])
        resource = request.url.path.strip("/")
        return httpx.Response(
            200, json=[{"resource": resource}] if page == 1 else []
        )

    api = http.RestApi(
        "https://api.test", transport=httpx.MockTransport(handler)
    )
    loaders: dict[str, fakes.CollectingLoader[dict[str, Any]]] = {}

    def step(resource: str) -> contract.PipelineStep:
        loaders[resource] = fakes.CollectingLoader()
        extractor = operators.ApiExtractor(
            api.endpoint(f"/{resource}", paginator=http.PageNumberPaginator())
        )
        job = pipeline.ETLJob(
            pipeline.AsyncExtractorAdapter(extractor),
            fakes.Passthrough[dict[str, Any]](),
            loaders[resource],
        )
        return contract.PipelineStep(resource, job)

    class CrmModule(pipeline.BaseOrchestrator):

        def steps(self) -> Sequence[contract.PipelineStep]:
            return [step("orders"), step("customers")]

    result = CrmModule(max_workers=2).run(fakes.make_context("crm"))

    assert result.status is contract.ETLStatus.SUCCESS
    assert loaders["orders"].rows == [{"resource": "orders"}]
    assert loaders["customers"].rows == [{"resource": "customers"}]
