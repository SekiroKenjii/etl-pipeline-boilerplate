"""Logs, traces and metrics of a real run reach an OpenTelemetry Collector.

Runs when `ETL_TEST_OTEL_URL` is set, with `ETL_TEST_OTEL_DIR` the directory
where the collector writes what it receives (`make db-up` starts it with
`deploy/otel-collector/file.yaml`).
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
import json
import os
import pathlib
import time
from typing import Any
import uuid

import pytest
import sqlalchemy as sa

from app import bootstrap
from modules.crm import demo

ROOT = pathlib.Path(__file__).resolve().parents[2]


def _lines(path: pathlib.Path) -> Iterator[dict[str, Any]]:
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                yield json.loads(line)


def _resource_items(
    payload: dict[str, Any], kind: str, service: str
) -> Iterator[dict[str, Any]]:
    """The scope items (spans, metrics, log records) of our service."""
    resources, scopes, items = {
        "traces": ("resourceSpans", "scopeSpans", "spans"),
        "metrics": ("resourceMetrics", "scopeMetrics", "metrics"),
        "logs": ("resourceLogs", "scopeLogs", "logRecords"),
    }[kind]
    for resource in payload.get(resources, []):
        attributes = {
            a["key"]: a["value"].get("stringValue")
            for a in resource["resource"]["attributes"]
        }
        if attributes.get("service.name") != service:
            continue
        for scope in resource.get(scopes, []):
            yield from scope.get(items, [])


def _wait[T](find: Callable[[], T | None], seconds: float = 30) -> T:
    deadline = time.monotonic() + seconds
    while True:
        found = find()
        if found or time.monotonic() > deadline:
            assert found, "nothing arrived at the collector"
            return found
        time.sleep(0.5)


def test_a_run_is_logged_traced_and_measured(tmp_path: pathlib.Path) -> None:
    endpoint = os.environ.get("ETL_TEST_OTEL_URL")
    directory = os.environ.get("ETL_TEST_OTEL_DIR")
    if not endpoint or not directory:
        pytest.skip("set ETL_TEST_OTEL_URL and ETL_TEST_OTEL_DIR")
    out = pathlib.Path(directory)
    service = f"etl-test-{uuid.uuid4().hex[:8]}"
    legacy = sa.create_engine(f"sqlite:///{tmp_path / 'legacy.db'}")
    crm = sa.create_engine(f"sqlite:///{tmp_path / 'crm.db'}")
    demo.seed(legacy, crm)
    settings = {
        "service_name": service,
        "logging": {"otlp": {"endpoint": endpoint, "export_seconds": 1}},
        "databases": {
            "legacy": {"url": str(legacy.url)},
            "crm": {"url": str(crm.url)},
        },
        "bookkeeping": {"database": "crm", "create": True},
        "modules": {
            "crm": {
                "module": "modules.crm:create",
                "settings": {
                    "mappings": str(
                        ROOT / "mappings" / "compiled" / "crm.json"
                    ),
                    "source_tables": {
                        "customer": "CUST_MASTER",
                        "order": "ORD_MASTER",
                    },
                    "target_tables": {"customer": "customer", "order": "order"},
                    "max_reject_rate": 0.6,
                    "orphans": "park",
                },
            }
        },
    }
    try:
        with bootstrap.bootstrap(
            "test", directory=tmp_path / "none", overrides=settings
        ) as application:
            outcome = application.kernel.run("crm.full_load")
    finally:
        legacy.dispose()
        crm.dispose()
    assert outcome.result is not None

    def spans() -> list[dict[str, Any]]:
        return [
            span
            for payload in _lines(out / "traces.jsonl")
            for span in _resource_items(payload, "traces", service)
        ]

    found = _wait(
        lambda: [s for s in spans() if s["name"] == "etl.step"] or None
    )
    pipeline = next(s for s in spans() if s["name"] == "etl.pipeline")
    assert {s["traceId"] for s in found} == {pipeline["traceId"]}
    assert len(found) == 2  # customer, order.

    def loaded() -> list[dict[str, Any]]:
        return [
            metric
            for payload in _lines(out / "metrics.jsonl")
            for metric in _resource_items(payload, "metrics", service)
            if metric["name"] == "etl.rows.loaded"
        ]

    metric = _wait(lambda: loaded() or None)[0]
    total = sum(int(point["asInt"]) for point in metric["sum"]["dataPoints"])
    assert total == 4  # 2 customers, 2 orders.

    def logs() -> list[dict[str, Any]]:
        return [
            record
            for payload in _lines(out / "logs.jsonl")
            for record in _resource_items(payload, "logs", service)
            if record.get("traceId") == pipeline["traceId"]
        ]

    assert any(
        "step customer" in record["body"]["stringValue"]
        for record in _wait(lambda: logs() or None)
    )
