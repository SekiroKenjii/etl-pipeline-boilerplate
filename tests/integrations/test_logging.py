"""Logs reach a real backend by OpenTelemetry: Seq, which takes OTLP.

Runs when `ETL_TEST_SEQ_URL` is set (`make db-up` starts Seq).
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any
import uuid

import httpx
import pytest

from core import observability
from infra import observability as infra_observability
from tests.factories import fakes


def _events(seq: str, marker: str) -> list[dict[str, Any]]:
    """The events of Seq whose message holds `marker`, once they arrive."""
    deadline = time.monotonic() + 20
    while True:
        response = httpx.get(
            f"{seq}/api/events",
            params={"count": 200, "render": "true"},
            headers={"Accept": "application/json"},
        )
        response.raise_for_status()
        found = [
            event
            for event in response.json()
            if marker in event.get("RenderedMessage", "")
        ]
        if found or time.monotonic() > deadline:
            return found
        time.sleep(0.5)


def test_logs_reach_seq_with_their_context() -> None:
    seq = os.environ.get("ETL_TEST_SEQ_URL")
    if not seq:
        pytest.skip("set ETL_TEST_SEQ_URL to run the Seq test")
    marker = uuid.uuid4().hex
    configured = infra_observability.configure_logging(
        service="etl-test",
        environment="test",
        otlp=infra_observability.Otlp(f"{seq}/ingest/otlp"),
    )
    ctx = fakes.make_context("crm")
    try:
        with observability.log_context(**observability.run_fields(ctx)):
            logging.getLogger("etl.test").warning(
                "loaded %s with secret %s",
                marker,
                "postgresql://etl:hunter2@db/crm",
                extra={"etl.rows": 42},
            )
    finally:
        configured.stop()

    (event,) = _events(seq, marker)
    properties = {p["Name"]: p["Value"] for p in event["Properties"]}
    flat = repr(properties)
    assert event["Level"] in ("Warning", "warning", "WARN")
    assert ctx.run.run_id in flat
    assert "42" in flat
    assert "hunter2" not in event["RenderedMessage"]
