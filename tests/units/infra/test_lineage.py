"""OpenLineage events of runs and steps, in memory and over HTTP."""

from __future__ import annotations

from collections.abc import Generator, Sequence
import http.server
import json
import threading
from typing import Any
import uuid

import pytest

from core import contract
from core import pipeline
from infra.observability import lineage
from tests.factories import fakes

LEGACY = contract.Dataset("legacy", "CUST_MASTER")
CRM = contract.Dataset("crm", "customer")


class _Pipeline(pipeline.BaseOrchestrator):

    def __init__(self, listener: lineage.LineageListener, fail: bool) -> None:
        super().__init__(listeners=[listener])
        self._fail = fail

    def steps(self) -> Sequence[contract.PipelineStep]:
        def step(ctx: contract.ETLContext, upstream: Any) -> Any:
            if self._fail:
                raise contract.FatalError("source gone")
            result = contract.ETLResult()
            result.complete(contract.ETLStatus.SUCCESS)
            return result

        return [
            contract.PipelineStep(
                "customer", step, inputs=(LEGACY,), outputs=(CRM,)
            )
        ]


def _run(
    fail: bool = False, run_id: str | None = None
) -> tuple[list[dict[str, Any]], str]:
    events: list[dict[str, Any]] = []
    ctx = fakes.make_context("crm.full_load")
    if run_id is not None:
        ctx = contract.ETLContext(
            contract.ETLRunInfo(pipeline_name="crm.full_load", run_id=run_id)
        )
    _Pipeline(lineage.LineageListener(events.append, "etl"), fail).run(ctx)
    return events, events[0]["run"]["runId"]


def test_runs_and_steps_start_then_complete() -> None:
    events, run_id = _run()

    assert [(e["eventType"], e["job"]["name"]) for e in events] == [
        ("START", "crm.full_load"),
        ("START", "crm.full_load.customer"),
        ("COMPLETE", "crm.full_load.customer"),
        ("COMPLETE", "crm.full_load"),
    ]
    step = events[1]
    assert step["inputs"] == [{"namespace": "legacy", "name": "CUST_MASTER"}]
    assert step["outputs"] == [{"namespace": "crm", "name": "customer"}]
    assert step["run"]["facets"]["parent"]["run"]["runId"] == run_id
    assert events[0]["run"]["runId"] == run_id
    assert all(
        e["schemaURL"].startswith("https://openlineage.io/") for e in events
    )
    uuid.UUID(step["run"]["runId"])  # A UUID, as the specification wants.


@pytest.mark.parametrize(
    "run_id", ["crm-full-load-29012345", "night-2026-11-03", "x"]
)
def test_run_ids_that_are_not_uuids_stand_for_stable_uuids(
    run_id: str,
) -> None:
    events, derived = _run(run_id=run_id)
    again, _ = _run(run_id=run_id)

    assert len(events) == 4  # Every event sent, none lost to the id.
    assert uuid.UUID(derived).version == 5
    assert [e["run"]["runId"] for e in events] == [
        e["run"]["runId"] for e in again
    ]
    assert events[1]["run"]["facets"]["parent"]["run"]["runId"] == derived


def test_uuid_run_ids_are_kept() -> None:
    run_id = str(uuid.uuid4())

    _, sent = _run(run_id=run_id)

    assert sent == run_id


def test_errors_are_redacted() -> None:
    events: list[dict[str, Any]] = []

    class Leaky(pipeline.BaseOrchestrator):

        def steps(self) -> Sequence[contract.PipelineStep]:
            def step(ctx: contract.ETLContext, upstream: Any) -> Any:
                raise contract.FatalError("no postgresql://etl:S3cr3t@db/x")

            return [contract.PipelineStep("customer", step)]

    Leaky(listeners=[lineage.LineageListener(events.append)]).run(
        fakes.make_context("crm")
    )

    assert "S3cr3t" not in json.dumps(events)
    assert "etl:***@db" in json.dumps(events)


def test_errors_hide_the_secret_names_of_the_settings() -> None:
    events: list[dict[str, Any]] = []

    class Leaky(pipeline.BaseOrchestrator):

        def steps(self) -> Sequence[contract.PipelineStep]:
            def step(ctx: contract.ETLContext, upstream: Any) -> Any:
                raise contract.FatalError("bad row: iban=DE8937040044")

            return [contract.PipelineStep("customer", step)]

    listener = lineage.LineageListener(events.append, redact=("iban",))
    Leaky(listeners=[listener]).run(fakes.make_context("crm"))

    assert [e for e in events if e["eventType"] == "FAIL"]
    assert "DE8937040044" not in json.dumps(events)


def test_failures_carry_their_error() -> None:
    events, _ = _run(fail=True)

    failed = [e for e in events if e["eventType"] == "FAIL"]

    assert [e["job"]["name"] for e in failed] == [
        "crm.full_load.customer",
        "crm.full_load",
    ]
    assert "source gone" in str(failed[0]["run"]["facets"]["errorMessage"])


def test_a_backend_down_never_fails_a_run() -> None:
    def broken(event: dict[str, Any]) -> None:
        raise OSError("connection refused")

    ctx = fakes.make_context("crm.full_load")
    result = _Pipeline(lineage.LineageListener(broken), False).run(ctx)

    assert result.status is contract.ETLStatus.SUCCESS


@pytest.fixture
def server() -> Generator[tuple[str, list[dict[str, Any]]]]:
    received: list[dict[str, Any]] = []

    class Handler(http.server.BaseHTTPRequestHandler):

        # pylint: disable-next=invalid-name  # The http.server API.
        def do_POST(self) -> None:
            body = self.rfile.read(int(self.headers["Content-Length"]))
            received.append({
                "path": self.path,
                "auth": self.headers.get("Authorization"),
                "event": json.loads(body),
            })
            self.send_response(201)
            self.end_headers()

        def log_message(self, format: str, *args: Any) -> None:  # pylint: disable=redefined-builtin
            del format, args  # Quiet.

    httpd = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{httpd.server_port}", received
    httpd.shutdown()
    httpd.server_close()


def test_events_are_posted_to_the_endpoint(
    server: tuple[str, list[dict[str, Any]]],
) -> None:
    url, received = server
    send = lineage.http_transport(url, api_key="k3y")

    send({"eventType": "START"})

    assert received == [{
        "path": "/api/v1/lineage",
        "auth": "Bearer k3y",
        "event": {"eventType": "START"},
    }]
    with pytest.raises(ValueError, match="not an HTTP endpoint"):
        lineage.http_transport("file:///etc/passwd")


@pytest.fixture
def redirecting() -> Generator[tuple[str, list[str | None]]]:
    """A server redirecting every post to itself on another path; yields its
    URL and the Authorization headers of the requests it got."""
    seen: list[str | None] = []

    class Handler(http.server.BaseHTTPRequestHandler):

        def _answer(self) -> None:
            seen.append(self.headers.get("Authorization"))
            length = int(self.headers.get("Content-Length") or 0)
            self.rfile.read(length)
            if self.path.startswith("/elsewhere"):
                self.send_response(200)
            else:
                self.send_response(302)
                self.send_header("Location", "/elsewhere")
            self.end_headers()

        do_POST = do_GET = _answer  # noqa: N815 - the http.server API.

        def log_message(self, format: str, *args: Any) -> None:  # pylint: disable=redefined-builtin
            del format, args  # Quiet.

    httpd = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{httpd.server_port}", seen
    httpd.shutdown()
    httpd.server_close()


def test_redirects_are_not_followed_with_the_token(
    redirecting: tuple[str, list[str | None]],
) -> None:
    url, seen = redirecting
    send = lineage.http_transport(url, api_key="k3y")

    with pytest.raises(OSError, match="HTTP 302 .*redirect, not followed"):
        send({"eventType": "START"})

    assert seen == ["Bearer k3y"]  # The redirect was not followed.
