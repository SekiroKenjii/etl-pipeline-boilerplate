"""Lineage of runs as OpenLineage events: to Marquez, DataHub, OpenMetadata.

Each run and each step sends START, then COMPLETE, FAIL or ABORT: a job per
pipeline and per step (`crm.full_load`, `crm.full_load.customer`), the
step's run pointing to its pipeline's, with the datasets each step reads and
writes.

Events go by HTTP from the steps' threads; a backend that does not answer is
logged, never fails a run. A run id that is not a UUID stands for a UUID
derived from it and the pipeline, stable across attempts and resumes. Error
messages are redacted before they leave.
"""

from __future__ import annotations

from collections.abc import Callable, Collection, Mapping, Sequence
import datetime
import email.message
import json
import logging
import threading
from typing import IO
import urllib.error
import urllib.request
import uuid

from core import contract
from core import observability

PRODUCER = "https://github.com/example/etl-pipeline-boilerplate"
_SCHEMA = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
_PARENT = (
    "https://openlineage.io/spec/facets/1-1-0/ParentRunFacet.json"
    "#/$defs/ParentRunFacet"
)
_ERROR = (
    "https://openlineage.io/spec/facets/1-0-1/ErrorMessageRunFacet.json"
    "#/$defs/ErrorMessageRunFacet"
)
# Never change it: runs already sent would get other ids.
_RUN_NAMESPACE = uuid.UUID("5d1f6a1e-2c4b-5e8a-9f3d-0b7c6e4a2d91")
_LOG = logging.getLogger(__name__)

type _Transport = Callable[[dict[str, object]], None]


def http_transport(
    url: str,
    *,
    api_key: str | None = None,
    timeout_seconds: float = 5.0,
) -> _Transport:
    """Posts events to an OpenLineage HTTP endpoint.

    Args:
        url: The endpoint, e.g. `http://marquez:5000`; `/api/v1/lineage` is
            added if missing.
        api_key: Sent as a bearer token, if any.
        timeout_seconds: How long a post may take.

    Redirects are not followed: the token would go to wherever the
    endpoint points, over plain HTTP perhaps. A redirect fails the post
    (logged by the listener): configure the final URL.
    """
    endpoint = url.rstrip("/")
    if not endpoint.endswith("/api/v1/lineage"):
        endpoint += "/api/v1/lineage"
    if not endpoint.startswith(("http://", "https://")):
        raise ValueError(f"not an HTTP endpoint: {url}")
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    opener = urllib.request.build_opener(_NoRedirects())

    def send(event: dict[str, object]) -> None:
        request = urllib.request.Request(
            endpoint,
            data=json.dumps(event).encode(),
            headers=headers,
            method="POST",
        )
        # An HTTP(S) URL, checked above.
        try:
            with opener.open(  # nosec B310
                request, timeout=timeout_seconds
            ) as response:
                response.read()  # Drained: the connection can be reused.
        except urllib.error.HTTPError as exc:
            with exc:  # Its body is a file: closed.
                redirect = 300 <= exc.code < 400
                note = ": a redirect, not followed" if redirect else ""
                raise OSError(
                    f"HTTP {exc.code} from {endpoint}{note}"
                ) from None

    return send


class _NoRedirects(urllib.request.HTTPRedirectHandler):
    """Refuses redirects: a 3xx is an `HTTPError`, the token stays here."""

    def redirect_request(  # pylint: disable=too-many-positional-arguments
        self,
        req: urllib.request.Request,
        fp: IO[bytes],
        code: int,
        msg: str,
        headers: email.message.Message,
        newurl: str,
    ) -> None:
        del req, fp, code, msg, headers, newurl


class LineageListener(contract.PipelineListener):
    """Sends the OpenLineage events of runs and steps.

    Attributes:
        namespace: The namespace of the jobs: the service, by default.
        redact: More secret names hidden in the errors, on top of
            `observability.SECRET_NAMES`: the logs' `redact`.
    """

    def __init__(
        self,
        transport: _Transport,
        namespace: str = "etl",
        redact: Collection[str] = (),
    ) -> None:
        self.namespace = namespace
        self.redact = tuple(redact)
        self._send = transport
        self._lock = threading.Lock()

    def on_pipeline_start(self, ctx: contract.ETLContext) -> None:
        self._emit("START", ctx.run.pipeline_name, _run_id(ctx))

    def on_pipeline_end(
        self, result: contract.PipelineResult, ctx: contract.ETLContext
    ) -> None:
        self._emit(
            _event_type(result.status),
            ctx.run.pipeline_name,
            _run_id(ctx),
            error=None
            if result.succeeded
            else _error(result.message, result.errors, self.redact),
        )

    def on_step_start(
        self,
        step: contract.PipelineStep,
        ctx: contract.ETLContext,
        attempt: int,
    ) -> None:
        if attempt == 1:
            self._emit_step("START", step, ctx)

    def on_step_end(
        self,
        step: contract.PipelineStep,
        result: contract.ETLResult,
        ctx: contract.ETLContext,
    ) -> None:
        self._emit_step(
            _event_type(result.status),
            step,
            ctx,
            error=(
                _error(result.message, result.errors, self.redact)
                if result.status is contract.ETLStatus.FAILED
                else None
            ),
        )

    def _emit_step(
        self,
        event_type: str,
        step: contract.PipelineStep,
        ctx: contract.ETLContext,
        error: str | None = None,
    ) -> None:
        pipeline_name = ctx.run.pipeline_name
        run_id = _run_id(ctx)
        self._emit(
            event_type,
            f"{pipeline_name}.{step.name}",
            str(uuid.uuid5(uuid.UUID(run_id), step.name)),
            parent=(pipeline_name, run_id),
            inputs=step.inputs,
            outputs=step.outputs,
            error=error,
        )

    def _emit(
        self,
        event_type: str,
        job: str,
        run_id: str,
        *,
        parent: tuple[str, str] | None = None,
        inputs: tuple[contract.Dataset, ...] = (),
        outputs: tuple[contract.Dataset, ...] = (),
        error: str | None = None,
    ) -> None:
        """Sends one event; a failure to send is only logged."""
        facets: dict[str, object] = {}
        if parent is not None:
            facets["parent"] = {
                "_producer": PRODUCER,
                "_schemaURL": _PARENT,
                "run": {"runId": parent[1]},
                "job": {"namespace": self.namespace, "name": parent[0]},
            }
        if error:
            facets["errorMessage"] = {
                "_producer": PRODUCER,
                "_schemaURL": _ERROR,
                "message": error,
                "programmingLanguage": "python",
            }
        event: dict[str, object] = {
            "eventType": event_type,
            "eventTime": datetime.datetime.now(datetime.UTC).isoformat(),
            "producer": PRODUCER,
            "schemaURL": _SCHEMA,
            "run": {"runId": run_id, "facets": facets},
            "job": {"namespace": self.namespace, "name": job},
            "inputs": [_dataset(d) for d in inputs],
            "outputs": [_dataset(d) for d in outputs],
        }
        try:
            self._send(event)
        # Lineage never fails a run: report, and go on.
        except Exception as exc:  # pylint: disable=broad-exception-caught
            _LOG.warning("lineage event of %s not sent: %s", job, exc)


def _error(
    message: str | None,
    errors: Sequence[contract.ETLError],
    redact: Collection[str],
) -> str:
    """What went wrong, redacted: the first error, else the summary."""
    found = errors[0].message if errors else message or "failed"
    return observability.redact_text(found, redact)


def _run_id(ctx: contract.ETLContext) -> str:
    """The run as a UUID, as OpenLineage wants it.

    A run id that is not a UUID stands for one derived from it and the
    pipeline: stable across attempts, distinct across pipelines sharing a
    label.
    """
    try:
        return str(uuid.UUID(ctx.run.run_id))
    except ValueError:
        name = f"{ctx.run.pipeline_name}/{ctx.run.run_id}"
        return str(uuid.uuid5(_RUN_NAMESPACE, name))


def _event_type(status: contract.ETLStatus) -> str:
    """The OpenLineage event type of a status."""
    return {
        contract.ETLStatus.FAILED: "FAIL",
        contract.ETLStatus.CANCELLED: "ABORT",
    }.get(status, "COMPLETE")


def _dataset(dataset: contract.Dataset) -> Mapping[str, str]:
    return {"namespace": dataset.namespace, "name": dataset.name}
