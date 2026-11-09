"""Why a run failed reaches whoever asks: `etl run`'s text and JSON summary,
the ledger, `etl runs --run`; a refusal is logged without a traceback."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
import io
import json
import logging

import pytest

from app import bootstrap
from cli import run as cli_run
from config import settings as config
from core import contract
from core import pipeline
from core import runtime
from core.observability import listener as run_listener
from core.operators import reconciliation
from core.runtime import names

HINT = "`--param confirm_deletes=crm.customer:60`"
REFUSAL = (
    "MissingSourceRows(legacy.CUSTOMER): refusing to delete 60 of the 100"
    " rows of crm.customer loaded: 60.0% of them, above 10.0%. Check the"
    f" source; if they are deleted indeed, run again with {HINT}"
)


class _Steps(pipeline.BaseOrchestrator):

    def __init__(self, error: Exception) -> None:
        super().__init__(sleep=lambda seconds: None)
        self._error = error

    def steps(self) -> Sequence[contract.PipelineStep]:
        return [contract.PipelineStep("deletes", self._raise)]

    def _raise(
        self, ctx: contract.ETLContext, upstream: object
    ) -> contract.ETLResult:
        del ctx, upstream  # Unused.
        raise self._error


class _Module(runtime.Module):

    def __init__(self, error: Exception) -> None:
        self._error = error

    @property
    def name(self) -> str:
        return "m"

    def pipelines(self) -> dict[str, runtime.PipelineFactory]:
        return {"p": lambda resources: _Steps(self._error)}


def _application(
    error: Exception,
) -> tuple[bootstrap.Application, runtime.InMemoryRunLedger]:
    """The CLI's wiring: a kernel with the logging listener and a ledger."""
    ledger = runtime.InMemoryRunLedger()
    resources = runtime.Resources(
        [runtime.ResourceSpec(names.LEDGER, lambda r: ledger)]
    )
    kernel = runtime.Kernel(
        resources, [_Module(error)], listeners=[run_listener.LoggingListener()]
    )
    return (
        bootstrap.Application(config.AppSettings(), resources, kernel),
        ledger,
    )


def _args(output_format: str) -> argparse.Namespace:
    return argparse.Namespace(
        pipeline="m.p",
        since=None,
        until=None,
        dry_run=False,
        param=[],
        step=[],
        resume=False,
        label=None,
        rekey=False,
        partial_ok=False,
        format=output_format,
    )


def _run(
    application: bootstrap.Application, output_format: str
) -> tuple[int, str]:
    out = io.StringIO()
    code = cli_run._run(application.kernel, _args(output_format), out)
    return int(code), out.getvalue()


def _warnings(
    caplog: pytest.LogCaptureFixture, text: str
) -> list[logging.LogRecord]:
    return [
        record
        for record in caplog.records
        if record.levelno >= logging.WARNING and text in record.getMessage()
    ]


def test_a_refusal_tells_its_reason_everywhere(
    caplog: pytest.LogCaptureFixture,
) -> None:
    application, ledger = _application(
        reconciliation.DeletesNotConfirmed(REFUSAL)
    )

    with caplog.at_level(logging.INFO):
        code, text = _run(application, "text")
        json_code, output = _run(application, "json")

    assert (code, json_code) == (2, 2)
    assert HINT in text, text
    summary = json.loads(output.splitlines()[-1])
    assert summary["exit_code"] == 2
    assert (
        summary["message"] == f"deletes: failed after 1 attempt(s): {REFUSAL}"
    )
    record = ledger.get(summary["run_id"])
    assert record is not None
    assert record.message == summary["message"]
    assert record.steps["deletes"].message == (
        f"failed after 1 attempt(s): {REFUSAL}"
    )
    shown = io.StringIO()
    cli_run._run_steps(application, summary["run_id"], shown)
    assert shown.getvalue().count(HINT) == 2, shown.getvalue()
    logged = _warnings(caplog, HINT)
    assert logged
    assert all(record.exc_info is None for record in logged), logged


def test_any_other_error_keeps_its_traceback_in_the_logs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    application, ledger = _application(contract.FatalError("no table customer"))

    with caplog.at_level(logging.INFO):
        code, text = _run(application, "text")

    assert code == 1
    assert "deletes: failed after 1 attempt(s): no table customer" in text
    (record,) = ledger.recent("m.p")
    assert (
        record.message
        == "deletes: failed after 1 attempt(s): no table customer"
    )
    logged = _warnings(caplog, "no table customer")
    assert logged
    assert any(record.exc_info for record in logged)


def test_a_json_summary_keeps_its_shape() -> None:
    application, _ = _application(contract.FatalError("boom"))

    _, output = _run(application, "json")

    summary = json.loads(output.splitlines()[-1])
    assert set(summary) == {
        "pipeline",
        "run_id",
        "status",
        "exit_code",
        "resumed_from",
        "stats",
        "message",
    }
    assert summary["status"] == "failed"
