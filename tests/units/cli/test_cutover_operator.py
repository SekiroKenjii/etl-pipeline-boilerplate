"""Who saved a cutover report: the operator, as for every audited command."""

from __future__ import annotations

import pathlib

import pytest

from cli import audit
from cli import cutover as cli_cutover
from core import cutover
from core import runtime

# pylint: disable=protected-access


def test_a_saved_report_is_recorded_with_the_operator(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(audit.OPERATOR_VARIABLE, "CHG-1042 jane.doe")
    ledger = runtime.InMemoryRunLedger()
    sealed = cutover.seal({
        "module": "crm",
        "generated_at": "2026-11-01T00:00:00+00:00",
        "verdict": "PASSED",
        "entities": [],
    })

    cli_cutover._record(ledger, sealed, tmp_path / "report.json")
    status, detail = cli_cutover._check(ledger, sealed)

    record = ledger.get(sealed["digest"])
    assert record is not None
    assert record.meta["user"] == "CHG-1042 jane.doe"
    assert status == "OK"
    assert "saved by CHG-1042 jane.doe" in detail
