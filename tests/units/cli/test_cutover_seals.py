"""Sealed cutover reports across a change of the canonical form."""

from __future__ import annotations

import pathlib
from typing import Any

from cli import cutover as cli_cutover
from core import cutover
from core import runtime
from core.cutover import checksum

# pylint: disable=protected-access


def _report(provenance: dict[str, Any], key: object) -> dict[str, Any]:
    return {
        "module": "crm",
        "generated_at": "2026-11-01T00:00:00+00:00",
        "verdict": "FAILED",
        "passed": False,
        "provenance": provenance,
        "entities": [{
            "entity": "customer",
            "status": "mismatch",
            "different": 1,
            "samples": [{"kind": "different", "key": [key], "columns": {}}],
        }],
        "runs": [],
    }


def test_a_report_sealed_before_the_canonical_form_changed_verifies(
    tmp_path: pathlib.Path,
) -> None:
    ledger = runtime.InMemoryRunLedger()
    old = cutover.seal(_report({"env": "uat"}, "1.5"))
    new = cutover.seal(
        _report({"env": "uat", "canonical_form": checksum.FORM}, 1.5)
    )
    for sealed in (old, new):
        cli_cutover._record(ledger, sealed, tmp_path / f"{sealed['digest']}")

    old_status, old_detail = cli_cutover._check(ledger, old)
    new_status, new_detail = cli_cutover._check(ledger, new)
    comparison = cutover.compare(old, new)

    assert (old_status, new_status) == ("OK", "OK")
    assert old_detail.endswith("; env uat, canonical form 1")
    assert new_detail.endswith(f"; env uat, canonical form {checksum.FORM}")
    assert comparison.provenance == {"canonical_form": (None, checksum.FORM)}
    assert "- canonical_form: - → `2`" in cutover.comparison_markdown(
        comparison
    )
