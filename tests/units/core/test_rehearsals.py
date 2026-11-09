"""Cutover rehearsals: sealed reports, compared; the HTML page."""

from __future__ import annotations

import datetime
from typing import Any

import pytest

from core import cutover


def _seal(report: dict[str, Any]) -> dict[str, Any]:
    """A report sealed, as JSON its reader indexes."""
    return cutover.seal(report)


def _report(**entity: Any) -> dict[str, Any]:
    customer = {
        "entity": "customer",
        "status": "mismatch",
        "source_rows": 4,
        "rejected": 2,
        "matched": 2,
        "missing": 0,
        **entity,
    }
    return {
        "module": "crm",
        "generated_at": "2026-11-02T10:00:00+00:00",
        "passed": customer["status"] == "match",
        "entities": [customer],
        "runs": [],
    }


class TestSeal:

    def test_a_report_sealed_again_verifies_alone(self) -> None:
        # Why the digest is recorded in the run ledger too: alone, it shows
        # an edit, not an edit sealed again (`etl cutover verify`).
        sealed = _seal(_report())
        forged = _seal({**sealed, "passed": True})

        assert cutover.verify(forged)
        assert forged["digest"] != sealed["digest"]

    def test_a_sealed_report_verifies_until_changed(self) -> None:
        sealed = _seal(_report())

        assert cutover.verify(sealed)
        assert len(sealed["digest"]) == 64
        assert _seal(_report())["digest"] == sealed["digest"]
        sealed["entities"][0]["rejected"] = 0
        assert not cutover.verify(sealed)

    def test_an_unsealed_report_does_not_verify(self) -> None:
        assert not cutover.verify(_report())


class TestCompare:

    def test_what_changed_between_rehearsals(self) -> None:
        before = _seal(_report())
        after = _seal(_report(status="match", rejected=0, matched=4))
        after["entities"].append({"entity": "order", "status": "match"})

        comparison = cutover.compare(before, after)
        text = cutover.comparison_markdown(comparison)

        assert comparison.passed == (False, True)
        customer, order = comparison.changed
        assert (customer.before, customer.after) == ("mismatch", "match")
        assert customer.deltas == {"rejected": -2, "matched": 2}
        assert (order.before, order.after) == (None, "match")
        assert (
            "| customer | mismatch → match | rejected -2, matched +2 |" in text
        )
        assert before["digest"][:16] in text

    def test_nothing_changed(self) -> None:
        report = _seal(_report())

        comparison = cutover.compare(report, report)

        assert comparison.changed == []
        assert "Nothing changed." in cutover.comparison_markdown(comparison)

    def test_what_produced_them_and_their_verdicts(self) -> None:
        before = {
            **_report(),
            "provenance": {
                "env": "uat",
                "module": {"mappings": "abc", "keys": "k1"},
                "databases": {"target": [{"name": "crm", "host": "a"}]},
            },
        }
        after = {
            **_report(status="match", rejected=0),
            "verdict": "PASSED",
            "provenance": {
                "env": "uat",
                "module": {"mappings": "def", "keys": "k1"},
                "databases": {"target": [{"name": "crm", "host": "b"}]},
            },
        }

        comparison = cutover.compare(before, after)
        text = cutover.comparison_markdown(comparison)

        # An older report has no verdict: its `passed` tells it.
        assert comparison.verdicts == ("FAILED", "PASSED")
        assert comparison.provenance == {
            "module.mappings": ("abc", "def"),
            "databases.target": (
                [{"name": "crm", "host": "a"}],
                [{"name": "crm", "host": "b"}],
            ),
        }
        assert "- module.mappings: `abc` → `def`" in text
        assert "FAILED, digest" in text
        assert "PASSED, digest" in text

    def test_reports_of_other_modules_do_not_compare(self) -> None:
        crm = _report()
        other = {**crm, "module": "billing"}

        with pytest.raises(ValueError, match="'crm' and 'billing'"):
            cutover.compare(crm, other)


def test_the_html_page_escapes_what_it_shows() -> None:
    entity = cutover.EntityReport(
        "customer<script>",
        source_rows=4,
        rejected=1,
        accepted=1,
        rejections={"name: <b>empty</b>": 1},
        expected_rows=3,
        target_rows=3,
        matched=3,
    )
    report = cutover.CutoverReport(
        "crm",
        datetime.datetime(2026, 11, 2, tzinfo=datetime.UTC),
        [entity],
        provenance={"env": "<uat>", "module": {"mappings": "abc"}},
    )

    page = cutover.to_html(report, "ab" * 32)

    assert page.startswith("<!doctype html>")
    assert '"verdict match-with-rejects">PASSED-WITH-REJECTS<' in page
    assert "<li>env: <code>&lt;uat&gt;</code></li>" in page
    assert "<li>module.mappings: <code>abc</code></li>" in page
    assert "<script>" not in page
    assert "customer&lt;script&gt;" in page
    assert "name: &lt;b&gt;empty&lt;/b&gt;" in page
    assert "ab" * 32 in page
