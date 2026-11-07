"""What every issue sink must do: one conformance check for all of them."""

from __future__ import annotations

from collections.abc import Callable
import datetime

from core import contract
from tests.factories import fakes


def _issue(
    record_id: str | None, value: object = "31/02/2020"
) -> contract.QualityIssue:
    return contract.QualityIssue(
        "mapping_issues",
        contract.Checkpoint.AFTER_TRANSFORM,
        contract.QualityAction.REJECT,
        "not a date",
        field="born",
        value=value,
        record_id=record_id,
    )


def check_issue_sink(
    sink: contract.IIssueSink,
    record_ids: Callable[[], list[str | None]],
) -> None:
    """Writes issues, erases those of some records, purges the old ones.

    Args:
        sink: The sink.
        record_ids: Reads back the record ids of the issues stored, in any
            order, from wherever the sink stores them.
    """
    first = fakes.make_context("crm.full_load")
    second = fakes.make_context("crm.sync")
    sink.write([_issue("1003"), _issue("1004"), _issue(None)], first)
    sink.write([_issue("1003", value={"phone": "0901"}), _issue("O-3")], second)

    assert sorted(record_ids(), key=str) == sorted(
        ["1003", "1003", "1004", None, "O-3"], key=str
    )
    # A right to erasure: every issue of the records, in every run.
    assert sink.erase(["1003", "O-3", "nope"]) == 3
    assert sink.erase(["1003"]) == 0
    assert sink.erase([]) == 0
    assert sorted(record_ids(), key=str) == sorted(["1004", None], key=str)

    an_hour_ago = datetime.datetime.now(datetime.UTC) - datetime.timedelta(
        hours=1
    )
    assert sink.purge(an_hour_ago) == 0
    later = datetime.datetime.now(datetime.UTC) + datetime.timedelta(minutes=1)
    assert sink.purge(later) == 2
    assert record_ids() == []
