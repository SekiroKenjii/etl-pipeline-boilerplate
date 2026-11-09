"""What every quarantine must do: one conformance check for all of them."""

from __future__ import annotations

import datetime
from typing import Any, cast

from core import contract
from tests.factories import fakes

OPEN = contract.QuarantineStatus.OPEN
REPLAYED = contract.QuarantineStatus.REPLAYED
CLOSED = contract.QuarantineStatus.CLOSED


def fields(entry: contract.QuarantineEntry) -> dict[str, Any]:
    """The record of an entry: a dict, as rows and mapped records are kept."""
    assert isinstance(entry.record, dict), entry.record
    return cast(dict[str, Any], entry.record)


def _rejected(
    key: str | None,
    *,
    message: str = "not a date",
    part: int = 0,
    name: str = "Chi",
    checkpoint: contract.Checkpoint = contract.Checkpoint.AFTER_TRANSFORM,
) -> contract.RejectedRecord[dict[str, object]]:
    issue = contract.QualityIssue(
        "mapping",
        checkpoint,
        contract.QualityAction.REJECT,
        message,
        field="born",
        value="31/02/2020",
    )
    return contract.RejectedRecord(
        {
            "mapping": "customer",
            "entity": "crm.customer",
            "source_key": key,
            "part": part,
            "deleted": False,
            "data": {"name": name},
        },
        issue.checkpoint,
        (issue,),
    )


def check_quarantine(quarantine: contract.IQuarantine) -> None:
    """Stores, reads, resolves and purges records, as the port says."""
    first = fakes.make_context("crm.full_load")
    second = fakes.make_context("crm.sync")
    quarantine.put([_rejected("1003"), _rejected("1004")], first)
    quarantine.put([_rejected("1005")], second)

    entries = list(quarantine.entries())
    assert [fields(e)["source_key"] for e in entries] == [
        "1003",
        "1004",
        "1005",
    ]
    one = entries[0]
    assert (one.pipeline, one.run_id, one.status) == (
        "crm.full_load",
        first.run.run_id,
        OPEN,
    )
    assert one.checkpoint == "after_transform"
    assert fields(one)["data"] == {"name": "Chi"}
    assert one.issues[0]["message"] == "not a date"
    assert one.rejected_at.tzinfo is not None
    assert [
        fields(e)["source_key"] for e in quarantine.entries("crm.sync")
    ] == ["1005"]

    assert quarantine.resolve([one.id], REPLAYED) == 1
    assert quarantine.resolve([one.id], REPLAYED) == 0  # Already.
    assert quarantine.resolve([entries[1].id], CLOSED) == 1
    assert [e.id for e in quarantine.entries()] == [entries[2].id]
    replayed = list(quarantine.entries(status=REPLAYED))
    assert [e.id for e in replayed] == [one.id]
    assert replayed[0].resolved_at is not None
    assert len(list(quarantine.entries(status=None))) == 3

    assert quarantine.erase([entries[2].id]) == 1
    assert quarantine.erase([entries[2].id]) == 0
    assert [e.id for e in quarantine.entries(status=None)] == [
        one.id,
        entries[1].id,
    ]

    an_hour_ago = one.rejected_at - datetime.timedelta(hours=1)
    assert quarantine.purge(an_hour_ago) == 0
    later = datetime.datetime.now(datetime.UTC) + datetime.timedelta(minutes=1)
    assert quarantine.purge(later) == 2
    assert list(quarantine.entries(status=None)) == []

    check_rejected_again(quarantine)


def check_rejected_again(quarantine: contract.IQuarantine) -> None:
    """A record rejected again refreshes its open entry: never a second one.

    The rows at the last watermark are read again by the next run, so a
    rejected row at that boundary is rejected twice; a resumed run, too.
    """
    first = fakes.make_context("crm.full_load")
    second = fakes.make_context("crm.full_load")
    other = fakes.make_context("crm.sync")
    quarantine.put([_rejected("1003"), _rejected("1004")], first)
    quarantine.put([_rejected(None), _rejected(None, name="Anh")], first)
    quarantine.put(
        [
            _rejected("1003", message="still not a date", name="Chi B"),
            _rejected("1003", message="twice in a batch", name="Chi C"),
            _rejected("1003", part=1),  # Another row of a split.
            _rejected("1003", checkpoint=contract.Checkpoint.AFTER_EXTRACT),
            _rejected(None),  # The same data, no source key.
        ],
        second,
    )
    quarantine.put([_rejected("1003")], other)  # Another pipeline.

    entries = list(quarantine.entries(status=None))
    assert all(e.status is OPEN for e in entries)
    found = {
        (
            e.pipeline,
            e.checkpoint,
            fields(e)["source_key"],
            fields(e)["part"],
            fields(e)["data"]["name"],
        ): e
        for e in entries
    }
    assert sorted(found, key=str) == sorted(
        [
            ("crm.full_load", "after_transform", "1003", 0, "Chi C"),
            ("crm.full_load", "after_transform", "1004", 0, "Chi"),
            ("crm.full_load", "after_transform", None, 0, "Chi"),
            ("crm.full_load", "after_transform", None, 0, "Anh"),
            ("crm.full_load", "after_transform", "1003", 1, "Chi"),
            ("crm.full_load", "after_extract", "1003", 0, "Chi"),
            ("crm.sync", "after_transform", "1003", 0, "Chi"),
        ],
        key=str,
    )
    assert len(entries) == len(found)
    again = found["crm.full_load", "after_transform", "1003", 0, "Chi C"]
    assert again.run_id == second.run.run_id
    assert [i["message"] for i in again.issues] == ["twice in a batch"]
    same = [e for e in entries if fields(e)["source_key"] == "1003"]
    assert len({e.key for e in same}) == len(same)

    # Once replayed, a record rejected again is a new open entry.
    assert quarantine.resolve([again.id], REPLAYED) == 1
    quarantine.put([_rejected("1003")], second)
    mine = [
        e
        for e in quarantine.entries("crm.full_load", status=None)
        if (e.checkpoint, fields(e)["source_key"], fields(e)["part"])
        == ("after_transform", "1003", 0)
    ]
    assert sorted(e.status for e in mine) == [OPEN, REPLAYED]
    assert len({e.id for e in mine}) == 2
    later = datetime.datetime.now(datetime.UTC) + datetime.timedelta(minutes=1)
    quarantine.purge(later)
