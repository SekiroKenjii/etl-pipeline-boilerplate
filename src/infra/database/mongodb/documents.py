"""The documents of MongoDB, as the adapters write them and the server sends
them: read back with `cast`, so a renamed field is a type error."""

from __future__ import annotations

import datetime
from typing import NotRequired, TypedDict

from bson import objectid
from bson import timestamp

from core import contract

__all__ = [
    "ChangeEventDocument",
    "CountedDocument",
    "Document",
    "DuplicatesDocument",
    "KeyDocument",
    "KeyIdDocument",
    "ParkedDocument",
    "ParkedIdDocument",
    "QuarantineDocument",
    "ResumeToken",
    "RunDocument",
    "StateDocument",
    "StepDocument",
]

type Document = dict[str, object]
type ResumeToken = dict[str, contract.StateValue]


class KeyIdDocument(TypedDict):
    """The `_id` of a key of the crosswalk."""

    entity: str
    key: str


class KeyDocument(TypedDict):
    """A key of the crosswalk, `etl_key_registry`."""

    _id: KeyIdDocument
    natural_key: list[str]


class StateDocument(TypedDict):
    """A value of the state store, `etl_state`."""

    value: contract.StateValue


class ParkedIdDocument(TypedDict):
    """The `_id` of a parked record."""

    queue: str
    identity: str


class ParkedDocument(TypedDict):
    """A parked record, `etl_parked`."""

    _id: ParkedIdDocument
    payload: contract.StateValue
    reason: str
    attempts: int
    parked_at: datetime.datetime


class QuarantineDocument(TypedDict):
    """A rejected record, `etl_quarantine`."""

    _id: objectid.ObjectId
    pipeline: str
    run_id: str
    checkpoint: str
    record: contract.StateValue
    issues: list[dict[str, contract.StateValue]]
    rejected_at: datetime.datetime
    status: str
    resolved_at: NotRequired[datetime.datetime | None]


class DuplicatesDocument(TypedDict):
    """A group of the records quarantined twice, as `$group` answers."""

    ids: list[objectid.ObjectId]


class RunDocument(TypedDict):
    """A run of the ledger, `etl_runs`."""

    _id: str
    pipeline: str
    status: str
    started_at: datetime.datetime
    completed_at: NotRequired[datetime.datetime | None]
    stats: NotRequired[dict[str, int] | None]
    message: NotRequired[str | None]
    host: NotRequired[str]
    dry_run: NotRequired[bool]
    meta: NotRequired[dict[str, str] | None]


class StepDocument(TypedDict):
    """A step of a run, in its `steps`."""

    step: str
    status: str
    completed_at: datetime.datetime
    stats: NotRequired[dict[str, int] | None]
    message: NotRequired[str | None]
    host: NotRequired[str]
    attempts: NotRequired[int]


class ChangeEventDocument(TypedDict):
    """A change stream event, as the server sends it."""

    _id: ResumeToken
    operationType: str
    clusterTime: timestamp.Timestamp
    documentKey: contract.Row
    fullDocument: NotRequired[contract.Row | None]
    wallTime: NotRequired[datetime.datetime]


class CountedDocument(TypedDict):
    """What a `{"$count": "n"}` stage answers."""

    n: int
