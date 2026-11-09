"""MongoDB change streams, as a change source."""

from __future__ import annotations

from collections.abc import Iterator
import datetime
from typing import cast

from bson import timestamp

from core import contract
from infra.database.mongodb import client
from infra.database.mongodb import documents

_STREAM_ENDING_OPERATIONS = frozenset(
    {"drop", "rename", "dropDatabase", "invalidate"}
)


class MongoChangeStream(client.MongoAdapter, contract.IChangeSource):
    """The changes of a collection, from its change stream.

    Needs a replica set or a sharded cluster. Positions are resume tokens;
    inserts and updates come with the document as it is now
    (`updateLookup`), deletes with its `_id`. The oplog holds the changes for
    a while only: a position older than the oplog fails, and the collection
    needs a new snapshot (delete the state key). Size the oplog for the
    longest pause between two runs.
    """

    def __init__(
        self,
        db: client.MongoDatabase | str,
        collection: str,
        *,
        await_ms: int = 200,
    ) -> None:
        """Initializes the source.

        Args:
            db: A shared database, or a MongoDB URI.
            collection: The collection whose changes are read.
            await_ms: How long the server waits for changes before answering
                that there are none yet.
        """
        super().__init__(db)
        self.collection = collection
        self._await_ms = await_ms

    @property
    def table(self) -> str:
        return self.collection

    @property
    def name(self) -> str:
        return f"{type(self).__name__}({self.collection})"

    def position(self) -> contract.StateValue:
        """See `contract.IChangeSource.position`.

        A resume token, with the cluster time it stands for: tokens of
        events and of quiet streams do not compare, times do.
        """
        with client.errors(f"opening the change stream of {self.collection}"):
            with self.db.collection(self.collection).watch(
                max_await_time_ms=self._await_ms
            ) as stream:
                stream.try_next()
                token = dict(stream.resume_token or {})
            hello = self.db.client.admin.command("hello")
        return _position(token, hello["operationTime"])

    def changes(
        self, after: contract.StateValue, until: contract.StateValue = None
    ) -> Iterator[contract.ChangeEvent]:
        """See `contract.IChangeSource.changes`.

        Reads until the stream has no more changes, or reaches changes after
        `until`.

        Raises:
            FatalError: If the collection was dropped or renamed, or `after`
                is older than the oplog.
        """
        token, _ = _split(after)
        limit = _split(until)[1] if until is not None else None
        action = f"reading the changes of {self.collection}"
        with (
            client.errors(action),
            self.db.collection(self.collection).watch(
                resume_after=token,
                full_document="updateLookup",
                max_await_time_ms=self._await_ms,
            ) as stream,
        ):
            while True:
                found = stream.try_next()
                if found is None:
                    return
                event = cast(documents.ChangeEventDocument, found)
                time = event["clusterTime"]
                if limit is not None and (time.time, time.inc) > limit:
                    return
                change = self._event(event)
                if change is not None:
                    yield change

    def _event(
        self, event: documents.ChangeEventDocument
    ) -> contract.ChangeEvent | None:
        """Turns a change stream event into a change; None to skip it.

        Raises:
            FatalError: If the stream cannot go on.
        """
        operation = event["operationType"]
        if operation in _STREAM_ENDING_OPERATIONS:
            raise contract.FatalError(
                f"{self.collection}: change stream ended by {operation!r}"
            )
        key = event["documentKey"]["_id"]
        committed = event.get("wallTime")
        if operation == "delete":
            row = dict(event["documentKey"])
            kind = contract.ChangeOperation.DELETE
        elif operation in ("insert", "update", "replace"):
            document = event.get("fullDocument")
            if document is None:
                # Deleted since: its delete follows.
                return None
            row = document
            kind = (
                contract.ChangeOperation.INSERT
                if operation == "insert"
                else contract.ChangeOperation.UPDATE
            )
        else:
            return None
        position = _position(dict(event["_id"]), event["clusterTime"])
        return contract.ChangeEvent(
            table=self.collection,
            operation=kind,
            row=row,
            key=(key,),
            position=position,
            committed_at=(
                committed if isinstance(committed, datetime.datetime) else None
            ),
        )


def _position(
    token: documents.ResumeToken, time: timestamp.Timestamp
) -> contract.StateValue:
    """A position: a resume token and its cluster time (a BSON Timestamp)."""
    return {"token": token, "time": [time.time, time.inc]}


def _split(
    position: contract.StateValue,
) -> tuple[documents.ResumeToken | None, tuple[int, int]]:
    """Returns the resume token and the cluster time of a position.

    Raises:
        FatalError: If it is not a change stream position.
    """
    if not isinstance(position, dict) or not isinstance(
        position.get("time"), list
    ):
        raise contract.FatalError(f"Not a change stream position: {position!r}")
    token = position.get("token")
    if token is not None and not isinstance(token, dict):
        raise contract.FatalError(f"Not a change stream position: {position!r}")
    seconds, increment = cast(list[int], position["time"])
    return token, (int(seconds), int(increment))
