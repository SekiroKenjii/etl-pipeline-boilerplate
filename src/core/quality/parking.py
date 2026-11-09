"""Parking: rejected records retried later, instead of quarantined for good."""

from __future__ import annotations

from collections.abc import Callable, Collection, Iterator, Sequence
import copy
import dataclasses
import datetime
import threading
import typing

from core import contract
from shared import utils


class InMemoryParkingLot(contract.IParkingLot):
    """Keeps parked records in memory, for tests and local development."""

    def __init__(self) -> None:
        self._queues: dict[str, dict[str, contract.ParkedEntry]] = {}
        self._lock = threading.Lock()

    def park(self, queue: str, entries: Sequence[contract.ParkedEntry]) -> None:
        with self._lock:
            parked = self._queues.setdefault(queue, {})
            for entry in entries:
                previous = parked.get(entry.identity)
                parked[entry.identity] = dataclasses.replace(
                    entry,
                    payload=copy.deepcopy(entry.payload),
                    attempts=previous.attempts + 1 if previous else 1,
                    parked_at=(
                        previous.parked_at if previous else utils.utc_now()
                    ),
                )

    def entries(self, queue: str) -> Iterator[contract.ParkedEntry]:
        with self._lock:
            entries = sorted(
                self._queues.get(queue, {}).values(), key=lambda e: e.identity
            )
        yield from entries

    def release(
        self, queue: str, entries: Sequence[contract.ParkedEntry]
    ) -> None:
        with self._lock:
            parked = self._queues.get(queue, {})
            for entry in entries:
                current = parked.get(entry.identity)
                if current is not None and current.attempts == entry.attempts:
                    del parked[entry.identity]


class ParkingQuarantine[T](contract.IQuarantine):
    """Parks the records rejected by some rules only; quarantines the others.

    Typically the referential integrity rules of foreign keys whose parents may
    arrive late: their orphans are parked, and replayed by a
    `operators.ParkedRecordExtractor` step until their parents are loaded. A
    record also rejected by another rule (bad data) goes to the fallback
    quarantine: replaying it would not make it any better.

        ParkingQuarantine(
            lot, "order",
            rules={"references:order.customer_key"},
            encode=mapping.encode_record,
            identity=mapping.record_identity,
            fallback=JsonLinesQuarantine("var/quarantine"),
        )

    Parked records still count as rejected in the step's stats and in the
    reject rate of the checkpoint: they are not loaded yet.

    Attributes:
        lot: Where records are parked.
        queue: The queue they are parked in, usually the mapping.
        rules: The rules whose rejections are parked.
        fallback: Where the other rejected records go.
    """

    def __init__(
        self,
        lot: contract.IParkingLot,
        queue: str,
        *,
        rules: Collection[str],
        encode: Callable[[T], contract.StateValue],
        identity: Callable[[T], str],
        fallback: contract.IQuarantine,
    ) -> None:
        """Initializes the quarantine.

        Args:
            lot: Where records are parked.
            queue: The queue they are parked in, usually the mapping.
            rules: The names of the rules whose rejections are parked.
            encode: Turns a record into JSON-compatible data.
            identity: Identifies a record in the queue: parking it again counts
                one more attempt.
            fallback: Where the other rejected records go.
        """
        self.lot = lot
        self.queue = queue
        self.rules = frozenset(rules)
        self.fallback = fallback
        self._encode = encode
        self._identity = identity

    def entries(
        self,
        pipeline: str | None = None,
        status: contract.QuarantineStatus | None = (
            contract.QuarantineStatus.OPEN
        ),
    ) -> Iterator[contract.QuarantineEntry]:
        """The fallback's entries: parked records are read from the lot."""
        return self.fallback.entries(pipeline, status)

    def resolve(
        self, ids: Collection[str], status: contract.QuarantineStatus
    ) -> int:
        return self.fallback.resolve(ids, status)

    def erase(self, ids: Collection[str]) -> int:
        return self.fallback.erase(ids)

    def purge(self, before: datetime.datetime) -> int:
        return self.fallback.purge(before)

    def put(
        self,
        records: Sequence[contract.RejectedRecord[object]],
        ctx: contract.ETLContext,
    ) -> None:
        parked: list[contract.ParkedEntry] = []
        others: list[contract.RejectedRecord[object]] = []
        for rejected in records:
            reasons = [
                issue
                for issue in rejected.issues
                if issue.action is contract.QualityAction.REJECT
            ]
            if reasons and all(issue.rule in self.rules for issue in reasons):
                # Its gate's checkpoint gives it the records of type T only.
                record = typing.cast(T, rejected.record)
                parked.append(
                    contract.ParkedEntry(
                        identity=self._identity(record),
                        payload=self._encode(record),
                        reason="; ".join(
                            f"{issue.rule}: {issue.message}"
                            for issue in reasons
                        ),
                    )
                )
            else:
                others.append(rejected)

        if parked:
            self.lot.park(self.queue, parked)
        if others:
            self.fallback.put(others, ctx)
