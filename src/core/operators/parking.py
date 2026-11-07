"""An extractor replaying the records of a parking lot."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence

from core import contract
from core import pipeline


class ParkedRecordExtractor[T](pipeline.IDataExtractor[T]):
    """Replays the parked records of a queue, to load them at last.

    Run it after the steps loading their parents, with the same gate and loader
    as the step that parked them:

        PipelineStep(
            "order:parked",
            ETLJob(
                ParkedRecordExtractor(lot, "order", mapping.decode_record),
                Passthrough(), order_loader, order_gate,
            ),
            dependencies=["customer"],
        )

    A record loaded is released when its batch is acknowledged; one still
    waiting is parked again by the gate's `ParkingQuarantine`, one attempt more.

    Attributes:
        lot: Where the records are parked.
        queue: The queue to replay.
        max_attempts: Records parked that many times stay in the lot and are
            not replayed any more. None replays them forever.
        expired: The records skipped because of `max_attempts`, in this run.
    """

    def __init__(
        self,
        lot: contract.IParkingLot,
        queue: str,
        decode: Callable[[contract.StateValue], T],
        *,
        max_attempts: int | None = None,
    ) -> None:
        """Initializes the extractor.

        Args:
            lot: Where the records are parked.
            queue: The queue to replay.
            decode: Turns a payload back into a record.
            max_attempts: Records parked that many times are not replayed.

        Raises:
            ValueError: If `max_attempts` is below 1.
        """
        if max_attempts is not None and max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        self.lot = lot
        self.queue = queue
        self.max_attempts = max_attempts
        self.expired = 0
        self._decode = decode
        # Holds the record so that its id is not reused before its ack.
        self._entries: dict[int, tuple[T, contract.ParkedEntry]] = {}

    @property
    def name(self) -> str:
        return f"{type(self).__name__}({self.queue})"

    def teardown(self, ctx: contract.ETLContext) -> None:
        self._entries.clear()

    def extract(self, ctx: contract.ETLContext) -> Iterator[T]:
        self.expired = 0
        for entry in self.lot.entries(self.queue):
            if self.max_attempts is not None and (
                entry.attempts >= self.max_attempts
            ):
                self.expired += 1
                continue
            record = self._decode(entry.payload)
            self._entries[id(record)] = (record, entry)
            yield record

    def acknowledge(
        self, records: Sequence[T], ctx: contract.ETLContext
    ) -> None:
        """Releases the records of the batch; those parked again stay."""
        entries = [
            self._entries.pop(id(record))[1]
            for record in records
            if id(record) in self._entries
        ]
        self.lot.release(self.queue, entries)
