"""An extractor taking quarantined records through their pipeline again."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Sequence
import itertools

from core import contract
from core import pipeline

type Fetch[T] = Callable[
    [Sequence[contract.QuarantineEntry], contract.ETLContext], Iterable[T]
]


class QuarantineReplay[T](pipeline.IDataExtractor[T]):
    """Replays the open records of a quarantine, once their cause is fixed.

    Use the same transformer, gate and loader as the step that rejected them:

        QuarantineReplay(
            quarantine,
            select=lambda e: e.record.get("mapping") == "customer",
            fetch=read_source_rows,
            entry_identity=lambda e: e.record["source_key"],
            identity=lineage,
        )

    `fetch` reads the records again (usually from the source, by the entries'
    keys), so they go through the mapping as it is now. An entry is marked
    REPLAYED when its record's batch is acknowledged (loaded, or rejected again
    into a new entry). Entries whose record cannot be fetched stay OPEN.

    Attributes:
        quarantine: The quarantine.
        replayed: The entries marked replayed by this run.
        unmatched: The entries left open by this run, no record fetched.
    """

    def __init__(
        self,
        quarantine: contract.IQuarantine,
        *,
        fetch: Fetch[T],
        entry_identity: Callable[[contract.QuarantineEntry], str | None],
        identity: Callable[[T], str],
        select: Callable[[contract.QuarantineEntry], bool] = lambda _: True,
        pipeline_name: str | None = None,
        chunk_size: int = 500,
    ) -> None:
        """Initializes the extractor.

        Args:
            quarantine: The quarantine.
            fetch: Gets the records of a chunk of entries.
            entry_identity: Identifies the record of an entry; None skips it.
            identity: Identifies a fetched record, as `entry_identity` does.
            select: The entries to replay, among the open ones.
            pipeline_name: Only the entries of this pipeline; all when None.
            chunk_size: The entries fetched at once.

        Raises:
            ValueError: If `chunk_size` is below 1.
        """
        if chunk_size < 1:
            raise ValueError("chunk_size must be >= 1")
        self.quarantine = quarantine
        self.replayed = 0
        self.unmatched = 0
        self._fetch = fetch
        self._entry_identity = entry_identity
        self._identity = identity
        self._select = select
        self._pipeline = pipeline_name
        self._chunk = chunk_size
        self._pending: dict[str, list[str]] = {}

    def teardown(self, ctx: contract.ETLContext) -> None:
        self._pending.clear()

    def extract(self, ctx: contract.ETLContext) -> Iterator[T]:
        self.replayed = self.unmatched = 0
        by_identity: dict[str, list[contract.QuarantineEntry]] = {}
        for entry in self.quarantine.entries(self._pipeline):
            identity = (
                self._entry_identity(entry) if self._select(entry) else None
            )
            if identity is not None:
                by_identity.setdefault(identity, []).append(entry)
        self._pending = {
            identity: [entry.id for entry in entries]
            for identity, entries in by_identity.items()
        }
        fetched: set[str] = set()
        for chunk in itertools.batched(by_identity.values(), self._chunk):
            for record in self._fetch([group[0] for group in chunk], ctx):
                fetched.add(self._identity(record))
                yield record
        self.unmatched = sum(
            len(ids)
            for identity, ids in self._pending.items()
            if identity not in fetched
        )

    def acknowledge(
        self, records: Sequence[T], ctx: contract.ETLContext
    ) -> None:
        """Marks the entries of the records handled REPLAYED."""
        ids = [
            id_
            for record in records
            for id_ in self._pending.pop(self._identity(record), [])
        ]
        if ids:
            self.replayed += self.quarantine.resolve(
                ids, contract.QuarantineStatus.REPLAYED
            )
