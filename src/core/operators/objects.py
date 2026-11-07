"""An extractor of the rows of every object of a file or blob store."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
import dataclasses
import fnmatch

from core import contract
from core import pipeline
from shared import utils


@dataclasses.dataclass(frozen=True, slots=True)
class ObjectRecord:
    """A row and where it comes from (lineage).

    Attributes:
        key: The object it was read from.
        version: The version of that object.
        index: Its position in the object, from 0.
        data: The row.
    """

    key: str
    version: str
    index: int
    data: contract.Row


class ObjectStoreExtractor(pipeline.IDataExtractor[ObjectRecord]):
    """Extracts the rows of every object of a store, in key order.

    With a state store, an object already processed in the same version is
    skipped. An object is recorded as processed only once fully read and all its
    rows acknowledged: a run failing mid-object reprocesses it (at-least-once).

    Attributes:
        store: Where the objects are.
        record_format: How objects are decoded into rows.
        prefix: Only the objects whose key starts with it are read.
        pattern: A glob on the key, such as `*.csv`.
        concurrency: The objects read in parallel.
    """

    def __init__(
        self,
        store: contract.IObjectStore,
        record_format: contract.IRecordFormat,
        *,
        prefix: str = "",
        pattern: str | None = None,
        state: contract.IStateStore | None = None,
        state_key: str | None = None,
        concurrency: int = 1,
    ) -> None:
        """Initializes the extractor.

        Args:
            store: Where the objects are.
            record_format: How objects are decoded into rows.
            prefix: Only the objects whose key starts with it are read.
            pattern: A glob on the key, such as `*.csv`.
            state: Records the objects processed, for incremental runs.
            state_key: The state key; derived from `prefix` and `pattern` by
                default.
            concurrency: The objects read in parallel, in threads: useful for
                many small remote objects. Each object read ahead is held in
                memory: keep 1 for large objects.

        Raises:
            ValueError: If `concurrency` is below 1.
        """
        if concurrency < 1:
            raise ValueError("concurrency must be >= 1")

        self.store = store
        self.record_format = record_format
        self.prefix = prefix
        self.pattern = pattern
        self.concurrency = concurrency
        self._state = state
        self._state_key = (
            state_key or f"objects:{prefix or '*'}:{pattern or '*'}"
        )
        self._rows_of_fully_read: dict[str, int] = {}
        self._ack_counts: dict[str, int] = {}
        self._versions: dict[str, str] = {}

    def pending_objects(
        self, ctx: contract.ETLContext
    ) -> Iterator[contract.ObjectInfo]:
        """Yields the objects to read: matching, and not processed yet."""
        processed = self._processed(ctx)
        for obj in self.store.list(self.prefix):
            if self.pattern is not None and not fnmatch.fnmatch(
                obj.key, self.pattern
            ):
                continue
            if processed.get(obj.key) == obj.version:
                continue
            yield obj

    def extract(self, ctx: contract.ETLContext) -> Iterator[ObjectRecord]:
        """Yields the rows of every pending object, object after object."""
        objects = self.pending_objects(ctx)

        if self.concurrency == 1:
            for obj in objects:
                yield from self._rows(obj, self._stream_rows(obj), ctx)
            return

        read_ahead = utils.bounded_map(
            lambda obj: (obj, list(self._stream_rows(obj))),
            objects,
            max_workers=self.concurrency,
        )
        for obj, rows in read_ahead:
            yield from self._rows(obj, iter(rows), ctx)

    def acknowledge(
        self, records: Sequence[ObjectRecord], ctx: contract.ETLContext
    ) -> None:
        """Records as processed the objects whose rows are all acknowledged."""
        for record in records:
            self._ack_counts[record.key] = (
                self._ack_counts.get(record.key, 0) + 1
            )
            self._complete_if_done(record.key, ctx)

    def _stream_rows(self, obj: contract.ObjectInfo) -> Iterator[contract.Row]:
        with self.store.open(obj) as stream:
            yield from self.record_format.read(stream)

    def _rows(
        self,
        obj: contract.ObjectInfo,
        rows: Iterator[contract.Row],
        ctx: contract.ETLContext,
    ) -> Iterator[ObjectRecord]:
        """Yields the rows of `obj` with their lineage, then counts them."""
        self._versions[obj.key] = obj.version
        count = 0
        for count, row in enumerate(rows, start=1):
            yield ObjectRecord(obj.key, obj.version, count - 1, row)

        # Fully read: its last rows may already be acknowledged, or it had none.
        self._rows_of_fully_read[obj.key] = count
        self._complete_if_done(obj.key, ctx)

    def _complete_if_done(self, key: str, ctx: contract.ETLContext) -> None:
        """Records `key` as processed once it is fully read and acknowledged."""
        total = self._rows_of_fully_read.get(key)
        if total is None or self._ack_counts.get(key, 0) < total:
            return

        version = self._versions.pop(key)
        del self._rows_of_fully_read[key]
        self._ack_counts.pop(key, None)

        if self._state is not None:
            processed: dict[str, contract.StateValue] = {
                **self._processed(ctx),
                key: version,
            }
            self._state.set(ctx.run.pipeline_name, self._state_key, processed)

    def _processed(self, ctx: contract.ETLContext) -> dict[str, str]:
        if self._state is None:
            return {}

        value = self._state.get(ctx.run.pipeline_name, self._state_key)
        if not isinstance(value, dict):
            return {}
        return {key: str(version) for key, version in value.items()}
