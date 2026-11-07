"""A transformer applying a compiled mapping."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping
import typing
from typing import cast

from core import contract
from core import pipeline
from core.mapping import compiled
from core.mapping import results


class MappingTransformer[TIn](
    pipeline.IDataTransformer[TIn, results.MappedRecord]
):
    """Applies a compiled mapping to each source record.

    It never drops a record because of bad data: a value that cannot be mapped
    becomes an issue of the `MappedRecord`, and the quality gate decides
    (`MappingIssuesRule` at AFTER_TRANSFORM). Rows dropped on purpose by the
    mapping's row filter, or split into no row, are counted as `filtered`.
    A source whose columns do not match the mapping fails the run: no row of
    it can be trusted.

        MappingTransformer(
            customers, row=lambda r: r.data, key=lambda r: f"{r.key}#{r.index}"
        )

    Attributes:
        mapping: The mapping applied.
    """

    def __init__(
        self,
        mapping: compiled.CompiledMapping,
        *,
        row: Callable[[TIn], Mapping[str, object]] | None = None,
        key: Callable[[TIn], str | None] | None = None,
        scope: str | None = None,
    ) -> None:
        """Initializes the transformer.

        Args:
            mapping: The mapping to apply.
            row: Gets the source row out of a record; the record itself by
                default.
            key: Gets the identifier of a record, for lineage.
            scope: For a mapping splitting rows, the target column holding
                the parent key (`customer_key`): an updated or deleted source
                row first deletes the rows of its parent, so that children
                gone from the source go from the target.

        Raises:
            PipelineConfigError: If `scope` is given to a mapping that does not
                split rows.
        """
        if scope is not None and not mapping.splits_rows:
            raise contract.PipelineConfigError(
                "scope only applies to a mapping splitting rows, not to"
                f" {mapping.name!r}"
            )
        self.mapping = mapping
        self.scope = scope
        self._row = row
        self._key = key
        self._filtered = 0

    @property
    def name(self) -> str:
        return f"{type(self).__name__}({self.mapping.name})"

    def transform(
        self, records: Iterable[TIn], ctx: contract.ETLContext
    ) -> Iterator[results.MappedRecord]:
        """Maps each record; see `pipeline.IDataTransformer.transform`.

        Change events (`contract.ChangeEvent`) are applied: a DELETE becomes a
        deleted record; several changes of one row in the batch count as the
        last one only.

        Raises:
            FatalError: If a row is not a mapping, or the source does not have
                the shape the mapping expects.
        """
        for record in _last_changes(records):
            if isinstance(record, contract.ChangeEvent):
                yield from self._change(record)
                continue
            row = self._row(record) if self._row is not None else record
            if not isinstance(row, Mapping):
                raise contract.FatalError(
                    f"{self.name} expects rows as mappings, got"
                    f" {type(row).__name__}: pass `row=` to extract them"
                )
            mapped = self.mapping.map(
                typing.cast(Mapping[str, object], row),
                self._key(record) if self._key else None,
            )
            if not mapped:
                self._filtered += 1
            yield from mapped

    def _change(
        self, event: contract.ChangeEvent
    ) -> Iterator[results.MappedRecord]:
        """Maps one change of a source row."""
        # The key function gets the records as they come: here, events.
        source_key = self._key(cast(TIn, event)) if self._key else None
        scope = self.scope
        replacing = scope is not None and event.operation in (
            contract.ChangeOperation.UPDATE,
            contract.ChangeOperation.DELETE,
        )
        if scope is not None and replacing:
            yield self.mapping.scope_deletion(event.row, scope, source_key)
        if event.deleted:
            if not replacing:
                yield self.mapping.deletion(event.row, source_key)
            return
        mapped = self.mapping.map(event.row, source_key)
        if not mapped:
            self._filtered += 1
        yield from mapped

    def collect_stats(self) -> contract.ETLStats:
        stats, self._filtered = contract.ETLStats(filtered=self._filtered), 0
        return stats


def _last_changes[T](records: Iterable[T]) -> list[T]:
    """Keeps the last change of each row, in order; other records as they are.

    A batch may hold several changes of one row; applying the last is enough,
    and replacing children by scope needs it.
    """
    items = list(records)
    last: dict[tuple[object, ...], int] = {}
    for index, item in enumerate(items):
        if isinstance(item, contract.ChangeEvent):
            last[(item.table, *item.key)] = index
    return [
        item
        for index, item in enumerate(items)
        if not isinstance(item, contract.ChangeEvent)
        or last[(item.table, *item.key)] == index
    ]
