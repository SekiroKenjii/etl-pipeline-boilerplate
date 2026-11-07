"""What a cutover reconciles: each target entity against its source.

A module describes, per entity, how its source rows become the target rows
expected (its mapping), and where the target rows are; `core.cutover`
compares both, and reports what is missing, extra or different.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
import dataclasses

from core.contract import formats
from core.contract import parking
from core.contract import tables


@dataclasses.dataclass(frozen=True, slots=True)
class Expected:
    """What a source row should have become in the target.

    Attributes:
        rows: The target rows it maps to (none if a filter drops it, several
            if it is split).
        rejected: Why it cannot be loaded, when it cannot (a date that does
            not exist, a code no lookup knows): the reasons of a rejection.
    """

    rows: Sequence[formats.Row] = ()
    rejected: Sequence[str] = ()


@dataclasses.dataclass(frozen=True, slots=True)
class Reconciliation:
    """How to reconcile one target entity with its source.

    Attributes:
        entity: The entity, as reports name it.
        source: Reads the source.
        source_query: The source rows: the table, its key as `order_by`, the
            filters of the migration's scope.
        expect: Maps a source row to what the target should hold: the
            module's mapping, run without loading anything.
        target: Reads the target.
        target_query: The target rows: the table, its key as `order_by`, the
            filters (e.g. not soft-deleted).
        key: The target columns identifying a row.
        columns: The target columns compared; the key ones included.
        parking_queue: The parking lot queue of its orphans, if any: rows
            parked there are missing for a known reason.
        parked_row: The target row a parked entry stands for (its key
            columns at least), or None if it cannot tell: a missing row is
            explained only by a parked entry with its key.
        source_database: The source database, a name of the settings'
            `databases`: the report says what it compared.
        target_database: Likewise, the target database.
    """

    entity: str
    source: tables.ITableReader
    source_query: tables.TableQuery
    expect: Callable[[formats.Row], Expected]
    target: tables.ITableReader
    target_query: tables.TableQuery
    key: tuple[str, ...]
    columns: tuple[str, ...]
    parking_queue: str | None = None
    parked_row: Callable[[parking.ParkedEntry], formats.Row | None] | None = (
        None
    )
    source_database: str | None = None
    target_database: str | None = None
