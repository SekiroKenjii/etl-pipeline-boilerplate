"""What the mapping-driven pipelines read from the settings of a module.

The core sees no settings library: any object with these attributes will
do. `config.MappingModuleSettings` is the one modules subclass, with the
validation, defaults and documentation of each value.
"""

from __future__ import annotations

from collections.abc import Mapping
import pathlib
from typing import Literal, Protocol

__all__ = ["LookupTable", "MappingSettings"]


class LookupTable(Protocol):
    """A lookup table of the mappings, read from a database table."""

    @property
    def database(self) -> str | None:
        """The database, a name of `databases`; the source's when None."""
        ...

    @property
    def table(self) -> str:
        """The table."""
        ...

    @property
    def key(self) -> str:
        """The column of the values translated."""
        ...

    @property
    def value(self) -> str:
        """The column of their translations."""
        ...


class MappingSettings(Protocol):
    """The settings of a mapping-driven module (`config.MappingModuleSettings`
    documents each)."""

    @property
    def source(self) -> str:
        """The source database, a name of `databases`."""
        ...

    @property
    def target(self) -> str:
        """The target database, a name of `databases`."""
        ...

    @property
    def mappings(self) -> pathlib.Path:
        """The compiled mappings."""
        ...

    @property
    def source_tables(self) -> Mapping[str, str]:
        """The source table of a mapping, when not the one of the workbook."""
        ...

    @property
    def target_tables(self) -> Mapping[str, str]:
        """The target table of a mapping, when not the one of the workbook."""
        ...

    @property
    def watermark(self) -> str | None:
        """The source column tracking changes, for `full_load`."""
        ...

    @property
    def batch_size(self) -> int:
        """The rows handled at once."""
        ...

    @property
    def max_reject_rate(self) -> float | None:
        """Fails a batch rejecting more than this share."""
        ...

    @property
    def orphans(self) -> Literal["reject", "park"]:
        """What becomes of rows whose parent is not loaded."""
        ...

    @property
    def deletes(self) -> Literal["soft", "hard", "ignore"]:
        """What `sync` and `deletes` do with deleted rows."""
        ...

    @property
    def max_delete_share(self) -> float | None:
        """The share of an entity's rows `deletes` may delete in a run."""
        ...

    @property
    def snapshot(self) -> Literal["table", "log"]:
        """Where the first `sync` gets the rows from."""
        ...

    @property
    def verify_loads(self) -> bool:
        """Whether every batch loaded is read back and compared."""
        ...

    @property
    def corrections(self) -> pathlib.Path | None:
        """A CSV of manual corrections of source rows, if any."""
        ...

    @property
    def partitions(self) -> int:
        """How many key ranges `full_load` reads each table by."""
        ...

    @property
    def lookups(self) -> Mapping[str, LookupTable]:
        """Lookup tables read from database tables, by sheet name."""
        ...

    @property
    def step_attempts(self) -> int:
        """How many times a step runs on a transient failure."""
        ...

    @property
    def retry_backoff_seconds(self) -> float:
        """The pause before the second attempt; it doubles at each one."""
        ...

    @property
    def spill_rows(self) -> int:
        """The rows `deletes` holds in memory before it sorts on disk."""
        ...

    @property
    def in_chunk(self) -> int:
        """The values per IN list when the loads are verified."""
        ...
