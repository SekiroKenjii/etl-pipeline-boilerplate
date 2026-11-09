"""The settings of a mapping-driven module: `[modules.<name>.settings]`.

A module subclasses `MappingModuleSettings`, giving its defaults:

    class BillingSettings(config.MappingModuleSettings):
        source: str = "legacy"
        target: str = "billing"
        mappings: pathlib.Path = pathlib.Path("mappings/compiled/billing.json")

Validated with `AppSettings.module_settings`, it also checks that the pools
of its databases hold the connections of its parallel steps.
"""

from __future__ import annotations

import collections
import pathlib
from typing import Literal, Self

import pydantic

from config import settings as app_settings
from config import tuning as config_tuning


class LookupTable(pydantic.BaseModel):
    """A lookup table read from a database table.

    Attributes:
        database: The database, a name of `databases`; the source's when
            None.
        table: The table.
        key: The column of the values translated.
        value: The column of their translations.
    """

    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    database: str | None = None
    table: str
    key: str
    value: str


class MappingModuleSettings(pydantic.BaseModel):
    """Where a module's data comes from and goes, and how it is handled.

    Attributes:
        source: The source database, a name of `databases`.
        target: The target database, a name of `databases`.
        mappings: The compiled mappings (`etl-mappings compile`).
        source_tables: The source table of each mapping, when not the one of
            the workbook (e.g. without its schema).
        target_tables: The target table of each mapping, likewise.
        watermark: The source column tracking changes, for `full_load`; None
            reads by key (append-only tables with an increasing key).
        batch_size: The rows handled at once.
        max_reject_rate: Fails a batch rejecting more than this share.
        orphans: What becomes of rows whose parent is not loaded (an order
            without its customer): `reject` into the quarantine, or `park`
            until it is.
        deletes: What `sync` and `deletes` do with deleted rows.
        max_delete_share: The share of an entity's loaded rows `deletes` may
            delete in a run, a tenth by default; above it, or when the source
            has no row at all, the run exits 2 until it confirms the number
            (`--param confirm_deletes=<entity>:<count>`). None for no limit.
        snapshot: Where the first `sync` gets the rows from: the source
            `table`, or the change `log` from its beginning when it holds its
            own snapshot (Debezium's `snapshot.mode=initial`).
        verify_loads: Whether every batch loaded is read back from the
            target and compared (`cutover.LoadedRowsCheck`): slower, sure.
        corrections: A CSV of manual corrections of source rows, reviewed
            like the mappings (`operators.Corrections`), if any.
        partitions: How many key ranges `full_load` reads each table by, in
            parallel; kept once chosen (reset the state to change it). The
            database pools must hold their connections: checked at load.
        lookups: Lookup tables read from database tables, by the name of
            their (empty) `lookup_<name>` sheet.
        step_attempts: How many times a step runs on a transient failure (a
            lost connection, a deadlock): it starts again from its last
            loaded batch.
        retry_backoff_seconds: The pause before the second attempt; it
            doubles at each one.
        spill_rows: The rows `deletes` holds in memory before it sorts the
            keys on disk; `tuning.spill_rows` unless set.
        in_chunk: The values per IN list when the loads are verified
            (`verify_loads`); `tuning.in_chunk`, else 1,000, unless set.
    """

    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    source: str
    target: str
    mappings: pathlib.Path
    source_tables: dict[str, str] = {}
    target_tables: dict[str, str] = {}
    watermark: str | None = None
    batch_size: int = pydantic.Field(default=1000, ge=1)
    max_reject_rate: float | None = pydantic.Field(default=0.05, ge=0, le=1)
    orphans: Literal["reject", "park"] = "reject"
    deletes: Literal["soft", "hard", "ignore"] = "soft"
    max_delete_share: float | None = pydantic.Field(default=0.1, ge=0, le=1)
    snapshot: Literal["table", "log"] = "table"
    verify_loads: bool = False
    corrections: pathlib.Path | None = None
    partitions: int = pydantic.Field(default=1, ge=1, le=64)
    lookups: dict[str, LookupTable] = {}
    step_attempts: int = pydantic.Field(default=3, ge=1, le=10)
    retry_backoff_seconds: float = pydantic.Field(default=5.0, ge=0)
    spill_rows: int = pydantic.Field(default=50_000, ge=1)
    in_chunk: int = pydantic.Field(default=config_tuning.IN_CHUNK_CAP, ge=1)

    @pydantic.model_validator(mode="before")
    @classmethod
    def _tuned(cls, data: object, info: pydantic.ValidationInfo) -> object:
        """Gives `spill_rows` and `in_chunk` the application's tuning
        values when the module does not set them (`module_settings`)."""
        settings = (info.context or {}).get("settings")
        if not isinstance(data, dict) or not isinstance(
            settings, app_settings.AppSettings
        ):
            return data
        effective = settings.tuning.effective()
        given: dict[str, object] = dict(data)
        given.setdefault("spill_rows", effective.spill_rows.value)
        given.setdefault(
            "in_chunk", effective.in_chunk.value or config_tuning.IN_CHUNK_CAP
        )
        return given

    @pydantic.model_validator(mode="after")
    def _pools(self, info: pydantic.ValidationInfo) -> Self:
        """Checks that the pools hold the connections of the parallel
        ranges, given the application's settings (`module_settings`)."""
        settings = (info.context or {}).get("settings")
        if not isinstance(settings, app_settings.AppSettings):
            return self
        steps = min(
            self.partitions,
            settings.tuning.effective().max_parallel_steps.value,
        )
        needs: collections.Counter[str] = collections.Counter()
        # Target: a transaction, plus SQL Server's bulk-copy connection.
        needs[self.source] += steps
        needs[self.target] += 2 * steps
        book = settings.bookkeeping.database
        if book is not None and book != self.target:
            needs[book] += steps
        settings.check_pools(needs, f"{self.partitions} partitions")
        return self
