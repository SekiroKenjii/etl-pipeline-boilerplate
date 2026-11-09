"""The `[tuning]` section: thresholds bound by the resources of the process,
derived from its budgets unless set (`docs/tuning.md`).

    settings.tuning.effective().spill_rows.value    # as this process uses it
    settings.tuning.effective().toml()              # the block, with sources

The budgets are read once at start-up (`shared.utils.limits`): the memory
limit of the container (cgroup v2 `memory.max`, v1 `memory.limit_in_bytes`,
else the host's `MemAvailable`) and its CPU quota (`cpu.max`, else the CPUs
the process may run on).
"""

from __future__ import annotations

from collections.abc import Iterator
import dataclasses
import shutil
import tempfile
from typing import Annotated, Final, Literal

import pydantic

from shared import utils

# A byte count: an int, or a string with a unit (`"256MiB"`).
_Bytes = Annotated[int, pydantic.BeforeValidator(utils.parse_byte_size)]

AUTO: Final = "auto"
type _Auto = Literal["auto"]
# A byte count, or `"auto"`: derived from the budgets.
type _BytesOrAuto = Annotated[_Bytes, pydantic.Field(ge=1)] | _Auto
type _CountOrAuto = Annotated[int, pydantic.Field(ge=1)] | _Auto

MIB = 1024**2
GIB = 1024**3
# The shares of the detected limits (`docs/tuning.md`).
MEMORY_BUDGET_SHARE = 0.6
MIN_MEMORY_BUDGET = 256 * MIB
KEY_CACHE_MEMORY_SHARE = 0.25
KEY_CACHE_DISK_SHARE = 0.5
MAX_KEY_CACHE_DISK = 2 * GIB
SPILL_SHARE = 0.2
SPILL_ROW_BYTES = 200
MIN_SPILL_ROWS = 10_000
MAX_SPILL_ROWS = 1_000_000
# Values per IN list where the database allows more: longer lists gain
# little and plan worse.
IN_CHUNK_CAP = 1_000
SET = "set"
DEFAULT = "default"


@dataclasses.dataclass(frozen=True, slots=True)
class Chosen[T]:
    """A tuning value as this process uses it, and where it came from.

    Attributes:
        value: The value; None when the run decides it (`bulk_copy_rows`
            chosen online, `in_chunk` by the dialect).
        source: `set` when the settings give it, else how it was derived,
            e.g. `60% of memory.max 4GiB`.
        text: The value as TOML writes it (`"256MiB"`, `12`, `"auto"`).
    """

    value: T
    source: str
    text: str

    @property
    def is_set(self) -> bool:
        return self.source == SET

    @property
    def is_derived(self) -> bool:
        """Whether the value came from the budgets, not the settings."""
        return self.source not in (SET, DEFAULT)


@dataclasses.dataclass(frozen=True, slots=True)
class EffectiveTuning:
    """Every `[tuning]` value as this process uses it (`TuningSettings`).

    Attributes:
        memory_budget: The bytes the process plans with.
        cpu_budget: The CPUs it plans with.
        key_cache_memory: The memory of the key cache.
        key_cache_disk: The disk of the key cache; 0 for none.
        spill_rows: The rows held in memory before a sort spills to disk.
        max_parallel_steps: The most steps a run runs at the same time.
        queue_size: The log records buffered before some are dropped.
        bulk_copy_rows: The rows from which a SQL Server batch is bulk
            copied; None to choose online per table.
        in_chunk: The values per IN list; None for the dialect's.
    """

    memory_budget: Chosen[int]
    cpu_budget: Chosen[int]
    key_cache_memory: Chosen[int]
    key_cache_disk: Chosen[int]
    spill_rows: Chosen[int]
    max_parallel_steps: Chosen[int]
    queue_size: Chosen[int]
    bulk_copy_rows: Chosen[int | None]
    in_chunk: Chosen[int | None]

    def items(self) -> Iterator[tuple[str, Chosen[int | None]]]:
        """Each value, by its key in `[tuning]`."""
        for field in dataclasses.fields(self):
            chosen: Chosen[int | None] = getattr(self, field.name)
            yield field.name, chosen

    def toml(self) -> str:
        """The `[tuning]` section with these values, each source as a
        comment: what `etl config tuning` prints."""
        lines = ["[tuning]"]
        for name, chosen in self.items():
            assignment = f"{name} = {chosen.text}"
            lines.append(f"{assignment:<32}# {chosen.source}")
        return "\n".join(lines) + "\n"

    def summary(self) -> str:
        """One line: every value, those derived from the budgets marked
        with `*`."""
        return ", ".join(
            f"{name}={chosen.text}{'*' if chosen.is_derived else ''}"
            for name, chosen in self.items()
        )


class TuningSettings(pydantic.BaseModel):
    """The thresholds bound by the resources of the process: derived from
    its budgets unless set (`docs/tuning.md`). `effective()` tells every
    value, and where each came from. Unknown keys are typos, and refused.

    Attributes:
        memory_budget: The bytes the process plans with: 60% of the memory
            limit found, at least 256 MiB; or bytes with a unit
            (`"1GiB"`, `shared.utils.parse_byte_size`).
        cpu_budget: The CPUs it plans with: the quota found, at least 1.
        key_cache_memory: The memory each process gives the key cache in
            front of the key registry: 25% of the memory budget, or bytes;
            about 280 MB per million keys. A key in the cache costs the
            registry nothing when it is checked or registered again; the
            keys past the memory go to the disk tier.
        key_cache_disk: The disk the key cache may take under `$TMPDIR`
            for the keys its memory evicted: the lesser of 2 GiB and half
            the free space there, or bytes; 0 for none. Past it, the key
            registry answers for them again: nothing is lost.
        spill_rows: The rows a sort holds in memory before it spills to
            disk (the cutover diff, `MissingSourceRows`): 20% of the memory
            budget at 200 bytes a row, from 10,000 to 1,000,000; or a
            count. A module may set its own.
        max_parallel_steps: The most steps a run runs at the same time,
            whatever its module asks: the CPU budget, or a count. The
            pools of the databases must hold the connections of that many
            steps.
        queue_size: Log records buffered before the slowest sink drops
            some.
        bulk_copy_rows: On SQL Server, the rows from which a batch is bulk
            copied into its staging table rather than sent as parameters:
            `"auto"` measures both on the first batches of each table and
            keeps the faster, checking again now and then; or a count,
            for every table.
        in_chunk: The values per IN list of a statement: `"auto"` derives
            it from the database's parameter limit, at most 1,000 (500 on
            SQL Server); or a count.
    """

    model_config = pydantic.ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="never"
    )

    memory_budget: _BytesOrAuto = AUTO
    cpu_budget: _CountOrAuto = AUTO
    key_cache_memory: _BytesOrAuto = AUTO
    key_cache_disk: Annotated[_Bytes, pydantic.Field(ge=0)] | _Auto = AUTO
    spill_rows: _CountOrAuto = AUTO
    max_parallel_steps: _CountOrAuto = AUTO
    queue_size: Annotated[int, pydantic.Field(ge=100)] = 10_000
    bulk_copy_rows: _CountOrAuto = AUTO
    in_chunk: _CountOrAuto = AUTO
    _effective: EffectiveTuning | None = pydantic.PrivateAttr(default=None)

    def effective(self) -> EffectiveTuning:
        """Every value as this process uses it, with its source; the
        budgets are read on the first call and kept."""
        if self._effective is None:
            try:
                free: int | None = shutil.disk_usage(tempfile.gettempdir()).free
            except OSError:
                free = None
            self._effective = resolve_tuning(self, utils.limits(), free)
        return self._effective


def resolve_tuning(
    tuning: TuningSettings, limits: utils.Limits, free_disk: int | None
) -> EffectiveTuning:
    """Derives every `[tuning]` value not set from the limits found.

    Args:
        tuning: The section, as set.
        limits: The memory and CPU limits of the process.
        free_disk: The free bytes of the temporary directory, if known.
    """
    memory = _memory_budget(tuning.memory_budget, limits)
    cpus = _cpu_budget(tuning.cpu_budget, limits)
    return EffectiveTuning(
        memory_budget=memory,
        cpu_budget=cpus,
        key_cache_memory=_key_cache_memory(tuning.key_cache_memory, memory),
        key_cache_disk=_key_cache_disk(tuning.key_cache_disk, free_disk),
        spill_rows=_spill_rows(tuning.spill_rows, memory),
        max_parallel_steps=_count_or(
            tuning.max_parallel_steps, cpus.value, "cpu_budget"
        ),
        queue_size=Chosen(
            tuning.queue_size,
            SET if "queue_size" in tuning.model_fields_set else DEFAULT,
            str(tuning.queue_size),
        ),
        bulk_copy_rows=_online(
            tuning.bulk_copy_rows,
            "chosen online per table; etl calibrate measures it",
        ),
        in_chunk=_online(
            tuning.in_chunk,
            f"the database's parameter limit, at most {IN_CHUNK_CAP:,}"
            " (SQL Server 500)",
        ),
    )


def size_text(count: int) -> str:
    """A byte count as TOML writes it: whole GiB, MiB or KiB with their
    unit, else the bytes."""
    for unit, scale in (("GiB", GIB), ("MiB", MIB), ("KiB", 1024)):
        if count and count % scale == 0:
            return f'"{count // scale}{unit}"'
    return str(count)


def human_size(count: int) -> str:
    """A byte count for a reader: `3.1GiB`, `477MiB`."""
    for unit, scale in (("GiB", GIB), ("MiB", MIB), ("KiB", 1024)):
        if count >= scale:
            amount = count / scale
            return (
                f"{amount:.1f}{unit}" if amount < 10 else f"{amount:.0f}{unit}"
            )
    return f"{count}B"


def _memory_budget(given: int | _Auto, limits: utils.Limits) -> Chosen[int]:
    """The share of the memory limit, at least the floor; or as given."""
    if isinstance(given, int):
        return Chosen(given, SET, size_text(given))
    if limits.memory is None:
        return Chosen(
            MIN_MEMORY_BUDGET,
            f"{limits.memory_source}: the floor",
            size_text(MIN_MEMORY_BUDGET),
        )
    share = int(limits.memory * MEMORY_BUDGET_SHARE) // MIB * MIB
    budget = max(MIN_MEMORY_BUDGET, share)
    percent = round(MEMORY_BUDGET_SHARE * 100)
    source = f"{percent}% of {limits.memory_source} {human_size(limits.memory)}"
    if budget > share:
        source += f", at least {human_size(MIN_MEMORY_BUDGET)}"
    return Chosen(budget, source, size_text(budget))


def _cpu_budget(given: int | _Auto, limits: utils.Limits) -> Chosen[int]:
    if isinstance(given, int):
        return Chosen(given, SET, str(given))
    if limits.cpus is None:
        return Chosen(1, f"{limits.cpu_source}: 1 CPU", "1")
    count = max(1, int(limits.cpus))
    return Chosen(count, f"{limits.cpu_source} {limits.cpus:g}", str(count))


def _key_cache_memory(given: int | _Auto, memory: Chosen[int]) -> Chosen[int]:
    if isinstance(given, int):
        return Chosen(given, SET, size_text(given))
    budget = max(MIB, int(memory.value * KEY_CACHE_MEMORY_SHARE) // MIB * MIB)
    percent = round(KEY_CACHE_MEMORY_SHARE * 100)
    return Chosen(budget, f"{percent}% of memory_budget", size_text(budget))


def _key_cache_disk(given: int | _Auto, free_disk: int | None) -> Chosen[int]:
    """Half the free space of the temporary directory, capped; or as
    given."""
    if isinstance(given, int):
        return Chosen(given, SET, size_text(given))
    if free_disk is None:
        return Chosen(
            MAX_KEY_CACHE_DISK,
            f"the free space of {tempfile.gettempdir()} is unknown: the cap",
            size_text(MAX_KEY_CACHE_DISK),
        )
    half = int(free_disk * KEY_CACHE_DISK_SHARE) // MIB * MIB
    budget = min(MAX_KEY_CACHE_DISK, half)
    percent = round(KEY_CACHE_DISK_SHARE * 100)
    return Chosen(
        budget,
        f"the lesser of {human_size(MAX_KEY_CACHE_DISK)} and {percent}% of"
        f" the {human_size(free_disk)} free under {tempfile.gettempdir()}",
        size_text(budget),
    )


def _spill_rows(given: int | _Auto, memory: Chosen[int]) -> Chosen[int]:
    if isinstance(given, int):
        return Chosen(given, SET, str(given))
    rows = int(memory.value * SPILL_SHARE) // SPILL_ROW_BYTES
    bounded = min(MAX_SPILL_ROWS, max(MIN_SPILL_ROWS, rows))
    percent = round(SPILL_SHARE * 100)
    source = f"{percent}% of memory_budget at {SPILL_ROW_BYTES} bytes a row"
    if bounded != rows:
        source += f", from {MIN_SPILL_ROWS:,} to {MAX_SPILL_ROWS:,}"
    return Chosen(bounded, source, str(bounded))


def _count_or(given: int | _Auto, derived: int, source: str) -> Chosen[int]:
    if isinstance(given, int):
        return Chosen(given, SET, str(given))
    return Chosen(derived, source, str(derived))


def _online(given: int | _Auto, source: str) -> Chosen[int | None]:
    if isinstance(given, int):
        return Chosen(given, SET, str(given))
    return Chosen(None, source, f'"{AUTO}"')
