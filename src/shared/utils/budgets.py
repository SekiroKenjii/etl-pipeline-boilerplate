"""What the process may use: the memory and CPU limits of its container, or
of the host when it has none.

    utils.memory_limit()   # bytes, or None when nothing bounds the process
    utils.cpu_limit()      # CPUs, as a float (a quota may be half a CPU)
    utils.limits()         # both, each with the file that said so

Read once at start-up, so that thresholds (cache budgets, spill rows,
parallel steps) follow the pod rather than the laptop they were tuned on.
Nothing here raises: a file missing, unreadable or holding something else
means "no limit found", and the next source answers.
"""

from __future__ import annotations

import dataclasses
import os
import pathlib

# cgroup v2, then v1: the paths as a container sees them, under `root`.
_V2_MEMORY = "sys/fs/cgroup/memory.max"
_V2_CPU = "sys/fs/cgroup/cpu.max"
_V1_MEMORY = "sys/fs/cgroup/memory/memory.limit_in_bytes"
_V1_CPU_QUOTA = "sys/fs/cgroup/cpu/cpu.cfs_quota_us"
_V1_CPU_PERIOD = "sys/fs/cgroup/cpu/cpu.cfs_period_us"
_MEMINFO = "proc/meminfo"
# cgroup v1 writes "no limit" as the largest page-aligned 64-bit value
# (9223372036854771712); anything that large means the same.
_V1_UNLIMITED = 1 << 60
NO_LIMIT = "no limit found"


@dataclasses.dataclass(frozen=True, slots=True)
class Limits:
    """What bounds the process, and what said so.

    Attributes:
        memory: The bytes it may use; None when nothing says.
        memory_source: The file the memory came from (`memory.max`,
            `memory.limit_in_bytes`, `MemAvailable`), or `NO_LIMIT`.
        cpus: The CPUs it may use; None when nothing says.
        cpu_source: The file the CPUs came from (`cpu.max`,
            `cpu.cfs_quota_us`), `sched_getaffinity`, `cpu_count`, or
            `NO_LIMIT`.
    """

    memory: int | None
    memory_source: str
    cpus: float | None
    cpu_source: str


def limits(root: str | os.PathLike[str] = "/") -> Limits:
    """Returns the memory and CPU limits, each with its source."""
    memory, memory_source = _memory(pathlib.Path(root))
    cpus, cpu_source = _cpus(pathlib.Path(root))
    return Limits(memory, memory_source, cpus, cpu_source)


def memory_limit(root: str | os.PathLike[str] = "/") -> int | None:
    """Returns the bytes the process may use, or None when nothing says.

    In order: the cgroup v2 limit (`memory.max`; `max` means none), the
    cgroup v1 limit (`memory.limit_in_bytes`; its "unlimited" value means
    none), then the memory available on the host (`MemAvailable` of
    `/proc/meminfo`).

    Args:
        root: Where `sys` and `proc` are mounted: `/` but in tests.
    """
    return _memory(pathlib.Path(root))[0]


def cpu_limit(root: str | os.PathLike[str] = "/") -> float | None:
    """Returns the CPUs the process may use, or None when nothing says.

    In order: the cgroup v2 quota (`cpu.max`: `quota period`, or `max`
    for none), the cgroup v1 quota (`cpu.cfs_quota_us` over
    `cpu.cfs_period_us`; -1 for none), then the CPUs the process is
    allowed to run on (`os.sched_getaffinity`, else `os.cpu_count`).

    Args:
        root: Where `sys` is mounted: `/` but in tests.
    """
    return _cpus(pathlib.Path(root))[0]


def _memory(base: pathlib.Path) -> tuple[int | None, str]:
    """The memory limit under `base`, and the file it came from."""
    found = _read(base / _V2_MEMORY)
    if found is not None:
        if found == "max":
            return None, NO_LIMIT
        limit = _positive(found)
        if limit is not None:
            return limit, "memory.max"
    found = _read(base / _V1_MEMORY)
    if found is not None:
        limit = _positive(found)
        if limit is None or limit >= _V1_UNLIMITED:
            return None, NO_LIMIT
        return limit, "memory.limit_in_bytes"
    available = _available(base / _MEMINFO)
    if available is None:
        return None, NO_LIMIT
    return available, "MemAvailable"


def _cpus(base: pathlib.Path) -> tuple[float | None, str]:
    """The CPU quota under `base`, and the file it came from."""
    found = _read(base / _V2_CPU)
    if found is not None:
        parts = found.split()
        if len(parts) == 2 and parts[0] != "max":
            quota = _positive(parts[0])
            period = _positive(parts[1])
            if quota is not None and period:
                return quota / period, "cpu.max"
        return _affinity()
    quota_text = _read(base / _V1_CPU_QUOTA)
    period_text = _read(base / _V1_CPU_PERIOD)
    if quota_text is not None and period_text is not None:
        quota = _positive(quota_text)
        period = _positive(period_text)
        if quota is not None and period:
            return quota / period, "cpu.cfs_quota_us"
    return _affinity()


def _read(path: pathlib.Path) -> str | None:
    """The stripped text of a file; None when it cannot be read."""
    try:
        return path.read_text(encoding="ascii").strip()
    except (OSError, UnicodeDecodeError):
        return None


def _positive(text: str) -> int | None:
    """An integer of `text` above 0; None for anything else."""
    try:
        value = int(text)
    except ValueError:
        return None
    return value if value > 0 else None


def _available(path: pathlib.Path) -> int | None:
    """`MemAvailable` of a `meminfo` file, in bytes; None without one."""
    text = _read(path)
    if text is None:
        return None
    for line in text.splitlines():
        name, _, rest = line.partition(":")
        if name.strip() != "MemAvailable":
            continue
        parts = rest.split()
        if not parts:
            return None
        amount = _positive(parts[0])
        if amount is None:
            return None
        unit = parts[1].lower() if len(parts) > 1 else "kb"
        return amount * {"kb": 1024, "mb": 1024**2, "b": 1}.get(unit, 1024)
    return None


def _affinity() -> tuple[float | None, str]:
    """The CPUs the process may run on; None when the OS does not say."""
    try:
        return float(len(os.sched_getaffinity(0))), "sched_getaffinity"
    except (AttributeError, OSError):
        count = os.cpu_count()
        return (float(count), "cpu_count") if count else (None, NO_LIMIT)
