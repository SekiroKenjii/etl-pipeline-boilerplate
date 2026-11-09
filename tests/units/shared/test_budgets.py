"""`memory_limit` and `cpu_limit`: the container's limits, read from fake
cgroup trees; never an exception."""

from __future__ import annotations

import os
import pathlib

import pytest

from shared import utils

GIB = 1024**3


def _tree(root: pathlib.Path, **files: str) -> pathlib.Path:
    """Writes `files` under `root`: `sys_fs_cgroup_memory_max` is
    `sys/fs/cgroup/memory.max`."""
    for name, text in files.items():
        path = root.joinpath(*_path(name))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="ascii")
    return root


def _path(name: str) -> list[str]:
    return {
        "v2_memory": ["sys", "fs", "cgroup", "memory.max"],
        "v2_cpu": ["sys", "fs", "cgroup", "cpu.max"],
        "v1_memory": ["sys", "fs", "cgroup", "memory", "memory.limit_in_bytes"],
        "v1_quota": ["sys", "fs", "cgroup", "cpu", "cpu.cfs_quota_us"],
        "v1_period": ["sys", "fs", "cgroup", "cpu", "cpu.cfs_period_us"],
        "meminfo": ["proc", "meminfo"],
    }[name]


MEMINFO = (
    "MemTotal:       15229004 kB\n"
    "MemFree:          348000 kB\n"
    "MemAvailable:    4645888 kB\n"
)


class TestMemoryLimit:

    def test_cgroup_v2_limit(self, tmp_path: pathlib.Path) -> None:
        root = _tree(tmp_path, v2_memory=f"{4 * GIB}\n", meminfo=MEMINFO)

        assert utils.memory_limit(root) == 4 * GIB

    def test_cgroup_v2_max_is_no_limit(self, tmp_path: pathlib.Path) -> None:
        root = _tree(tmp_path, v2_memory="max\n", meminfo=MEMINFO)

        assert utils.memory_limit(root) is None

    def test_cgroup_v1_limit(self, tmp_path: pathlib.Path) -> None:
        root = _tree(tmp_path, v1_memory=f"{2 * GIB}\n", meminfo=MEMINFO)

        assert utils.memory_limit(root) == 2 * GIB

    @pytest.mark.parametrize("text", ["9223372036854771712", str(1 << 63)])
    def test_cgroup_v1_unlimited_is_no_limit(
        self, tmp_path: pathlib.Path, text: str
    ) -> None:
        root = _tree(tmp_path, v1_memory=text, meminfo=MEMINFO)

        assert utils.memory_limit(root) is None

    def test_v2_wins_over_v1(self, tmp_path: pathlib.Path) -> None:
        root = _tree(tmp_path, v2_memory=str(GIB), v1_memory=str(2 * GIB))

        assert utils.memory_limit(root) == GIB

    def test_the_host_s_available_memory_without_a_cgroup_limit(
        self, tmp_path: pathlib.Path
    ) -> None:
        root = _tree(tmp_path, meminfo=MEMINFO)

        assert utils.memory_limit(root) == 4645888 * 1024

    def test_nothing_at_all_is_none(self, tmp_path: pathlib.Path) -> None:
        assert utils.memory_limit(tmp_path) is None

    @pytest.mark.parametrize("text", ["", "garbage", "-1", "0", "1.5e9"])
    def test_garbage_in_the_cgroup_file_falls_back(
        self, tmp_path: pathlib.Path, text: str
    ) -> None:
        # A cgroup file that says nothing usable leaves the question open.
        root = _tree(tmp_path, v2_memory=text, meminfo=MEMINFO)

        assert utils.memory_limit(root) in (None, 4645888 * 1024)

    def test_a_meminfo_without_mem_available_is_none(
        self, tmp_path: pathlib.Path
    ) -> None:
        root = _tree(tmp_path, meminfo="MemTotal: 10 kB\nMemFree: x\n")

        assert utils.memory_limit(root) is None

    def test_an_unreadable_file_is_skipped(
        self, tmp_path: pathlib.Path
    ) -> None:
        root = _tree(tmp_path, v2_memory=str(GIB), meminfo=MEMINFO)
        path = root / "sys" / "fs" / "cgroup" / "memory.max"
        path.write_bytes(b"\xff\xfe")

        assert utils.memory_limit(root) == 4645888 * 1024

    def test_the_real_root_never_raises(self) -> None:
        found = utils.memory_limit()

        assert found is None or found > 0


class TestCpuLimit:

    def test_cgroup_v2_quota(self, tmp_path: pathlib.Path) -> None:
        root = _tree(tmp_path, v2_cpu="150000 100000\n")

        assert utils.cpu_limit(root) == 1.5

    def test_cgroup_v2_max_is_the_affinity(
        self, tmp_path: pathlib.Path
    ) -> None:
        root = _tree(tmp_path, v2_cpu="max 100000\n")

        assert utils.cpu_limit(root) == len(os.sched_getaffinity(0))

    def test_cgroup_v1_quota(self, tmp_path: pathlib.Path) -> None:
        root = _tree(tmp_path, v1_quota="50000", v1_period="100000")

        assert utils.cpu_limit(root) == 0.5

    def test_cgroup_v1_no_quota_is_the_affinity(
        self, tmp_path: pathlib.Path
    ) -> None:
        root = _tree(tmp_path, v1_quota="-1", v1_period="100000")

        assert utils.cpu_limit(root) == len(os.sched_getaffinity(0))

    def test_nothing_at_all_is_the_affinity(
        self, tmp_path: pathlib.Path
    ) -> None:
        assert utils.cpu_limit(tmp_path) == len(os.sched_getaffinity(0))

    @pytest.mark.parametrize(
        "text", ["", "garbage", "100000", "x 100000", "100000 0", "1 2 3"]
    )
    def test_garbage_falls_back_to_the_affinity(
        self, tmp_path: pathlib.Path, text: str
    ) -> None:
        root = _tree(tmp_path, v2_cpu=text)

        assert utils.cpu_limit(root) == len(os.sched_getaffinity(0))

    def test_the_real_root_never_raises(self) -> None:
        found = utils.cpu_limit()

        assert found is not None
        assert found > 0


class TestLimits:

    def test_each_limit_names_its_source(self, tmp_path: pathlib.Path) -> None:
        root = _tree(tmp_path, v2_memory=str(GIB), v2_cpu="200000 100000")

        found = utils.limits(root)

        assert found == utils.Limits(GIB, "memory.max", 2.0, "cpu.max")

    def test_the_host_s_sources(self, tmp_path: pathlib.Path) -> None:
        root = _tree(tmp_path, meminfo=MEMINFO)

        found = utils.limits(root)

        assert (found.memory_source, found.cpu_source) == (
            "MemAvailable",
            "sched_getaffinity",
        )

    def test_nothing_found(self, tmp_path: pathlib.Path) -> None:
        found = utils.limits(tmp_path)

        assert (found.memory, found.memory_source) == (None, utils.NO_LIMIT)
        assert found.cpus == len(os.sched_getaffinity(0))
