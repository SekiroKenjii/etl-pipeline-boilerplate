"""The diff benchmark measures the partitioning it was asked for."""

from __future__ import annotations

import json
import pathlib
from typing import NoReturn

from benchmarks import bench_diff
import pytest

from core import contract
from infra.database import relational


def _job(
    tmp_path: pathlib.Path, rows: int, *, hashed: bool
) -> dict[str, object]:
    source = f"sqlite:///{tmp_path}/source.db"
    target = f"sqlite:///{tmp_path}/target.db"
    bench_diff._seed(relational.Database(source), rows)
    bench_diff._target(
        relational.Database(target), "bench_diff_0_01", rows, 0.01
    )
    return {
        "source": source,
        "target": target,
        "table": "bench_diff_0_01",
        "rows": rows,
        "drift": 0.01,
        "hash": hashed,
    }


def _measure(
    capsys: pytest.CaptureFixture[str], job: dict[str, object]
) -> dict[str, object]:
    assert bench_diff._child(job) == 0
    measure: dict[str, object] = json.loads(
        capsys.readouterr().out.splitlines()[-1]
    )
    return measure


@pytest.mark.parametrize(
    ("hashed", "partitioning"), [(True, "hash"), (False, "range")]
)
def test_the_partitioning_measured_is_the_one_asked_for(
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
    hashed: bool,
    partitioning: str,
) -> None:
    measure = _measure(capsys, _job(tmp_path, 300, hashed=hashed))

    assert measure["partitioning"] == partitioning
    assert measure["differences"] == 3


def test_an_empty_diff_is_not_reported_as_hashed(
    tmp_path: pathlib.Path, capsys: pytest.CaptureFixture[str]
) -> None:
    measure = _measure(capsys, _job(tmp_path, 0, hashed=False))

    assert measure["partitioning"] != "hash"


def test_a_renamed_option_of_the_diff_fails_the_benchmark(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def reconcile(
        reconciliation: contract.Reconciliation, *, by_range: bool = True
    ) -> NoReturn:
        del reconciliation, by_range  # Unused.
        raise AssertionError("diffed without --hash")

    monkeypatch.setattr(bench_diff.cutover, "reconcile", reconcile)
    job = _job(tmp_path, 10, hashed=True)

    with pytest.raises(TypeError, match="ranges"):
        bench_diff._child(job)
