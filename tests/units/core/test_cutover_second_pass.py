"""Cutover: the second pass of the data diff, which reads again the ranges
whose checksums differ, and the keys and values it compares."""

from __future__ import annotations

import dataclasses
import decimal
import functools
from typing import Any

import pytest

from core import contract
from core import cutover
from tests.factories import fake_ports
from tests.factories import reconciliations


@functools.total_ordering
class _Backwards:
    """A key the database orders otherwise than its text, as SQL Server
    orders a `uniqueidentifier` by its last bytes."""

    def __init__(self, value: int) -> None:
        self.value = value

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _Backwards) and other.value == self.value

    def __lt__(self, other: _Backwards) -> bool:
        return self.value > other.value

    def __hash__(self) -> int:
        return self.value

    def __str__(self) -> str:
        return f"{self.value:08d}"


class _Counting(fake_ports.InMemoryTableReader):
    """Counts the rows each query yields; drops the rows of a range query
    when told to, as a database ordering otherwise would."""

    def __init__(
        self, tables: dict[str, list[dict[str, Any]]], *, drop: bool = False
    ) -> None:
        super().__init__(tables)
        self.drop = drop
        self.read_rows: list[int] = []

    def read(self, query: contract.TableQuery) -> Any:
        self.read_rows.append(0)
        for row in super().read(query):
            if self.drop and query.lower is not None:
                continue
            self.read_rows[-1] += 1
            yield row


def _drifted(rows: int, keys: Any = str) -> tuple[list[dict[str, Any]], ...]:
    """Source rows, and a target missing one, with one different and one
    extra."""
    source = [{"id": keys(i), "name": f"n{i}"} for i in range(rows)]
    target = [{"id": r["id"], "name": str(r["name"]).upper()} for r in source]
    del target[rows // 2]
    target[rows // 3]["name"] = "CHANGED"
    target.append({"id": keys(rows + 1), "name": "EXTRA"})
    return source, target


class TestTheSecondPass:

    def _run(self, reader: _Counting, **options: Any) -> cutover.EntityReport:
        reconciliation = dataclasses.replace(
            reconciliations.reconciliation([], []),
            source=reader,
            target=reader,
            source_query=contract.TableQuery("src", order_by=("id",)),
        )
        return cutover.reconcile(reconciliation, **options)

    def test_reads_again_only_the_ranges_that_differ(self) -> None:
        source, target = _drifted(10_000, keys=lambda i: f"{i:08d}")
        reader = _Counting({"src": source, "dst": target})

        report = self._run(reader)

        assert report.partitioning == "range"
        assert (report.missing, report.different, report.extra) == (1, 1, 1)
        assert report.matched == 9_998
        # The target in full, the source twice, then 3 ranges of the target:
        # those of the 3 differences.
        scanned, mapped, mapped_again, *ranges = reader.read_rows
        assert (scanned, mapped, mapped_again) == (10_000, 10_000, 10_000)
        assert len(ranges) == 3
        assert sum(ranges) <= 3 * (cutover.ROWS_PER_PARTITION + 1)

    def test_keys_the_target_orders_otherwise_split_by_hash(self) -> None:
        source, target = _drifted(1_000, keys=_Backwards)
        reader = _Counting({"src": source, "dst": target})

        report = self._run(reader)

        assert report.partitioning == "hash"
        assert (report.missing, report.different, report.extra) == (1, 1, 1)
        assert report.matched == 998

    def test_ranges_the_database_reads_otherwise_are_read_again_whole(
        self,
    ) -> None:
        source, target = _drifted(1_000, keys=lambda i: i)
        reader = _Counting({"src": source, "dst": target}, drop=True)

        report = self._run(reader)

        assert report.partitioning == "range"
        assert (report.missing, report.different, report.extra) == (1, 1, 1)
        assert report.matched == 998

    @pytest.mark.parametrize("ranges", [True, False])
    def test_rows_past_the_memory_are_sorted_on_disk(
        self, ranges: bool
    ) -> None:
        source, target = _drifted(3_000, keys=lambda i: i)
        target = target[:1_000]  # 2,000 missing, sorted in files of 7.

        report = self._run(
            _Counting({"src": source, "dst": target}),
            spill_rows=7,
            ranges=ranges,
            samples=3,
        )

        assert (report.missing, report.matched) == (2_000, 1_000)
        assert (report.different, report.extra) == (0, 0)
        assert [d.kind for d in report.samples] == ["missing"] * 3
        if ranges:  # In the order of the keys.
            assert [d.key for d in report.samples] == [
                (1000,),
                (1001,),
                (1002,),
            ]

    @pytest.mark.parametrize("spill_rows", [1, 50_000])
    @pytest.mark.parametrize("ranges", [True, False])
    def test_a_number_and_its_text_differ_keyed_by_decimals(
        self, ranges: bool, spill_rows: int
    ) -> None:
        source = [
            {"id": decimal.Decimal(i) / 4, "total": decimal.Decimal(i) / 8}
            for i in range(1, 400)
        ]
        target = [
            {"id": float(row["id"]), "total": row["total"]} for row in source
        ]
        target[8]["total"] = str(target[8]["total"])
        reader = _Counting({"src": source, "dst": target})
        reconciliation = dataclasses.replace(
            reconciliations.reconciliation([], []),
            source=reader,
            target=reader,
            expect=lambda row: contract.Expected(rows=[row]),
            columns=("id", "total"),
        )

        report = cutover.reconcile(
            reconciliation, ranges=ranges, spill_rows=spill_rows
        )

        assert report.partitioning == ("range" if ranges else "hash")
        assert (report.matched, report.different) == (398, 1)
        (different,) = report.samples
        assert different.key == (cutover.canonical_value(source[8]["id"]),)
        assert different.columns == {
            "total": (cutover.canonical_value(source[8]["total"]), "1.125")
        }

    def test_an_empty_target_misses_every_row(self) -> None:
        source, _ = _drifted(500, keys=lambda i: i)

        report = self._run(_Counting({"src": source, "dst": []}))

        assert (report.missing, report.matched, report.target_rows) == (
            500,
            0,
            0,
        )

    def test_duplicates_on_either_side(self) -> None:
        source, target = _drifted(500, keys=lambda i: i)
        source.append(dict(source[10]))
        target.append(dict(target[20]))

        report = self._run(_Counting({"src": source, "dst": target}))

        assert report.duplicates == 2
        assert {d.key for d in report.samples if d.kind == "duplicate"} == {
            (10,),
            (20,),
        }
