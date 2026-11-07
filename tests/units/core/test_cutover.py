"""Cutover: canonical values, the data diff, the report, load verification."""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import functools
import pathlib
from typing import Any
import uuid

import pytest

from core import contract
from core import cutover
from core import quality
from core import runtime
from core.cutover import checksum
from tests.factories import fake_ports
from tests.factories import fakes

UTC = datetime.UTC
NOW = datetime.datetime(2026, 11, 3, tzinfo=UTC)


class TestCanonicalValue:

    @pytest.mark.parametrize(
        ("left", "right"),
        [
            (decimal.Decimal("120.50"), 120.5),
            (decimal.Decimal("120"), 120),
            (120.0, 120),
            (True, 1),
            (uuid.UUID(int=1), "00000000-0000-0000-0000-000000000001"),
            (
                datetime.datetime(
                    2024,
                    1,
                    1,
                    7,
                    tzinfo=datetime.timezone(datetime.timedelta(hours=7)),
                ),
                datetime.datetime(2024, 1, 1, 0),
            ),
            ("", None),
            ("Müller", "Müller"),  # NFD and NFC.
        ],
    )
    def test_the_same_value_read_from_two_databases(
        self, left: object, right: object
    ) -> None:
        assert cutover.canonical_value(left) == cutover.canonical_value(right)

    @pytest.mark.parametrize(
        ("left", "right"),
        [
            ("1", 1),
            (decimal.Decimal("1.5"), decimal.Decimal("1.05")),
            ("1.5", decimal.Decimal("1.5")),
            ("1.5", 1.5),
            ("NaN", float("nan")),
            ("Infinity", decimal.Decimal("Infinity")),
        ],
    )
    def test_different_values_stay_different(
        self, left: object, right: object
    ) -> None:
        assert cutover.canonical_value(left) != cutover.canonical_value(right)

    def test_a_number_and_its_text_digest_apart(self) -> None:
        number = checksum.canonical_row({"v": decimal.Decimal("1.50")}, ["v"])
        text = checksum.canonical_row({"v": "1.5"}, ["v"])

        assert checksum.text(number) != checksum.text(text)
        assert checksum.digest(number) != checksum.digest(text)

    def test_a_number_not_a_number_equals_itself(self) -> None:
        assert cutover.canonical_value(
            decimal.Decimal("NaN")
        ) == cutover.canonical_value(float("nan"))

    def test_reports_show_a_number_apart_from_its_text(self) -> None:
        text, number, inexact = map(
            cutover.canonical_value,
            ("1.5", 1.5, decimal.Decimal("0.1000000000000000000001")),
        )

        assert (checksum.shown(text), checksum.shown(number)) == ("1.5", 1.5)
        assert (repr(text), repr(number)) == ("'1.5'", "1.5")
        assert checksum.shown(inexact) == "0.1000000000000000000001"


class TestTimestampResolution:
    """Timestamps compare to the microsecond, unless told a coarser
    resolution: then both sides round to its nearest tick."""

    def test_microseconds_by_default_a_coarser_side_differs(self) -> None:
        precise = datetime.datetime(2026, 1, 1, 10, 0, 0, 123456)
        sql_server = datetime.datetime(2026, 1, 1, 10, 0, 0, 123000)

        assert cutover.canonical_value(precise) != cutover.canonical_value(
            sql_server
        )

    @pytest.mark.parametrize(
        ("precise", "stored", "resolution"),
        [
            # SQL Server `datetime`: 1/300 s, read back as milliseconds.
            ("10:00:00.123456", "10:00:00.123", 300),
            ("10:00:00.125000", "10:00:00.127", 300),
            ("10:00:00.001700", "10:00:00.003", 300),
            ("10:00:00.998000", "10:00:00.997", 300),
            ("10:00:00.999000", "10:00:01.000", 300),
            # Milliseconds, whole seconds (MySQL DATETIME, Oracle DATE).
            ("10:00:00.123456", "10:00:00.123", 1000),
            ("10:00:00.499999", "10:00:00", 1),
            ("10:00:00.500000", "10:00:01", 1),
            ("23:59:59.700000", "00:00:00", 1),
        ],
    )
    def test_both_sides_round_to_the_coarser_resolution(
        self, precise: str, stored: str, resolution: int
    ) -> None:
        day = datetime.date(2026, 1, 1)
        left = datetime.datetime.combine(
            day, datetime.time.fromisoformat(precise)
        )
        right = datetime.datetime.combine(
            day, datetime.time.fromisoformat(stored)
        )
        if stored == "00:00:00":
            right += datetime.timedelta(days=1)

        assert cutover.canonical_value(
            left, resolution
        ) == cutover.canonical_value(right, resolution)
        assert cutover.canonical_value(
            left.time(), resolution
        ) == cutover.canonical_value(right.time(), resolution)

    def test_a_tick_apart_still_differs(self) -> None:
        left = datetime.datetime(2026, 1, 1, 10, 0, 0, 120000)
        right = datetime.datetime(2026, 1, 1, 10, 0, 0, 127000)

        assert cutover.canonical_value(left, 300) != cutover.canonical_value(
            right, 300
        )

    def test_a_resolution_out_of_range_is_refused(self) -> None:
        moment = datetime.datetime(2026, 1, 1, 10, 0, 0, 1)
        reconciliation = _reconciliation([], [])

        with pytest.raises(ValueError, match="resolution must be 1 to"):
            cutover.canonical_value(moment, 0)
        with pytest.raises(ValueError, match="resolution must be 1 to"):
            cutover.reconcile(reconciliation, resolution=0)

    def test_the_diff_compares_at_the_resolution(self) -> None:
        source = [{
            "id": 1,
            "name": "an",
            "at": datetime.datetime(2026, 1, 1, 10, 0, 0, 125000),
        }]
        target = [{
            "id": 1,
            "name": "AN",
            "at": datetime.datetime(2026, 1, 1, 10, 0, 0, 127000),
        }]
        reconciliation = dataclasses.replace(
            _reconciliation(source, target),
            expect=lambda row: contract.Expected(
                rows=[{**row, "name": str(row["name"]).upper()}]
            ),
            columns=("id", "name", "at"),
        )

        exact = cutover.reconcile(reconciliation)
        coarse = cutover.reconcile(reconciliation, resolution=300)

        assert (exact.status, exact.different) == ("mismatch", 1)
        assert (coarse.status, coarse.matched) == ("match", 1)


def _expect(row: contract.Row) -> contract.Expected:
    """A mapping: id and name, uppercase; a row without name is rejected."""
    if not row.get("name"):
        return contract.Expected(rejected=["name: required"])
    return contract.Expected(
        rows=[{"id": row["id"], "name": str(row["name"]).upper()}]
    )


def _reconciliation(
    source: list[dict[str, Any]],
    target: list[dict[str, Any]],
    *,
    filters: tuple[contract.Condition, ...] = (),
) -> contract.Reconciliation:
    reader = fake_ports.InMemoryTableReader({"src": source, "dst": target})
    return contract.Reconciliation(
        entity="customer",
        source=reader,
        source_query=contract.TableQuery("src", order_by=("id",)),
        expect=_expect,
        target=reader,
        target_query=contract.TableQuery(
            "dst", order_by=("id",), filters=filters
        ),
        key=("id",),
        columns=("id", "name"),
        parking_queue="customer",
    )


SOURCE: list[dict[str, Any]] = [
    {"id": 1, "name": "an"},
    {"id": 2, "name": "binh"},
    {"id": 3, "name": None},  # Rejected.
    {"id": 4, "name": "dung"},  # Parked, say.
    {"id": 5, "name": "em"},
]


class TestReconcile:

    def test_a_target_holding_what_is_expected_but_rejects_mismatches(
        self,
    ) -> None:
        target = [
            {"id": 1, "name": "AN", "is_deleted": False},
            {"id": 2, "name": "BINH", "is_deleted": False},
            {"id": 4, "name": "DUNG", "is_deleted": False},
            {"id": 5, "name": "EM", "is_deleted": False},
            {"id": 9, "name": "OLD", "is_deleted": True},
        ]
        live = (contract.Condition("is_deleted", contract.Operator.EQ, False),)

        report = cutover.reconcile(
            _reconciliation(SOURCE, target, filters=live)
        )
        accepted = cutover.reconcile(
            _reconciliation(SOURCE, target, filters=live),
            accepted={"name: required": 1},
        )

        # A rejected row is not in the target: unless accepted, a mismatch.
        assert report.status == "mismatch"
        assert (report.source_rows, report.rejected) == (5, 1)
        assert (report.accepted, report.unaccepted) == (0, 1)
        assert report.rejections == {"name: required": 1}
        assert (report.expected_rows, report.target_rows) == (4, 4)
        assert report.matched == 4
        assert accepted.status == "match-with-rejects"
        assert (accepted.accepted, accepted.unaccepted) == (1, 0)

    @pytest.mark.parametrize("ranges", [True, False])
    @pytest.mark.parametrize("buckets", [None, 1, 2, 7, 4096])
    def test_differences_are_found_whatever_the_partitions(
        self, buckets: int | None, ranges: bool
    ) -> None:
        target = [
            {"id": 1, "name": "AN"},
            {"id": 2, "name": "BINH!"},  # Different.
            {"id": 5, "name": "EM"},
            {"id": 6, "name": "GIA"},  # Extra.
            {"id": 6, "name": "GIA"},  # And twice.
        ]

        report = cutover.reconcile(
            _reconciliation(SOURCE, target),
            buckets=buckets,
            ranges=ranges,
        )

        assert report.status == "mismatch"
        assert report.partitioning == ("range" if ranges else "hash")
        assert (
            report.matched,
            report.missing,
            report.extra,
            report.different,
            report.duplicates,
        ) == (2, 1, 0, 1, 1)
        kinds = {(d.kind, d.key) for d in report.samples}
        assert kinds == {
            ("different", (2,)),
            ("missing", (4,)),
            ("duplicate", (6,)),
        }
        (different,) = [d for d in report.samples if d.kind == "different"]
        assert different.columns == {"name": ("BINH", "BINH!")}

    def test_parked_orphans_explain_missing_rows_by_key(self) -> None:
        target = [
            {"id": 1, "name": "AN"},
            {"id": 2, "name": "BINH"},
            {"id": 5, "name": "EM"},
        ]

        report = cutover.reconcile(
            _reconciliation(SOURCE, target),
            parked=1,
            parked_keys={"[4]"},
            accepted={"name: required": 1},
        )

        assert (report.missing, report.parked_missing) == (1, 1)
        assert report.unexplained_missing == 0
        assert report.status == "match-with-rejects"
        assert report.samples == []

    def test_parked_orphans_of_other_keys_explain_nothing(self) -> None:
        # Regression: the parked count was subtracted from the missing rows,
        # whatever their keys.
        target = [{"id": 1, "name": "AN"}, {"id": 5, "name": "EM"}]

        report = cutover.reconcile(
            _reconciliation(SOURCE, target),
            parked=5,
            parked_keys={"[7]", "[8]", "[9]", "[10]", "[4]"},
            accepted={"name: required": 1},
        )

        assert (report.missing, report.parked_missing) == (2, 1)
        assert report.unexplained_missing == 1
        assert report.status == "mismatch"
        assert [(d.kind, d.key) for d in report.samples] == [("missing", (2,))]

    def test_samples_are_capped(self) -> None:
        report = cutover.reconcile(_reconciliation(SOURCE, []), samples=2)

        assert report.missing == 4
        assert len(report.samples) == 2

    def test_counting_only(self) -> None:
        report = cutover.count(
            _reconciliation(SOURCE, [{"id": 1, "name": "AN"}]),
        )

        assert (report.source_rows, report.target_rows) == (5, 1)
        assert report.status == "counted"


class _Parking(contract.IParkingLot):

    def __init__(self, *ids: int) -> None:
        self.waiting = [
            contract.ParkedEntry(str(i), {"id": i}, "no parent") for i in ids
        ]

    def park(self, queue: str, entries: Any) -> None:
        raise NotImplementedError

    def entries(self, queue: str) -> Any:
        return iter(self.waiting)

    def release(self, queue: str, entries: Any) -> None:
        raise NotImplementedError


def _parked_row(entry: contract.ParkedEntry) -> contract.Row | None:
    if not isinstance(entry.payload, dict):
        return None
    row: contract.Row = dict(entry.payload)
    return row


def test_the_report_of_a_module() -> None:
    ledger = runtime.InMemoryRunLedger()
    for name in ("crm.full_load", "crm.full_load", "billing.load"):
        run = contract.ETLRunInfo(pipeline_name=name)
        ledger.start(run, "host")
    target = [{"id": 1, "name": "AN"}, {"id": 2, "name": "BINH"}]
    reconciliation = dataclasses.replace(
        _reconciliation(SOURCE, target + [{"id": 5, "name": "EM"}]),
        parked_row=_parked_row,
    )
    accepted = cutover.AcceptedRejections({"customer": {"name: required": 1}})

    report = cutover.build(
        "crm",
        [reconciliation],
        parking=_Parking(4),
        ledger=ledger,
        accepted=accepted,
        provenance={"env": "uat", "module": {"mappings": "abc"}},
    )
    data = report.as_dict()
    text = cutover.to_markdown(report)

    assert report.passed
    assert report.verdict == "PASSED-WITH-REJECTS"
    assert [run.pipeline for run in report.runs] == ["crm.full_load"]
    assert (data["passed"], data["verdict"]) == (True, "PASSED-WITH-REJECTS")
    assert data["provenance"] == {"env": "uat", "module": {"mappings": "abc"}}
    (entity,) = data["entities"]
    assert (entity["parked"], entity["parked_missing"]) == (1, 1)
    assert (entity["accepted"], entity["unaccepted"]) == (1, 0)
    assert entity["status"] == "match-with-rejects"
    assert "Verdict: **PASSED-WITH-REJECTS**." in text
    assert "1 × name: required" in text
    assert "- module.mappings: `abc`" in text

    unaccepted = cutover.build("crm", [reconciliation], parking=_Parking(4))
    assert unaccepted.verdict == "FAILED"
    passing = cutover.build(
        "crm",
        [_reconciliation(SOURCE[:2], target)],
    )
    assert passing.verdict == "PASSED"
    failing = cutover.build("crm", [_reconciliation(SOURCE, [])])
    assert not failing.passed
    assert failing.verdict == "FAILED"
    assert "missing `1`" in cutover.to_markdown(failing)
    counted = cutover.build(
        "crm",
        [_reconciliation(SOURCE, [])],
        counted_only=True,
    )
    assert counted.verdict == "COUNTED"


class TestVerdict:
    """The verdicts a reviewer reproduced as wrong."""

    def test_an_entity_whose_rows_are_all_rejected_mismatches(self) -> None:
        entity = cutover.EntityReport(
            "customer",
            source_rows=10,
            rejected=10,
            rejections={"full_name: is required but empty": 10},
        )

        assert entity.status == "mismatch"
        assert not cutover.CutoverReport("crm", NOW, [entity]).passed

    def test_parked_orphans_do_not_explain_missing_rows_by_count(
        self,
    ) -> None:
        stale = cutover.EntityReport(
            "order", expected_rows=10, target_rows=9, matched=9, missing=1,
            parked=1,
        )  # fmt: skip
        more = cutover.EntityReport("order", missing=3, parked=5)

        assert (stale.status, stale.unexplained_missing) == ("mismatch", 1)
        assert (more.status, more.unexplained_missing) == ("mismatch", 3)


class TestAcceptedRejections:

    def test_read_from_the_rejected_sheet_saved_as_csv(
        self, tmp_path: pathlib.Path
    ) -> None:
        path = tmp_path / "accepted.csv"
        path.write_text(
            "Entity,Reason,Rows\n"
            "customer,full_name: is required but empty,3\n"
            "order,*,2\n",
            encoding="utf-8",
        )

        accepted = cutover.AcceptedRejections.from_csv(path)

        assert accepted.of("customer") == {
            "full_name: is required but empty": 3
        }
        assert accepted.of("order") == {"*": 2}
        assert accepted.of("other") == {}
        assert accepted.as_dict()["source"] == "accepted.csv"
        assert len(accepted.digest) == 64

    @pytest.mark.parametrize(
        ("content", "message"),
        [
            ("entity,rows\n", "needs the columns entity, reason, rows"),
            ("entity,reason,rows\ncustomer,x,0\n", ":2: rows must be at"),
            ("entity,reason,rows\ncustomer,x,many\n", "a whole number"),
            ("entity,reason,rows\n,x,1\n", "needs an entity and a reason"),
            (
                "entity,reason,rows\ncustomer,x,1\ncustomer,x,2\n",
                ":3: customer 'x' is accepted twice",
            ),
        ],
    )
    def test_a_wrong_file_is_refused(
        self, tmp_path: pathlib.Path, content: str, message: str
    ) -> None:
        path = tmp_path / "accepted.csv"
        path.write_text(content, encoding="utf-8")

        with pytest.raises(ValueError, match=message):
            cutover.AcceptedRejections.from_csv(path)

    def test_no_more_rows_than_accepted_and_for_their_reasons(self) -> None:
        source = [{"id": i, "name": None} for i in range(3)]

        def rejected(row: contract.Row) -> contract.Expected:
            reasons = ["name: required"]
            if row["id"] == 2:
                reasons.append("id: too big")
            return contract.Expected(rejected=reasons)

        def report(accepted: dict[str, int]) -> cutover.EntityReport:
            reconciliation = dataclasses.replace(
                _reconciliation(source, []), expect=rejected
            )
            return cutover.reconcile(reconciliation, accepted=accepted)

        assert report({"name: required": 3}).accepted == 2
        assert report({"name: required": 1}).accepted == 1
        assert report({"name: required": 3, "*": 1}).accepted == 3
        assert report({"*": 3}).status == "match-with-rejects"
        assert report({"*": 2}).status == "mismatch"


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
            _reconciliation([], []),
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
            _reconciliation([], []),
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


class TestLoadedRowsCheck:

    def _check(
        self, target: list[dict[str, Any]]
    ) -> cutover.LoadedRowsCheck[dict[str, Any]]:
        return cutover.LoadedRowsCheck[dict[str, Any]](
            fake_ports.InMemoryTableReader({"dst": target}),
            "dst",
            key=("id",),
            columns=("name", "total"),
            row=lambda record: None if record.get("deleted") else record,
        )

    def test_rows_read_back_as_loaded_pass(self) -> None:
        check = self._check(
            [{"id": 1, "name": "An", "total": decimal.Decimal("1.50")}]
        )

        assert (
            list(
                check.check_batch(
                    [
                        {"id": 1, "name": "An", "total": 1.5},
                        {"id": 2, "deleted": True},
                    ],
                    fakes.make_context(),
                )
            )
            == []
        )

    def test_lost_and_changed_rows_are_violations(self) -> None:
        check = self._check([{"id": 1, "name": "An", "total": 1}])

        violations = list(
            check.check_batch(
                [
                    {"id": 1, "name": "An", "total": 2},
                    {"id": 2, "name": "Bruno", "total": 1},
                ],
                fakes.make_context(),
            )
        )

        assert [(v.record_id, v.message) for v in violations] == [
            ("[1]", "differs in the target: total 2 became 1"),
            ("[2]", "not found in the target after its load"),
        ]

    def test_as_an_after_load_rule_it_fails_the_batch(self) -> None:
        gate = quality.QualityGate({
            contract.Checkpoint.AFTER_LOAD: quality.CheckSet(
                batch=[self._check([])]
            )
        })

        outcome = gate.inspect(
            contract.Checkpoint.AFTER_LOAD,
            [{"id": 1, "name": "An", "total": 1}],
            fakes.make_context(),
        )

        assert outcome.report is not None
        assert outcome.report.failed
