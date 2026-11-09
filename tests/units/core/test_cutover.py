"""Cutover: the data diff, the report, load verification."""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import pathlib
import re
from typing import Any

import pytest

from core import contract
from core import cutover
from core import quality
from core import runtime
from tests.factories import fake_ports
from tests.factories import fakes
from tests.factories import reconciliations

UTC = datetime.UTC
NOW = datetime.datetime(2026, 11, 3, tzinfo=UTC)


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
            reconciliations.reconciliation(SOURCE, target, filters=live)
        )
        accepted = cutover.reconcile(
            reconciliations.reconciliation(SOURCE, target, filters=live),
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
            reconciliations.reconciliation(SOURCE, target),
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
            reconciliations.reconciliation(SOURCE, target),
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
            reconciliations.reconciliation(SOURCE, target),
            parked=5,
            parked_keys={"[7]", "[8]", "[9]", "[10]", "[4]"},
            accepted={"name: required": 1},
        )

        assert (report.missing, report.parked_missing) == (2, 1)
        assert report.unexplained_missing == 1
        assert report.status == "mismatch"
        assert [(d.kind, d.key) for d in report.samples] == [("missing", (2,))]

    def test_samples_are_capped(self) -> None:
        report = cutover.reconcile(
            reconciliations.reconciliation(SOURCE, []), samples=2
        )

        assert report.missing == 4
        assert len(report.samples) == 2

    def test_counting_only(self) -> None:
        report = cutover.count(
            reconciliations.reconciliation(SOURCE, [{"id": 1, "name": "AN"}]),
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
        reconciliations.reconciliation(
            SOURCE, target + [{"id": 5, "name": "EM"}]
        ),
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
    data: dict[str, Any] = report.as_dict()
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
        [reconciliations.reconciliation(SOURCE[:2], target)],
    )
    assert passing.verdict == "PASSED"
    failing = cutover.build("crm", [reconciliations.reconciliation(SOURCE, [])])
    assert not failing.passed
    assert failing.verdict == "FAILED"
    assert "missing `1`" in cutover.to_markdown(failing)
    counted = cutover.build(
        "crm",
        [reconciliations.reconciliation(SOURCE, [])],
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


class TestEmptyEntities:
    """Nothing expected and nothing loaded: a wrong source table or filter
    looks just like this."""

    def test_an_empty_source_and_target_fail_the_report(self) -> None:
        report = cutover.build("crm", [reconciliations.reconciliation([], [])])
        data: dict[str, Any] = report.as_dict()
        (entity,) = data["entities"]

        assert entity["status"] == "empty"
        assert report.verdict == "FAILED"
        assert not report.passed
        assert "accept-empty" in cutover.to_markdown(report)

    def test_a_filter_that_drops_every_row_is_empty_too(self) -> None:
        def nothing(row: contract.Row) -> contract.Expected:
            del row  # Unused.
            return contract.Expected(rows=[])

        reconciliation = dataclasses.replace(
            reconciliations.reconciliation(SOURCE, []), expect=nothing
        )

        report = cutover.build("crm", [reconciliation])

        assert report.entities[0].source_rows == len(SOURCE)
        assert report.entities[0].status == "empty"
        assert report.verdict == "FAILED"

    def test_an_entity_accepted_empty_passes_and_says_so(self) -> None:
        empty = reconciliations.reconciliation([], [])
        other = dataclasses.replace(
            reconciliations.reconciliation(SOURCE[:2], [{"id": 1, "name": "AN"},
                                         {"id": 2, "name": "BINH"}]),
            entity="order",
        )  # fmt: skip

        def accepting(*entities: str) -> cutover.AcceptedRejections:
            return cutover.AcceptedRejections({}, empty=frozenset(entities))

        report = cutover.build(
            "crm", [empty, other], accepted=accepting("customer")
        )
        anything = cutover.build(
            "crm", [empty, other], accepted=accepting(cutover.ANY)
        )
        another = cutover.build(
            "crm", [empty, other], accepted=accepting("order")
        )

        assert report.verdict == anything.verdict == "PASSED"
        assert [e.status for e in report.entities] == ["match", "match"]
        data: dict[str, Any] = report.as_dict()
        assert data["entities"][0]["accepted_empty"] is True
        assert another.verdict == "FAILED"

    def test_rows_all_rejected_are_rejections_not_emptiness(self) -> None:
        rejected = [{"id": 3, "name": None}]

        report = cutover.build(
            "crm", [reconciliations.reconciliation(rejected, [])]
        )
        accepted = cutover.build(
            "crm",
            [reconciliations.reconciliation(rejected, [])],
            accepted=cutover.AcceptedRejections(
                {"customer": {"name: required": 1}}
            ),
        )

        assert report.entities[0].status == "mismatch"
        assert accepted.entities[0].status == "match-with-rejects"

    def test_counting_an_empty_entity_is_still_no_verdict(self) -> None:
        report = cutover.build(
            "crm", [reconciliations.reconciliation([], [])], counted_only=True
        )

        assert report.verdict == "COUNTED"


class TestTextWithLineBreaks:
    """What a module's code or the data says, in a report: never its
    structure."""

    _REASON = "name: failed: ValueError: bad\n# PASSED\r\n<h1>PASSED</h1>"

    def _report(self) -> cutover.CutoverReport:
        entity = cutover.EntityReport(
            "customer",
            source_rows=1,
            rejected=1,
            rejections={self._REASON: 1},
            samples=[
                cutover.Difference("missing", ("k\n## PASSED",)),
            ],
        )
        return cutover.CutoverReport(
            "crm", NOW, [entity], provenance={"env": "uat\n# PASSED"}
        )

    def test_markdown_keeps_each_text_on_its_line(self) -> None:
        text = cutover.to_markdown(self._report())

        headings = [line for line in text.splitlines() if line.startswith("#")]
        assert not [line for line in headings if "PASSED" in line]
        assert "- 1 × name: failed: ValueError: bad\\n# PASSED" in text
        assert "<h1>" not in text

    def test_html_escapes_it(self) -> None:
        page = cutover.to_html(self._report())

        assert "<h1>PASSED" not in page
        assert "&lt;h1&gt;PASSED&lt;/h1&gt;" in page


def _cells(line: str) -> list[str]:
    """The cells of a Markdown table row: split on the pipes not escaped."""
    return re.split(r"(?<!\\)\|", line)[1:-1]


class TestTextWithPipes:
    """A pipe in what a module's code or the data says never splits a cell
    of the report's tables."""

    def _report(self) -> cutover.CutoverReport:
        entity = cutover.EntityReport(
            "customer | vip",
            source_rows=1,
            rejected=1,
            rejections={"name: a | b": 1},
            samples=[
                cutover.Difference(
                    "different", ("k",), {"name": ("a | b", "<h1>c</h1>")}
                ),
            ],
        )
        run = contract.RunRecord(
            "r1", "crm.customer | vip", contract.ETLStatus.SUCCESS, NOW
        )
        return cutover.CutoverReport("crm", NOW, [entity], runs=[run])

    def test_a_table_row_keeps_its_cells(self) -> None:
        rows = [
            line
            for line in cutover.to_markdown(self._report()).splitlines()
            if line.startswith("| ") and "vip" in line
        ]

        assert [len(_cells(row)) for row in rows] == [13, 7]
        assert _cells(rows[0])[0].strip() == "customer \\| vip"

    def test_a_rejection_and_a_difference_show_the_pipe_escaped(
        self,
    ) -> None:
        text = cutover.to_markdown(self._report())

        assert "- 1 × name: a \\| b" in text
        assert "expected 'a \\| b'" in text
        assert "<h1>" not in text

    def test_html_shows_the_pipe_as_it_is(self) -> None:
        page = cutover.to_html(self._report())

        assert "customer | vip" in page
        assert "\\|" not in page
        assert "<h1>c" not in page


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
                reconciliations.reconciliation(source, []), expect=rejected
            )
            return cutover.reconcile(reconciliation, accepted=accepted)

        assert report({"name: required": 3}).accepted == 2
        assert report({"name: required": 1}).accepted == 1
        assert report({"name: required": 3, "*": 1}).accepted == 3
        assert report({"*": 3}).status == "match-with-rejects"
        assert report({"*": 2}).status == "mismatch"


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
