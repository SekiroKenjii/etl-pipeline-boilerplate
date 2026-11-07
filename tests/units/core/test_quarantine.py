"""Quarantines kept, replayed and purged; corrections of source rows."""

from __future__ import annotations

import pathlib
from typing import Any

import pytest

from core import contract
from core import operators
from core import pipeline
from core import quality
from tests.factories import fakes
from tests.factories import quarantines


def test_the_memory_quarantine() -> None:
    quarantines.check_quarantine(quality.InMemoryQuarantine())


def test_the_file_quarantine(tmp_path: pathlib.Path) -> None:
    quarantines.check_quarantine(quality.JsonLinesQuarantine(tmp_path))


def test_parking_quarantines_read_their_fallback() -> None:
    fallback = quality.InMemoryQuarantine()
    quarantines.check_quarantine(
        quality.ParkingQuarantine(
            quality.InMemoryParkingLot(),
            "order",
            rules={"never"},
            encode=lambda record: record,
            identity=str,
            fallback=fallback,
        )
    )


def _quarantine(*keys: str) -> quality.InMemoryQuarantine:
    store = quality.InMemoryQuarantine()
    ctx = fakes.make_context("crm.full_load")
    issue = contract.QualityIssue(
        "r",
        contract.Checkpoint.AFTER_TRANSFORM,
        contract.QualityAction.REJECT,
        "bad",
    )
    store.put(
        [
            # Rejected twice with other data (or before an upgrade): two
            # open entries of one key.
            contract.RejectedRecord(
                {"key": key, "n": n}, issue.checkpoint, (issue,)
            )
            for n, key in enumerate(keys)
        ],
        ctx,
    )
    return store


class TestQuarantineReplay:

    def _run(
        self,
        store: quality.InMemoryQuarantine,
        source: dict[str, dict[str, Any]],
        *,
        dry_run: bool = False,
    ) -> tuple[
        list[dict[str, Any]], operators.QuarantineReplay[dict[str, Any]]
    ]:
        def fetch(
            entries: Any, ctx: contract.ETLContext
        ) -> list[dict[str, Any]]:
            return [
                source[e.record["key"]]
                for e in entries
                if e.record["key"] in source
            ]

        extractor = operators.QuarantineReplay[dict[str, Any]](
            store,
            fetch=fetch,
            entry_identity=lambda e: e.record["key"],
            identity=lambda row: row["key"],
            chunk_size=1,
        )
        loader = fakes.CollectingLoader[dict[str, Any]]()
        pipeline.ETLJob(
            extractor, operators.Passthrough[dict[str, Any]](), loader
        )(fakes.make_context("crm.replay", dry_run=dry_run), {})
        return loader.rows, extractor

    def test_what_is_fetched_is_replayed_the_rest_stays_open(self) -> None:
        store = _quarantine("a", "b", "a")  # "a" rejected twice.

        rows, extractor = self._run(store, {"a": {"key": "a", "v": 1}})

        assert rows == [{"key": "a", "v": 1}]  # Once.
        assert (extractor.replayed, extractor.unmatched) == (2, 1)
        assert [e.record["key"] for e in store.entries()] == ["b"]

    def test_a_dry_run_resolves_nothing(self) -> None:
        store = _quarantine("a")

        self._run(store, {"a": {"key": "a"}}, dry_run=True)

        assert len(list(store.entries())) == 1


class TestCorrections:

    def _file(self, tmp_path: pathlib.Path, text: str) -> pathlib.Path:
        path = tmp_path / "fixes.csv"
        # As Excel saves a CSV: with a byte order mark.
        path.write_text(text, encoding="utf-8-sig")
        return path

    def test_read_from_a_file_and_applied(self, tmp_path: pathlib.Path) -> None:
        corrections = operators.Corrections.from_csv(
            self._file(
                tmp_path,
                "_id,mapping,source_key,_issues,CRT_DT,CTRY\n"
                "7,customer,1003,born: not a date,28/02/2020,\n",
            )
        )

        assert len(corrections) == 1
        assert corrections.apply(
            "customer", "1003", {"CRT_DT": "31/02/2020", "CTRY": "VN"}
        ) == {"CRT_DT": "28/02/2020", "CTRY": "VN"}
        row: contract.Row = {"CRT_DT": "x"}
        assert corrections.apply("customer", "1004", row) is row
        assert corrections.apply("order", "1003", row) is row

    def test_rows_and_changes_are_corrected_before_their_mapping(self) -> None:
        corrections = operators.Corrections({"t": {"1": {"name": "Fixed"}}})
        transformer = corrections.transformer(
            "t",
            lambda r: str(
                (r.row if isinstance(r, contract.ChangeEvent) else r)["id"]
            ),
        )
        change = contract.ChangeEvent(
            "t", contract.ChangeOperation.UPDATE, {"id": 1, "name": "x"}, (1,)
        )

        out = list(
            transformer.transform(
                [{"id": 1, "name": "x"}, {"id": 2, "name": "y"}, change],
                fakes.make_context(),
            )
        )

        assert out[0] == {"id": 1, "name": "Fixed"}
        assert out[1] == {"id": 2, "name": "y"}
        assert out[2].row == {"id": 1, "name": "Fixed"}
        assert out[2].operation is contract.ChangeOperation.UPDATE
        assert transformer.collect_stats().corrected == 2
        assert transformer.collect_stats().corrected == 0

    @pytest.mark.parametrize(
        ("text", "message"),
        [
            ("mapping,CRT_DT\ncustomer,x\n", "mapping and source_key"),
            ("mapping,source_key\n,1\n", "no mapping or source_key"),
        ],
    )
    def test_bad_files_are_refused(
        self, tmp_path: pathlib.Path, text: str, message: str
    ) -> None:
        path = self._file(tmp_path, text)

        with pytest.raises(contract.PipelineConfigError, match=message):
            operators.Corrections.from_csv(path)

    def test_a_missing_file_is_refused(self, tmp_path: pathlib.Path) -> None:
        with pytest.raises(contract.PipelineConfigError, match="Cannot read"):
            operators.Corrections.from_csv(tmp_path / "nope.csv")
