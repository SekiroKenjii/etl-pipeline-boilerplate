"""Cutover: the canonical value of each type, and the resolution of
timestamps."""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import uuid

import pytest

from core import contract
from core import cutover
from core.cutover import checksum
from tests.factories import reconciliations


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
        reconciliation = reconciliations.reconciliation([], [])

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
            reconciliations.reconciliation(source, target),
            expect=lambda row: contract.Expected(
                rows=[{**row, "name": str(row["name"]).upper()}]
            ),
            columns=("id", "name", "at"),
        )

        exact = cutover.reconcile(reconciliation)
        coarse = cutover.reconcile(reconciliation, resolution=300)

        assert (exact.status, exact.different) == ("mismatch", 1)
        assert (coarse.status, coarse.matched) == ("match", 1)
