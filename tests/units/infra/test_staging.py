"""The online choice between staging by parameters and by bulk copy, with a
fake clock: the trials, the choice, the hysteresis, the re-checks."""

from __future__ import annotations

from collections.abc import Callable, Mapping
import logging

import pytest
from sqlalchemy import event as sa_event
import sqlalchemy as sa

from core import contract
from infra.database.relational import dialects
from infra.database.relational import staging
from infra.database.relational import types as relational_types

TABLE = "dbo.customer"
ROWS = 1000


class _Clock:

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def _seconds(
    parameters: float, bulk_copy: float
) -> dict[relational_types.Staging, float]:
    """The seconds a batch takes by each strategy."""
    return {staging.PARAMETERS: parameters, staging.BULK_COPY: bulk_copy}


def _batches(
    chooser: staging.StagingChooser,
    seconds: Mapping[relational_types.Staging, float],
    count: int,
    table: str = TABLE,
    rows: int = ROWS,
) -> list[relational_types.Staging]:
    """Runs `count` batches, each taking `seconds` of its strategy."""
    picked = []
    for _ in range(count):
        strategy = chooser.pick(table, rows)
        chooser.record(table, strategy, rows, seconds[strategy])
        picked.append(strategy)
    return picked


class TestTheFirstBatches:

    def test_the_first_three_alternate_then_the_faster_one_is_kept(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        chooser = staging.StagingChooser()
        seconds = _seconds(0.120, 0.014)

        with caplog.at_level(logging.INFO):
            picked = _batches(chooser, seconds, 6)

        assert picked[:3] == [
            staging.PARAMETERS,
            staging.BULK_COPY,
            staging.PARAMETERS,
        ]
        assert picked[3:] == [staging.BULK_COPY] * 3
        assert chooser.chosen(TABLE) == staging.BULK_COPY
        (record,) = [r for r in caplog.records if "staging by" in r.message]
        assert record.levelno == logging.INFO
        assert (
            "dbo.customer: staging by bulk copy chosen after 3 batch(es)"
            in (record.message)
        )
        assert "bulk copy 0.014 ms a row, parameters 0.120 ms a row" in (
            record.message
        )

    def test_parameters_win_when_they_are_faster(self) -> None:
        chooser = staging.StagingChooser()
        seconds = _seconds(0.010, 0.030)

        picked = _batches(chooser, seconds, 5)

        assert picked[3:] == [staging.PARAMETERS] * 2
        assert chooser.chosen(TABLE) == staging.PARAMETERS

    def test_each_table_is_chosen_on_its_own(self) -> None:
        chooser = staging.StagingChooser()
        _batches(chooser, {staging.PARAMETERS: 0.1, staging.BULK_COPY: 0.01}, 3)
        _batches(
            chooser,
            {staging.PARAMETERS: 0.01, staging.BULK_COPY: 0.1},
            3,
            table="dbo.order",
        )

        assert chooser.chosen(TABLE) == staging.BULK_COPY
        assert chooser.chosen("dbo.order") == staging.PARAMETERS
        assert chooser.chosen("dbo.other") is None

    def test_small_batches_go_by_parameters_unmeasured(self) -> None:
        chooser = staging.StagingChooser()

        picked = _batches(
            chooser,
            {staging.PARAMETERS: 1.0, staging.BULK_COPY: 1.0},
            5,
            rows=50,
        )

        assert picked == [staging.PARAMETERS] * 5
        assert chooser.chosen(TABLE) is None

    def test_the_timings_are_per_row(self) -> None:
        chooser = staging.StagingChooser()
        # Bulk copy of 100 rows takes longer than parameters for 2,000, but
        # far less per row.
        chooser.record(TABLE, chooser.pick(TABLE, 2000), 2000, 0.200)
        chooser.record(TABLE, chooser.pick(TABLE, 100), 100, 0.020)
        chooser.record(TABLE, chooser.pick(TABLE, 2000), 2000, 0.200)

        assert chooser.chosen(TABLE) == staging.PARAMETERS


class TestReChecks:

    def test_the_other_way_is_tried_every_two_hundred_batches(self) -> None:
        chooser = staging.StagingChooser()
        seconds = _seconds(0.120, 0.014)

        picked = _batches(chooser, seconds, 450)

        others = [i for i, s in enumerate(picked) if s == staging.PARAMETERS]
        assert others == [0, 2, 199, 399]

    def test_a_switch_needs_twenty_percent(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        chooser = staging.StagingChooser(recheck_every=10)
        seconds = _seconds(0.120, 0.100)
        _batches(chooser, seconds, 9)
        assert chooser.chosen(TABLE) == staging.BULK_COPY

        # The re-check: parameters 15% faster than the way kept: no switch.
        seconds = _seconds(0.085, 0.100)
        with caplog.at_level(logging.INFO):
            picked = _batches(chooser, seconds, 1)
        assert picked == [staging.PARAMETERS]
        assert chooser.chosen(TABLE) == staging.BULK_COPY
        assert not [r for r in caplog.records if "instead" in r.message]

        # Ten batches later, 25% faster: switched, and said.
        seconds = _seconds(0.075, 0.100)
        with caplog.at_level(logging.INFO):
            picked = _batches(chooser, seconds, 10)
        assert picked[-1] == staging.PARAMETERS
        assert chooser.chosen(TABLE) == staging.PARAMETERS
        (record,) = [r for r in caplog.records if "instead" in r.message]
        assert "staging by parameters chosen instead after 20 batch(es)" in (
            record.message
        )

    def test_the_cadence_and_the_share_are_given(self) -> None:
        chooser = staging.StagingChooser(
            trials=2, recheck_every=5, hysteresis=0
        )
        seconds = _seconds(0.120, 0.014)

        picked = _batches(chooser, seconds, 12)

        assert picked[2] == staging.BULK_COPY  # Chosen after two trials.
        assert [i for i, s in enumerate(picked) if s == staging.PARAMETERS] == [
            0,
            4,
            9,
        ]

    @pytest.mark.parametrize(
        "make",
        [
            lambda: staging.StagingChooser(trials=1),
            lambda: staging.StagingChooser(recheck_every=0),
            lambda: staging.StagingChooser(hysteresis=1.0),
        ],
    )
    def test_bad_arguments_are_refused(
        self, make: Callable[[], staging.StagingChooser]
    ) -> None:
        with pytest.raises(ValueError):
            make()


class TestTheDialect:
    """What SQL Server's dialect asks the chooser, and times for it."""

    def test_a_set_threshold_decides_without_the_chooser(self) -> None:
        dialect = dialects.MssqlDialect(bulk_copy_rows=2000)

        assert dialect.chooser is None
        assert not dialect._bulk_copies(TABLE, 1999, None)
        assert dialect._bulk_copies(TABLE, 2000, None)
        # The key registry's own threshold.
        assert dialect._bulk_copies("etl_key_registry", 300, 300)
        assert not dialect._bulk_copies("etl_key_registry", 299, 300)

    def test_auto_asks_the_chooser_whatever_the_minimum(self) -> None:
        dialect = dialects.MssqlDialect()
        assert dialect.chooser is not None
        registry = "etl_key_registry"

        picked = []
        for strategy in (staging.PARAMETERS, staging.BULK_COPY):
            picked.append(dialect._bulk_copies(registry, 1000, 300))
            dialect.chooser.record(registry, strategy, 1000, 0.1)

        assert picked == [False, True]
        # A pick by parameters is to be timed by `fill_stage`.
        assert dialect._timing == {registry: 1}

    def test_parameter_staging_is_timed_for_the_chooser(self) -> None:
        clock = _Clock()
        chooser = staging.StagingChooser(clock=clock)
        dialect = dialects.MssqlDialect(chooser=chooser)
        target = sa.Table(
            "customer", sa.MetaData(), sa.Column("id", sa.Integer)
        )
        stage = dialect.stage(target, ["id"])
        engine = sa.create_engine("sqlite://")
        rows: list[contract.Row] = [{"id": i} for i in range(ROWS)]

        # Every statement takes half a second of the fake clock.
        sa_event.listen(
            engine,
            "before_cursor_execute",
            lambda *args: setattr(clock, "now", clock.now + 0.5),
        )

        picked = dialect._bulk_copies("customer", ROWS, None)
        try:
            with engine.begin() as connection:
                dialect.create_stage(connection, stage)
                dialect.fill_stage(connection, stage, rows)
                # A fill the chooser did not ask for (a delete): not timed.
                dialect.fill_stage(connection, stage, rows)
        finally:
            engine.dispose()

        assert picked is False
        assert chooser._tables["customer"].seconds == {
            staging.PARAMETERS: 0.5 / ROWS
        }
        assert chooser._tables["customer"].batches == 1
        assert dialect._timing == {"customer": 0}
