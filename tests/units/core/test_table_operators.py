from __future__ import annotations

from collections.abc import Sequence
import datetime
from typing import Any

import pytest

from core import contract
from core import operators
from core import pipeline
from core import state as core_state
from tests.factories import fake_ports
from tests.factories import fakes

T0 = datetime.datetime(2026, 10, 1, tzinfo=datetime.UTC)


def _at(minutes: int) -> datetime.datetime:
    return T0 + datetime.timedelta(minutes=minutes)


def _rows(*items: tuple[int, int]) -> list[dict[str, Any]]:
    """Rows of `(id, minutes)`: `updated_at` is T0 plus the minutes."""
    return [{"id": i, "updated_at": _at(m), "name": f"n{i}"} for i, m in items]


def _extractor(
    reader: contract.ITableReader,
    state: contract.IStateStore | None,
    **options: Any,
) -> operators.TableExtractor:
    options.setdefault("watermark", "updated_at")
    return operators.TableExtractor(
        reader, "crm.customer", key=["id"], state=state, **options
    )


def _run(
    extractor: operators.TableExtractor,
    batch_size: int = 2,
    ctx: contract.ETLContext | None = None,
    loader: fakes.CollectingLoader[contract.Row] | None = None,
) -> list[Any]:
    loader = loader or fakes.CollectingLoader[contract.Row]()
    job = pipeline.ETLJob(
        extractor,
        operators.Passthrough[contract.Row](),
        loader,
        batch_size=batch_size,
    )
    job(ctx or fakes.make_context("crm"), {})
    return [row["id"] for row in loader.rows]


class TestTableExtractor:

    def test_rows_come_by_watermark_then_key(self) -> None:
        reader = fake_ports.InMemoryTableReader(
            {"crm.customer": _rows((3, 1), (1, 2), (2, 1))}
        )

        assert _run(_extractor(reader, state=None)) == [2, 3, 1]

    def test_the_next_run_starts_after_the_last_loaded_row(self) -> None:
        table = _rows((1, 1), (2, 2))
        reader = fake_ports.InMemoryTableReader({"crm.customer": table})
        state = core_state.InMemoryStateStore()

        assert _run(_extractor(reader, state)) == [1, 2]
        # A row updated, a row inserted with the same watermark (a tie).
        table[0]["updated_at"] = _at(5)
        table.append({"id": 3, "updated_at": _at(2), "name": "n3"})
        table.append({"id": 4, "updated_at": _at(6), "name": "n4"})

        # Row 2, at the largest watermark of the first run, is read again.
        assert _run(_extractor(reader, state)) == [2, 3, 1, 4]
        assert _run(_extractor(reader, state)) == [4]

    def test_a_failed_batch_is_read_again(self) -> None:
        reader = fake_ports.InMemoryTableReader(
            {"crm.customer": _rows((1, 1), (2, 2), (3, 3))}
        )
        state = core_state.InMemoryStateStore()
        failing = fakes.CollectingLoader[contract.Row](fail_on_batch=2)
        extractor = _extractor(reader, state)

        with pytest.raises(contract.RetryableError):
            _run(extractor, loader=failing)

        assert _run(_extractor(reader, state)) == [3]

    def test_a_run_reads_up_to_the_watermark_at_its_start(self) -> None:
        reader = fake_ports.InMemoryTableReader(
            {"crm.customer": _rows((1, 1), (2, 2))}
        )
        extractor = _extractor(reader, state=None)

        query = extractor.query(fakes.make_context())

        assert query is not None
        assert query.upper == _at(2)

    def test_an_empty_table_is_not_read(self) -> None:
        reader = fake_ports.InMemoryTableReader({"crm.customer": []})

        assert _run(_extractor(reader, state=None)) == []
        assert not reader.queries

    def test_a_backfill_window_never_moves_the_position_back(self) -> None:
        reader = fake_ports.InMemoryTableReader(
            {"crm.customer": _rows((1, 1), (2, 2), (3, 3))}
        )
        state = core_state.InMemoryStateStore()
        _run(_extractor(reader, state))
        backfill = contract.ETLContext(
            contract.ETLRunInfo("crm", since=_at(2), until=_at(2))
        )

        assert _run(_extractor(reader, state), ctx=backfill) == [2]
        assert _extractor(reader, state).saved_position(backfill) == (_at(3), 3)

    def test_lookback_reads_a_window_again(self) -> None:
        reader = fake_ports.InMemoryTableReader(
            {"crm.customer": _rows((1, 1), (2, 5), (3, 10))}
        )
        state = core_state.InMemoryStateStore()
        _run(_extractor(reader, state))

        rerun = _extractor(
            reader, state, lookback=datetime.timedelta(minutes=5)
        )

        assert _run(rerun) == [2, 3]

    def test_a_lookback_must_fit_the_watermark(self) -> None:
        reader = fake_ports.InMemoryTableReader({"crm.customer": _rows((1, 1))})
        state = core_state.InMemoryStateStore()
        _run(_extractor(reader, state))
        misfit = _extractor(reader, state, lookback=5)

        with pytest.raises(contract.FatalError, match="does not apply"):
            _run(misfit)

    def test_without_watermark_rows_come_by_key(self) -> None:
        table: list[dict[str, Any]] = [{"id": 2}, {"id": 1}]
        reader = fake_ports.InMemoryTableReader({"crm.customer": table})
        state = core_state.InMemoryStateStore()

        assert _run(_extractor(reader, state, watermark=None)) == [1, 2]
        table.append({"id": 3})
        assert _run(_extractor(reader, state, watermark=None)) == [3]

    def test_a_position_on_other_columns_is_refused(self) -> None:
        reader = fake_ports.InMemoryTableReader({"crm.customer": _rows((1, 1))})
        state = core_state.InMemoryStateStore()
        _run(_extractor(reader, state))
        by_key = _extractor(reader, state, watermark=None)

        with pytest.raises(contract.FatalError, match="Delete it"):
            _run(by_key)

    def test_a_late_commit_at_the_largest_watermark_is_not_lost(
        self,
    ) -> None:
        # A coarse watermark: rows 5 and 9 at the same second, read; row 3
        # commits after the run, with that second too.
        table = _rows((5, 1), (9, 1))
        reader = fake_ports.InMemoryTableReader({"crm.customer": table})
        state = core_state.InMemoryStateStore()

        assert _run(_extractor(reader, state)) == [5, 9]
        table.append({"id": 3, "updated_at": _at(1), "name": "late"})

        assert _run(_extractor(reader, state)) == [3, 5, 9]

    def test_rows_at_the_largest_watermark_are_read_again_once(
        self,
    ) -> None:
        table = _rows((1, 1), (2, 2), (3, 3))
        reader = fake_ports.InMemoryTableReader({"crm.customer": table})
        state = core_state.InMemoryStateStore()

        assert _run(_extractor(reader, state), batch_size=1) == [1, 2, 3]
        assert _run(_extractor(reader, state)) == [3]
        assert _run(_extractor(reader, state)) == []
        table.append({"id": 4, "updated_at": _at(4), "name": "n4"})

        assert _run(_extractor(reader, state)) == [4]
        assert _run(_extractor(reader, state)) == [4]
        assert _run(_extractor(reader, state)) == []

    @pytest.mark.parametrize("aware", [False, True])
    def test_a_window_is_compared_like_the_column(self, aware: bool) -> None:
        # Naive columns (SQL Server datetime2, PostgreSQL timestamp) hold UTC.
        zone = datetime.UTC if aware else None
        rows = [
            {
                "id": i,
                "updated_at": datetime.datetime(2026, 10, 1, h, tzinfo=zone),
            }
            for i, h in ((1, 1), (2, 2), (3, 3))
        ]
        reader = fake_ports.InMemoryTableReader({"crm.customer": rows})
        paris = datetime.timezone(datetime.timedelta(hours=2))
        window = contract.ETLContext(
            contract.ETLRunInfo(
                "crm",
                # 02:00 and 03:00 UTC; naive given means UTC.
                since=datetime.datetime(2026, 10, 1, 4, tzinfo=paris),
                until=datetime.datetime(2026, 10, 1, 3),
            )
        )

        assert _run(_extractor(reader, state=None), ctx=window) == [2, 3]
        assert _run(
            _extractor(reader, state=None, snapshot=False), ctx=window
        ) == [2, 3]

    def test_dry_runs_save_nothing(self) -> None:
        reader = fake_ports.InMemoryTableReader({"crm.customer": _rows((1, 1))})
        state = core_state.InMemoryStateStore()

        _run(_extractor(reader, state), ctx=fakes.make_context(dry_run=True))

        assert state.get("test-pipeline", "table:crm.customer") is None

    @pytest.mark.parametrize(
        ("options", "message"),
        [
            ({"key": []}, "key must name"),
            ({"key": ["id"], "lookback": 5}, "lookback needs a watermark"),
            (
                {"key": ["id"], "watermark": "updated_at", "columns": ["id"]},
                "columns must include",
            ),
        ],
    )
    def test_invalid_options(self, options: Any, message: str) -> None:
        reader = fake_ports.InMemoryTableReader({})

        with pytest.raises(ValueError, match=message):
            operators.TableExtractor(reader, "crm.customer", **options)


class _CancellingLoader(fakes.CollectingLoader[contract.Row]):
    """Cancels the run once it loaded `after` batches: SIGTERM, a lost lease."""

    def __init__(self, after: int) -> None:
        super().__init__()
        self._after = after

    def load(
        self, records: Sequence[contract.Row], ctx: contract.ETLContext
    ) -> contract.ETLStats:
        stats = super().load(records, ctx)
        if self.batches == self._after:
            ctx.cancel()
        return stats


class TestReadingAgain:
    """The rows at the saved watermark are read again until a run has read
    them all, whatever dies or stops half-way."""

    def _late_commits(self) -> tuple[list[dict[str, Any]], Any]:
        """Row 9 read at minute 1, then 1 to 4 committed late at minute 1."""
        table = _rows((9, 1))
        reader = fake_ports.InMemoryTableReader({"crm.customer": table})
        state = core_state.InMemoryStateStore()
        assert _run(_extractor(reader, state)) == [9]
        table.extend(_rows((1, 1), (2, 1), (3, 1), (4, 1)))
        return table, (reader, state)

    def test_a_run_dying_half_way_loses_no_late_row(self) -> None:
        _, (reader, state) = self._late_commits()
        failing = fakes.CollectingLoader[contract.Row](fail_on_batch=2)
        extractor = _extractor(reader, state)

        with pytest.raises(contract.RetryableError):
            _run(extractor, batch_size=1, loader=failing)

        assert failing.rows == [{"id": 1, "updated_at": _at(1), "name": "n1"}]
        assert _run(_extractor(reader, state)) == [2, 3, 4, 9]
        assert _run(_extractor(reader, state)) == []

    def test_a_run_cancelled_half_way_loses_no_late_row(self) -> None:
        _, (reader, state) = self._late_commits()

        assert _run(
            _extractor(reader, state),
            batch_size=1,
            loader=_CancellingLoader(after=2),
        ) == [1, 2]
        assert _run(_extractor(reader, state)) == [3, 4, 9]
        assert _run(_extractor(reader, state)) == []

    def test_a_run_dying_twice_half_way_loses_no_late_row(self) -> None:
        _, (reader, state) = self._late_commits()

        for _ in range(2):
            failing = fakes.CollectingLoader[contract.Row](fail_on_batch=2)
            extractor = _extractor(reader, state)
            with pytest.raises(contract.RetryableError):
                _run(extractor, batch_size=1, loader=failing)

        assert _run(_extractor(reader, state)) == [3, 4, 9]

    def test_a_dry_run_half_way_leaves_the_read_again_to_come(self) -> None:
        _, (reader, state) = self._late_commits()

        _run(_extractor(reader, state), ctx=fakes.make_context(dry_run=True))
        _run(_extractor(reader, state), ctx=fakes.make_context(dry_run=True))

        assert _run(_extractor(reader, state)) == [1, 2, 3, 4, 9]

    def test_a_static_table_is_read_again_once_on_full_batches(self) -> None:
        reader = fake_ports.InMemoryTableReader(
            {"crm.customer": _rows((1, 1), (2, 1), (3, 1), (4, 1))}
        )
        state = core_state.InMemoryStateStore()

        assert _run(_extractor(reader, state)) == [1, 2, 3, 4]
        # Two full batches: the read ends after the last one is loaded.
        assert _run(_extractor(reader, state)) == [1, 2, 3, 4]
        assert _run(_extractor(reader, state)) == []
        assert _run(_extractor(reader, state)) == []

    def test_a_death_after_the_read_again_does_not_repeat_it(self) -> None:
        table, (reader, state) = self._late_commits()
        table.extend(_rows((20, 2), (21, 2)))
        failing = fakes.CollectingLoader[contract.Row](fail_on_batch=6)
        extractor = _extractor(reader, state)

        with pytest.raises(contract.RetryableError):
            _run(extractor, batch_size=1, loader=failing)

        # Minute 1 was read again whole; minute 2, the largest, is next.
        assert [row["id"] for row in failing.rows] == [1, 2, 3, 4, 9]
        assert _run(_extractor(reader, state)) == [20, 21]
        assert _run(_extractor(reader, state)) == [20, 21]
        assert _run(_extractor(reader, state)) == []

    def test_a_backfill_below_the_watermark_keeps_the_read_again(
        self,
    ) -> None:
        table, (reader, state) = self._late_commits()
        # Row 5 came late with an old watermark: what backfills are for.
        table.extend(_rows((5, -7), (20, 20)))
        backfill = contract.ETLContext(
            contract.ETLRunInfo("crm", since=_at(-10), until=_at(-5))
        )

        assert _run(_extractor(reader, state), ctx=backfill) == [5]
        assert _run(_extractor(reader, state)) == [1, 2, 3, 4, 9, 20]

    def test_a_backfill_above_the_watermark_does_not_skip_rows(self) -> None:
        table, (reader, state) = self._late_commits()
        table.extend(_rows((20, 20), (30, 30)))
        backfill = contract.ETLContext(
            contract.ETLRunInfo("crm", since=_at(25), until=_at(40))
        )

        assert _run(_extractor(reader, state), ctx=backfill) == [30]
        assert _run(_extractor(reader, state)) == [1, 2, 3, 4, 9, 20, 30]

    def test_a_backfill_before_any_run_is_not_taken_as_one(self) -> None:
        reader = fake_ports.InMemoryTableReader(
            {"crm.customer": _rows((1, 1), (2, 5))}
        )
        state = core_state.InMemoryStateStore()
        backfill = contract.ETLContext(contract.ETLRunInfo("crm", since=_at(5)))

        assert _run(_extractor(reader, state), ctx=backfill) == [2]
        assert _run(_extractor(reader, state)) == [1, 2]

    def test_a_backfill_over_the_watermark_reads_it_again(self) -> None:
        table, (reader, state) = self._late_commits()
        table.extend(_rows((20, 20)))
        backfill = contract.ETLContext(
            contract.ETLRunInfo("crm", since=_at(0), until=_at(10))
        )

        assert _run(_extractor(reader, state), ctx=backfill) == [1, 2, 3, 4, 9]
        assert _run(_extractor(reader, state)) == [20]


class TestTableQuery:

    def test_order_by_is_required(self) -> None:
        with pytest.raises(ValueError, match="order_by"):
            contract.TableQuery("t", order_by=())

    def test_after_cannot_be_longer_than_order_by(self) -> None:
        with pytest.raises(ValueError, match="after has 2"):
            contract.TableQuery("t", order_by=("id",), after=(1, 2))

    def test_upserts_need_a_key(self) -> None:
        with pytest.raises(ValueError, match="needs a key"):
            contract.TableTarget("t")


class TestTableLoader:

    def test_counts_come_from_the_writer(self) -> None:
        writer = fake_ports.InMemoryTableWriter()
        loader = operators.TableLoader[contract.Row](
            writer, contract.TableTarget("crm.customer", key=("id",))
        )
        ctx = fakes.make_context()
        loader.load([{"id": 1, "name": "a"}], ctx)

        stats = loader.load(
            [
                {"id": 1, "name": "b"},
                {"id": 2, "name": "c"},
                {"id": 2, "name": "c"},
            ],
            ctx,
        )

        assert (stats.successful, stats.created, stats.updated) == (3, 1, 1)

    def test_records_are_turned_into_rows(self) -> None:
        writer = fake_ports.InMemoryTableWriter()
        loader = operators.TableLoader[fakes.User](
            writer,
            contract.TableTarget("crm.user", key=("id",)),
            row=lambda user: {"id": user.id, "email": user.email},
        )

        loader.load([fakes.User("u1", "a@x", True)], fakes.make_context())

        assert writer.tables["crm.user"] == {
            ("u1",): {"id": "u1", "email": "a@x"}
        }

    def test_records_that_are_not_rows_need_a_row_function(self) -> None:
        loader = operators.TableLoader[Any](
            fake_ports.InMemoryTableWriter(),
            contract.TableTarget("crm.user", key=("id",)),
        )
        users = [fakes.User("u1", "a@x", True)]
        ctx = fakes.make_context()

        with pytest.raises(contract.FatalError, match="pass `row=`"):
            loader.load(users, ctx)
