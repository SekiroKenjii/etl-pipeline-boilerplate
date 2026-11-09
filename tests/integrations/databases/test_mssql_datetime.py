"""SQL Server `datetime` and `smalldatetime` watermarks compare as the
column stores them.

mssql-python binds every Python datetime as `datetime2(7)`; SQL Server then
converts the `datetime` column, not the parameter: `.003` becomes
`.0033333`, so `col <= :p` misses the row and `col > :p` matches it again.
Every comparison the reader builds casts the parameter to the column's type
instead, and keeps the index seek. `datetime2` columns are the controls.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
import datetime
import itertools
from xml.etree import ElementTree

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import mssql

from core import contract
from core import operators
from core import pipeline
from infra.database import relational
from tests.factories import fakes
from tests.integrations.databases import conftest

T0 = datetime.datetime(2026, 10, 1, 10, 0, 0)
_SHOWPLAN = "{http://schemas.microsoft.com/sqlserver/2004/07/showplan}"


@pytest.fixture
def mssql_backend(backend: conftest.Backend) -> conftest.Backend:
    if backend.name != "mssql":
        pytest.skip("SQL Server's datetime rounding")
    return backend


def _ms(milliseconds: int) -> datetime.datetime:
    return T0 + datetime.timedelta(milliseconds=milliseconds)


def _seconds(seconds: int) -> datetime.datetime:
    return T0 + datetime.timedelta(seconds=seconds)


def _datetime2(precision: int) -> sa.types.TypeEngine[datetime.datetime]:
    # SQLAlchemy's dialect types are not typed.
    return mssql.DATETIME2(precision)  # type: ignore[no-untyped-call]


# The column type, two Python values the server rounds to one stored value
# (`.002` and `.004` become `.003`; `:40` and `:50` become the next minute),
# and a value it stores apart.
_ROUNDED = {
    "datetime .003": (mssql.DATETIME(), _ms(2), _ms(4), _ms(237)),
    "datetime .007": (mssql.DATETIME(), _ms(6), _ms(8), _ms(237)),
    "smalldatetime": (mssql.SMALLDATETIME(), _seconds(40), _seconds(50), T0),
    "datetime2(3)": (_datetime2(3), _ms(2), _ms(2), _ms(237)),
    "datetime2(7)": (_datetime2(7), _ms(2), _ms(2), _ms(237)),
}
_WATERMARKS = pytest.mark.parametrize(
    "column_type, first, same, other",
    list(_ROUNDED.values()),
    ids=list(_ROUNDED),
)


def _events(
    backend: conftest.Backend,
    column_type: sa.types.TypeEngine[datetime.datetime],
) -> sa.Table:
    table = backend.table(
        "event",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("tenant", sa.Unicode(2), nullable=False),
        sa.Column("w", column_type, nullable=False),
    )
    backend.create()
    backend.execute(
        sa.text(f"CREATE INDEX ix_{table.name} ON {table.name} (w, id)")
    )
    return table


def _stored(
    backend: conftest.Backend, table: sa.Table
) -> dict[int, datetime.datetime]:
    """The stored stamp of every row, by id."""
    return dict(backend.rows(table, "id", "w"))


def _ids(
    backend: conftest.Backend,
    query: contract.TableQuery,
    *,
    page_size: int = 10,
    at_most: int = 200,
) -> list[object]:
    """The ids `query` yields, capped: a looping read must still end."""
    reader = relational.RelationalTableReader(backend.db, page_size=page_size)
    return [row["id"] for row in itertools.islice(reader.read(query), at_most)]


class TestKeyset:

    @_WATERMARKS
    def test_rows_sharing_one_stamp_across_pages_are_read_once(
        self,
        mssql_backend: conftest.Backend,
        column_type: sa.types.TypeEngine[datetime.datetime],
        first: datetime.datetime,
        same: datetime.datetime,
        other: datetime.datetime,
    ) -> None:
        del other  # Unused.
        table = _events(mssql_backend, column_type)
        mssql_backend.insert(
            table,
            *(
                {"id": i, "tenant": "a", "w": first if i % 2 else same}
                for i in range(25)
            ),
        )
        assert len(set(_stored(mssql_backend, table).values())) == 1

        read = _ids(
            mssql_backend,
            contract.TableQuery(table.name, order_by=("w", "id")),
            page_size=10,
        )

        assert read == list(range(25))

    @_WATERMARKS
    def test_a_second_order_column_between_the_stamp_and_the_key(
        self,
        mssql_backend: conftest.Backend,
        column_type: sa.types.TypeEngine[datetime.datetime],
        first: datetime.datetime,
        same: datetime.datetime,
        other: datetime.datetime,
    ) -> None:
        table = _events(mssql_backend, column_type)
        mssql_backend.insert(
            table,
            *(
                {"id": i, "tenant": "a" if i < 12 else "b", "w": first}
                for i in range(24)
            ),
            {"id": 24, "tenant": "a", "w": other},
        )
        stored = _stored(mssql_backend, table)
        apart_first = stored[24] < stored[0]
        order = ("w", "tenant", "id")

        read = _ids(
            mssql_backend,
            contract.TableQuery(table.name, order_by=order),
            page_size=5,
        )
        resumed = _ids(
            mssql_backend,
            contract.TableQuery(
                table.name, order_by=order, after=(same, "a", 5)
            ),
            page_size=5,
        )

        at_first = list(range(24))
        assert read == ([24] + at_first if apart_first else at_first + [24])
        assert resumed == list(range(6, 24)) + ([] if apart_first else [24])

    def test_keyset_pages_still_seek_the_index(
        self, mssql_backend: conftest.Backend
    ) -> None:
        table = _events(mssql_backend, mssql.DATETIME())
        mssql_backend.insert(
            table,
            *({"id": i, "tenant": "a", "w": _ms(3)} for i in range(25)),
        )
        query = contract.TableQuery(
            table.name, order_by=("w", "id"), columns=("id", "w")
        )

        _ids(mssql_backend, query, page_size=10)

        reads = _reads_of(mssql_backend, table, "%w >= %")
        assert reads
        assert set(reads) == {"Index Seek"}, reads


class TestBounds:

    @_WATERMARKS
    def test_lower_and_upper_match_the_stored_stamp(
        self,
        mssql_backend: conftest.Backend,
        column_type: sa.types.TypeEngine[datetime.datetime],
        first: datetime.datetime,
        same: datetime.datetime,
        other: datetime.datetime,
    ) -> None:
        table = _events(mssql_backend, column_type)
        mssql_backend.insert(
            table,
            {"id": 1, "tenant": "a", "w": first},
            {"id": 2, "tenant": "a", "w": same},
            {"id": 3, "tenant": "a", "w": other},
        )
        stored = _stored(mssql_backend, table)
        assert stored[1] == stored[2] != stored[3]
        by_stamp = sorted(stored, key=lambda i: (stored[i], i))

        def ids(
            lower: datetime.datetime | None = None,
            upper: datetime.datetime | None = None,
        ) -> list[object]:
            return _ids(
                mssql_backend,
                contract.TableQuery(
                    table.name, order_by=("w", "id"), lower=lower, upper=upper
                ),
            )

        assert ids(lower=first, upper=first) == [1, 2]
        assert ids(lower=same, upper=same) == [1, 2]
        assert ids(lower=stored[1], upper=stored[1]) == [1, 2]
        assert ids(upper=first) == [
            i for i in by_stamp if stored[i] <= stored[1]
        ]
        assert ids(lower=first) == [
            i for i in by_stamp if stored[i] >= stored[1]
        ]

    @_WATERMARKS
    def test_conditions_match_the_stored_stamp(
        self,
        mssql_backend: conftest.Backend,
        column_type: sa.types.TypeEngine[datetime.datetime],
        first: datetime.datetime,
        same: datetime.datetime,
        other: datetime.datetime,
    ) -> None:
        table = _events(mssql_backend, column_type)
        mssql_backend.insert(
            table,
            {"id": 1, "tenant": "a", "w": first},
            {"id": 2, "tenant": "a", "w": same},
            {"id": 3, "tenant": "a", "w": other},
        )
        stored = _stored(mssql_backend, table)
        apart_above = stored[3] > stored[1]
        reader = relational.RelationalTableReader(mssql_backend.db)
        operator = contract.Operator

        def count(name: str, *values: datetime.datetime) -> int:
            condition = (
                contract.Condition("w", operator.IN, list(values))
                if name == "IN"
                else contract.Condition("w", operator[name], values[0])
            )
            return reader.count(table.name, [condition])

        assert count("EQ", first) == 2
        assert count("EQ", same) == 2
        assert count("NE", first) == 1
        assert count("IN", first, other) == 3
        assert count("LE", first) == (2 if apart_above else 3)
        assert count("GE", first) == (3 if apart_above else 2)
        assert count("GT", first) == (1 if apart_above else 0)
        assert count("LT", first) == (0 if apart_above else 1)


class TestLateCommits:
    """The late-commit scenario of `TableExtractor` on the real reader.

    Run 1 reads the only row and marks its stamp for a recheck; four rows
    committed late at that stamp with lower keys; run 2 dies at its second
    batch of one; run 3 reads the rest, the row of run 1 included.
    """

    @_WATERMARKS
    def test_rows_committed_late_at_the_largest_stamp_are_read(
        self,
        mssql_backend: conftest.Backend,
        column_type: sa.types.TypeEngine[datetime.datetime],
        first: datetime.datetime,
        same: datetime.datetime,
        other: datetime.datetime,
    ) -> None:
        del other  # Unused.
        table = _events(mssql_backend, column_type)
        relational.create_etl_tables(
            mssql_backend.db, prefix=mssql_backend.prefix
        )
        state = relational.RelationalStateStore(
            mssql_backend.db, prefix=mssql_backend.prefix
        )
        run = _runner(mssql_backend, table, state)
        mssql_backend.insert(table, {"id": 9, "tenant": "a", "w": first})

        assert run() == [9]
        assert _saved(state, table)["recheck"] is True

        mssql_backend.insert(
            table, *({"id": i, "tenant": "a", "w": same} for i in (1, 2, 3, 4))
        )
        assert len(set(_stored(mssql_backend, table).values())) == 1

        with pytest.raises(contract.RetryableError):
            run(batch_size=1, fail_on_batch=2)
        assert _saved(state, table)["recheck"] is True

        assert run() == [2, 3, 4, 9]
        assert _saved(state, table)["recheck"] is False
        assert run() == []


def _runner(
    backend: conftest.Backend,
    table: sa.Table,
    state: contract.IStateStore,
) -> Callable[..., list[object]]:
    """Runs the extractor of `table` through a job; returns the ids loaded."""

    def run(
        batch_size: int = 100, fail_on_batch: int | None = None
    ) -> list[object]:
        loader = fakes.CollectingLoader[contract.Row](fail_on_batch)
        job = pipeline.ETLJob(
            operators.TableExtractor(
                relational.RelationalTableReader(backend.db),
                table.name,
                key=["id"],
                watermark="w",
                state=state,
                snapshot=True,
            ),
            operators.Passthrough[contract.Row](),
            loader,
            batch_size=batch_size,
        )
        job(contract.ETLContext(contract.ETLRunInfo("events")), {})
        return [row["id"] for row in loader.rows]

    return run


def _saved(
    state: contract.IStateStore, table: sa.Table
) -> Mapping[str, object]:
    saved = state.get("events", f"table:{table.name}")
    assert isinstance(saved, dict)
    return saved


def _reads_of(
    backend: conftest.Backend, table: sa.Table, like: str
) -> list[str]:
    """The physical operators reading `table` in the cached plans of the
    statements on it matching `like`."""
    with backend.db.engine.connect() as connection:
        plans = [
            str(plan)
            for (plan,) in connection.execute(
                sa.text(
                    "SELECT CAST(qp.query_plan AS NVARCHAR(MAX))"
                    " FROM sys.dm_exec_query_stats qs"
                    " CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st"
                    " CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp"
                    " WHERE st.text LIKE :table AND st.text LIKE :like"
                ),
                {"table": f"%FROM {table.name}%", "like": like},
            )
        ]
    reads: list[str] = []
    for plan in plans:
        for op in ElementTree.fromstring(plan).iter(f"{_SHOWPLAN}RelOp"):
            reads.extend(
                op.get("PhysicalOp", "")
                for details in op
                for found in details.findall(f"{_SHOWPLAN}Object")
                if found.get("Table") == f"[{table.name}]"
            )
    return reads
