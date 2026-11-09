"""Reading and writing tables, the same way on every database."""

from __future__ import annotations

import concurrent.futures
import datetime
import decimal
from typing import Any
import uuid

import pytest
import sqlalchemy as sa

from core import contract
from infra.database import relational
from tests.integrations.databases import conftest

# No microseconds, no time zone: what every database keeps as it is.
T0 = datetime.datetime(2026, 10, 1, 8, 0, 0)


def _at(minutes: int) -> datetime.datetime:
    return T0 + datetime.timedelta(minutes=minutes)


class TestReader:

    @pytest.fixture
    def customers(self, backend: conftest.Backend) -> sa.Table:
        table = backend.table(
            "customer",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
            sa.Column("updated_at", sa.DateTime, nullable=False),
            sa.Column("name", sa.Unicode(50)),
            sa.Column("tenant", sa.Unicode(10)),
        )
        backend.create()
        backend.insert(
            table,
            *(
                {
                    "id": i,
                    "updated_at": _at(minutes),
                    "name": f"n{i}",
                    "tenant": "a" if i != 3 else "b",
                }
                for i, minutes in [(1, 1), (2, 1), (3, 1), (4, 2), (5, 3)]
            ),
        )
        return table

    def _ids(self, backend: conftest.Backend, **query: Any) -> list[Any]:
        query.setdefault("order_by", ("updated_at", "id"))
        reader = relational.RelationalTableReader(backend.db, page_size=2)
        return [
            row["id"]
            for row in reader.read(
                contract.TableQuery(backend.prefix + "customer", **query)
            )
        ]

    @pytest.mark.usefixtures("customers")
    def test_pages_never_skip_nor_repeat_rows_on_ties(
        self, backend: conftest.Backend
    ) -> None:
        assert self._ids(backend) == [1, 2, 3, 4, 5]

    @pytest.mark.usefixtures("customers")
    def test_after_lower_upper_and_filters(
        self, backend: conftest.Backend
    ) -> None:
        eq = contract.Operator.EQ

        assert self._ids(backend, after=(_at(1), 2)) == [3, 4, 5]
        assert self._ids(backend, after=(_at(1),)) == [4, 5]
        assert self._ids(backend, lower=_at(2), upper=_at(2)) == [4]
        assert self._ids(
            backend,
            columns=("id", "updated_at"),
            filters=(
                contract.Condition("tenant", eq, "a"),
                contract.Condition("id", contract.Operator.IN, [1, 3, 5]),
                contract.Condition("name", contract.Operator.IS_NOT_NULL),
            ),
        ) == [1, 5]

    @pytest.mark.usefixtures("customers")
    def test_the_max_of_a_watermark(self, backend: conftest.Backend) -> None:
        reader = relational.RelationalTableReader(backend.db)
        table = backend.prefix + "customer"
        nobody = contract.Condition("tenant", contract.Operator.EQ, "zz")

        assert reader.max_value(table, "updated_at") == _at(3)
        assert reader.max_value(table, "updated_at", [nobody]) is None

    @pytest.mark.usefixtures("customers")
    def test_counts(self, backend: conftest.Backend) -> None:
        reader = relational.RelationalTableReader(backend.db)
        table = backend.prefix + "customer"
        tenant = contract.Condition("tenant", contract.Operator.EQ, "a")

        assert reader.count(table) == 5
        assert reader.count(table, [tenant]) == 4

    def test_rows_with_null_in_the_order_are_refused(
        self, backend: conftest.Backend
    ) -> None:
        table = backend.table(
            "event",
            sa.Column("id", sa.Integer),
            sa.Column("at", sa.DateTime),
        )
        backend.create()
        backend.insert(table, {"id": 1, "at": T0}, {"id": 2, "at": None})
        reader = relational.RelationalTableReader(backend.db)
        query = contract.TableQuery(table.name, order_by=("at", "id"))

        with pytest.raises(contract.FatalError, match="NULL"):
            list(reader.read(query))

    @pytest.mark.usefixtures("customers")
    def test_unknown_names_are_reported(
        self, backend: conftest.Backend
    ) -> None:
        reader = relational.RelationalTableReader(backend.db)
        query = contract.TableQuery(
            backend.prefix + "customer", order_by=("nope",)
        )

        with pytest.raises(contract.FatalError, match="has no column 'nope'"):
            list(reader.read(query))
        with pytest.raises(contract.FatalError, match="No table"):
            reader.max_value("no_such_table", "id")


class TestWriter:

    @pytest.fixture
    def customers(self, backend: conftest.Backend) -> sa.Table:
        table = backend.table(
            "customer",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
            sa.Column("name", sa.Unicode(50)),
            sa.Column("created_at", sa.DateTime),
        )
        backend.create()
        return table

    def _write(
        self,
        backend: conftest.Backend,
        rows: list[contract.Row],
        **target: Any,
    ) -> contract.WriteResult:
        target.setdefault("key", ("id",))
        writer = relational.RelationalTableWriter(backend.db)
        return writer.write(
            contract.TableTarget(backend.prefix + "customer", **target), rows
        )

    def test_upserts_count_what_changed(
        self, backend: conftest.Backend, customers: sa.Table
    ) -> None:
        self._write(
            backend,
            [
                {"id": 1, "name": "a"},
                {"id": 2, "name": "b"},
                {"id": 4, "name": None},
            ],
        )

        result = self._write(
            backend,
            [
                {"id": 1, "name": "a"},  # unchanged
                {"id": 2, "name": "old"},
                {"id": 2, "name": "B"},  # the last row of a key wins
                {"id": 3, "name": "c"},
                {"id": 4, "name": "from NULL"},
            ],
        )

        assert (result.created, result.updated, result.unchanged) == (1, 2, 2)
        assert backend.rows(customers, "id", "name") == [
            (1, "a"),
            (2, "B"),
            (3, "c"),
            (4, "from NULL"),
        ]

    def test_writers_racing_on_the_same_keys(
        self, backend: conftest.Backend, customers: sa.Table
    ) -> None:
        target = contract.TableTarget(customers.name, key=("id",))
        writer = relational.RelationalTableWriter(backend.db)

        def write(round_: int) -> contract.WriteResult:
            rows: list[contract.Row] = [
                {"id": i, "name": f"r{round_ % 2}"} for i in range(50)
            ]
            return writer.write(target, rows)

        with concurrent.futures.ThreadPoolExecutor(4) as threads:
            results = list(threads.map(write, range(8)))

        assert sum(r.created for r in results) == 50
        assert len(backend.rows(customers, "id")) == 50

    def test_large_batches(
        self, backend: conftest.Backend, customers: sa.Table
    ) -> None:
        # Large enough for SQL Server's bulk copy.
        rows: list[contract.Row] = [
            {"id": i, "name": f"n{i}", "created_at": T0} for i in range(2500)
        ]

        first = self._write(backend, rows)
        rows[7] = {**rows[7], "name": "changed"}
        second = self._write(backend, rows)

        assert (first.created, second.updated, second.unchanged) == (
            2500,
            1,
            2499,
        )
        assert backend.rows(customers, "id", "name")[7] == (7, "changed")

    def test_a_value_set_back_to_null_is_a_change(
        self, backend: conftest.Backend, customers: sa.Table
    ) -> None:
        self._write(backend, [{"id": 1, "name": "a"}])

        result = self._write(backend, [{"id": 1, "name": None}])

        assert result.updated == 1
        assert backend.rows(customers, "id", "name") == [(1, None)]

    def test_insert_missing_never_overwrites(
        self, backend: conftest.Backend, customers: sa.Table
    ) -> None:
        self._write(backend, [{"id": 1, "name": "real"}])

        result = self._write(
            backend,
            [{"id": 1, "name": "placeholder"}, {"id": 2, "name": "new"}],
            mode=contract.WriteMode.INSERT_MISSING,
        )

        assert (result.created, result.unchanged) == (1, 1)
        assert backend.rows(customers, "id", "name") == [
            (1, "real"),
            (2, "new"),
        ]

    def test_update_columns_leave_the_others_alone(
        self, backend: conftest.Backend, customers: sa.Table
    ) -> None:
        self._write(backend, [{"id": 1, "name": "a", "created_at": T0}])

        self._write(
            backend,
            [{"id": 1, "name": "b", "created_at": _at(9)}],
            update_columns=("name",),
        )

        assert backend.rows(customers, "name", "created_at") == [("b", T0)]

    def test_trailing_blanks_are_a_change(
        self, backend: conftest.Backend, customers: sa.Table
    ) -> None:
        # SQL Server compares 'a' and 'a ' as equal, under any collation.
        self._write(backend, [{"id": 1, "name": "a"}])

        added = self._write(backend, [{"id": 1, "name": "a "}])
        same = self._write(backend, [{"id": 1, "name": "a "}])
        removed = self._write(backend, [{"id": 1, "name": "a"}])

        assert (added.updated, same.unchanged, removed.updated) == (1, 1, 1)
        assert backend.rows(customers, "id", "name") == [(1, "a")]

    @pytest.mark.parametrize(
        "mode",
        [
            contract.WriteMode.UPSERT,
            contract.WriteMode.INSERT_MISSING,
            contract.WriteMode.UPDATE,
        ],
    )
    def test_null_in_a_key_is_refused(
        self, backend: conftest.Backend, mode: contract.WriteMode
    ) -> None:
        # Matching no row, it would be inserted again at every run.
        contacts = backend.table(
            "contact",
            sa.Column("tenant", sa.Unicode(10)),
            sa.Column("email", sa.Unicode(50), nullable=False),
            sa.Column("name", sa.Unicode(50)),
        )
        sa.Index(
            f"{contacts.name}_key",
            contacts.c.tenant,
            contacts.c.email,
            unique=True,
        )
        backend.create()
        target = contract.TableTarget(
            contacts.name, key=("tenant", "email"), mode=mode
        )
        writer = relational.RelationalTableWriter(backend.db)
        rows: list[contract.Row] = [
            {"tenant": "a", "email": "x@a.example", "name": "x"},
            {"tenant": None, "email": "y@a.example", "name": "y"},
        ]

        with pytest.raises(contract.FatalError, match=r"Row 1 .*\['tenant'\]"):
            writer.write(target, rows)

        assert backend.rows(contacts, "email") == []

    @pytest.mark.parametrize(
        "rows",
        [
            [{"tenant": "a", "email": None}],
            # Half the batch matches: none of it is deleted.
            [
                {"tenant": "a", "email": "x@a.example"},
                {"tenant": None, "email": "y"},
            ],
        ],
        ids=["null", "null-after-a-match"],
    )
    def test_null_in_a_column_to_delete_by_is_refused(
        self, backend: conftest.Backend, rows: list[contract.Row]
    ) -> None:
        # It would match no row: the delete would be lost, silently.
        contacts = backend.table(
            "contact",
            sa.Column("tenant", sa.Unicode(10)),
            sa.Column("email", sa.Unicode(50)),
        )
        backend.create()
        backend.insert(
            contacts,
            {"tenant": "a", "email": "x@a.example"},
            {"tenant": "a", "email": None},
        )
        writer = relational.RelationalTableWriter(backend.db)

        with pytest.raises(contract.FatalError, match="NULL"):
            writer.delete(contacts.name, rows)

        assert len(backend.rows(contacts, "tenant", "email")) == 2

    def test_keys_equal_to_the_target_are_one_key(
        self, backend: conftest.Backend
    ) -> None:
        # 'ABC' and 'abc' differ in Python: which row is the last of the key
        # would be the database's choice, or an error of its own.
        collation = {
            "postgresql": "etl_test_ci",
            "mssql": "SQL_Latin1_General_CP1_CI_AS",
            "mysql": "utf8mb4_0900_ai_ci",
        }.get(backend.name)
        if collation is None:
            pytest.skip(f"no case-insensitive key collation on {backend.name}")
        if backend.name == "postgresql":
            backend.execute(
                sa.text(
                    "CREATE COLLATION IF NOT EXISTS etl_test_ci (provider ="
                    " icu, locale = 'und-u-ks-level2', deterministic = false)"
                )
            )
        codes = backend.table(
            "code",
            sa.Column(
                "code", sa.Unicode(20, collation=collation), primary_key=True
            ),
            sa.Column("label", sa.Unicode(50)),
        )
        backend.create()
        writer = relational.RelationalTableWriter(backend.db)
        rows: list[contract.Row] = [
            {"code": "ABC", "label": "first"},
            {"code": "abc", "label": "second"},
        ]

        for mode in (
            contract.WriteMode.UPSERT,
            contract.WriteMode.INSERT_MISSING,
        ):
            target = contract.TableTarget(codes.name, key=("code",), mode=mode)
            with pytest.raises(contract.FatalError, match="more than one row"):
                writer.write(target, rows)

        assert backend.rows(codes, "code") == []

    def test_a_failing_batch_writes_nothing(
        self, backend: conftest.Backend, customers: sa.Table
    ) -> None:
        self._write(backend, [{"id": 1, "name": "a"}])

        with pytest.raises(contract.FatalError):
            self._write(
                backend,
                [{"id": 2, "name": "b"}, {"id": 1, "name": "again"}],
                mode=contract.WriteMode.INSERT,
                key=(),
            )

        assert backend.rows(customers, "id") == [(1,)]

    @pytest.mark.usefixtures("customers")
    def test_rows_must_fit_the_table(self, backend: conftest.Backend) -> None:
        with pytest.raises(ValueError, match="Row 1"):
            self._write(backend, [{"id": 1, "name": "a"}, {"id": 2}])
        with pytest.raises(ValueError, match="lack"):
            self._write(backend, [{"name": "a"}])
        with pytest.raises(contract.FatalError, match="no column 'nme'"):
            self._write(backend, [{"id": 1, "nme": "a"}])

    def test_values_of_every_common_type(
        self, backend: conftest.Backend
    ) -> None:
        things = backend.table(
            "thing",
            # Oracle, SQLite and MySQL have no UUID type: text, as usually done.
            sa.Column(
                "id",
                sa.Uuid().with_variant(
                    sa.Unicode(36), "oracle", "sqlite", "mysql"
                ),
                primary_key=True,
            ),
            sa.Column("label", sa.Unicode(50)),
            sa.Column("amount", sa.Numeric(10, 2)),
            sa.Column("flag", sa.Boolean),
            sa.Column("day", sa.Date),
            sa.Column("at", sa.DateTime),
            sa.Column("raw", sa.LargeBinary),
            sa.Column("note", sa.UnicodeText),
        )
        backend.create()
        key = uuid.UUID(int=7)
        row: contract.Row = {
            # Mappings produce UUIDs as text.
            "id": str(key),
            "label": "Zoë Ångström",
            "amount": decimal.Decimal("10.50"),
            "flag": True,
            "day": datetime.date(2026, 1, 2),
            "at": T0,
            "raw": b"\x00\x01",
            "note": "long " * 1000,
        }
        target = contract.TableTarget(things.name, key=("id",))
        writer = relational.RelationalTableWriter(backend.db)

        writer.write(target, [row])
        again = writer.write(target, [{**row, "note": "short"}])

        reader = relational.RelationalTableReader(backend.db)
        (read,) = list(reader.read(contract.TableQuery(things.name, ("id",))))
        assert again.updated == 1
        expected = {k: v for k, v in row.items() if k != "id"}
        expected["note"] = "short"
        if backend.name == "oracle":
            # An Oracle DATE has a time: it reads back as a datetime.
            expected["day"] = datetime.datetime(2026, 1, 2)
        assert uuid.UUID(str(read.pop("id"))) == key
        assert read == expected


class TestDatabase:

    def test_closing_disposes_of_an_owned_engine_only(
        self, backend: conftest.Backend
    ) -> None:
        owned = relational.Database(backend.url)
        lent = relational.Database(owned.engine)
        with owned.connect("testing") as connection:
            connection.execute(sa.select(1))
        pool = owned.engine.pool
        if not isinstance(pool, sa.QueuePool):
            return  # A SQLite file pools no connection.

        lent.close()
        assert pool.checkedin() == 1
        owned.close()

        assert pool.checkedin() == 0

    @pytest.mark.parametrize("platform", [".net", "jdbc"])
    def test_connection_strings_of_other_platforms_connect(
        self, backend: conftest.Backend, platform: str
    ) -> None:
        if backend.name == "sqlite":
            pytest.skip("SQLite has no server to connect to")
        url = sa.make_url(backend.url)
        host, port, user, password = (
            url.host,
            url.port,
            url.username,
            url.password,
        )
        database = url.database
        service = url.query.get("service_name")
        text = {
            (".net", "mssql"): (
                f"Server=tcp:{host},{port};Initial Catalog={database};"
                f"User ID={user};Password={password};Encrypt=False;"
                "TrustServerCertificate=True;Connect Timeout=10;Pooling=true"
            ),
            (".net", "postgresql"): (
                f"Host={host};Port={port};Database={database};"
                f"Username={user};Password={password};Timeout=10"
            ),
            (".net", "oracle"): (
                f"User Id={user};Password={password};"
                f"Data Source={host}:{port}/{service};Connection Timeout=10"
            ),
            (".net", "mysql"): (
                f"Server={host};Port={port};Database={database};Uid={user};"
                f"Pwd={password};SslMode=None"
            ),
            ("jdbc", "mssql"): (
                f"jdbc:sqlserver://{host}:{port};databaseName={database};"
                f"user={user};password={password};encrypt=false;"
                "trustServerCertificate=true"
            ),
            ("jdbc", "postgresql"): (
                f"jdbc:postgresql://{host}:{port}/{database}?user={user}"
                f"&password={password}"
            ),
            (
                "jdbc",
                "oracle",
            ): f"jdbc:oracle:thin:{user}/{password}@//{host}:{port}/{service}",
            ("jdbc", "mysql"): (
                f"jdbc:mysql://{host}:{port}/{database}?user={user}"
                f"&password={password}"
            ),
        }[(platform, backend.name)]

        db = relational.Database(text)
        try:
            with db.connect("connecting") as connection:
                assert (
                    connection.execute(sa.select(sa.literal(1))).scalar() == 1
                )
        finally:
            db.engine.dispose()
        assert db.connection is not None
        assert not db.connection.guessed

    def test_an_unreachable_database_is_retryable(self) -> None:
        db = relational.Database("sqlite:////no/such/dir/etl.db")

        with pytest.raises(contract.RetryableError, match="unable to open"):
            with db.connect("connecting"):
                pass
