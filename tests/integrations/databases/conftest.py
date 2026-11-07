"""The conformance suite of the relational adapters: every test runs on every
database.

SQLite always runs. Postgres, SQL Server, Oracle and MySQL run when their URL
is set (`make db-up` starts them):

    ETL_TEST_POSTGRES_URL=postgresql+psycopg://etl:etl@localhost:55432/etl
    ETL_TEST_MSSQL_URL=mssql+mssqlpython://sa:...@localhost:51433/master?...
    ETL_TEST_ORACLE_URL=oracle+oracledb://etl:etl@localhost:51521/?...
    ETL_TEST_MYSQL_URL=mysql+pymysql://etl:etl@127.0.0.1:53306/etl

Killing an Oracle session takes a privileged account (test_errors.py): the
`system` user of the test container, whose password is its ORACLE_PASSWORD.

    ETL_TEST_ORACLE_ADMIN_URL=oracle+oracledb://system:etl@localhost:51521/?...

Tests share each database: their tables get a unique prefix, and are dropped
afterwards.
"""

from __future__ import annotations

from collections.abc import Generator
import dataclasses
import os
import pathlib
from typing import Any
import uuid

import pytest
import sqlalchemy as sa

from infra.database import relational

BACKENDS = {
    "sqlite": None,
    "postgresql": "ETL_TEST_POSTGRES_URL",
    "mssql": "ETL_TEST_MSSQL_URL",
    "oracle": "ETL_TEST_ORACLE_URL",
    "mysql": "ETL_TEST_MYSQL_URL",
}


@dataclasses.dataclass
class Backend:
    """A database under test, and the tables of the current test.

    Attributes:
        name: The SQLAlchemy dialect name.
        url: The database URL.
        db: The database, shared by the adapters of the test.
        prefix: Prefixes the tables of the test.
        schema: The default schema.
        metadata: The tables of the test, dropped after it.
    """

    name: str
    url: str
    db: relational.Database
    prefix: str
    schema: str
    metadata: sa.MetaData = dataclasses.field(default_factory=sa.MetaData)

    def table(self, name: str, *columns: sa.Column[Any]) -> sa.Table:
        """Declares a table of the test, `<prefix><name>`."""
        return sa.Table(self.prefix + name, self.metadata, *columns)

    def create(self) -> None:
        self.metadata.create_all(self.db.engine)

    def insert(self, table: sa.Table, *rows: dict[str, Any]) -> None:
        with self.db.engine.begin() as connection:
            connection.execute(table.insert(), list(rows))

    def execute(self, statement: sa.Executable) -> None:
        with self.db.engine.begin() as connection:
            connection.execute(statement)

    def rows(self, table: sa.Table, *columns: str) -> list[tuple[Any, ...]]:
        """Returns the rows of `columns`, ordered by the first one.

        Only by the first one: Oracle cannot order by a LOB.
        """
        selected = [table.c[name] for name in columns]
        with self.db.engine.connect() as connection:
            return [
                tuple(row)
                for row in connection.execute(
                    sa.select(*selected).order_by(selected[0])
                )
            ]


@pytest.fixture(params=list(BACKENDS))
def backend(
    request: pytest.FixtureRequest, tmp_path: pathlib.Path
) -> Generator[Backend]:
    name: str = request.param
    variable = BACKENDS[name]
    url = (
        f"sqlite:///{tmp_path / 'etl.db'}"
        if variable is None
        else os.environ.get(variable)
    )
    if not url:
        pytest.skip(f"set {variable} to run the {name} tests")

    db = relational.Database(url)
    schema = sa.inspect(db.engine).default_schema_name or "main"
    prefix = f"t{uuid.uuid4().hex[:7]}_"
    tested = Backend(name, url, db, prefix, schema)
    try:
        yield tested
    finally:
        tested.metadata.drop_all(db.engine)
        relational.drop_etl_tables(db, prefix=prefix)
        db.close()
