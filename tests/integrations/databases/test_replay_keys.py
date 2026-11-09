"""Replaying rejected rows reads them by their source key, a text, in the
type of the key column: the same way on every database."""

from __future__ import annotations

from collections.abc import Sequence
import datetime
import decimal
from typing import Any
import uuid

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

from core import contract
from core import keys
from core.mapping_pipelines import kinds
from infra.database import relational
from tests.integrations.databases import conftest

_KEYS: dict[str, tuple[sa.types.TypeEngine[Any], list[object]]] = {
    "integer": (sa.Integer(), [7, 99, 1003]),
    "decimal": (
        sa.Numeric(10, 2),
        [
            decimal.Decimal("2.25"),
            decimal.Decimal("3"),
            decimal.Decimal("10.50"),
        ],
    ),
    "text": (sa.Unicode(10), ["B7", "C001", "a9"]),
    "date": (sa.Date(), [datetime.date(2025, 5, 5), datetime.date(2026, 1, 2)]),
    "datetime": (
        sa.DateTime(),
        [datetime.datetime(2025, 5, 5), datetime.datetime(2026, 1, 2, 3, 4, 5)],
    ),
    "uuid": (sa.Uuid(), [uuid.UUID(int=5), uuid.UUID(int=9)]),
    "boolean": (sa.Boolean(), [False, True]),
}


def _table(backend: conftest.Backend, kind: str) -> str:
    type_, values = _KEYS[kind]
    table = backend.table(
        kind,
        sa.Column("k", type_, primary_key=True, autoincrement=False),
        sa.Column("n", sa.Integer),
    )
    backend.create()
    backend.insert(table, *({"k": v, "n": i} for i, v in enumerate(values)))
    return table.name


def _source_keys(reader: contract.ITableReader, table: str) -> list[str]:
    """The source keys of the rows, as the quarantine keeps them."""
    query = contract.TableQuery(table, order_by=("k",))
    return [keys.canonical_text([row["k"]]) for row in reader.read(query)]


def _replayed(
    reader: contract.ITableReader, table: str, texts: list[str]
) -> list[str]:
    """The source keys of the rows Replay reads for `texts`."""
    values = kinds.key_values(reader.column_family(table, "k"), texts)
    query = contract.TableQuery(
        table,
        order_by=("k",),
        filters=(contract.Condition("k", contract.Operator.IN, values),),
    )
    return [keys.canonical_text([row["k"]]) for row in reader.read(query)]


@pytest.mark.parametrize("kind", list(_KEYS))
def test_rejected_rows_are_read_by_their_source_key(
    backend: conftest.Backend, kind: str
) -> None:
    table = _table(backend, kind)
    reader = relational.RelationalTableReader(backend.db)
    texts = _source_keys(reader, table)

    assert _replayed(reader, table, texts) == texts
    assert _replayed(reader, table, texts[:1]) == texts[:1]


@pytest.mark.parametrize("kind", ["integer", "decimal", "date", "uuid"])
def test_a_key_not_of_the_column_s_type_matches_no_row(
    backend: conftest.Backend, kind: str
) -> None:
    table = _table(backend, kind)
    reader = relational.RelationalTableReader(backend.db)
    texts = _source_keys(reader, table)

    assert _replayed(reader, table, ["not a key", texts[0]]) == texts[:1]


def test_the_keys_of_an_empty_table_match_no_row(
    backend: conftest.Backend,
) -> None:
    table = backend.table(
        "empty",
        sa.Column("k", sa.Integer, primary_key=True, autoincrement=False),
    )
    backend.create()
    reader = relational.RelationalTableReader(backend.db)

    assert not _replayed(reader, table.name, ["1"])
    assert not _replayed(reader, table.name, [])


_PAST_BIGINT = [7, 2**63 + 5, 2**64 - 1]
_BIGINT = [-(2**63), 0, 2**63 - 1]


def _numbers(
    backend: conftest.Backend,
    type_: sa.types.TypeEngine[Any],
    values: Sequence[object],
) -> str:
    table = backend.table(
        "numbers",
        sa.Column("k", type_, primary_key=True, autoincrement=False),
    )
    backend.create()
    backend.insert(table, *({"k": v} for v in values))
    return table.name


def test_a_decimal_key_past_bigint_is_read_by_its_source_key(
    backend: conftest.Backend,
) -> None:
    if backend.name == "sqlite":
        pytest.skip("SQLite keeps a NUMERIC past 2**63 as a float")
    table = _numbers(
        backend,
        sa.Numeric(20, 0),
        [decimal.Decimal(v) for v in _PAST_BIGINT],
    )
    reader = relational.RelationalTableReader(backend.db)
    texts = _source_keys(reader, table)

    assert texts == [keys.canonical_text([v]) for v in _PAST_BIGINT]
    assert _replayed(reader, table, texts) == texts
    assert _replayed(reader, table, texts[1:2]) == texts[1:2]


def test_a_bigint_key_is_read_by_its_source_key(
    backend: conftest.Backend,
) -> None:
    table = _numbers(backend, sa.BigInteger(), _BIGINT)
    reader = relational.RelationalTableReader(backend.db)
    texts = _source_keys(reader, table)

    assert _replayed(reader, table, texts) == texts


def test_a_key_past_the_range_of_its_column_matches_no_row_or_is_fatal(
    backend: conftest.Backend,
) -> None:
    table = _numbers(backend, sa.BigInteger(), _BIGINT)
    reader = relational.RelationalTableReader(backend.db)
    past = str(2**64)

    try:
        found = _replayed(reader, table, [past])
    except contract.FatalError as exc:
        assert past not in str(exc)
    else:
        assert not found


# SQLAlchemy's BIT declares no operator classes yet: a warning, no error.
@pytest.mark.filterwarnings(
    "ignore:Type object <class 'sqlalchemy.dialects.mysql.types.BIT'>"
    ":sqlalchemy.exc.SADeprecationWarning"
)
@pytest.mark.parametrize("bits", [1, 8])
def test_a_mysql_bit_key_is_read_by_its_source_key(
    backend: conftest.Backend, bits: int
) -> None:
    if backend.name != "mysql":
        pytest.skip("BIT(n) is MySQL's")
    values = [0, 1] if bits == 1 else [0, 5, 255]
    table = _numbers(backend, mysql.BIT(bits), values)
    reader = relational.RelationalTableReader(backend.db)
    texts = _source_keys(reader, table)

    assert texts == [keys.canonical_text([v]) for v in values]
    assert _replayed(reader, table, texts) == texts
    assert _replayed(reader, table, texts[-1:]) == texts[-1:]
    assert reader.column_family(table, "k") == contract.TypeFamily.INTEGER
