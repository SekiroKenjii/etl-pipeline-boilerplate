"""The drivers' bulk loaders, filling a staging table: COPY with psycopg,
bulk copy (BCP) with mssql-python; and the values of a batch adapted to the
columns where drivers are strict."""

from __future__ import annotations

from collections.abc import Generator, Sequence
import contextlib
from typing import Any
import uuid

import psycopg
from psycopg import sql as pg_sql
from psycopg.types import json as pg_json
from sqlalchemy import exc as sa_exc
import sqlalchemy as sa

from core import contract


def coerce(
    table: sa.Table, rows: Sequence[contract.Row]
) -> list[dict[str, object]]:
    """Adapts values to the column types where drivers are strict.

    A UUID may come as text (mappings produce text): UUID columns get
    `uuid.UUID` objects.
    """
    uuids = [
        c.name
        for c in table.c
        if isinstance(c.type, sa.Uuid) and c.type.as_uuid
    ]
    if not uuids:
        return [dict(row) for row in rows]
    coerced = []
    for row in rows:
        values = dict(row)
        for name in uuids:
            value = values.get(name)
            if isinstance(value, str):
                values[name] = uuid.UUID(value)
        coerced.append(values)
    return coerced


def copy_rows(
    connection: sa.Connection, stage: sa.Table, rows: Sequence[contract.Row]
) -> None:
    """COPY ... FROM STDIN of the batch into the staging table, through the
    psycopg connection behind `connection`.

    Raises:
        DBAPIError: If COPY fails: the driver's error, wrapped as SQLAlchemy
            does for its statements.
    """
    driver = connection.connection.driver_connection
    if driver is None:
        raise contract.FatalError("COPY: no driver connection")
    json_columns = [isinstance(c.type, sa.JSON) for c in stage.c]
    statement = pg_sql.SQL("COPY {} ({}) FROM STDIN").format(
        pg_sql.Identifier(stage.name),
        pg_sql.SQL(", ").join(pg_sql.Identifier(c.name) for c in stage.c),
    )
    try:
        with driver.cursor() as cursor, cursor.copy(statement) as copy:
            for row in rows:
                copy.write_row([
                    pg_json.Jsonb(row[c.name])
                    if is_json and row[c.name] is not None
                    else row[c.name]
                    for c, is_json in zip(stage.c, json_columns)
                ])
    except psycopg.Error as exc:
        # SQLAlchemy is untyped here: psycopg's errors are not its
        # `DBAPIModule.Error`.
        error: Any = exc
        lost = connection.dialect.is_disconnect(error, driver, None)
        if lost:
            connection.invalidate(exc)
        raise sa_exc.DBAPIError.instance(
            f"COPY {stage.name} FROM STDIN",
            None,
            exc,
            psycopg.Error,
            hide_parameters=True,
            connection_invalidated=lost,
            dialect=connection.dialect,
        ) from exc


@contextlib.contextmanager
def bulk_copied(
    engine: sa.Engine, stage: sa.Table, rows: Sequence[dict[str, object]]
) -> Generator[sa.Table]:
    """Yields the global temporary table `stage`, created and filled by bulk
    copy (BCP) with mssql-python, and dropped at the end.

    Bulk copy runs on a connection of its own, which cannot see the
    session's `#` tables: the batch goes into a global `##` table, created
    on a connection held until the caller is done. SQL Server drops it with
    that session, should the process die.

    Raises:
        DBAPIError: If the bulk copy fails: its error, wrapped for
            `Dialect.transient` to tell whether to try again.
    """
    columns = [c.name for c in stage.c]
    with engine.connect() as holder:
        stage.create(holder)
        holder.commit()
        try:
            driver = holder.connection.driver_connection
            if driver is None:
                raise contract.FatalError("bulk copy: no driver connection")
            cursor = driver.cursor()
            try:
                cursor.bulkcopy(
                    stage.name,
                    [tuple(row[name] for name in columns) for row in rows],
                    column_mappings=columns,
                    keep_nulls=True,
                    timeout=0,
                )
            except (RuntimeError, ValueError, TypeError) as exc:
                # Server or network errors, or data the copy cannot convert.
                raise sa_exc.DBAPIError(
                    f"bulk copy into {stage.name}",
                    None,
                    exc,
                    hide_parameters=True,
                ) from exc
            finally:
                cursor.close()
            yield stage
        finally:
            stage.drop(holder)
            holder.commit()
