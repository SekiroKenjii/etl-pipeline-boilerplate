"""Checks of the batches written by any table writer: SQL or documents."""

from __future__ import annotations

from collections.abc import Sequence

from core import contract


def columns(
    target: contract.TableTarget, rows: Sequence[contract.Row]
) -> list[str]:
    """Returns the columns of the batch, checked against the target.

    Raises:
        ValueError: If rows differ in columns, or lack a key column or a
            column to update.
        FatalError: If a row has NULL in a key column, unless the rows are
            only inserted: it would match no row, and be inserted again at
            every run.
    """
    names = list(rows[0])
    expected = set(names)
    for index, row in enumerate(rows):
        if set(row) != expected:
            raise ValueError(
                f"Row {index} of the batch for {target.table} has columns"
                f" {sorted(row)}, row 0 {sorted(expected)}"
            )
    needed = [*target.key, *(target.update_columns or ())]
    missing = sorted(set(needed) - expected)
    if missing:
        raise ValueError(
            f"The rows for {target.table} lack the column(s) {missing}"
        )
    if target.mode is not contract.WriteMode.INSERT:
        for index, row in enumerate(rows):
            nulls = [k for k in target.key if row[k] is None]
            if nulls:
                raise contract.FatalError(
                    f"Row {index} of the batch for {target.table} has NULL in"
                    f" its key column(s) {nulls}: it would match no row, and"
                    " be written again at every run"
                )
    return names


def last_by_key(
    target: contract.TableTarget, rows: Sequence[contract.Row]
) -> list[contract.Row]:
    """Keeps the last row of each key: a merge cannot touch a row twice.

    Plain INSERTs keep every row: a duplicate fails the batch, as it should.
    """
    if target.mode is contract.WriteMode.INSERT:
        return list(rows)
    last = {tuple(row[k] for k in target.key): row for row in rows}
    return list(last.values())


def match_columns(table: str, rows: Sequence[contract.Row]) -> list[str]:
    """Returns the columns rows to delete are matched on.

    Raises:
        ValueError: If rows differ in columns, or have none.
        FatalError: If a row has NULL in a column: in SQL it would match no
            row, and the delete be lost; in MongoDB it would match every
            document without the field.
    """
    names = list(rows[0])
    if not names:
        raise ValueError(f"Rows of {table} to delete need columns to match")
    for index, row in enumerate(rows):
        if set(row) != set(names):
            raise ValueError(
                f"Row {index} of {table} to delete has columns"
                f" {sorted(row)}, row 0 {sorted(names)}"
            )
        nulls = [name for name in names if row[name] is None]
        if nulls:
            raise contract.FatalError(
                f"Row {index} of {table} to delete has NULL in {nulls}: it"
                " would match no row, or in MongoDB every document without"
                " the field. Delete by columns that are never NULL."
            )
    return names
