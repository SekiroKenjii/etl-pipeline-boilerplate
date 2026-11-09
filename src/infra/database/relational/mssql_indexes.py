"""What SQL Server's catalog tells of the indexes of a table."""

from __future__ import annotations

import sqlalchemy as sa


def seekable(
    connection: sa.Connection, table: sa.Table, columns: frozenset[str]
) -> bool:
    """Whether an index of `table` seeks rows by all `columns` at once.

    An index led by the columns, in any order, or a unique index on some of
    them; enabled, unfiltered, row store (clustered or not).
    """
    name = connection.dialect.identifier_preparer.format_table(table)
    found = connection.execute(
        sa.text(
            "SELECT i.index_id, i.is_unique, c.name"
            " FROM sys.indexes i"
            " JOIN sys.index_columns ic ON ic.object_id = i.object_id"
            " AND ic.index_id = i.index_id"
            " JOIN sys.columns c ON c.object_id = ic.object_id"
            " AND c.column_id = ic.column_id"
            " WHERE i.object_id = OBJECT_ID(:table) AND i.type IN (1, 2)"
            " AND i.is_disabled = 0 AND i.is_hypothetical = 0"
            " AND i.has_filter = 0 AND ic.key_ordinal > 0"
            " ORDER BY i.index_id, ic.key_ordinal"
        ),
        {"table": name},
    )
    indexes: dict[int, tuple[bool, list[str]]] = {}
    for index_id, unique, column_name in found:
        indexes.setdefault(index_id, (bool(unique), []))[1].append(
            str(column_name).lower()
        )
    for unique, keys in indexes.values():
        leading = frozenset(keys[: len(columns)])
        if leading == columns or (unique and frozenset(keys) <= columns):
            return True
    return False
