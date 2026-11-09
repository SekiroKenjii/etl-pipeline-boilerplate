"""SQL Server `datetime` and `smalldatetime` values, compared as the column
stores them.

The drivers bind a Python datetime as `datetime2(7)`; comparing it with a
`datetime` column converts the column, whose `.003` becomes `.0033333`:
`col <= :p` would miss the row at `.003` and `col > :p` match it again.
Cast to the column's type, the parameter rounds as the column stores it;
cast on the parameter's side, the comparison still seeks an index.
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import mssql

# The date-time types a `datetime2(7)` parameter compares with exactly.
_EXACT = (mssql.DATETIME2, mssql.DATETIMEOFFSET)


def bind(column: sa.ColumnElement[object], value: object) -> object:
    """Returns `value` as the operand of a comparison with `column`: cast to
    the column's type for `datetime` and `smalldatetime` columns, itself for
    any other."""
    if value is None or not isinstance(column.type, sa.DateTime):
        return value
    if isinstance(column.type, _EXACT):
        return value
    return sa.cast(value, column.type)
