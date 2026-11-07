"""One open quarantine entry per record.

Revision ID: 0004
Revises: 0003
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

# Tables as they were at this revision, not imported from the current code.
# pylint: disable=duplicate-code


# pylint: disable=invalid-name
revision = "0004"
down_revision: str | None = "0003"
branch_labels: str | None = None
depends_on: str | None = None
# pylint: enable=invalid-name

# MySQL has no partial index, nor one on expressions null for other rows.
_PARTIAL_INDEX_DIALECTS = ("postgresql", "mssql", "sqlite")


def upgrade(*, schema: str | None, prefix: str) -> None:
    """Makes the open entries of a record unique.

    Of the open entries of a record (quarantined again before 0003), the
    latest keeps the key; the others, still open, get a key of their own,
    `legacy-<id>`. Nothing is deleted.
    """
    dialect = op.get_context().dialect.name
    if dialect not in (*_PARTIAL_INDEX_DIALECTS, "oracle"):
        return
    name = f"{prefix}quarantine"
    quarantine = sa.table(
        name,
        sa.column("id", sa.BigInteger),
        sa.column("pipeline", sa.Unicode(200)),
        sa.column("status", sa.Unicode(16)),
        sa.column("record_key", sa.Unicode(64)),
        schema=schema,
    )
    columns = quarantine.c
    later = quarantine.alias("later")
    op.execute(
        quarantine.update()
        .where(
            columns.status == "open",
            sa.exists().where(
                later.c.pipeline == columns.pipeline,
                later.c.record_key == columns.record_key,
                later.c.status == "open",
                later.c.id > columns.id,
            ),
        )
        .values(
            record_key=sa.literal("legacy-", sa.Unicode(7))
            + sa.cast(columns.id, sa.Unicode(20))
        )
    )
    is_open = columns.status == "open"
    if dialect == "oracle":
        # Oracle leaves out of an index the rows whose values are all null.
        op.create_index(
            f"{prefix}quarantine_open",
            name,
            [
                sa.case((is_open, columns.pipeline)),
                sa.case((is_open, columns.record_key)),
            ],
            unique=True,
            schema=schema,
        )
        return
    op.create_index(
        f"{prefix}quarantine_open",
        name,
        ["pipeline", "record_key"],
        unique=True,
        schema=schema,
        postgresql_where=is_open,
        mssql_where=is_open,
        sqlite_where=is_open,
    )
