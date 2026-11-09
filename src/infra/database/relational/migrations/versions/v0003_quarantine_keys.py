"""The keys of quarantined records; indexes of the issues.

Revision ID: 0003
Revises: 0002
"""

from __future__ import annotations

import json

from alembic import context
from alembic import op
import sqlalchemy as sa

from core import contract

# Tables as they were at this revision, not imported from the current code.
# pylint: disable=duplicate-code


# pylint: disable=invalid-name
revision = "0003"
down_revision: str | None = "0002"
branch_labels: str | None = None
depends_on: str | None = None
# pylint: enable=invalid-name

_PAGE = 500


def upgrade(*, schema: str | None, prefix: str) -> None:
    """Gives each quarantined record its key, and indexes the issues.

    Online, the key is computed as the quarantine does
    (`QuarantineEntry.key_of`): a record rejected again refreshes its entry.
    The SQL of `etl-db ddl` cannot compute it: the entries get a key of
    their own, `legacy-<id>`, and a record of them rejected again gets a new
    entry, once.
    """
    name = f"{prefix}quarantine"
    op.add_column(name, sa.Column("record_key", sa.Unicode(64)), schema=schema)
    quarantine = sa.table(
        name,
        sa.column("id", sa.BigInteger),
        sa.column("pipeline", sa.Unicode(200)),
        sa.column("checkpoint", sa.Unicode(32)),
        sa.column("record", sa.UnicodeText),
        sa.column("record_key", sa.Unicode(64)),
        schema=schema,
    )
    op.execute(
        quarantine.update().values(
            record_key=sa.literal("legacy-", sa.Unicode(7))
            + sa.cast(quarantine.c.id, sa.Unicode(20))
        )
    )
    if not context.is_offline_mode():
        _identify(op.get_bind(), quarantine)
    if op.get_context().dialect.name == "sqlite":
        # SQLite changes a column by copying the table.
        with op.batch_alter_table(
            name, schema=schema, copy_from=_before(name, schema, prefix)
        ) as batch:
            batch.alter_column(
                "record_key", existing_type=sa.Unicode(64), nullable=False
            )
    else:
        op.alter_column(
            name,
            "record_key",
            existing_type=sa.Unicode(64),
            nullable=False,
            schema=schema,
        )
    op.create_index(
        f"{prefix}quarantine_record",
        name,
        ["pipeline", "record_key"],
        schema=schema,
    )
    issues = f"{prefix}quality_issues"
    op.create_index(
        f"{prefix}quality_issues_record", issues, ["record_id"], schema=schema
    )
    op.create_index(
        f"{prefix}quality_issues_found", issues, ["found_at"], schema=schema
    )


def _identify(connection: sa.Connection, quarantine: sa.TableClause) -> None:
    """Computes the key of each quarantined record, page by page."""
    columns = quarantine.c
    after: int | None = None
    while True:
        page = (
            sa.select(
                columns.id, columns.pipeline, columns.checkpoint, columns.record
            )
            .order_by(columns.id)
            .limit(_PAGE)
        )
        if after is not None:
            page = page.where(columns.id > after)
        rows = connection.execute(page).all()
        keys: list[dict[str, object]] = []
        for id_, pipeline, checkpoint, record in rows:
            try:
                data = json.loads(record)
            except ValueError:
                continue  # Keeps its own key.
            keys.append({
                "p_id": id_,
                "p_key": contract.QuarantineEntry.key_of(
                    pipeline, checkpoint, data
                ),
            })
        if keys:
            connection.execute(
                quarantine.update()
                .where(columns.id == sa.bindparam("p_id"))
                .values(record_key=sa.bindparam("p_key")),
                keys,
            )
        if len(rows) < _PAGE:
            return
        after = rows[-1][0]


def _before(name: str, schema: str | None, prefix: str) -> sa.Table:
    """The quarantine of revision 0002, `record_key` added: what SQLite
    copies."""
    return sa.Table(
        name,
        sa.MetaData(),
        sa.Column(
            "id",
            sa.Integer(),
            sa.Identity(),
            primary_key=True,
        ),
        sa.Column("pipeline", sa.Unicode(200), nullable=False),
        sa.Column("run_id", sa.Unicode(64), nullable=False),
        sa.Column("checkpoint", sa.Unicode(32), nullable=False),
        sa.Column("record", sa.UnicodeText, nullable=False),
        sa.Column("issues", sa.UnicodeText, nullable=False),
        sa.Column("rejected_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "status", sa.Unicode(16), nullable=False, server_default="open"
        ),
        sa.Column("resolved_at", sa.DateTime(timezone=True)),
        sa.Column("record_key", sa.Unicode(64)),
        sa.Index(f"{prefix}quarantine_run", "pipeline", "run_id"),
        sa.Index(f"{prefix}quarantine_status", "status", "pipeline"),
        schema=schema,
    )
