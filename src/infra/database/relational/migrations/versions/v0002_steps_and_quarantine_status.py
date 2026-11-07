"""The steps of the runs; the status of quarantined records.

Revision ID: 0002
Revises: 0001
"""

from __future__ import annotations

from alembic import context
from alembic import op
import sqlalchemy as sa

# Tables as they were at this revision, not imported from the current code.
# pylint: disable=duplicate-code


# pylint: disable=invalid-name
revision = "0002"
down_revision: str | None = "0001"
branch_labels: str | None = None
depends_on: str | None = None
# pylint: enable=invalid-name


def upgrade(*, schema: str | None, prefix: str) -> None:
    """Adds the status of quarantined records, and the table of the steps.

    The records quarantined before are open. The table of the steps may be
    there already: `create_all` added the missing tables, not the columns.
    """
    quarantine = f"{prefix}quarantine"
    op.add_column(
        quarantine,
        sa.Column(
            "status", sa.Unicode(16), nullable=False, server_default="open"
        ),
        schema=schema,
    )
    op.add_column(
        quarantine,
        sa.Column("resolved_at", sa.DateTime(timezone=True)),
        schema=schema,
    )
    op.create_index(
        f"{prefix}quarantine_status",
        quarantine,
        ["status", "pipeline"],
        schema=schema,
    )
    steps = f"{prefix}run_steps"
    if not context.is_offline_mode() and sa.inspect(op.get_bind()).has_table(
        steps, schema=schema
    ):
        return
    op.create_table(
        steps,
        sa.Column("run_id", sa.Unicode(64), primary_key=True),
        sa.Column("step", sa.Unicode(200), primary_key=True),
        sa.Column("status", sa.Unicode(32), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("stats", sa.UnicodeText),
        sa.Column("message", sa.UnicodeText),
        sa.Column("host", sa.Unicode(200), nullable=False),
        sa.Column("attempts", sa.Integer, nullable=False),
        schema=schema,
    )
