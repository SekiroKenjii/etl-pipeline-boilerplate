"""The tables of 0.1.0: the baseline.

Revision ID: 0001
Revises: none
"""

from __future__ import annotations

from typing import Any

from alembic import op
import sqlalchemy as sa

# Tables as they were at this revision, not imported from the current code.
# pylint: disable=duplicate-code


# pylint: disable=invalid-name
revision = "0001"
down_revision: str | None = None
branch_labels: str | None = None
depends_on: str | None = None
# pylint: enable=invalid-name


def upgrade(*, schema: str | None, prefix: str) -> None:
    """Creates the tables as 0.1.0 created them."""
    op.create_table(
        f"{prefix}key_registry",
        sa.Column("entity", sa.Unicode(150), primary_key=True),
        sa.Column("surrogate_key", sa.Unicode(150), primary_key=True),
        sa.Column("natural_key", sa.Unicode(400), nullable=False),
        _stamp("registered_at"),
        sa.Index(f"{prefix}key_registry_natural", "entity", "natural_key"),
        schema=schema,
    )
    op.create_table(
        f"{prefix}state",
        sa.Column("namespace", sa.Unicode(150), primary_key=True),
        sa.Column("key", sa.Unicode(250), primary_key=True),
        sa.Column("value", sa.UnicodeText, nullable=False),
        _stamp("updated_at"),
        schema=schema,
    )
    op.create_table(
        f"{prefix}parked",
        sa.Column("queue", sa.Unicode(100), primary_key=True),
        sa.Column("identity", sa.Unicode(300), primary_key=True),
        sa.Column("payload", sa.UnicodeText, nullable=False),
        sa.Column("reason", sa.UnicodeText, nullable=False),
        sa.Column("attempts", sa.Integer, nullable=False),
        _stamp("parked_at"),
        _stamp("last_parked_at"),
        schema=schema,
    )
    op.create_table(
        f"{prefix}quarantine",
        _identity(),
        sa.Column("pipeline", sa.Unicode(200), nullable=False),
        sa.Column("run_id", sa.Unicode(64), nullable=False),
        sa.Column("checkpoint", sa.Unicode(32), nullable=False),
        sa.Column("record", sa.UnicodeText, nullable=False),
        sa.Column("issues", sa.UnicodeText, nullable=False),
        _stamp("rejected_at"),
        sa.Index(f"{prefix}quarantine_run", "pipeline", "run_id"),
        schema=schema,
    )
    op.create_table(
        f"{prefix}quality_issues",
        _identity(),
        sa.Column("pipeline", sa.Unicode(200), nullable=False),
        sa.Column("run_id", sa.Unicode(64), nullable=False),
        sa.Column("rule", sa.Unicode(200), nullable=False),
        sa.Column("checkpoint", sa.Unicode(32), nullable=False),
        sa.Column("action", sa.Unicode(16), nullable=False),
        sa.Column("message", sa.UnicodeText, nullable=False),
        sa.Column("field", sa.Unicode(200)),
        sa.Column("value", sa.UnicodeText),
        sa.Column("record_id", sa.Unicode(400)),
        _stamp("found_at"),
        sa.Index(f"{prefix}quality_issues_run", "pipeline", "run_id"),
        schema=schema,
    )
    op.create_table(
        f"{prefix}runs",
        sa.Column("run_id", sa.Unicode(64), primary_key=True),
        sa.Column("pipeline", sa.Unicode(200), nullable=False),
        sa.Column("status", sa.Unicode(32), nullable=False),
        _stamp("started_at"),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("stats", sa.UnicodeText),
        sa.Column("message", sa.UnicodeText),
        sa.Column("host", sa.Unicode(200), nullable=False),
        sa.Column("dry_run", sa.Boolean, nullable=False),
        sa.Column("meta", sa.UnicodeText),
        sa.Index(f"{prefix}runs_pipeline", "pipeline", "started_at"),
        schema=schema,
    )
    op.create_table(
        f"{prefix}locks",
        sa.Column("name", sa.Unicode(200), primary_key=True),
        sa.Column("owner", sa.Unicode(200), nullable=False),
        sa.Column("expires_at", sa.Double, nullable=False),
        schema=schema,
    )


def _identity() -> sa.Column[Any]:
    # SQLite only auto-increments an INTEGER primary key.
    return sa.Column(
        "id",
        sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
        sa.Identity(),
        primary_key=True,
    )


def _stamp(name: str) -> sa.Column[Any]:
    return sa.Column(name, sa.DateTime(timezone=True), nullable=False)
