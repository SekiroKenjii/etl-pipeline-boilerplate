"""The ETL tables as earlier code created them, with `create_all` and no
version table: to test that `etl-db upgrade` tells their revision and
upgrades them.

Copied from the code of each state, not from the revisions: the tests check
the revisions against what really ran.

- "0001": 0.1.0.
- "0001+steps": 0.1.0, then a later `create_all` added `run_steps`, not the
  columns (review D4).
- "0002": the code before the review (the steps, the quarantine's status).
- "0003": wave 2 of the review (the quarantine's record keys, the indexes of
  the issues), before the unique index of the open entries.
"""

from __future__ import annotations

from typing import Any

import sqlalchemy as sa

STATES = ("0001", "0001+steps", "0002", "0003")


def metadata(
    state: str, schema: str | None = None, prefix: str = "etl_"
) -> sa.MetaData:
    """The ETL tables of a state of the code: one of `STATES`."""
    revision = state.split("+", maxsplit=1)[0]
    since_0002 = revision >= "0002"
    since_0003 = revision >= "0003"
    meta = sa.MetaData(schema=schema)

    def identity() -> sa.Column[int]:
        return sa.Column(
            "id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            sa.Identity(),
            primary_key=True,
        )

    def stamp(name: str) -> sa.Column[Any]:
        return sa.Column(name, sa.DateTime(timezone=True), nullable=False)

    sa.Table(
        f"{prefix}key_registry",
        meta,
        sa.Column("entity", sa.Unicode(150), primary_key=True),
        sa.Column("surrogate_key", sa.Unicode(150), primary_key=True),
        sa.Column("natural_key", sa.Unicode(400), nullable=False),
        stamp("registered_at"),
        sa.Index(f"{prefix}key_registry_natural", "entity", "natural_key"),
    )
    sa.Table(
        f"{prefix}state",
        meta,
        sa.Column("namespace", sa.Unicode(150), primary_key=True),
        sa.Column("key", sa.Unicode(250), primary_key=True),
        sa.Column("value", sa.UnicodeText, nullable=False),
        stamp("updated_at"),
    )
    sa.Table(
        f"{prefix}parked",
        meta,
        sa.Column("queue", sa.Unicode(100), primary_key=True),
        sa.Column("identity", sa.Unicode(300), primary_key=True),
        sa.Column("payload", sa.UnicodeText, nullable=False),
        sa.Column("reason", sa.UnicodeText, nullable=False),
        sa.Column("attempts", sa.Integer, nullable=False),
        stamp("parked_at"),
        stamp("last_parked_at"),
    )
    quarantine: list[Any] = [
        identity(),
        sa.Column("pipeline", sa.Unicode(200), nullable=False),
        sa.Column("run_id", sa.Unicode(64), nullable=False),
        sa.Column("checkpoint", sa.Unicode(32), nullable=False),
        sa.Column("record", sa.UnicodeText, nullable=False),
        sa.Column("issues", sa.UnicodeText, nullable=False),
        stamp("rejected_at"),
        sa.Index(f"{prefix}quarantine_run", "pipeline", "run_id"),
    ]
    if since_0002:
        quarantine += [
            sa.Column(
                "status",
                sa.Unicode(16),
                nullable=False,
                server_default="open",
            ),
            sa.Column("resolved_at", sa.DateTime(timezone=True)),
            sa.Index(f"{prefix}quarantine_status", "status", "pipeline"),
        ]
    if since_0003:
        quarantine += [
            sa.Column("record_key", sa.Unicode(64), nullable=False),
            sa.Index(f"{prefix}quarantine_record", "pipeline", "record_key"),
        ]
    sa.Table(f"{prefix}quarantine", meta, *quarantine)
    issues: list[Any] = [
        identity(),
        sa.Column("pipeline", sa.Unicode(200), nullable=False),
        sa.Column("run_id", sa.Unicode(64), nullable=False),
        sa.Column("rule", sa.Unicode(200), nullable=False),
        sa.Column("checkpoint", sa.Unicode(32), nullable=False),
        sa.Column("action", sa.Unicode(16), nullable=False),
        sa.Column("message", sa.UnicodeText, nullable=False),
        sa.Column("field", sa.Unicode(200)),
        sa.Column("value", sa.UnicodeText),
        sa.Column("record_id", sa.Unicode(400)),
        stamp("found_at"),
        sa.Index(f"{prefix}quality_issues_run", "pipeline", "run_id"),
    ]
    if since_0003:
        issues += [
            sa.Index(f"{prefix}quality_issues_record", "record_id"),
            sa.Index(f"{prefix}quality_issues_found", "found_at"),
        ]
    sa.Table(f"{prefix}quality_issues", meta, *issues)
    sa.Table(
        f"{prefix}runs",
        meta,
        sa.Column("run_id", sa.Unicode(64), primary_key=True),
        sa.Column("pipeline", sa.Unicode(200), nullable=False),
        sa.Column("status", sa.Unicode(32), nullable=False),
        stamp("started_at"),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("stats", sa.UnicodeText),
        sa.Column("message", sa.UnicodeText),
        sa.Column("host", sa.Unicode(200), nullable=False),
        sa.Column("dry_run", sa.Boolean, nullable=False),
        sa.Column("meta", sa.UnicodeText),
        sa.Index(f"{prefix}runs_pipeline", "pipeline", "started_at"),
    )
    if since_0002 or state.endswith("+steps"):
        sa.Table(
            f"{prefix}run_steps",
            meta,
            sa.Column("run_id", sa.Unicode(64), primary_key=True),
            sa.Column("step", sa.Unicode(200), primary_key=True),
            sa.Column("status", sa.Unicode(32), nullable=False),
            stamp("completed_at"),
            sa.Column("stats", sa.UnicodeText),
            sa.Column("message", sa.UnicodeText),
            sa.Column("host", sa.Unicode(200), nullable=False),
            sa.Column("attempts", sa.Integer, nullable=False),
        )
    sa.Table(
        f"{prefix}locks",
        meta,
        sa.Column("name", sa.Unicode(200), primary_key=True),
        sa.Column("owner", sa.Unicode(200), nullable=False),
        sa.Column("expires_at", sa.Double, nullable=False),
    )
    return meta
