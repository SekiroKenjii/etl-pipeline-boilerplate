"""The ETL tables, as the latest revision of `migrations`: their columns,
indexes and names, in one schema with one prefix."""

from __future__ import annotations

from collections.abc import Callable
import dataclasses
import datetime
import functools

import sqlalchemy as sa

from core import contract
from shared import utils

DEFAULT_PREFIX = "etl_"
# One open quarantine entry per record: a partial index, or on Oracle one on
# expressions null elsewhere. MySQL has neither: writers read before writing.
_PARTIAL_INDEX_DIALECTS = ("postgresql", "mssql", "sqlite")
OPEN_ENTRY_INDEX_DIALECTS = (*_PARTIAL_INDEX_DIALECTS, "oracle")


@dataclasses.dataclass(frozen=True, slots=True)
class EtlTables:
    """The tables of the ETL's records, in one schema, with one name prefix.

    Attributes:
        metadata: Their SQLAlchemy metadata.
        key_registry: The crosswalk of loaded keys.
        state: The state kept between runs.
        parked: The records waiting to be retried.
        quarantine: The records rejected by the quality gate.
        quality_issues: Every issue the quality gate found.
        runs: The history of the runs.
        run_steps: How each step of each run ended.
        locks: The locks of the steps running, `<pipeline>/<step>`.
    """

    metadata: sa.MetaData
    key_registry: sa.Table
    state: sa.Table
    parked: sa.Table
    quarantine: sa.Table
    quality_issues: sa.Table
    runs: sa.Table
    run_steps: sa.Table
    locks: sa.Table


@functools.cache
def etl_tables(
    schema: str | None = None, prefix: str = DEFAULT_PREFIX
) -> EtlTables:
    """Returns the ETL tables in `schema`, named `<prefix><table>`.

    Args:
        schema: The schema; the default schema of the connection if None.
        prefix: Prefixes the table names, e.g. `etl_key_registry`.
    """
    metadata = sa.MetaData(schema=schema)
    now = utils.utc_now

    def identity() -> sa.Column[int]:
        # SQLite only auto-increments an INTEGER primary key.
        return sa.Column(
            "id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            sa.Identity(),
            primary_key=True,
        )

    def stamp(name: str) -> sa.Column[datetime.datetime]:
        return sa.Column(
            name, sa.DateTime(timezone=True), nullable=False, default=now
        )

    # Key lengths stay under SQL Server's 900 bytes per index key.
    key_registry = sa.Table(
        f"{prefix}key_registry",
        metadata,
        sa.Column("entity", sa.Unicode(150), primary_key=True),
        sa.Column("surrogate_key", sa.Unicode(150), primary_key=True),
        sa.Column("natural_key", sa.Unicode(400), nullable=False),
        stamp("registered_at"),
        sa.Index(f"{prefix}key_registry_natural", "entity", "natural_key"),
    )
    state = sa.Table(
        f"{prefix}state",
        metadata,
        sa.Column("namespace", sa.Unicode(150), primary_key=True),
        sa.Column("key", sa.Unicode(250), primary_key=True),
        sa.Column("value", sa.UnicodeText, nullable=False),
        stamp("updated_at"),
    )
    parked = sa.Table(
        f"{prefix}parked",
        metadata,
        sa.Column("queue", sa.Unicode(100), primary_key=True),
        sa.Column("identity", sa.Unicode(300), primary_key=True),
        sa.Column("payload", sa.UnicodeText, nullable=False),
        sa.Column("reason", sa.UnicodeText, nullable=False),
        sa.Column("attempts", sa.Integer, nullable=False),
        stamp("parked_at"),
        stamp("last_parked_at"),
    )
    quarantine = sa.Table(
        f"{prefix}quarantine",
        metadata,
        identity(),
        sa.Column("pipeline", sa.Unicode(200), nullable=False),
        sa.Column("run_id", sa.Unicode(64), nullable=False),
        sa.Column("checkpoint", sa.Unicode(32), nullable=False),
        sa.Column("record", sa.UnicodeText, nullable=False),
        sa.Column("issues", sa.UnicodeText, nullable=False),
        stamp("rejected_at"),
        sa.Column(
            "status",
            sa.Unicode(16),
            nullable=False,
            default=contract.QuarantineStatus.OPEN.value,
            server_default=contract.QuarantineStatus.OPEN.value,
        ),
        sa.Column("resolved_at", sa.DateTime(timezone=True)),
        # `QuarantineEntry.key_of`: a record rejected again refreshes its entry.
        sa.Column("record_key", sa.Unicode(64), nullable=False),
        sa.Index(f"{prefix}quarantine_run", "pipeline", "run_id"),
        sa.Index(f"{prefix}quarantine_status", "status", "pipeline"),
        sa.Index(f"{prefix}quarantine_record", "pipeline", "record_key"),
    )
    _open_entry_index(quarantine, f"{prefix}quarantine_open")
    quality_issues = sa.Table(
        f"{prefix}quality_issues",
        metadata,
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
        sa.Index(f"{prefix}quality_issues_record", "record_id"),
        sa.Index(f"{prefix}quality_issues_found", "found_at"),
    )
    runs = sa.Table(
        f"{prefix}runs",
        metadata,
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
    run_steps = sa.Table(
        f"{prefix}run_steps",
        metadata,
        sa.Column("run_id", sa.Unicode(64), primary_key=True),
        sa.Column("step", sa.Unicode(200), primary_key=True),
        sa.Column("status", sa.Unicode(32), nullable=False),
        stamp("completed_at"),
        sa.Column("stats", sa.UnicodeText),
        sa.Column("message", sa.UnicodeText),
        sa.Column("host", sa.Unicode(200), nullable=False),
        sa.Column("attempts", sa.Integer, nullable=False),
    )
    locks = sa.Table(
        f"{prefix}locks",
        metadata,
        sa.Column("name", sa.Unicode(200), primary_key=True),
        sa.Column("owner", sa.Unicode(200), nullable=False),
        # Epoch seconds as DOUBLE: MySQL's FLOAT would round them to minutes.
        sa.Column("expires_at", sa.Double, nullable=False),
    )
    return EtlTables(
        metadata,
        key_registry,
        state,
        parked,
        quarantine,
        quality_issues,
        runs,
        run_steps,
        locks,
    )


def _only_on(*databases: str) -> Callable[..., bool]:
    """Whether DDL applies to a database: for `ddl_if`."""

    def applies(*_: object, dialect: sa.Dialect, **__: object) -> bool:
        return dialect.name in databases

    return applies


def _open_entry_index(quarantine: sa.Table, name: str) -> None:
    """Declares the unique index of the open entries of the quarantine.

    Two writers rejecting the same record at once cannot both add an open
    entry for it: the second one fails on the index, and tries again
    (`RelationalQuarantine.put`). Resolved entries are not in the index.
    """
    columns = quarantine.c
    is_open = columns.status == contract.QuarantineStatus.OPEN.value
    sa.Index(
        name,
        columns.pipeline,
        columns.record_key,
        unique=True,
        postgresql_where=is_open,
        mssql_where=is_open,
        sqlite_where=is_open,
    ).ddl_if(callable_=_only_on(*_PARTIAL_INDEX_DIALECTS))
    # Oracle leaves out of an index the rows whose values are all null.
    sa.Index(
        name,
        sa.case((is_open, columns.pipeline)),
        sa.case((is_open, columns.record_key)),
        unique=True,
    ).ddl_if(callable_=_only_on("oracle"))
