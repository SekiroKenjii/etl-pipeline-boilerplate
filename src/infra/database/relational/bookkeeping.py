"""The ETL's own records in a relational database: key crosswalk, state,
parked records, quarantine, quality issues, runs and their locks.

The tables are declared here as the latest revision of `migrations`, which
creates or upgrades them (`etl-db upgrade`, `etl-db ddl`). JSON is stored as
text. The records are shared by every host running the pipelines.
"""

from __future__ import annotations

from collections.abc import Callable, Collection, Iterator, Mapping, Sequence
import dataclasses
import datetime
import functools
import json
import time
from typing import Any

import sqlalchemy as sa

from core import contract
from infra.database.relational import database
from infra.database.relational import dialects
from shared import utils

DEFAULT_PREFIX = "etl_"
# Values per IN list: Oracle takes 1000 at most, SQL Server 2100 parameters.
_CHUNK = 500
_PAGE_SIZE = 1000
_KEY_PAGE_SIZE = 10_000
_CONCURRENT_KEY_WRITE_ATTEMPTS = 8
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
    now: Callable[[], Any] = utils.utc_now

    def identity() -> sa.Column[int]:
        # SQLite only auto-increments an INTEGER primary key.
        return sa.Column(
            "id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            sa.Identity(),
            primary_key=True,
        )

    def stamp(name: str) -> sa.Column[Any]:
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

    def applies(*_: Any, dialect: sa.Dialect, **__: Any) -> bool:
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


def _dumps(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, default=utils.to_jsonable)


def _chunks[T](items: Sequence[T]) -> Iterator[Sequence[T]]:
    for start in range(0, len(items), _CHUNK):
        yield items[start : start + _CHUNK]


class _Bookkeeper(database.RelationalAdapter):
    """An adapter writing to the ETL tables.

    Attributes:
        tables: The ETL tables.
    """

    def __init__(
        self,
        db: database.Database | str,
        *,
        schema: str | None = None,
        prefix: str = DEFAULT_PREFIX,
    ) -> None:
        """Initializes the adapter.

        Args:
            db: A shared database, or a SQLAlchemy URL.
            schema: The schema of the ETL tables.
            prefix: The prefix of their names.
        """
        super().__init__(db)
        self.tables = etl_tables(schema, prefix)

    def _retry_conflicts(self, attempt: Callable[[], None]) -> None:
        """Runs `attempt` again when a concurrent writer took a key first.

        Each attempt reads what exists before writing what is missing; a
        writer inserting the same key in between makes it fail on the primary
        key, and the next attempt then sees that key.
        """
        utils.retry(
            attempt,
            when=lambda exc: dialects.is_unique_violation(exc.__cause__),
            attempts=_CONCURRENT_KEY_WRITE_ATTEMPTS,
        )


class RelationalKeyRegistry(_Bookkeeper, contract.IKeyRegistry):
    """The crosswalk of loaded keys, in `etl_key_registry`.

    Shared by every host and step, and safe for steps registering the same
    keys concurrently. Put a `keys.CachedKeyRegistry` in front of it to save
    round trips.
    """

    def register(self, entries: Sequence[contract.KeyEntry]) -> None:
        unique = {(e.entity, e.surrogate_key): e for e in entries}
        if not unique:
            return
        by_entity: dict[str, list[contract.KeyEntry]] = {}
        for entry in unique.values():
            by_entity.setdefault(entry.entity, []).append(entry)

        def attempt() -> None:
            table = self.tables.key_registry
            with self.db.begin(f"registering {len(unique)} key(s)") as conn:
                for entity, items in by_entity.items():
                    known = self._known(
                        conn, entity, [e.surrogate_key for e in items]
                    )
                    database.insert_many(
                        conn,
                        table,
                        [
                            {
                                "entity": e.entity,
                                "surrogate_key": e.surrogate_key,
                                "natural_key": _dumps(list(e.natural_key)),
                                "registered_at": utils.utc_now(),
                            }
                            for e in items
                            if e.surrogate_key not in known
                        ],
                    )

        self._retry_conflicts(attempt)

    def known(self, entity: str, surrogate_keys: Collection[str]) -> set[str]:
        if not surrogate_keys:
            return set()
        with self.db.connect(f"looking up {entity} keys") as connection:
            return self._known(connection, entity, list(surrogate_keys))

    def entries(self, entity: str) -> Iterator[contract.KeyEntry]:
        table = self.tables.key_registry
        after: str | None = None
        while True:
            statement = (
                sa.select(table.c.surrogate_key, table.c.natural_key)
                .where(table.c.entity == entity)
                .order_by(table.c.surrogate_key)
                .limit(_KEY_PAGE_SIZE)
            )
            if after is not None:
                statement = statement.where(table.c.surrogate_key > after)
            with self.db.connect(f"reading the {entity} keys") as conn:
                rows = conn.execute(statement).all()
            for surrogate, natural in rows:
                yield contract.KeyEntry(
                    entity, surrogate, tuple(json.loads(natural))
                )
            if len(rows) < _KEY_PAGE_SIZE:
                return
            after = rows[-1][0]

    def natural_key(
        self, entity: str, surrogate_key: str
    ) -> tuple[str, ...] | None:
        table = self.tables.key_registry
        with self.db.connect(f"looking up {entity} {surrogate_key}") as conn:
            found = conn.execute(
                sa.select(table.c.natural_key).where(
                    table.c.entity == entity,
                    table.c.surrogate_key == surrogate_key,
                )
            ).scalar()
        return None if found is None else tuple(json.loads(found))

    def _known(
        self, connection: sa.Connection, entity: str, keys: Sequence[str]
    ) -> set[str]:
        """Returns the keys registered, querying a chunk of them at a time."""
        table = self.tables.key_registry
        found: set[str] = set()
        for chunk in _chunks(keys):
            found.update(
                connection.execute(
                    sa.select(table.c.surrogate_key).where(
                        table.c.entity == entity,
                        table.c.surrogate_key.in_(chunk),
                    )
                ).scalars()
            )
        return found

    def forget(self, entity: str, natural_key: Sequence[str]) -> list[str]:
        table = self.tables.key_registry
        match = sa.and_(
            table.c.entity == entity,
            table.c.natural_key == _dumps(list(natural_key)),
        )
        with self.db.begin(f"forgetting a key of {entity}") as conn:
            gone = list(
                conn.execute(sa.select(table.c.surrogate_key).where(match))
                .scalars()
                .all()
            )
            conn.execute(table.delete().where(match))
        return gone


class RelationalStateStore(_Bookkeeper, contract.IStateStore):
    """The state kept between runs, in `etl_state`, as JSON text.

    Unlike `JsonFileStateStore`, it is shared by every host running the
    pipelines.
    """

    def get(self, namespace: str, key: str) -> contract.StateValue:
        table = self.tables.state
        with self.db.connect(f"reading the state {namespace}/{key}") as conn:
            found = conn.execute(
                sa.select(table.c.value).where(
                    table.c.namespace == namespace, table.c.key == key
                )
            ).scalar()
        value: contract.StateValue = (
            None if found is None else json.loads(found)
        )
        return value

    def set(self, namespace: str, key: str, value: contract.StateValue) -> None:
        table = self.tables.state
        where = (table.c.namespace == namespace, table.c.key == key)
        text = _dumps(value)

        def attempt() -> None:
            with self.db.begin(f"saving the state {namespace}/{key}") as conn:
                updated = conn.execute(
                    table.update()
                    .where(*where)
                    .values(value=text, updated_at=utils.utc_now())
                ).rowcount
                if not updated:
                    conn.execute(
                        table.insert().values(
                            namespace=namespace,
                            key=key,
                            value=text,
                            updated_at=utils.utc_now(),
                        )
                    )

        self._retry_conflicts(attempt)

    def delete(self, namespace: str, key: str) -> None:
        table = self.tables.state
        with self.db.begin(f"deleting the state {namespace}/{key}") as conn:
            conn.execute(
                table.delete().where(
                    table.c.namespace == namespace, table.c.key == key
                )
            )


class RelationalParkingLot(_Bookkeeper, contract.IParkingLot):
    """Parked records, in `etl_parked`, one queue per mapping.

    Query it to see what waits, and since when: `attempts` grows at every
    run that could not load the record.
    """

    def park(self, queue: str, entries: Sequence[contract.ParkedEntry]) -> None:
        latest = {entry.identity: entry for entry in entries}
        if not latest:
            return
        table = self.tables.parked

        def attempt() -> None:
            with self.db.begin(f"parking records in {queue}") as conn:
                for entry in latest.values():
                    now = utils.utc_now()
                    updated = conn.execute(
                        table.update()
                        .where(
                            table.c.queue == queue,
                            table.c.identity == entry.identity,
                        )
                        .values(
                            payload=_dumps(entry.payload),
                            reason=entry.reason,
                            attempts=table.c.attempts + 1,
                            last_parked_at=now,
                        )
                    ).rowcount
                    if not updated:
                        conn.execute(
                            table.insert().values(
                                queue=queue,
                                identity=entry.identity,
                                payload=_dumps(entry.payload),
                                reason=entry.reason,
                                attempts=1,
                                parked_at=now,
                                last_parked_at=now,
                            )
                        )

        self._retry_conflicts(attempt)

    def entries(self, queue: str) -> Iterator[contract.ParkedEntry]:
        table = self.tables.parked
        after: str | None = None
        while True:
            statement = (
                sa.select(
                    table.c.identity,
                    table.c.payload,
                    table.c.reason,
                    table.c.attempts,
                    table.c.parked_at,
                )
                .where(table.c.queue == queue)
                .order_by(table.c.identity)
                .limit(_PAGE_SIZE)
            )
            if after is not None:
                statement = statement.where(table.c.identity > after)
            with self.db.connect(f"reading the parked {queue}") as conn:
                rows = conn.execute(statement).all()
            for identity, payload, reason, attempts, parked_at in rows:
                yield contract.ParkedEntry(
                    identity,
                    json.loads(payload),
                    reason,
                    attempts,
                    _utc(parked_at),
                )
            if len(rows) < _PAGE_SIZE:
                return
            after = rows[-1][0]

    def release(
        self, queue: str, entries: Sequence[contract.ParkedEntry]
    ) -> None:
        if not entries:
            return
        table = self.tables.parked
        with self.db.begin(f"releasing parked records of {queue}") as conn:
            conn.execute(
                table.delete().where(
                    table.c.queue == queue,
                    table.c.identity == sa.bindparam("p_identity"),
                    table.c.attempts == sa.bindparam("p_attempts"),
                ),
                [
                    {"p_identity": e.identity, "p_attempts": e.attempts}
                    for e in entries
                ],
            )


class RelationalQuarantine(_Bookkeeper, contract.IQuarantine):
    """Rejected records, in `etl_quarantine`, with the issues that rejected
    them, as JSON text."""

    def __init__(
        self,
        db: database.Database | str,
        *,
        schema: str | None = None,
        prefix: str = DEFAULT_PREFIX,
        serialize: Callable[[object], object] | None = None,
    ) -> None:
        """Initializes the quarantine.

        Args:
            db: A shared database, or a SQLAlchemy URL.
            schema: The schema of the ETL tables.
            prefix: The prefix of their names.
            serialize: Turns a record into JSON-friendly data, e.g. to hide
                sensitive fields. By default records are stored as they are,
                through `utils.to_jsonable`.
        """
        super().__init__(db, schema=schema, prefix=prefix)
        self._serialize = serialize

    def put(
        self,
        records: Sequence[contract.RejectedRecord[Any]],
        ctx: contract.ETLContext,
    ) -> None:
        """See `contract.IQuarantine.put`: a record rejected again updates
        its open entry, keeping its id.

        Safe for steps rejecting the same record at once: the open entries
        are unique (`OPEN_ENTRY_INDEX_DIALECTS`), and a writer adding one
        another just added tries again, then updates it.
        """
        if not records:
            return
        table = self.tables.quarantine
        pipeline = ctx.run.pipeline_name
        now = utils.utc_now()
        rows: dict[str, dict[str, Any]] = {}
        for rejected in records:
            record = self._dump(rejected.record)
            key = contract.QuarantineEntry.key_of(
                pipeline, rejected.checkpoint.value, record
            )
            # The last one, if twice in the batch.
            rows[key] = {
                "run_id": ctx.run.run_id,
                "checkpoint": rejected.checkpoint.value,
                "record": _dumps(record),
                "issues": _dumps(
                    [issue.as_dict() for issue in rejected.issues]
                ),
                "rejected_at": now,
            }

        def attempt() -> None:
            with self.db.begin(f"quarantining {len(rows)} record(s)") as conn:
                open_ids: dict[str, int] = {}
                for chunk in _chunks(list(rows)):
                    for key, id_ in conn.execute(
                        sa.select(table.c.record_key, table.c.id).where(
                            table.c.pipeline == pipeline,
                            table.c.status
                            == contract.QuarantineStatus.OPEN.value,
                            table.c.record_key.in_(chunk),
                        )
                    ):
                        open_ids[key] = id_
                again = [
                    {"p_id": open_ids[key], **row}
                    for key, row in rows.items()
                    if key in open_ids
                ]
                if again:
                    conn.execute(
                        table.update().where(
                            table.c.id == sa.bindparam("p_id")
                        ),
                        again,
                    )
                new = [
                    {"pipeline": pipeline, "record_key": key, **row}
                    for key, row in rows.items()
                    if key not in open_ids
                ]
                if new:
                    conn.execute(table.insert(), new)

        self._retry_conflicts(attempt)

    def _dump(self, record: object) -> object:
        return self._serialize(record) if self._serialize else record

    def entries(
        self,
        pipeline: str | None = None,
        status: contract.QuarantineStatus | None = (
            contract.QuarantineStatus.OPEN
        ),
    ) -> Iterator[contract.QuarantineEntry]:
        table = self.tables.quarantine
        statement = sa.select(table).order_by(table.c.id)
        if pipeline is not None:
            statement = statement.where(table.c.pipeline == pipeline)
        if status is not None:
            statement = statement.where(table.c.status == status.value)
        # Pages by id: no transaction stays open while the caller works.
        after = None
        while True:
            page = statement.limit(_CHUNK)
            if after is not None:
                page = page.where(table.c.id > after)
            with self.db.connect("reading the quarantine") as conn:
                rows = conn.execute(page).mappings().all()
            for row in rows:
                yield contract.QuarantineEntry(
                    id=str(row["id"]),
                    pipeline=row["pipeline"],
                    run_id=row["run_id"],
                    checkpoint=row["checkpoint"],
                    record=json.loads(row["record"]),
                    issues=tuple(json.loads(row["issues"])),
                    rejected_at=_utc(row["rejected_at"]),
                    status=contract.QuarantineStatus(row["status"]),
                    resolved_at=(
                        _utc(row["resolved_at"]) if row["resolved_at"] else None
                    ),
                )
            if len(rows) < _CHUNK:
                return
            after = rows[-1]["id"]

    def resolve(
        self, ids: Collection[str], status: contract.QuarantineStatus
    ) -> int:
        table = self.tables.quarantine
        numbers = sorted({int(id_) for id_ in ids})
        changed = 0
        with self.db.begin(f"resolving {len(numbers)} record(s)") as conn:
            for chunk in _chunks(numbers):
                changed += conn.execute(
                    table.update()
                    .where(
                        table.c.id.in_(chunk), table.c.status != status.value
                    )
                    .values(status=status.value, resolved_at=utils.utc_now())
                ).rowcount
        return changed

    def erase(self, ids: Collection[str]) -> int:
        table = self.tables.quarantine
        numbers = sorted({int(id_) for id_ in ids})
        erased = 0
        with self.db.begin(f"erasing {len(numbers)} record(s)") as conn:
            for chunk in _chunks(numbers):
                erased += conn.execute(
                    table.delete().where(table.c.id.in_(chunk))
                ).rowcount
        return erased

    def purge(self, before: datetime.datetime) -> int:
        table = self.tables.quarantine
        with self.db.begin("purging the quarantine") as conn:
            return conn.execute(
                table.delete().where(table.c.rejected_at < before)
            ).rowcount


def _utc(moment: datetime.datetime) -> datetime.datetime:
    """A time read back: SQLite and MySQL drop the time zone, UTC here."""
    return moment if moment.tzinfo else moment.replace(tzinfo=datetime.UTC)


class RelationalIssueSink(_Bookkeeper, contract.IIssueSink):
    """Every quality issue, in `etl_quality_issues`."""

    def write(
        self, issues: Sequence[contract.QualityIssue], ctx: contract.ETLContext
    ) -> None:
        if not issues:
            return
        now = utils.utc_now()
        with self.db.begin(f"writing {len(issues)} quality issue(s)") as conn:
            conn.execute(
                self.tables.quality_issues.insert(),
                [
                    {
                        "pipeline": ctx.run.pipeline_name,
                        "run_id": ctx.run.run_id,
                        "rule": issue.rule,
                        "checkpoint": issue.checkpoint.value,
                        "action": issue.action.value,
                        "message": issue.message,
                        "field": issue.field,
                        "value": (
                            None if issue.value is None else _dumps(issue.value)
                        ),
                        "record_id": issue.record_id,
                        "found_at": now,
                    }
                    for issue in issues
                ],
            )

    def erase(self, record_ids: Collection[str]) -> int:
        table = self.tables.quality_issues
        wanted = sorted(set(record_ids))
        erased = 0
        with self.db.begin(
            f"erasing the issues of {len(wanted)} record(s)"
        ) as conn:
            for chunk in _chunks(wanted):
                erased += conn.execute(
                    table.delete().where(table.c.record_id.in_(chunk))
                ).rowcount
        return erased

    def purge(self, before: datetime.datetime) -> int:
        table = self.tables.quality_issues
        with self.db.begin("purging the quality issues") as conn:
            return conn.execute(
                table.delete().where(table.c.found_at < before)
            ).rowcount


class RelationalRunLedger(_Bookkeeper, contract.IRunLedger):
    """The history of the runs, in `etl_runs`, and of their steps, in
    `etl_run_steps`."""

    def start(
        self,
        run: contract.ETLRunInfo,
        host: str,
        meta: Mapping[str, str] | None = None,
    ) -> None:
        try:
            with self.db.begin(f"recording the run {run.run_id}") as conn:
                conn.execute(
                    self.tables.runs.insert().values(
                        run_id=run.run_id,
                        pipeline=run.pipeline_name,
                        status=contract.ETLStatus.IN_PROGRESS.value,
                        started_at=run.triggered_at,
                        host=host,
                        dry_run=run.dry_run,
                        meta=_dumps(dict(meta or {})),
                    )
                )
        except contract.FatalError as exc:
            # Idempotent: a start retried after a lost commit reply is recorded.
            if not dialects.is_unique_violation(exc.__cause__):
                raise

    def record_step(
        self, run_id: str, step: str, result: contract.ETLResult, host: str
    ) -> None:
        record = contract.StepRecord.of(step, result, host)
        table = self.tables.run_steps
        values = {
            "status": record.status.value,
            "completed_at": record.completed_at,
            "stats": _dumps(record.stats),
            "message": record.message,
            "host": record.host,
            "attempts": record.attempts,
        }

        def attempt() -> None:
            with self.db.begin(f"recording the step {step}") as conn:
                updated = conn.execute(
                    table.update()
                    .where(table.c.run_id == run_id, table.c.step == step)
                    .values(**values)
                ).rowcount
                if not updated:
                    conn.execute(
                        table.insert().values(
                            run_id=run_id, step=step, **values
                        )
                    )

        self._retry_conflicts(attempt)

    def finish(self, result: contract.PipelineResult) -> None:
        table = self.tables.runs
        with self.db.begin(f"recording the end of {result.run_id}") as conn:
            conn.execute(
                table.update()
                .where(table.c.run_id == result.run_id)
                .values(
                    status=result.status.value,
                    completed_at=result.completed_at,
                    stats=_dumps(result.stats.as_dict()),
                    message=result.message,
                )
            )

    def recent(
        self, pipeline_name: str | None, limit: int = 20
    ) -> list[contract.RunRecord]:
        table = self.tables.runs
        statement = (
            sa.select(table).order_by(table.c.started_at.desc()).limit(limit)
        )
        if pipeline_name is not None:
            statement = statement.where(table.c.pipeline == pipeline_name)
        with self.db.connect("reading the runs") as conn:
            rows = conn.execute(statement).mappings().all()
        return [_run_record(row) for row in rows]

    def get(self, run_id: str) -> contract.RunRecord | None:
        runs_table, steps = self.tables.runs, self.tables.run_steps
        with self.db.connect(f"reading the run {run_id}") as conn:
            row = (
                conn.execute(
                    sa.select(runs_table).where(runs_table.c.run_id == run_id)
                )
                .mappings()
                .first()
            )
            if row is None:
                return None
            step_rows = (
                conn.execute(sa.select(steps).where(steps.c.run_id == run_id))
                .mappings()
                .all()
            )
        return dataclasses.replace(
            _run_record(row),
            steps={
                step["step"]: contract.StepRecord(
                    step=step["step"],
                    status=contract.ETLStatus(step["status"]),
                    completed_at=_utc(step["completed_at"]),
                    stats=json.loads(step["stats"]) if step["stats"] else {},
                    message=step["message"],
                    host=step["host"],
                    attempts=step["attempts"],
                )
                for step in step_rows
            },
        )


def _run_record(row: sa.RowMapping) -> contract.RunRecord:
    return contract.RunRecord(
        run_id=row["run_id"],
        pipeline=row["pipeline"],
        status=contract.ETLStatus(row["status"]),
        started_at=_utc(row["started_at"]),
        completed_at=_utc(row["completed_at"]) if row["completed_at"] else None,
        stats=json.loads(row["stats"]) if row["stats"] else {},
        message=row["message"],
        host=row["host"],
        dry_run=bool(row["dry_run"]),
        meta=json.loads(row["meta"]) if row["meta"] else {},
    )


class RelationalRunLock(_Bookkeeper, contract.IRunLock):
    """The locks of the steps running, in `etl_locks`: leases, shared by
    every host."""

    def acquire(self, name: str, owner: str, lease_seconds: float) -> bool:
        table = self.tables.locks
        now = time.time()
        with self.db.begin(f"taking the lock {name}") as conn:
            taken = conn.execute(
                table.update()
                .where(
                    table.c.name == name,
                    sa.or_(table.c.expires_at < now, table.c.owner == owner),
                )
                .values(owner=owner, expires_at=now + lease_seconds)
            ).rowcount
        if taken:
            return True
        try:
            with self.db.begin(f"taking the lock {name}") as conn:
                conn.execute(
                    table.insert().values(
                        name=name, owner=owner, expires_at=now + lease_seconds
                    )
                )
        except contract.FatalError as exc:
            if dialects.is_unique_violation(exc.__cause__):
                return False  # Held by another owner.
            raise
        return True

    def renew(self, name: str, owner: str, lease_seconds: float) -> bool:
        table = self.tables.locks
        with self.db.begin(f"renewing the lock {name}") as conn:
            return bool(
                conn.execute(
                    table.update()
                    .where(table.c.name == name, table.c.owner == owner)
                    .values(expires_at=time.time() + lease_seconds)
                ).rowcount
            )

    def release(self, name: str, owner: str) -> None:
        table = self.tables.locks
        with self.db.begin(f"releasing the lock {name}") as conn:
            conn.execute(
                table.delete().where(
                    table.c.name == name, table.c.owner == owner
                )
            )
