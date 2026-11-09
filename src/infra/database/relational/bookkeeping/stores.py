"""The key crosswalk, the state kept between runs and the parked records,
in the ETL tables."""

from __future__ import annotations

from collections.abc import Collection, Iterator, Sequence
import json

import sqlalchemy as sa

from core import contract
from infra.database.relational import database
from infra.database.relational.bookkeeping import _base
from shared import utils

_PAGE_SIZE = 1000
_KEY_PAGE_SIZE = 10_000


class RelationalKeyRegistry(_base.Bookkeeper, contract.IKeyRegistry):
    """The crosswalk of loaded keys, in `etl_key_registry`.

    Shared by every host and step, and safe for steps registering the same
    keys concurrently. Put a `keys.TieredKeyCache` in front of it to save
    round trips.
    """

    def register(self, entries: Sequence[contract.KeyEntry]) -> None:
        """See `contract.IKeyRegistry.register`; a batch is one INSERT of
        the keys missing, with no read first (`Dialect.insert_missing`)."""
        unique = {(e.entity, e.surrogate_key): e for e in entries}
        if not unique:
            return
        now = utils.utc_now()
        rows: list[contract.Row] = [
            {
                "entity": e.entity,
                "surrogate_key": e.surrogate_key,
                "natural_key": _base.dumps(list(e.natural_key)),
                "registered_at": now,
            }
            for e in unique.values()
        ]
        table = self.tables.key_registry
        key = [c.name for c in table.primary_key.columns]
        dialect = self.db.dialect
        action = f"registering {len(rows)} key(s)"

        def attempt() -> None:
            with database.errors(action, dialect):
                dialect.prepare_join(self.db.engine, table, key)
                dialect.insert_missing(self.db.engine, table, rows, key)

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
        for chunk in self._chunks(keys):
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
            table.c.natural_key == _base.dumps(list(natural_key)),
        )
        with self.db.begin(f"forgetting a key of {entity}") as conn:
            gone = list(
                conn.execute(sa.select(table.c.surrogate_key).where(match))
                .scalars()
                .all()
            )
            conn.execute(table.delete().where(match))
        return gone


class RelationalStateStore(_base.Bookkeeper, contract.IStateStore):
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
        text = _base.dumps(value)

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


class RelationalParkingLot(_base.Bookkeeper, contract.IParkingLot):
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
                            payload=_base.dumps(entry.payload),
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
                                payload=_base.dumps(entry.payload),
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
                    _base.utc(parked_at),
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
