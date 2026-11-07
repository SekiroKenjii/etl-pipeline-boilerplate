"""Change sources of relational databases: each database has its own log.

- SQL Server: Change Tracking (`SqlServerChangeTracking`), the net changes
  of each row since a version, deletes included; no SQL Server Agent.
- Postgres: logical replication (`PostgresLogicalReplication`) through a
  slot and `pgoutput`; needs `wal_level = logical`.

Oracle's log needs privileges a migration seldom gets: read Oracle by
watermark, find its deletes with `operators.MissingSourceRows`, or feed
Debezium or GoldenGate changes through a queue.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
import dataclasses
import datetime
import struct
from typing import Any, cast

import sqlalchemy as sa

from core import contract
from infra.database.relational import database
from shared import utils

_PAGE_SIZE = 10_000

_AND = " AND "


class SqlServerChangeTracking(
    database.RelationalAdapter, contract.IChangeSource
):
    """The changes of a SQL Server table, from Change Tracking.

    Change Tracking records which rows changed, not their history: each
    change is the last state of its row (read from the table), or its key
    if it was deleted. Enable it with `enable_change_tracking`. It keeps
    changes for a retention period (2 days by default): a position older
    than that fails, and the table needs a new snapshot.

    Positions are `{"version": v}`, or `{"version": v, "key": [...]}` within
    a version, so that a batch boundary in the middle of a version loses
    nothing.

    Attributes:
        key: The columns of the primary key.
    """

    def __init__(
        self,
        db: database.Database | str,
        table: str,
        key: Sequence[str] | None = None,
        *,
        page_size: int = _PAGE_SIZE,
    ) -> None:
        """Initializes the source.

        Args:
            db: A shared database, or a connection string.
            table: The table, `schema.table`.
            key: The columns of its primary key; read from the table if None.
            page_size: The changes fetched per query.
        """
        super().__init__(db)
        self._table = table
        self._key = tuple(key) if key else None
        self.page_size = page_size

    @property
    def table(self) -> str:
        return self._table

    @property
    def key(self) -> tuple[str, ...]:
        """The columns of the primary key, read from the table if not given.

        Raises:
            FatalError: If the table has no primary key.
        """
        if self._key is None:
            primary = tuple(
                c.name for c in self.db.table(self._table).primary_key
            )
            if not primary:
                raise contract.FatalError(
                    f"{self._table} has no primary key: Change Tracking needs"
                    " one"
                )
            self._key = primary
        return self._key

    def position(self) -> contract.StateValue:
        with self.db.connect(f"reading the version of {self._table}") as conn:
            version = conn.execute(
                sa.text("SELECT CHANGE_TRACKING_CURRENT_VERSION()")
            ).scalar()
        if version is None:
            raise contract.FatalError(
                "Change Tracking is not enabled on the database:"
                " see enable_change_tracking"
            )
        return {"version": int(version)}

    def changes(
        self, after: contract.StateValue, until: contract.StateValue = None
    ) -> Iterator[contract.ChangeEvent]:
        """See `contract.IChangeSource.changes`.

        Raises:
            FatalError: If Change Tracking dropped the changes after `after`.
        """
        version, last = _version(after)
        # Mid-version positions read that version again, past their key.
        base = version - 1 if last is not None else version
        limit = _version(until)[0] if until is not None else None
        self._check_retention(base)

        key = self.key
        while True:
            page = self._page(base, limit, version, last)
            for row in page:
                event = self._event(row, key)
                if event is not None:
                    yield event
            if len(page) < self.page_size:
                return
            version = page[-1]["etl_ct_version"]
            last = tuple(page[-1][f"etl_ct_{c}"] for c in key)

    def _check_retention(self, base: int) -> None:
        """Fails if Change Tracking no longer has the changes after `base`.

        Raises:
            FatalError: If it does not, or is not enabled on the table.
        """
        with self.db.connect(f"reading the changes of {self._table}") as conn:
            minimum = conn.execute(
                sa.text(
                    "SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID(:t))"
                ),
                {"t": self._table},
            ).scalar()
        if minimum is None:
            raise contract.FatalError(
                f"Change Tracking is not enabled on {self._table}"
            )
        if base < int(minimum):
            raise contract.FatalError(
                f"{self._table}: the changes after version {base} are gone"
                f" (Change Tracking keeps them from {minimum}): load a new"
                " snapshot, deleting the state key"
            )

    def _page(
        self,
        base: int,
        limit: int | None,
        version: int,
        last: tuple[Any, ...] | None,
    ) -> list[dict[str, Any]]:
        """Reads one page of changes, in (version, key) order."""
        quote = self.db.engine.dialect.identifier_preparer.quote
        table = self.db.engine.dialect.identifier_preparer.format_table(
            self.db.table(self._table)
        )
        key = self.key
        params: dict[str, Any] = {"base": base, "n": self.page_size}
        conditions = []
        if limit is not None:
            conditions.append("ct.SYS_CHANGE_VERSION <= :limit")
            params["limit"] = limit
        if last is not None:
            # (version, key...) > (v, k...), expanded: no row values.
            alternatives = ["ct.SYS_CHANGE_VERSION > :v"]
            equal = ["ct.SYS_CHANGE_VERSION = :v"]
            for i, column in enumerate(key):
                alternatives.append(
                    "("
                    + _AND.join([*equal, f"ct.{quote(column)} > :k{i}"])
                    + ")"
                )
                equal.append(f"ct.{quote(column)} = :k{i}")
                params[f"k{i}"] = last[i]
            params["v"] = version
            conditions.append("(" + " OR ".join(alternatives) + ")")
        statement = (
            "SELECT TOP (:n) ct.SYS_CHANGE_VERSION AS etl_ct_version,"
            " ct.SYS_CHANGE_OPERATION AS etl_ct_operation, "
            + ", ".join(f"ct.{quote(c)} AS {quote('etl_ct_' + c)}" for c in key)
            + f", t.* FROM CHANGETABLE(CHANGES {table}, :base) AS ct"
            f" LEFT JOIN {table} AS t ON "
            + _AND.join(f"t.{quote(c)} = ct.{quote(c)}" for c in key)
            + (" WHERE " + _AND.join(conditions) if conditions else "")
            + " ORDER BY ct.SYS_CHANGE_VERSION, "
            + ", ".join(f"ct.{quote(c)}" for c in key)
        )
        with self.db.connect(f"reading the changes of {self._table}") as conn:
            return [
                dict(row)
                for row in conn.execute(sa.text(statement), params).mappings()
            ]

    def _event(
        self, row: dict[str, Any], key: tuple[str, ...]
    ) -> contract.ChangeEvent | None:
        """Turns a row of CHANGETABLE into a change; None to skip it."""
        values = tuple(row[f"etl_ct_{c}"] for c in key)
        position = cast(
            contract.StateValue,
            {
                "version": int(row["etl_ct_version"]),
                "key": utils.to_tagged(list(values)),
            },
        )
        operation = row["etl_ct_operation"]
        data = {
            name: value
            for name, value in row.items()
            if not name.startswith("etl_ct_")
        }
        if operation == "D":
            return contract.ChangeEvent(
                self._table,
                contract.ChangeOperation.DELETE,
                dict(zip(key, values)),
                values,
                position,
            )
        if all(data.get(c) is None for c in key):
            # Deleted after this version: its delete comes in a later run.
            return None
        return contract.ChangeEvent(
            self._table,
            contract.ChangeOperation.INSERT
            if operation == "I"
            else contract.ChangeOperation.UPDATE,
            data,
            values,
            position,
        )


def _version(
    position: contract.StateValue,
) -> tuple[int, tuple[Any, ...] | None]:
    """Splits a Change Tracking position into its version and key.

    Raises:
        FatalError: If it is not a Change Tracking position.
    """
    if not isinstance(position, dict) or not isinstance(
        position.get("version"), int
    ):
        raise contract.FatalError(
            f"Not a Change Tracking position: {position!r}"
        )
    key = position.get("key")
    decoded = utils.from_tagged(key) if key is not None else None
    return cast(int, position["version"]), (
        tuple(cast(list[Any], decoded)) if decoded is not None else None
    )


def enable_change_tracking(
    db: database.Database, table: str, retention_days: int = 7
) -> None:
    """Enables Change Tracking on the database, if needed, and on `table`.

    The table needs a primary key; the database cannot be `master`.

    Raises:
        FatalError: If it cannot be enabled, e.g. for lack of privileges.
    """
    with db.connect("enabling Change Tracking") as conn:
        enabled = conn.execute(
            sa.text(
                "SELECT 1 FROM sys.change_tracking_databases"
                " WHERE database_id = DB_ID()"
            )
        ).first()
        if enabled is None:
            conn.execute(
                sa.text(
                    "ALTER DATABASE CURRENT SET CHANGE_TRACKING = ON"
                    f" (CHANGE_RETENTION = {int(retention_days)} DAYS,"
                    " AUTO_CLEANUP = ON)"
                )
            )
        tracked = conn.execute(
            sa.text(
                "SELECT 1 FROM sys.change_tracking_tables"
                " WHERE object_id = OBJECT_ID(:t)"
            ),
            {"t": table},
        ).first()
        if tracked is None:
            name = db.engine.dialect.identifier_preparer.format_table(
                db.table(table)
            )
            conn.execute(sa.text(f"ALTER TABLE {name} ENABLE CHANGE_TRACKING"))


_PG_EPOCH = datetime.datetime(2000, 1, 1, tzinfo=datetime.UTC)


class PostgresLogicalReplication(
    database.RelationalAdapter, contract.IChangeSource
):
    """The changes of a Postgres table, from a logical replication slot.

    The slot (`pgoutput` plugin) keeps the WAL until it is told the changes
    are loaded (`confirm`): a slot never read fills the disk. Create it with
    `create_replication`, drop it with `drop_replication` when the sync ends.

    Positions are `{"commit": lsn, "index": n, "safe": lsn}`: changes come in
    commit order, and the LSNs of changes are not monotonic across
    concurrent transactions; the commit LSN and the position within the
    transaction are. `safe` is the commit of the transaction before, all
    loaded: the slot is advanced to it.

    Needs psycopg (the `postgres` extra) and `wal_level = logical`. Deletes
    come with the replica identity (the primary key by default).
    """

    def __init__(
        self,
        db: database.Database | str,
        table: str,
        *,
        slot: str,
        publication: str,
    ) -> None:
        """Initializes the source.

        Args:
            db: A shared database, or a connection string.
            table: The table, `schema.table`, or `table` in the search path.
            slot: The replication slot.
            publication: The publication of the table.
        """
        super().__init__(db)
        self._table = table
        self.slot = slot
        self.publication = publication
        self._qualified: tuple[str, str] | None = None

    @property
    def table(self) -> str:
        return self._table

    def _namespace(self) -> tuple[str, str]:
        """The schema and name of the table, as the log names it.

        An unqualified table is found in the search path, as queries do.

        Raises:
            FatalError: If there is no such table.
        """
        if self._qualified is None:
            schema, name = database.split_name(self._table)
            quoted = ".".join(
                '"' + part.replace('"', '""') + '"'
                for part in (schema, name)
                if part is not None
            )
            with self.db.connect(f"finding {self._table}") as conn:
                found = conn.execute(
                    sa.text(
                        "SELECT n.nspname, c.relname FROM pg_class c"
                        " JOIN pg_namespace n ON n.oid = c.relnamespace"
                        " WHERE c.oid = to_regclass(:name)"
                    ),
                    {"name": quoted},
                ).first()
            if found is None:
                raise contract.FatalError(f"No table {self._table}")
            self._qualified = (str(found[0]), str(found[1]))
        return self._qualified

    def position(self) -> contract.StateValue:
        with self.db.connect("reading the WAL position") as conn:
            lsn = str(
                conn.execute(
                    sa.text("SELECT pg_current_wal_lsn()::text")
                ).scalar()
            )
        return {"commit": lsn, "index": -1, "safe": lsn}

    def changes(
        self, after: contract.StateValue, until: contract.StateValue = None
    ) -> Iterator[contract.ChangeEvent]:
        start = _pg_position(after)
        limit = _lsn(_pg_position(until).commit) if until is not None else None
        decoder = _PgOutput(self._namespace())
        safe = start.safe
        current_commit = ""
        index = -1

        # A transaction: the changes stream through a server-side cursor.
        with self.db.begin(f"reading the changes of {self._table}") as conn:
            driver = conn.connection.driver_connection
            if conn.dialect.driver != "psycopg" or driver is None:
                raise contract.FatalError(
                    "Postgres logical replication needs the psycopg driver"
                )
            loaders = _Loaders(driver)
            result = conn.execution_options(stream_results=True).execute(
                sa.text(
                    "SELECT data FROM pg_logical_slot_peek_binary_changes("
                    " :slot, CAST(:upto AS pg_lsn), NULL,"
                    " 'proto_version', '1', 'publication_names', :publication)"
                ),
                {
                    "slot": self.slot,
                    "upto": _until_text(until),
                    "publication": self.publication,
                },
            )
            try:
                for (data,) in result:
                    message = decoder.decode(bytes(data))
                    if message is None:
                        continue
                    if message.kind == "B":
                        if current_commit and _lsn(current_commit) > _lsn(safe):
                            safe = current_commit
                        current_commit, index = message.commit, -1
                        if limit is not None and _lsn(current_commit) > limit:
                            return
                        continue
                    index += 1
                    if message.kind != "change":
                        continue
                    if (_lsn(current_commit), index) <= (
                        _lsn(start.commit),
                        start.index,
                    ):
                        continue
                    key = {
                        k: loaders.load(v)
                        for k, v in message.key_values.items()
                    }
                    row = {k: loaders.load(v) for k, v in message.row.items()}
                    if message.unchanged:
                        current = self._current(conn, key)
                        if current is None:
                            continue  # Deleted since: its delete follows.
                        row = {
                            k: current[k] if v is _UNCHANGED else v
                            for k, v in row.items()
                        }
                    yield contract.ChangeEvent(
                        self._table,
                        message.operation,
                        row,
                        tuple(key.values()),
                        {
                            "commit": current_commit,
                            "index": index,
                            "safe": safe,
                        },
                        message.committed_at,
                    )
            finally:
                # Stopping early must not leave the server-side cursor open.
                result.close()

    def confirm(self, position: contract.StateValue) -> None:
        """Advances the slot to the last transaction fully loaded."""
        safe = _pg_position(position).safe
        with self.db.connect(f"advancing the slot {self.slot}") as conn:
            confirmed = conn.execute(
                sa.text(
                    "SELECT confirmed_flush_lsn::text FROM pg_replication_slots"
                    " WHERE slot_name = :slot"
                ),
                {"slot": self.slot},
            ).scalar()
            if confirmed is None or _lsn(safe) <= _lsn(str(confirmed)):
                return
            conn.execute(
                sa.text(
                    "SELECT pg_replication_slot_advance(:slot, CAST(:lsn AS"
                    " pg_lsn))"
                ),
                {"slot": self.slot, "lsn": safe},
            )

    def _current(
        self, conn: sa.Connection, key: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Reads a row as it is now: large values left unchanged by an update
        are not in the log."""
        table = self.db.table(self._table)
        found = (
            conn.execute(
                sa.select(table).where(
                    *(table.c[name] == value for name, value in key.items())
                )
            )
            .mappings()
            .first()
        )
        return None if found is None else dict(found)


def _until_text(until: contract.StateValue) -> str | None:
    return None if until is None else _pg_position(until).commit


@dataclasses.dataclass(frozen=True, slots=True)
class _PgPosition:
    commit: str
    index: int
    safe: str


def _pg_position(position: contract.StateValue) -> _PgPosition:
    """Reads a logical replication position.

    Raises:
        FatalError: If it is not one.
    """
    if not isinstance(position, dict) or not isinstance(
        position.get("commit"), str
    ):
        raise contract.FatalError(
            f"Not a logical replication position: {position!r}"
        )
    return _PgPosition(
        cast(str, position["commit"]),
        int(cast(int, position.get("index", -1))),
        str(position.get("safe", position["commit"])),
    )


def _lsn(text: str) -> int:
    """An LSN `X/Y` as a number."""
    high, _, low = text.partition("/")
    return (int(high, 16) << 32) + int(low, 16)


def _lsn_text(value: int) -> str:
    return f"{value >> 32:X}/{value & 0xFFFFFFFF:X}"


class _Text(str):
    """A value as Postgres wrote it, with its type, to load it."""

    oid: int = 25


@dataclasses.dataclass(slots=True)
class _Message:
    kind: str
    commit: str = ""
    operation: contract.ChangeOperation = contract.ChangeOperation.INSERT
    row: dict[str, Any] = dataclasses.field(default_factory=dict)
    key_values: dict[str, Any] = dataclasses.field(default_factory=dict)
    unchanged: bool = False
    committed_at: datetime.datetime | None = None


@dataclasses.dataclass(frozen=True, slots=True)
class _Relation:
    schema: str
    name: str
    columns: tuple[tuple[str, int, bool], ...]  # name, type oid, in key


class _PgOutput:
    """Decodes the messages of the `pgoutput` plugin, protocol version 1."""

    def __init__(self, table: tuple[str, str]) -> None:
        self._table = table
        self._relations: dict[int, _Relation] = {}
        self._committed_at: datetime.datetime | None = None

    def decode(self, data: bytes) -> _Message | None:
        """Returns a Begin, a change of the table, another change, or None.

        Raises:
            FatalError: If the table is truncated.
        """
        kind = chr(data[0])
        body = memoryview(data)[1:]
        if kind == "B":
            final_lsn, timestamp, _ = struct.unpack_from(">QqI", body)
            self._committed_at = _PG_EPOCH + datetime.timedelta(
                microseconds=timestamp
            )
            return _Message("B", commit=_lsn_text(final_lsn))
        if kind == "R":
            self._relation(body)
            return None
        if kind == "T":
            self._truncate(body)
            return _Message("other")
        if kind not in "IUD":
            return None if kind in "COYM" else _Message("other")

        (oid,) = struct.unpack_from(">I", body)
        relation = self._relations.get(oid)
        if relation is None or (relation.schema, relation.name) != self._table:
            return _Message("other")
        offset = 4
        old: list[Any] | None = None
        new: list[Any] | None = None
        while offset < len(body):
            marker = chr(body[offset])
            values, offset = self._tuple(body, offset + 1, relation)
            if marker == "N":
                new = values
            else:
                old = values
        operation = {
            "I": contract.ChangeOperation.INSERT,
            "U": contract.ChangeOperation.UPDATE,
            "D": contract.ChangeOperation.DELETE,
        }[kind]
        source = new if new is not None else old or []
        names = [name for name, _, _ in relation.columns]
        key_source = old if (kind == "D" and old is not None) else source
        key_values = {
            name: key_source[i]
            for i, (name, _, in_key) in enumerate(relation.columns)
            if in_key
        }
        if kind == "D":
            row = dict(key_values)
        else:
            row = dict(zip(names, source))
        return _Message(
            "change",
            operation=operation,
            row=row,
            key_values=key_values,
            unchanged=any(value is _UNCHANGED for value in row.values()),
            committed_at=self._committed_at,
        )

    def _relation(self, body: memoryview) -> None:
        """Remembers a table's columns: its changes refer to it by oid."""
        (oid,) = struct.unpack_from(">I", body)
        offset = 4
        schema, offset = _string(body, offset)
        name, offset = _string(body, offset)
        offset += 1  # Replica identity.
        (count,) = struct.unpack_from(">H", body, offset)
        offset += 2
        columns = []
        for _ in range(count):
            flags = body[offset]
            column, offset = _string(body, offset + 1)
            type_oid, _ = struct.unpack_from(">Ii", body, offset)
            offset += 8
            columns.append((column, type_oid, bool(flags & 1)))
        self._relations[oid] = _Relation(schema, name, tuple(columns))

    def _truncate(self, body: memoryview) -> None:
        (count,) = struct.unpack_from(">I", body)
        oids = struct.unpack_from(f">{count}I", body, 5)
        for oid in oids:
            relation = self._relations.get(oid)
            if relation and (relation.schema, relation.name) == self._table:
                raise contract.FatalError(
                    f"{'.'.join(self._table)} was truncated: load a new"
                    " snapshot, deleting the state key"
                )

    def _tuple(
        self, body: memoryview, offset: int, relation: _Relation
    ) -> tuple[list[Any], int]:
        """Reads the values of a row: NULL, unchanged, or text; returns them
        and where the row ends."""
        (count,) = struct.unpack_from(">H", body, offset)
        offset += 2
        values: list[Any] = []
        for i in range(count):
            kind = chr(body[offset])
            offset += 1
            if kind == "n":
                values.append(None)
            elif kind == "u":
                values.append(_UNCHANGED)
            else:
                (length,) = struct.unpack_from(">I", body, offset)
                offset += 4
                text = _Text(bytes(body[offset : offset + length]).decode())
                text.oid = relation.columns[i][1]
                values.append(text)
                offset += length
        return values, offset


_UNCHANGED = object()


def _string(body: memoryview, offset: int) -> tuple[str, int]:
    end = bytes(body[offset:]).index(0) + offset
    return bytes(body[offset:end]).decode(), end + 1


class _Loaders:
    """Turns the text of a value into its Python value, by its type."""

    def __init__(self, driver: Any) -> None:
        # Imported here: only Postgres through psycopg needs them.
        # pylint: disable=import-outside-toplevel
        from psycopg import adapt
        from psycopg import pq

        # pylint: enable=import-outside-toplevel

        self._transformer = adapt.Transformer(driver)
        self._format = pq.Format.TEXT

    def load(self, value: Any) -> Any:
        if not isinstance(value, _Text):
            return value
        loader = self._transformer.get_loader(value.oid, self._format)
        return loader.load(value.encode())


def create_replication(
    db: database.Database, table: str, *, slot: str, publication: str
) -> None:
    """Publishes `table` and creates the replication slot, if missing.

    Raises:
        FatalError: If they cannot be created, e.g. `wal_level` is not
            `logical`, or for lack of privileges.
    """
    name = db.engine.dialect.identifier_preparer.format_table(db.table(table))
    with db.connect("creating the logical replication") as conn:
        published = conn.execute(
            sa.text("SELECT 1 FROM pg_publication WHERE pubname = :p"),
            {"p": publication},
        ).first()
        if published is None:
            quoted = db.engine.dialect.identifier_preparer.quote(publication)
            conn.execute(
                sa.text(f"CREATE PUBLICATION {quoted} FOR TABLE {name}")
            )
        exists = conn.execute(
            sa.text("SELECT 1 FROM pg_replication_slots WHERE slot_name = :s"),
            {"s": slot},
        ).first()
        if exists is None:
            conn.execute(
                sa.text(
                    "SELECT pg_create_logical_replication_slot(:s, 'pgoutput')"
                ),
                {"s": slot},
            )


def drop_replication(
    db: database.Database, *, slot: str, publication: str
) -> None:
    """Drops the slot (releasing its WAL) and the publication, if present."""
    with db.connect("dropping the logical replication") as conn:
        conn.execute(
            sa.text(
                "SELECT pg_drop_replication_slot(slot_name)"
                " FROM pg_replication_slots WHERE slot_name = :s"
            ),
            {"s": slot},
        )
        quoted = db.engine.dialect.identifier_preparer.quote(publication)
        conn.execute(sa.text(f"DROP PUBLICATION IF EXISTS {quoted}"))
