"""In-memory fakes of the `core.contract` ports: `core` is tested alone."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator, Mapping, Sequence
import io
from typing import Any, BinaryIO

from core import contract
from shared import utils


class InMemoryObjectStore(contract.IObjectStore):
    """Objects are text; every `put` bumps the object's version."""

    def __init__(self) -> None:
        self.objects: dict[str, tuple[str, int]] = {}
        self.opened: list[str] = []

    def put(self, key: str, content: str) -> None:
        _, version = self.objects.get(key, ("", 0))
        self.objects[key] = (content, version + 1)

    def list(self, prefix: str = "") -> Iterator[contract.ObjectInfo]:
        for key in sorted(self.objects):
            if key.startswith(prefix):
                content, version = self.objects[key]
                yield contract.ObjectInfo(
                    key, len(content), utils.utc_now(), str(version)
                )

    def open(self, obj: contract.ObjectInfo) -> BinaryIO:
        self.opened.append(obj.key)
        return io.BytesIO(self.objects[obj.key][0].encode())


class LinesFormat:
    """One row per non-empty line: `{"value": "<line>"}`."""

    def read(self, stream: BinaryIO) -> Iterator[contract.Row]:
        for line in stream.read().decode().splitlines():
            if line:
                yield {"value": line}


class StaticSource(contract.IAsyncRecordSource[dict[str, Any]]):
    """Yields `records`, and remembers the params and lifecycle calls it got."""

    def __init__(self, records: list[dict[str, Any]]) -> None:
        self._records = records
        self.params: list[dict[str, Any]] = []
        self.events: list[str] = []

    async def asetup(self, ctx: contract.ETLContext) -> None:
        self.events.append("asetup")

    async def ateardown(self, ctx: contract.ETLContext) -> None:
        self.events.append("ateardown")

    # pylint: disable-next=invalid-overridden-method  # An async generator.
    async def records(
        self, params: Mapping[str, Any]
    ) -> AsyncIterator[dict[str, Any]]:
        self.params.append(dict(params))
        for record in self._records:
            yield record


class InMemoryTableReader(contract.ITableReader):
    """Tables are lists of rows; keyset semantics as a database would apply.

    Filters compare with Python operators. Remembers the queries it ran.
    """

    def __init__(self, tables: dict[str, list[dict[str, Any]]]) -> None:
        self.tables = tables
        self.queries: list[contract.TableQuery] = []

    def read(self, query: contract.TableQuery) -> Iterator[contract.Row]:
        self.queries.append(query)
        order = query.order_by
        rows = sorted(
            self.tables[query.table], key=lambda r: [r[c] for c in order]
        )
        for row in rows:
            if not _matches(row, query.filters):
                continue
            first = row[order[0]]
            if query.lower is not None and first < query.lower:
                continue
            if query.upper is not None and first > query.upper:
                continue
            if query.after is not None and tuple(
                row[c] for c in order[: len(query.after)]
            ) <= tuple(query.after):
                continue
            yield dict(row)

    def max_value(
        self,
        table: str,
        column: str,
        filters: Sequence[contract.Condition] = (),
    ) -> Any:
        return max((row[column] for row in self.tables[table]), default=None)

    def count(
        self, table: str, filters: Sequence[contract.Condition] = ()
    ) -> int:
        return sum(1 for row in self.tables[table] if _matches(row, filters))


def _matches(row: contract.Row, filters: Sequence[contract.Condition]) -> bool:
    """Whether `row` meets every condition."""
    return all(_meets(row.get(c.column), c) for c in filters)


def _meets(value: Any, condition: contract.Condition) -> bool:
    expected = condition.value
    match condition.operator:
        case contract.Operator.EQ:
            return bool(value == expected)
        case contract.Operator.NE:
            return bool(value != expected)
        case contract.Operator.LT:
            return bool(value < expected)
        case contract.Operator.LE:
            return bool(value <= expected)
        case contract.Operator.GT:
            return bool(value > expected)
        case contract.Operator.GE:
            return bool(value >= expected)
        case contract.Operator.IN:
            return value in condition.values
        case contract.Operator.NOT_IN:
            return value not in condition.values
        case contract.Operator.IS_NULL:
            return value is None
        case contract.Operator.IS_NOT_NULL:
            return value is not None


class InMemoryTableWriter(contract.ITableWriter):
    """Tables are dicts by key; counts like an upsert with change detection."""

    def __init__(self) -> None:
        self.tables: dict[str, dict[tuple[Any, ...], contract.Row]] = {}
        self.batches: list[tuple[str, int]] = []
        self.deletes: list[tuple[str, int]] = []

    def write(
        self, target: contract.TableTarget, rows: Sequence[contract.Row]
    ) -> contract.WriteResult:
        self.batches.append((target.table, len(rows)))
        table = self.tables.setdefault(target.table, {})
        result = contract.WriteResult()
        for row in rows:
            key = tuple(row[c] for c in target.key) or (len(table),)
            current = table.get(key)
            if target.mode is contract.WriteMode.UPDATE:
                if current is not None and any(
                    current.get(c) != v for c, v in row.items()
                ):
                    current.update(row)
                    result.updated += 1
                else:
                    result.unchanged += 1
                continue
            if current is None:
                table[key] = dict(row)
                result.created += 1
            elif (
                target.mode is contract.WriteMode.INSERT_MISSING
                or current == row
            ):
                result.unchanged += 1
            else:
                table[key] = dict(row)
                result.updated += 1
        return result

    def delete(self, table: str, rows: Sequence[contract.Row]) -> int:
        """Deletes the rows matching any of `rows`, by their columns."""
        stored = self.tables.setdefault(table, {})
        doomed = [
            key
            for key, current in stored.items()
            if any(all(current.get(c) == v for c, v in r.items()) for r in rows)
        ]
        for key in doomed:
            del stored[key]
        self.deletes.append((table, len(rows)))
        return len(doomed)


class InMemorySchemaInspector(contract.ISchemaInspector):
    """A schema described up front; profiles and inclusions computed from
    the rows given."""

    def __init__(
        self,
        tables: list[contract.TableInfo],
        rows: dict[str, list[contract.Row]],
        foreign_keys: list[contract.ForeignKeyInfo] | None = None,
    ) -> None:
        self._tables = tables
        self._rows = rows
        self._foreign_keys = foreign_keys or []
        self.inclusions: list[tuple[str, str, str, str]] = []

    def tables(
        self, schema: str, pattern: str | None = None
    ) -> list[contract.TableInfo]:
        return [
            t
            for t in self._tables
            if t.name.startswith(f"{schema}.")
            and contract.matches(t.bare_name, pattern)
        ]

    def foreign_keys(
        self, schema: str, pattern: str | None = None
    ) -> list[contract.ForeignKeyInfo]:
        return [
            fk
            for fk in self._foreign_keys
            if contract.matches(fk.table.rpartition(".")[2], pattern)
        ]

    def profile(
        self,
        table: str,
        columns: Sequence[str],
        sample_rows: int | None = None,
    ) -> dict[str, contract.ColumnProfile]:
        rows = self._rows[table][:sample_rows]
        profiles = {}
        for column in columns:
            values = [row[column] for row in rows if row[column] is not None]
            profiles[column] = contract.ColumnProfile(
                len(rows), len(values), len(set(values))
            )
        return profiles

    def inclusion(
        self,
        table: str,
        column: str,
        referenced_table: str,
        referenced_column: str,
    ) -> contract.InclusionCheck:
        self.inclusions.append(
            (table, column, referenced_table, referenced_column)
        )
        values = {
            row[column] for row in self._rows[table] if row[column] is not None
        }
        found = {row[referenced_column] for row in self._rows[referenced_table]}
        return contract.InclusionCheck(len(values), len(values - found))


class InMemoryChangeLog(contract.IChangeSource):
    """A change log of one table; positions are the number of changes."""

    def __init__(self, table: str, key: str = "id") -> None:
        self._table = table
        self._key = key
        self.log: list[tuple[contract.ChangeOperation, contract.Row]] = []
        self.confirmed: list[contract.StateValue] = []

    @property
    def table(self) -> str:
        return self._table

    def append(
        self, operation: contract.ChangeOperation, row: contract.Row
    ) -> None:
        self.log.append((operation, dict(row)))

    def position(self) -> contract.StateValue:
        return len(self.log)

    def changes(
        self, after: contract.StateValue, until: contract.StateValue = None
    ) -> Iterator[contract.ChangeEvent]:
        assert isinstance(after, int)
        end = len(self.log) if until is None else int(str(until))
        for index in range(after, end):
            operation, row = self.log[index]
            yield contract.ChangeEvent(
                self._table, operation, row, (row[self._key],), index + 1
            )

    def confirm(self, position: contract.StateValue) -> None:
        self.confirmed.append(position)


class InMemoryTopic(contract.IMessageSource):
    """A topic of one partition; `trimmed` messages expired already."""

    def __init__(self, topic: str = "topic") -> None:
        self._topic = topic
        self.log: list[tuple[bytes | None, bytes | None]] = []
        self.trimmed = 0
        self.confirmed: list[contract.StateValue] = []

    @property
    def topic(self) -> str:
        return self._topic

    def send(self, value: bytes | None, key: bytes | None = None) -> None:
        self.log.append((key, value))

    def beginning(self) -> contract.StateValue:
        return {"0": self.trimmed}

    def end(self) -> contract.StateValue:
        return {"0": len(self.log)}

    def messages(
        self, after: contract.StateValue, until: contract.StateValue = None
    ) -> Iterator[contract.Message]:
        assert isinstance(after, dict) and isinstance(after["0"], int)
        start = after["0"]
        if start < self.trimmed:
            raise contract.FatalError("expired")
        stop = until["0"] if isinstance(until, dict) else len(self.log)
        assert isinstance(stop, int)
        for offset in range(start, stop):
            key, value = self.log[offset]
            yield contract.Message(
                f"{self._topic}[0]@{offset}",
                key,
                value,
                position={"0": offset + 1},
            )

    def covers(
        self, position: contract.StateValue, message: contract.Message
    ) -> bool:
        if not isinstance(position, dict) or not isinstance(
            message.position, dict
        ):
            return False
        reached, after = position["0"], message.position["0"]
        assert isinstance(reached, int) and isinstance(after, int)
        return reached >= after

    def confirm(self, position: contract.StateValue) -> None:
        self.confirmed.append(position)
