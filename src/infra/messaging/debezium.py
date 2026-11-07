"""Debezium change events, read from a topic, as a change source.

The CDC path for databases whose log the ETL cannot read itself (Oracle
first); GoldenGate for Big Data writes the same envelope.

    DebeziumChangeSource(cluster.topic("legacy.CRM.CUSTOMER"), "CRM.CUSTOMER")

Reads the envelope (`op`, `before`, `after`, `source`) and records flattened
by `ExtractNewRecordState` (`__op`, `__deleted`). With the JSON converter's
schemas (`schemas.enable=true`), temporal and decimal values are decoded;
otherwise they stay as sent (then set `decimal.handling.mode=string`). For
Avro, pass a `decode` built on a Schema Registry deserializer.
"""

from __future__ import annotations

import base64
from collections.abc import Callable, Iterator, Mapping, Sequence
import datetime
import decimal
import json
import logging
from typing import Any

from core import contract

type Decoder = Callable[[bytes], Any]

_OPERATIONS = {
    "c": contract.ChangeOperation.INSERT,
    "r": contract.ChangeOperation.INSERT,  # A row of Debezium's snapshot.
    "u": contract.ChangeOperation.UPDATE,
    "d": contract.ChangeOperation.DELETE,
}
_EPOCH = datetime.datetime(1970, 1, 1)
_LOG = logging.getLogger(__name__)
_EPOCH_DATE = datetime.date(1970, 1, 1)


class _NotAChange(ValueError):
    """A message that is not a change event of the table."""


def decode_json(data: bytes) -> Any:
    """Decodes a JSON key or value, with or without its schema."""
    return json.loads(data)


class DebeziumChangeSource(contract.IChangeSource):
    """The changes of a table, from the topic Debezium writes them to.

    Positions are those of the topic. A first run may read the topic from
    its beginning (`ChangeExtractor(from_beginning=True)`): Debezium's own
    snapshot is there, as `r` events. Tombstones are skipped; a truncate
    fails the run, since the target needs a new snapshot.

    A message that is not a change event is set aside, and sent to the dead
    letters once the changes read before and after it are loaded (`flush`,
    before their position is saved): a batch read again after a failure
    sends it once, a dry run never. Messages skipped after the last change
    (a tombstone, a dead letter) are passed once loaded (`skipped_to`), so
    the next run does not read them again.

    Attributes:
        messages: The topic.
    """

    def __init__(
        self,
        messages: contract.IMessageSource,
        table: str,
        *,
        key: Sequence[str] | None = None,
        decode: Decoder = decode_json,
        dead_letters: contract.IDeadLetters | None = None,
    ) -> None:
        """Initializes the source.

        Args:
            messages: The topic of the table.
            table: The table, as the pipelines name it.
            key: The primary key columns; those of the message key, in its
                order, when None.
            decode: Decodes the keys and values of the messages.
            dead_letters: Where messages that are not change events go;
                they fail the run when None. A dead letter is sent again
                only when the run that sent it fails before saving its
                position.
        """
        self.messages = messages
        self._dead_letters = dead_letters
        # Held until their changes are loaded: id -> (message, reason).
        self._letters: dict[str, tuple[contract.Message, str]] = {}
        self._skipped_to: contract.StateValue = None
        self._table = table
        self._key = tuple(key) if key is not None else None
        self._decode = decode

    @property
    def table(self) -> str:
        return self._table

    @property
    def name(self) -> str:
        return f"{type(self).__name__}({self.messages.topic})"

    def close(self) -> None:
        self.messages.close()

    def position(self) -> contract.StateValue:
        return self.messages.end()

    def beginning(self) -> contract.StateValue:
        return self.messages.beginning()

    def changes(
        self, after: contract.StateValue, until: contract.StateValue = None
    ) -> Iterator[contract.ChangeEvent]:
        """See `contract.IChangeSource.changes`.

        Raises:
            FatalError: If the table was truncated, or a message is not a
                Debezium change event and there are no dead letters.
        """
        # What was set aside and not stored is read again from `after`.
        self._letters.clear()
        self._skipped_to = None
        for message in self.messages.messages(after, until):
            # None for the tombstone after a delete, kept for compaction.
            event = None if message.value is None else self._event(message)
            if event is None:
                self._skipped_to = message.position
                continue
            self._skipped_to = None
            yield event

    def skipped_to(self) -> contract.StateValue:
        return self._skipped_to

    def flush(self, position: contract.StateValue) -> None:
        """Sends the dead letters `position` is past, in the order read.

        Raises:
            RetryableError: If the dead letters cannot be reached.
            FatalError: If they refuse a message.
        """
        if self._dead_letters is None:
            return  # Nothing was set aside: what is no change failed.
        due = [
            where
            for where, (message, _) in self._letters.items()
            if self.messages.covers(position, message)
        ]
        for where in due:
            message, reason = self._letters[where]
            _LOG.warning("%s: sent to the dead letters", reason)
            self._dead_letters.send(message, reason)
            del self._letters[where]

    def confirm(self, position: contract.StateValue) -> None:
        self.messages.confirm(position)

    def _event(self, message: contract.Message) -> contract.ChangeEvent | None:
        """Turns a message into a change; None to skip it.

        Raises:
            FatalError: If the table was truncated, or the message is not a
                change event and there are no dead letters.
        """
        try:
            return self._change(message, message.id)
        except _NotAChange as exc:
            if self._dead_letters is None:
                raise contract.FatalError(str(exc)) from exc
            self._letters[message.id] = (message, str(exc))
            return None

    def _change(
        self, message: contract.Message, where: str
    ) -> contract.ChangeEvent | None:
        """Decodes a message.

        Raises:
            _NotAChange: If it is not a change event.
        """
        try:
            value = self._decode(message.value or b"")
            payload, schema = _unwrap(value)
            if not isinstance(payload, dict):
                raise ValueError("not an object")
            if "op" in payload:
                operation, row, committed = _envelope(payload, schema)
            else:
                operation, row = _flattened(payload)
                committed = payload.get("__source_ts_ms")
        except (ValueError, TypeError, KeyError, decimal.InvalidOperation) as e:
            raise _NotAChange(
                f"{where}: not a Debezium change event: {e}"
            ) from e
        if operation is None:
            return None
        return contract.ChangeEvent(
            table=self._table,
            operation=operation,
            row=row,
            key=self._key_of(message, row, where),
            position=message.position,
            committed_at=(
                datetime.datetime.fromtimestamp(committed / 1000, datetime.UTC)
                if isinstance(committed, int)
                else None
            ),
        )

    def _key_of(
        self, message: contract.Message, row: Mapping[str, Any], where: str
    ) -> tuple[Any, ...]:
        """The primary key of the row: its key columns, or the message key."""
        if self._key is not None:
            try:
                return tuple(row[column] for column in self._key)
            except KeyError as e:
                raise _NotAChange(f"{where}: key column {e} missing") from None
        if message.key is None:
            raise _NotAChange(
                f"{where}: the message has no key; give the key columns"
            )
        key, _ = _unwrap(self._decode(message.key))
        return tuple(key.values()) if isinstance(key, dict) else (key,)


class DebeziumChangeLog(contract.IChangeLog):
    """The change log of a database, as Debezium writes it to Kafka.

    The topic of a table is `<prefix>.<table>`, as Debezium names them
    (`legacy.CRM.CUSTOMER` for table `CRM.CUSTOMER` of the connector whose
    `topic.prefix` is `legacy`), unless `topics` says otherwise.
    """

    def __init__(
        self,
        messages: contract.IMessageLog,
        *,
        prefix: str | None = None,
        topics: Mapping[str, str] | None = None,
        decode: Decoder = decode_json,
        dead_letters: contract.IDeadLetters | None = None,
    ) -> None:
        self._messages = messages
        self._prefix = prefix
        self._topics = dict(topics or {})
        self._decode = decode
        self._dead_letters = dead_letters

    def source(self, table: str) -> contract.IChangeSource:
        topic = self._topics.get(table)
        if topic is None:
            if self._prefix is None:
                raise contract.PipelineConfigError(
                    f"No topic for the changes of {table}: set a topic prefix"
                    " or the topic of the table"
                )
            topic = f"{self._prefix}.{table}"
        return DebeziumChangeSource(
            self._messages.topic(topic),
            table,
            decode=self._decode,
            dead_letters=self._dead_letters,
        )


def _unwrap(value: Any) -> tuple[Any, Any]:
    """The payload and schema of a value of the JSON converter."""
    if isinstance(value, dict) and value.keys() == {"schema", "payload"}:
        return value["payload"], value["schema"]
    return value, None


def _envelope(
    payload: dict[str, Any], schema: Any
) -> tuple[contract.ChangeOperation | None, dict[str, Any], Any]:
    """The operation, row and commit time of a change envelope."""
    op = payload["op"]
    if op == "t":
        raise contract.FatalError(
            "the table was truncated: take a new snapshot of the target"
        )
    operation = _OPERATIONS.get(op)
    if operation is None:
        return None, {}, None  # A heartbeat or a message: nothing to load.
    side = "before" if operation is contract.ChangeOperation.DELETE else "after"
    row = payload[side]
    if not isinstance(row, dict):
        raise ValueError(f"no {side!r} row")
    source = payload.get("source") or {}
    return (
        operation,
        _typed(row, _fields(schema, side)),
        source.get("ts_ms", payload.get("ts_ms")),
    )


def _flattened(
    payload: dict[str, Any],
) -> tuple[contract.ChangeOperation | None, dict[str, Any]]:
    """The operation and row of a record of `ExtractNewRecordState`."""
    row = {k: v for k, v in payload.items() if not k.startswith("__")}
    if str(payload.get("__deleted", "")).lower() == "true":
        return contract.ChangeOperation.DELETE, row
    return _OPERATIONS.get(payload.get("__op", "c")), row


def _fields(schema: Any, side: str) -> dict[str, dict[str, Any]]:
    """The schemas of the columns of the `before` or `after` row."""
    if not isinstance(schema, dict):
        return {}
    for field in schema.get("fields", ()):
        if field.get("field") == side:
            return {
                column["field"]: column for column in field.get("fields", ())
            }
    return {}


def _typed(
    row: dict[str, Any], fields: Mapping[str, dict[str, Any]]
) -> dict[str, Any]:
    """Decodes the values whose schema has a logical type."""
    if not fields:
        return row
    return {
        column: _logical(value, fields.get(column))
        for column, value in row.items()
    }


def _logical(value: Any, field: dict[str, Any] | None) -> Any:
    """Decodes a temporal or decimal value of the JSON converter."""
    if value is None or field is None:
        return value
    match field.get("name"):
        case "io.debezium.time.Date" | "org.apache.kafka.connect.data.Date":
            return _EPOCH_DATE + datetime.timedelta(days=value)
        case (
            "io.debezium.time.Timestamp"
            | "org.apache.kafka.connect.data.Timestamp"
        ):
            return _EPOCH + datetime.timedelta(milliseconds=value)
        case "io.debezium.time.MicroTimestamp":
            return _EPOCH + datetime.timedelta(microseconds=value)
        case "io.debezium.time.NanoTimestamp":
            return _EPOCH + datetime.timedelta(microseconds=value // 1000)
        case "io.debezium.time.ZonedTimestamp":
            return datetime.datetime.fromisoformat(value)
        case "org.apache.kafka.connect.data.Decimal":
            scale = int(field.get("parameters", {}).get("scale", 0))
            return _decimal(base64.b64decode(value), scale)
        case "io.debezium.data.VariableScaleDecimal":
            return _decimal(base64.b64decode(value["value"]), value["scale"])
        case _:
            return value


def _decimal(unscaled: bytes, scale: int) -> decimal.Decimal:
    """A decimal sent as its unscaled two's complement big-endian bytes."""
    number = int.from_bytes(unscaled, "big", signed=True)
    return decimal.Decimal(number).scaleb(-scale)
