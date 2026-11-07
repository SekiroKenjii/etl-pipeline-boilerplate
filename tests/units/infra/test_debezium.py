"""Debezium change events, decoded from an in-memory topic."""

from __future__ import annotations

import base64
import datetime
import decimal
import json
from typing import Any

import pytest

from core import contract
from core import operators
from core import pipeline
from core import state as core_state
from infra import messaging
from tests.factories import fake_ports
from tests.factories import fakes

# The columns of the rows, as the JSON converter describes them.
_COLUMNS = [
    {"field": "ID", "type": "int32"},
    {"field": "NAME", "type": "string", "optional": True},
    {
        "field": "AMOUNT",
        "type": "bytes",
        "name": "org.apache.kafka.connect.data.Decimal",
        "parameters": {"scale": "2"},
    },
    {"field": "BORN", "type": "int32", "name": "io.debezium.time.Date"},
    {
        "field": "SEEN",
        "type": "int64",
        "name": "io.debezium.time.MicroTimestamp",
    },
    {
        "field": "AT",
        "type": "string",
        "name": "io.debezium.time.ZonedTimestamp",
    },
]
_SCHEMA = {
    "type": "struct",
    "fields": [
        {"field": "before", "type": "struct", "fields": _COLUMNS},
        {"field": "after", "type": "struct", "fields": _COLUMNS},
        {"field": "source", "type": "struct", "fields": []},
        {"field": "op", "type": "string"},
    ],
}


def _row(id_: int, name: str) -> dict[str, Any]:
    return {
        "ID": id_,
        "NAME": name,
        # 1234.56: 123456 unscaled, big-endian.
        "AMOUNT": base64.b64encode((123456).to_bytes(3, "big")).decode(),
        "BORN": 19723,  # 2024-01-01.
        "SEEN": 1_704_067_200_123_456,
        "AT": "2024-01-01T07:00:00+07:00",
    }


def _envelope(
    op: str,
    before: dict[str, Any] | None,
    after: dict[str, Any] | None,
    *,
    schema: bool = True,
) -> bytes:
    payload = {
        "op": op,
        "before": before,
        "after": after,
        "source": {
            "connector": "oracle",
            "scn": "123",
            "ts_ms": 1_704_067_200_000,
        },
        "ts_ms": 1_704_067_201_000,
    }
    value = {"schema": _SCHEMA, "payload": payload} if schema else payload
    return json.dumps(value).encode()


def _key(id_: int) -> bytes:
    return json.dumps({"schema": {}, "payload": {"ID": id_}}).encode()


def _changes(topic: fake_ports.InMemoryTopic) -> list[contract.ChangeEvent]:
    source = messaging.DebeziumChangeSource(topic, "CRM.CUSTOMER")
    return list(source.changes(source.beginning(), source.position()))


class TestDebeziumChangeSource:

    def test_envelopes_with_their_schema_become_typed_changes(self) -> None:
        topic = fake_ports.InMemoryTopic()
        topic.send(_envelope("r", None, _row(1, "An")), _key(1))
        topic.send(_envelope("u", _row(1, "An"), _row(1, "Anh")), _key(1))
        topic.send(_envelope("d", _row(1, "Anh"), None), _key(1))
        topic.send(None, _key(1))  # The tombstone.

        changes = _changes(topic)

        assert [(c.operation, c.key, c.row["NAME"]) for c in changes] == [
            (contract.ChangeOperation.INSERT, (1,), "An"),
            (contract.ChangeOperation.UPDATE, (1,), "Anh"),
            (contract.ChangeOperation.DELETE, (1,), "Anh"),
        ]
        row = changes[0].row
        assert row["AMOUNT"] == decimal.Decimal("1234.56")
        assert row["BORN"] == datetime.date(2024, 1, 1)
        assert row["SEEN"] == datetime.datetime(2024, 1, 1, 0, 0, 0, 123456)
        assert row["AT"] == datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)
        assert changes[0].committed_at == datetime.datetime(
            2024, 1, 1, tzinfo=datetime.UTC
        )
        assert [c.position for c in changes] == [{"0": 1}, {"0": 2}, {"0": 3}]

    def test_without_schemas_values_stay_as_sent(self) -> None:
        topic = fake_ports.InMemoryTopic()
        topic.send(
            _envelope("c", None, {"ID": 7, "AMOUNT": "1.50"}, schema=False),
            b'{"ID": 7}',
        )

        (change,) = _changes(topic)

        assert change.row == {"ID": 7, "AMOUNT": "1.50"}
        assert change.key == (7,)

    def test_flattened_records(self) -> None:
        topic = fake_ports.InMemoryTopic()
        topic.send(b'{"ID": 1, "NAME": "An", "__op": "c"}', b'{"ID": 1}')
        topic.send(
            b'{"ID": 1, "NAME": "An", "__op": "d", "__deleted": "true"}',
            b'{"ID": 1}',
        )

        changes = _changes(topic)

        assert [(c.operation.value, c.row) for c in changes] == [
            ("insert", {"ID": 1, "NAME": "An"}),
            ("delete", {"ID": 1, "NAME": "An"}),
        ]

    def test_key_columns_given(self) -> None:
        topic = fake_ports.InMemoryTopic()
        topic.send(_envelope("c", None, {"A": 1, "B": 2}, schema=False))
        source = messaging.DebeziumChangeSource(topic, "T", key=["B", "A"])

        (change,) = source.changes(source.beginning())

        assert change.key == (2, 1)

    @pytest.mark.parametrize(
        ("value", "message"),
        [
            (_envelope("t", None, None), "truncated"),
            (b"not json", "not a Debezium change event"),
            (b'{"op": "c", "after": null}', "not a Debezium change event"),
        ],
    )
    def test_what_cannot_be_loaded_fails(
        self, value: bytes, message: str
    ) -> None:
        topic = fake_ports.InMemoryTopic()
        topic.send(value, _key(1))

        with pytest.raises(contract.FatalError, match=message):
            _changes(topic)

    def test_a_message_without_key_needs_key_columns(self) -> None:
        topic = fake_ports.InMemoryTopic()
        topic.send(_envelope("c", None, {"ID": 1}, schema=False))

        with pytest.raises(contract.FatalError, match="give the key columns"):
            _changes(topic)

    def test_confirm_commits_the_topic(self) -> None:
        topic = fake_ports.InMemoryTopic()
        messaging.DebeziumChangeSource(topic, "T").confirm({"0": 3})

        assert topic.confirmed == [{"0": 3}]


class _Topics(contract.IMessageLog):

    def __init__(self) -> None:
        self.asked: list[str] = []

    def topic(self, name: str) -> contract.IMessageSource:
        self.asked.append(name)
        return fake_ports.InMemoryTopic(name)


def test_the_change_log_names_topics_as_debezium_does() -> None:
    topics = _Topics()
    log = messaging.DebeziumChangeLog(
        topics, prefix="legacy", topics={"ORDERS": "legacy.CRM.ORD_MASTER"}
    )

    assert log.source("CRM.CUSTOMER").table == "CRM.CUSTOMER"
    log.source("ORDERS")
    assert topics.asked == ["legacy.CRM.CUSTOMER", "legacy.CRM.ORD_MASTER"]
    unprefixed = messaging.DebeziumChangeLog(topics)
    with pytest.raises(contract.PipelineConfigError):
        unprefixed.source("T")


def test_debezium_snapshot_and_changes_from_the_beginning() -> None:
    """Debezium's own snapshot is in the topic: no snapshot of ours."""
    topic = fake_ports.InMemoryTopic()
    topic.send(_envelope("r", None, {"ID": 1}, schema=False), b'{"ID": 1}')
    topic.send(_envelope("r", None, {"ID": 2}, schema=False), b'{"ID": 2}')
    state = core_state.InMemoryStateStore()

    def run() -> list[tuple[str, Any]]:
        loader = fakes.CollectingLoader[contract.ChangeEvent]()
        pipeline.ETLJob(
            operators.ChangeExtractor(
                messaging.DebeziumChangeSource(topic, "T"),
                state=state,
                from_beginning=True,
            ),
            operators.Passthrough[contract.ChangeEvent](),
            loader,
        )(fakes.make_context("sync"), {})
        return [(e.operation.value, e.key) for e in loader.rows]

    assert run() == [("insert", (1,)), ("insert", (2,))]
    topic.send(_envelope("d", {"ID": 1}, None, schema=False), b'{"ID": 1}')
    assert run() == [("delete", (1,))]
    assert topic.confirmed[-1] == {"0": 3}
    assert run() == []


def test_snapshot_and_from_beginning_exclude_each_other() -> None:
    changes = fake_ports.InMemoryChangeLog("t")
    state = core_state.InMemoryStateStore()
    snapshot = operators.Snapshot(
        fake_ports.InMemoryTableReader({"t": []}),
        contract.TableQuery("t", order_by=("id",)),
    )

    with pytest.raises(ValueError, match="exclude"):
        operators.ChangeExtractor(
            changes, state=state, snapshot=snapshot, from_beginning=True
        )


def test_most_change_logs_have_no_beginning() -> None:
    changes = fake_ports.InMemoryChangeLog("t")

    with pytest.raises(contract.PipelineConfigError, match="no beginning"):
        changes.beginning()


class _DeadLetters(contract.IDeadLetters):
    """Keeps the offsets sent; the first `failures` sends fail."""

    def __init__(self, failures: int = 0) -> None:
        self.sent: list[tuple[int, str]] = []
        self._failures = failures

    def send(self, message: contract.Message, reason: str) -> None:
        if self._failures:
            self._failures -= 1
            raise contract.RetryableError("dead letters down")
        self.sent.append((int(message.id.rpartition("@")[2]), reason))


def test_what_is_not_a_change_goes_to_the_dead_letters() -> None:
    topic = fake_ports.InMemoryTopic()
    topic.send(_envelope("c", None, {"ID": 1}, schema=False), b'{"ID": 1}')
    topic.send(b"not json", b'{"ID": 2}')
    topic.send(_envelope("c", None, {"ID": 3}, schema=False))  # No key.
    topic.send(_envelope("c", None, {"ID": 4}, schema=False), b'{"ID": 4}')
    dead = _DeadLetters()
    source = messaging.DebeziumChangeSource(topic, "T", dead_letters=dead)

    changes = list(source.changes(source.beginning()))
    assert dead.sent == []  # Set aside until their changes are loaded.
    source.flush(changes[-1].position)

    assert [c.key for c in changes] == [(1,), (4,)]
    assert [offset for offset, _ in dead.sent] == [1, 2]
    assert "not a Debezium change event" in dead.sent[0][1]
    assert "has no key" in dead.sent[1][1]


def test_a_truncate_still_fails_with_dead_letters() -> None:
    topic = fake_ports.InMemoryTopic()
    topic.send(_envelope("t", None, None), _key(1))
    source = messaging.DebeziumChangeSource(
        topic, "T", dead_letters=_DeadLetters()
    )
    changes = source.changes(source.beginning())

    with pytest.raises(contract.FatalError, match="truncated"):
        list(changes)


def _sync(
    topic: fake_ports.InMemoryTopic,
    state: contract.IStateStore,
    dead: contract.IDeadLetters,
    loader: fakes.CollectingLoader[contract.ChangeEvent],
    *,
    batch_size: int = 1000,
) -> contract.ETLResult:
    """Runs a sync of the topic, its Debezium snapshot first."""
    return pipeline.ETLJob(
        operators.ChangeExtractor(
            messaging.DebeziumChangeSource(topic, "T", dead_letters=dead),
            state=state,
            from_beginning=True,
        ),
        operators.Passthrough[contract.ChangeEvent](),
        loader,
        batch_size=batch_size,
    )(fakes.make_context("sync"), {})


def _saved(state: contract.IStateStore) -> contract.StateValue:
    saved = state.get("sync", "changes:T")
    return saved.get("position") if isinstance(saved, dict) else None


class TestDeadLettersGoOutOnce:
    """B5: a dead letter is sent once its batch is loaded, and only then."""

    def test_a_retried_batch_sends_its_dead_letters_once(self) -> None:
        topic = fake_ports.InMemoryTopic()
        topic.send(_envelope("c", None, {"ID": 1}, schema=False), b'{"ID": 1}')
        topic.send(b"not json", b'{"ID": 2}')
        topic.send(_envelope("c", None, {"ID": 3}, schema=False), b'{"ID": 3}')
        topic.send(b"not json either", b'{"ID": 4}')
        topic.send(_envelope("c", None, {"ID": 5}, schema=False), b'{"ID": 5}')
        state = core_state.InMemoryStateStore()
        dead = _DeadLetters()
        loader = fakes.CollectingLoader[contract.ChangeEvent](fail_on_batch=1)

        with pytest.raises(contract.RetryableError):
            _sync(topic, state, dead, loader)
        assert dead.sent == []  # Not loaded: nothing goes out.
        _sync(topic, state, dead, loader)  # The retry.

        assert [offset for offset, _ in dead.sent] == [1, 3]
        assert [e.key for e in loader.rows] == [(1,), (3,), (5,)]

    def test_each_batch_sends_the_dead_letters_it_covers(self) -> None:
        topic = fake_ports.InMemoryTopic()
        for i in range(6):
            topic.send(
                _envelope("c", None, {"ID": i}, schema=False)
                if i % 2
                else b"x",
                _key(i),
            )
        state = core_state.InMemoryStateStore()
        dead = _DeadLetters()
        # Batches of 2 changes: (1, 3) then (5,); the second fails once.
        loader = fakes.CollectingLoader[contract.ChangeEvent](fail_on_batch=2)

        with pytest.raises(contract.RetryableError):
            _sync(topic, state, dead, loader, batch_size=2)
        assert [offset for offset, _ in dead.sent] == [0, 2]
        _sync(topic, state, dead, loader, batch_size=2)

        assert [offset for offset, _ in dead.sent] == [0, 2, 4]

    def test_a_dead_letter_last_on_the_topic_is_sent_once(self) -> None:
        topic = fake_ports.InMemoryTopic()
        topic.send(_envelope("c", None, {"ID": 1}, schema=False), b'{"ID": 1}')
        topic.send(b"not json", b'{"ID": 2}')
        topic.send(None, b'{"ID": 3}')  # A tombstone, last.
        state = core_state.InMemoryStateStore()
        dead = _DeadLetters()
        loader = fakes.CollectingLoader[contract.ChangeEvent]()

        _sync(topic, state, dead, loader)
        _sync(topic, state, dead, loader)

        assert [offset for offset, _ in dead.sent] == [1]
        assert _saved(state) == {"0": 3}
        assert topic.confirmed[-1] == {"0": 3}

    def test_a_run_of_dead_letters_only_moves_past_them(self) -> None:
        topic = fake_ports.InMemoryTopic()
        topic.send(_envelope("c", None, {"ID": 1}, schema=False), b'{"ID": 1}')
        state = core_state.InMemoryStateStore()
        dead = _DeadLetters()
        loader = fakes.CollectingLoader[contract.ChangeEvent]()
        _sync(topic, state, dead, loader)
        topic.send(b"not json", b'{"ID": 2}')

        _sync(topic, state, dead, loader)  # Reads the dead letter alone.
        _sync(topic, state, dead, loader)

        assert [offset for offset, _ in dead.sent] == [1]
        assert _saved(state) == {"0": 2}

    def test_a_dry_run_sends_no_dead_letter(self) -> None:
        topic = fake_ports.InMemoryTopic()
        topic.send(_envelope("c", None, {"ID": 1}, schema=False), b'{"ID": 1}')
        topic.send(b"not json", b'{"ID": 2}')
        dead = _DeadLetters()

        pipeline.ETLJob(
            operators.ChangeExtractor(
                messaging.DebeziumChangeSource(topic, "T", dead_letters=dead),
                state=core_state.InMemoryStateStore(),
                from_beginning=True,
            ),
            operators.Passthrough[contract.ChangeEvent](),
            fakes.CollectingLoader[contract.ChangeEvent](),
        )(fakes.make_context("sync", dry_run=True), {})

        assert dead.sent == []

    def test_the_position_waits_for_the_dead_letters(self) -> None:
        """A dead letter that cannot be sent keeps the position before it."""
        topic = fake_ports.InMemoryTopic()
        topic.send(_envelope("c", None, {"ID": 1}, schema=False), b'{"ID": 1}')
        topic.send(b"not json", b'{"ID": 2}')
        topic.send(_envelope("c", None, {"ID": 3}, schema=False), b'{"ID": 3}')
        state = core_state.InMemoryStateStore()
        dead = _DeadLetters(failures=1)
        loader = fakes.CollectingLoader[contract.ChangeEvent]()

        with pytest.raises(contract.RetryableError, match="dead letters down"):
            _sync(topic, state, dead, loader)
        assert _saved(state) == {"0": 0}
        assert topic.confirmed == []
        _sync(topic, state, dead, loader)

        assert [offset for offset, _ in dead.sent] == [1]
        assert _saved(state) == {"0": 3}


class _Snapshot(contract.ITableReader):
    """A table of rows; a dead letter reaches the topic while it is read."""

    def __init__(self, topic: fake_ports.InMemoryTopic) -> None:
        self._topic = topic

    def read(self, query: contract.TableQuery) -> Any:
        self._topic.send(b"not json", b'{"ID": 9}')
        yield from ({"ID": i} for i in range(3))

    def max_value(self, table: str, column: str, filters: Any = ()) -> Any:
        raise NotImplementedError

    def count(self, table: str, filters: Any = ()) -> int:
        raise NotImplementedError


def test_moving_past_dead_letters_waits_for_the_snapshot() -> None:
    """The snapshot's rows are loaded before the position moves past it."""
    topic = fake_ports.InMemoryTopic()
    state = core_state.InMemoryStateStore()
    dead = _DeadLetters()
    loader = fakes.CollectingLoader[contract.ChangeEvent](fail_on_batch=1)

    def sync() -> None:
        pipeline.ETLJob(
            operators.ChangeExtractor(
                messaging.DebeziumChangeSource(topic, "T", dead_letters=dead),
                state=state,
                snapshot=operators.Snapshot(
                    _Snapshot(topic), contract.TableQuery("T", ("ID",))
                ),
            ),
            operators.Passthrough[contract.ChangeEvent](),
            loader,
        )(fakes.make_context("sync"), {})

    with pytest.raises(contract.RetryableError):
        sync()
    assert (_saved(state), dead.sent) == (None, [])
    topic.log.clear()  # The snapshot is read again from an empty topic.
    sync()

    assert [e.key for e in loader.rows] == [(0,), (1,), (2,)]
    assert [offset for offset, _ in dead.sent] == [0]
    assert _saved(state) == {"0": 1}
