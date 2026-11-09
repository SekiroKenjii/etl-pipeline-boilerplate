"""Kafka topics and Debezium changes, on a real broker.

Runs when `ETL_TEST_KAFKA_URL` holds the bootstrap servers (`make db-up`).
"""

from __future__ import annotations

from collections.abc import Callable, Generator
import json
import os
import pathlib
from typing import Any
import uuid

import confluent_kafka
from confluent_kafka import admin
import pytest
import sqlalchemy as sa

from app import bootstrap
from core import contract
from core import operators
from core import pipeline
from core import runtime
from core import state as core_state
from infra import messaging
from infra.messaging import kafka
from modules.crm import demo
from tests.factories import fakes

ROOT = pathlib.Path(__file__).resolve().parents[2]
MAPPINGS = ROOT / "mappings" / "compiled" / "crm.json"

type NewTopic = Callable[..., str]


@pytest.fixture(scope="module")
def servers() -> str:
    url = os.environ.get("ETL_TEST_KAFKA_URL")
    if not url:
        pytest.skip("set ETL_TEST_KAFKA_URL to run the Kafka tests")
    return url


@pytest.fixture
def new_topic(servers: str) -> Generator[NewTopic]:
    client = admin.AdminClient({"bootstrap.servers": servers})
    created: list[str] = []

    def create(partitions: int = 3, name: str | None = None) -> str:
        topic = name or f"etl-test-{uuid.uuid4().hex[:8]}"
        for future in client.create_topics([
            admin.NewTopic(  # pyright: ignore[reportPrivateImportUsage]
                topic, partitions, 1
            )
        ]).values():
            future.result(timeout=30)
        created.append(topic)
        return topic

    yield create
    if created:
        for future in client.delete_topics(created).values():
            future.result(timeout=30)


def _cluster(servers: str, group: str = "etl-test") -> kafka.KafkaCluster:
    return kafka.KafkaCluster(
        {"bootstrap.servers": servers}, group_id=group, timeout_seconds=15
    )


def _send(
    servers: str, topic: str, values: list[bytes], partition: int | None = None
) -> None:
    producer = _cluster(servers).producer()
    for index, value in enumerate(values):
        producer.send(
            topic, value, key=str(index).encode(), partition=partition
        )
    producer.flush()


def _committed(servers: str, group: str, topic: str) -> dict[str, int]:
    consumer = confluent_kafka.Consumer(
        {"bootstrap.servers": servers, "group.id": group}
    )
    try:
        partitions = consumer.committed(
            [confluent_kafka.TopicPartition(topic, p) for p in range(3)],
            timeout=15,
        )
    finally:
        consumer.close()
    return {str(p.partition): p.offset for p in partitions if p.offset >= 0}


class TestKafkaTopic:

    def test_reads_every_partition_to_its_end_then_resumes(
        self, servers: str, new_topic: NewTopic
    ) -> None:
        name = new_topic()
        _send(servers, name, [f"m{i}".encode() for i in range(30)])
        topic = _cluster(servers).topic(name)
        try:
            first = list(topic.messages(topic.beginning()))
            _send(servers, name, [b"late"], partition=1)
            second = list(topic.messages(first[-1].position))
            third = list(topic.messages(second[-1].position))
        finally:
            topic.close()

        assert sorted(m.value or b"" for m in first) == sorted(
            f"m{i}".encode() for i in range(30)
        )
        assert first[-1].position == {
            str(p): n for p, n in _counts(first).items()
        }
        assert [(_where(m)[0], m.value) for m in second] == [(1, b"late")]
        assert third == []
        offsets = {_where(m) for m in first}
        assert len(offsets) == 30  # Each message once.

    def test_a_run_stops_at_the_end_it_started_with(
        self, servers: str, new_topic: NewTopic
    ) -> None:
        name = new_topic(partitions=1)
        _send(servers, name, [b"a", b"b"])
        topic = _cluster(servers).topic(name)
        try:
            end = topic.end()
            _send(servers, name, [b"c"])
            read = [m.value for m in topic.messages(topic.beginning(), end)]
        finally:
            topic.close()

        assert read == [b"a", b"b"]

    def test_confirm_commits_the_consumer_group(
        self, servers: str, new_topic: NewTopic
    ) -> None:
        name = new_topic()
        _send(servers, name, [b"a", b"b", b"c", b"d"])
        group = f"etl-test-{uuid.uuid4().hex[:6]}"
        topic = _cluster(servers, group).topic(name)
        try:
            messages = list(topic.messages(topic.beginning()))
            topic.confirm(messages[-1].position)
        finally:
            topic.close()

        assert _committed(servers, group, name) == messages[-1].position

    def test_transactions_leave_no_gap_unread(
        self, servers: str, new_topic: NewTopic
    ) -> None:
        name = new_topic(partitions=1)
        producer = confluent_kafka.Producer({
            "bootstrap.servers": servers,
            "transactional.id": f"etl-test-{uuid.uuid4().hex[:6]}",
        })
        producer.init_transactions(30)
        for value, commit in ((b"kept", True), (b"aborted", False)):
            producer.begin_transaction()
            producer.produce(name, value)
            producer.flush(30)
            if commit:
                producer.commit_transaction(30)
            else:
                producer.abort_transaction(30)
        topic = _cluster(servers).topic(name)
        try:
            read = [m.value for m in topic.messages(topic.beginning())]
        finally:
            topic.close()

        # The end counts the markers and the aborted message: the run ends.
        assert read == [b"kept"]

    def test_expired_messages_fail_the_run(
        self, servers: str, new_topic: NewTopic
    ) -> None:
        name = new_topic(partitions=1)
        _send(servers, name, [b"a", b"b", b"c"])
        client = admin.AdminClient({"bootstrap.servers": servers})
        for future in client.delete_records(
            [confluent_kafka.TopicPartition(name, 0, 2)]
        ).values():
            future.result(timeout=30)
        topic = _cluster(servers).topic(name)
        try:
            with pytest.raises(contract.FatalError, match="expired"):
                list(topic.messages({"0": 0}))
            assert [m.value for m in topic.messages(topic.beginning())] == [
                b"c"
            ]
        finally:
            topic.close()

    def test_a_missing_topic_fails(self, servers: str) -> None:
        topic = _cluster(servers).topic(f"etl-missing-{uuid.uuid4().hex[:6]}")
        try:
            with pytest.raises(contract.FatalError, match="does not exist"):
                topic.end()
        finally:
            topic.close()

    def test_dead_letters_keep_the_message_and_why(
        self, servers: str, new_topic: NewTopic
    ) -> None:
        name, dead = new_topic(partitions=1), new_topic(partitions=1)
        _send(servers, name, [b"not json"])
        cluster = _cluster(servers)
        topic = cluster.topic(name)
        source = messaging.DebeziumChangeSource(
            topic, "T", dead_letters=cluster.dead_letters(dead)
        )
        letters = cluster.topic(dead)
        try:
            changes = list(source.changes(source.beginning()))
            assert letters.end() == {"0": 0}  # Sent once loaded.
            source.flush(source.skipped_to())
            (letter,) = list(letters.messages(letters.beginning()))
        finally:
            topic.close()
            letters.close()

        assert changes == []
        assert (letter.key, letter.value) == (b"0", b"not json")
        assert letter.headers["etl.topic"] == name.encode()
        assert letter.headers["etl.offset"] == b"0"
        assert b"not a Debezium change event" in letter.headers["etl.error"]

    def test_ping(self, servers: str) -> None:
        _cluster(servers).ping()


class TestDebeziumDeadLetters:
    """B5: each message that is no change goes to the dead letters once."""

    def _sync(
        self,
        servers: str,
        name: str,
        dead: str,
        state: contract.IStateStore,
        loader: fakes.CollectingLoader[contract.ChangeEvent],
        group: str = "etl-test",
    ) -> None:
        cluster = _cluster(servers, group)
        topic = cluster.topic(name)
        try:
            pipeline.ETLJob(
                operators.ChangeExtractor(
                    messaging.DebeziumChangeSource(
                        topic, "T", dead_letters=cluster.dead_letters(dead)
                    ),
                    state=state,
                    from_beginning=True,
                ),
                operators.Passthrough[contract.ChangeEvent](),
                loader,
            )(fakes.make_context("sync"), {})
        finally:
            topic.close()

    def _letters(self, servers: str, dead: str) -> list[tuple[int, int]]:
        """Where the dead letters come from: partition and offset."""
        letters = _cluster(servers).topic(dead)
        try:
            return sorted(
                (int(m.headers["etl.partition"]), int(m.headers["etl.offset"]))
                for m in letters.messages(letters.beginning())
            )
        finally:
            letters.close()

    def test_a_retried_batch_dead_letters_each_message_once(
        self, servers: str, new_topic: NewTopic
    ) -> None:
        name, dead = new_topic(partitions=2), new_topic(partitions=1)
        change = _envelope("c", None, {"ID": 1})
        _send(servers, name, [change, b"not json", change], partition=0)
        _send(servers, name, [b"[]", change], partition=1)
        state = core_state.InMemoryStateStore()
        loader = fakes.CollectingLoader[contract.ChangeEvent](fail_on_batch=1)

        with pytest.raises(contract.RetryableError):
            self._sync(servers, name, dead, state, loader)
        assert self._letters(servers, dead) == []
        self._sync(servers, name, dead, state, loader)  # The retry.

        assert self._letters(servers, dead) == [(0, 1), (1, 0)]
        assert len(loader.rows) == 3

    def test_a_poison_message_last_is_dead_lettered_once(
        self, servers: str, new_topic: NewTopic
    ) -> None:
        name, dead = new_topic(partitions=1), new_topic(partitions=1)
        _send(servers, name, [_envelope("c", None, {"ID": 1}), b"not json"])
        state = core_state.InMemoryStateStore()
        group = f"etl-test-{uuid.uuid4().hex[:6]}"
        loader = fakes.CollectingLoader[contract.ChangeEvent]()

        for _ in range(2):
            self._sync(servers, name, dead, state, loader, group)

        assert self._letters(servers, dead) == [(0, 1)]
        assert len(loader.rows) == 1
        assert _committed(servers, group, name) == {"0": 2}


def _counts(messages: list[contract.Message]) -> dict[int, int]:
    counts: dict[int, int] = {p: 0 for p in range(3)}
    for message in messages:
        counts[_where(message)[0]] += 1
    return counts


def _where(message: contract.Message) -> tuple[int, int]:
    """The partition and offset of a message, from its id."""
    partition, _, offset = message.id.rpartition("[")[2].partition("]@")
    return int(partition), int(offset)


# Debezium, as its Oracle connector would write the legacy tables' changes.


def _envelope(
    op: str, before: dict[str, Any] | None, after: dict[str, Any] | None
) -> bytes:
    return json.dumps({
        "op": op,
        "before": before,
        "after": after,
        "source": {"connector": "oracle", "scn": "1", "ts_ms": 1_790_000_000},
        "ts_ms": 1_790_000_001,
    }).encode()


def _legacy_rows(engine: sa.Engine, table: str) -> list[dict[str, Any]]:
    with engine.connect() as connection:
        rows = connection.execute(sa.text(f'SELECT * FROM "{table}"'))
        return [
            {
                k: v.isoformat() if hasattr(v, "isoformat") else v
                for k, v in row._mapping.items()  # pylint: disable=protected-access
            }
            for row in rows
        ]


def test_crm_sync_from_debezium_topics(
    servers: str, new_topic: NewTopic, tmp_path: pathlib.Path
) -> None:
    """`crm.sync` with the legacy changes of Debezium, not of the database."""
    legacy = sa.create_engine(f"sqlite:///{tmp_path / 'legacy.db'}")
    crm = sa.create_engine(f"sqlite:///{tmp_path / 'crm.db'}")
    demo.seed(legacy, crm)
    prefix = f"legacy-{uuid.uuid4().hex[:6]}"
    keys = {"CUST_MASTER": "CUST_NO", "ORD_MASTER": "ORD_NO"}
    producer = _cluster(servers).producer()
    for table, key in keys.items():
        new_topic(partitions=1, name=f"{prefix}.{table}")
        # Debezium's snapshot: an `r` event per row.
        for row in _legacy_rows(legacy, table):
            producer.send(
                f"{prefix}.{table}",
                _envelope("r", None, row),
                key=json.dumps({key: row[key]}).encode(),
            )
    producer.flush()
    settings = {
        "kafka": {
            "main": {"bootstrap_servers": servers, "group_id": prefix},
        },
        "databases": {
            "legacy": {
                "url": str(legacy.url),
                "changes": {
                    "kind": "debezium",
                    "kafka": "main",
                    "topic_prefix": prefix,
                },
            },
            "crm": {"url": str(crm.url)},
        },
        "bookkeeping": {"database": "crm", "create": True},
        "modules": {
            "crm": {
                "module": "modules.crm:create",
                "settings": {
                    "mappings": str(MAPPINGS),
                    "source_tables": {
                        "customer": "CUST_MASTER",
                        "order": "ORD_MASTER",
                    },
                    "target_tables": {"customer": "customer", "order": "order"},
                    "max_reject_rate": 0.6,
                    "orphans": "park",
                    "snapshot": "log",
                },
            }
        },
    }

    def sync() -> runtime.RunOutcome:
        with bootstrap.bootstrap(
            "test", directory=tmp_path / "none", overrides=settings, logs=False
        ) as application:
            assert application.kernel.check()["kafka.main"].ok
            return application.kernel.run("crm.sync")

    try:
        first = sync()
        assert first.result is not None
        assert first.result.stats.created == 4  # 2 customers, 2 orders.

        customer = next(
            r
            for r in _legacy_rows(legacy, "CUST_MASTER")
            if r["CUST_NO"] == "1001"
        )
        order = next(
            r
            for r in _legacy_rows(legacy, "ORD_MASTER")
            if r["ORD_NO"] == "O-2"
        )
        producer.send(
            f"{prefix}.CUST_MASTER",
            _envelope("u", customer, {**customer, "FIRST_NM": "Annie"}),
            key=b'{"CUST_NO": "1001"}',
        )
        producer.send(
            f"{prefix}.ORD_MASTER",
            _envelope("d", order, None),
            key=b'{"ORD_NO": "O-2"}',
        )
        producer.send(f"{prefix}.ORD_MASTER", None, key=b'{"ORD_NO": "O-2"}')
        producer.flush()
        second = sync()

        assert second.result is not None
        stats = second.result.stats
        assert (stats.updated, stats.deleted) == (1, 1)
        with crm.connect() as connection:
            assert connection.exec_driver_sql(
                "SELECT full_name FROM customer WHERE customer_id = 1001"
            ).all() == [("Annie Müller",)]
            assert connection.exec_driver_sql(
                'SELECT count(*) FROM "order" WHERE is_deleted'
            ).all() == [(1,)]
        # Past the tombstone after the delete: the next run does not read it.
        assert _committed(servers, prefix, f"{prefix}.ORD_MASTER") == {"0": 5}
    finally:
        legacy.dispose()
        crm.dispose()
