"""Kafka topics, read run after run, on confluent-kafka (librdkafka).

    cluster = KafkaCluster({"bootstrap.servers": "kafka:9092"}, group_id="etl")
    extractor = operators.MessageExtractor(cluster.topic("orders"), state=...)

A run reads every partition from the offsets saved by the last one up to
their end at its start, then stops: Kafka becomes a batch source like the
others. Offsets are committed to the consumer group once loaded
(`confirm`), never automatically: the group's lag says how far behind the
ETL is.
"""

from __future__ import annotations

from collections.abc import Collection, Generator, Iterator, Mapping
import contextlib
import datetime
import re
import time
from typing import Any

import confluent_kafka
from confluent_kafka import admin as kafka_admin

from core import contract
from core import observability
from core.observability import metrics
from infra.messaging import clients as messaging_clients

_TOPIC_HEADER = "etl.topic"
_MESSAGE_ID = re.compile(r"(.+)\[(\d+)\]@(\d+)")
_MAX_REASON = 1000
_EOF = confluent_kafka.KafkaError._PARTITION_EOF  # pylint: disable=protected-access
# Secret librdkafka properties whose name does not say so.
_SECRET_PROPERTIES = frozenset({
    "sasl.oauthbearer.config",
    "ssl.key.pem",
    "ssl_key",
})

type _Headers = (
    dict[str, str | bytes | None] | list[tuple[str, str | bytes | None]]
)


class _Credentials:
    """librdkafka properties holding secrets, kept apart from the others.

    They are merged into the properties only to build a client, and hidden
    when shown: a cluster or topic printed, logged or inspected in a
    debugger does not show them.
    """

    __slots__ = ("_values",)

    def __init__(self, values: Mapping[str, object]) -> None:
        self._values = dict(values)

    def __repr__(self) -> str:
        return f"<credentials {sorted(self._values)}: hidden>"

    def merged(self, config: Mapping[str, object]) -> dict[str, object]:
        """The properties of a client: `config` and the credentials."""
        return {**config, **self._values}


def _split(
    config: Mapping[str, object],
    credentials: _Credentials | Mapping[str, object] | None,
) -> tuple[dict[str, object], _Credentials]:
    """The properties without secrets, and the secrets: those of `config`,
    and `credentials`."""
    secret = {
        key: value
        for key, value in config.items()
        if key in _SECRET_PROPERTIES or observability.is_secret_name(key)
    }
    public = {k: v for k, v in config.items() if k not in secret}
    if isinstance(credentials, _Credentials):
        return public, _Credentials(credentials.merged(secret))
    return public, _Credentials({**secret, **(credentials or {})})


@contextlib.contextmanager
def errors(action: str) -> Generator[None]:
    """Turns Kafka errors into retryable or fatal pipeline errors."""
    try:
        yield
    except confluent_kafka.KafkaException as exc:
        error = exc.args[0] if exc.args else None
        retriable = False
        if isinstance(error, confluent_kafka.KafkaError):
            retriable = error.retriable()
        kind = contract.RetryableError if retriable else contract.FatalError
        raise kind(f"Kafka error {action}: {exc}") from exc


class KafkaCluster(contract.IMessageLog):
    """A Kafka cluster: a `KafkaTopic` per topic, and a producer.

    Attributes:
        config: The librdkafka properties shared by consumers and producers
            (`bootstrap.servers`, `security.protocol`, `sasl.mechanism`...),
            without the secrets (`sasl.password`...): those are kept apart,
            and given to each client when it is built.
        group_id: The consumer group whose offsets the ETL commits.
        redact: More secret names hidden in the reasons of dead letters, on
            top of `observability.SECRET_NAMES`: the logs' `redact`.
    """

    def __init__(
        self,
        config: Mapping[str, object],
        *,
        credentials: Mapping[str, object] | None = None,
        group_id: str = "etl",
        timeout_seconds: float = 30.0,
        redact: Collection[str] = (),
    ) -> None:
        """Initializes the cluster.

        Args:
            config: The librdkafka properties; the secret ones among them
                are moved to the credentials.
            credentials: The librdkafka properties holding secrets.
            group_id: The consumer group of the ETL.
            timeout_seconds: How long to wait for the brokers.
            redact: More secret names hidden in the reasons of dead letters.
        """
        self.config, self._credentials = _split(config, credentials)
        self.group_id = group_id
        self.redact = tuple(redact)
        self._timeout = timeout_seconds

    def topic(self, name: str) -> KafkaTopic:
        return KafkaTopic(
            self.config,
            name,
            credentials=self._credentials,
            group_id=self.group_id,
            timeout_seconds=self._timeout,
        )

    def producer(self) -> KafkaProducer:
        """Returns a producer to the cluster."""
        return KafkaProducer(
            self._credentials.merged(self.config),
            timeout_seconds=self._timeout,
        )

    def dead_letters(self, topic: str) -> KafkaDeadLetters:
        """Returns a dead letter topic of the cluster."""
        return KafkaDeadLetters(self.producer(), topic, redact=self.redact)

    def ping(self) -> None:
        """Checks that a broker answers.

        Raises:
            RetryableError: If none does in time.
        """
        # confluent-kafka types the config narrower than librdkafka takes it
        # (callbacks such as `error_cb`).
        config: dict[str, Any] = self._credentials.merged(self.config)
        admin = kafka_admin.AdminClient(config)
        with errors("reaching the brokers"):
            admin.list_topics(timeout=self._timeout)


class KafkaTopic(contract.IMessageSource):
    """A topic, read partition by partition up to their end.

    Positions are `{"<partition>": next offset}` for every partition; a
    partition missing from a position (added since) is read from its start.
    The consumer is assigned partitions, it does not join the group: runs do
    not wait for rebalances, and only one runs at a time anyway (run lock).
    """

    def __init__(
        self,
        config: Mapping[str, object],
        topic: str,
        *,
        credentials: _Credentials | Mapping[str, object] | None = None,
        group_id: str = "etl",
        timeout_seconds: float = 30.0,
        poll_seconds: float = 0.5,
    ) -> None:
        """Initializes the source.

        Args:
            config: The librdkafka properties of the cluster.
            topic: The topic read.
            credentials: The properties holding secrets, given to the
                consumer only when it is built.
            group_id: The consumer group whose offsets `confirm` commits.
            timeout_seconds: How long to wait for a message once there should
                be one, before failing (retryable).
            poll_seconds: How long each poll waits.
        """
        public, self._credentials = _split(config, credentials)
        self._config = {
            **public,
            "group.id": group_id,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "enable.partition.eof": True,
            "auto.offset.reset": "earliest",
        }
        self._topic = topic
        self._group_id = group_id
        self._timeout = timeout_seconds
        self._poll = poll_seconds
        self._consumer: confluent_kafka.Consumer | None = None

    @property
    def topic(self) -> str:
        return self._topic

    @property
    def name(self) -> str:
        return f"{type(self).__name__}({self._topic})"

    def close(self) -> None:
        """Closes the consumer, if open; the next use opens another."""
        if self._consumer is not None:
            consumer, self._consumer = self._consumer, None
            consumer.close()

    def beginning(self) -> contract.StateValue:
        return {str(p): low for p, (low, _) in self._watermarks().items()}

    def end(self) -> contract.StateValue:
        return {str(p): high for p, (_, high) in self._watermarks().items()}

    def messages(
        self, after: contract.StateValue, until: contract.StateValue = None
    ) -> Iterator[contract.Message]:
        """See `contract.IMessageSource.messages`.

        Raises:
            FatalError: If `after` is older than what the topic keeps, or it
                is not a position of a topic.
            RetryableError: If the brokers stop answering.
        """
        watermarks = self._watermarks()
        start = _offsets(after)
        stop = (
            _offsets(until)
            if until is not None
            else {p: high for p, (_, high) in watermarks.items()}
        )
        positions: dict[int, int] = {}
        for partition, (low, _) in watermarks.items():
            offset = start.get(partition, low)
            if offset < low:
                raise contract.FatalError(
                    f"{self._topic}[{partition}] keeps offsets from {low}"
                    f" only, the position is {offset}: the messages in"
                    " between expired; take a new snapshot"
                )
            positions[partition] = offset
        pending = {
            p for p, offset in positions.items() if offset < stop.get(p, 0)
        }
        metrics.meter().gauge(
            "etl.messages.lag",
            sum(max(0, stop.get(p, 0) - o) for p, o in positions.items()),
            {_TOPIC_HEADER: self._topic, "etl.group": self._group_id},
            unit="{message}",
            description="Messages of the topic not loaded yet, at the start",
        )
        if not pending:
            return
        consumer = self._open()
        assigned = [
            confluent_kafka.TopicPartition(self._topic, p, positions[p])
            for p in sorted(pending)
        ]
        with errors(f"assigning {self._topic}"):
            consumer.assign(assigned)
            # Partitions paused by the last read stay paused otherwise.
            consumer.resume(assigned)
        try:
            yield from self._read(consumer, positions, stop, pending)
        finally:
            with errors(f"unassigning {self._topic}"):
                consumer.unassign()

    def covers(
        self, position: contract.StateValue, message: contract.Message
    ) -> bool:
        """See `contract.IMessageSource.covers`: every partition of
        `position` is at or past where it was after `message`.

        Raises:
            FatalError: If a position is not one of a topic.
        """
        if position is None or message.position is None:
            return False
        reached = _offsets(position)
        after = _offsets(message.position)
        return bool(after) and all(
            reached.get(partition, 0) >= offset
            for partition, offset in after.items()
        )

    def confirm(self, position: contract.StateValue) -> None:
        """Commits the offsets of `position` to the consumer group."""
        offsets = [
            confluent_kafka.TopicPartition(self._topic, p, offset)
            for p, offset in sorted(_offsets(position).items())
        ]
        if offsets:
            with errors(f"committing the offsets of {self._topic}"):
                self._open().commit(offsets=offsets, asynchronous=False)

    def _read(
        self,
        consumer: confluent_kafka.Consumer,
        positions: dict[int, int],
        stop: Mapping[int, int],
        pending: set[int],
    ) -> Iterator[contract.Message]:
        """Polls the pending partitions until each reaches its stop."""
        waiting_since = time.monotonic()
        while pending:
            with errors(f"reading {self._topic}"):
                message = consumer.poll(self._poll)
            error = message.error() if message is not None else None
            if message is None or (error is not None and error.code() == _EOF):
                # Transaction markers take offsets: the position is past them.
                self._done(consumer, stop, pending)
                if pending and time.monotonic() - waiting_since > self._timeout:
                    raise contract.RetryableError(
                        f"{self._topic}: no message for {self._timeout}s"
                        f" before the end of partitions {sorted(pending)}"
                    )
                continue
            if error is not None:
                raise confluent_kafka.KafkaException(error)
            waiting_since = time.monotonic()
            partition, offset = _coordinates(message)
            if partition not in pending or offset >= stop[partition]:
                self._done(consumer, stop, pending)
                continue
            positions[partition] = offset + 1
            if positions[partition] >= stop[partition]:
                pending.discard(partition)
                consumer.pause(
                    [confluent_kafka.TopicPartition(self._topic, partition)]
                )
            yield _message(message, self._topic, partition, offset, positions)

    def _done(
        self,
        consumer: confluent_kafka.Consumer,
        stop: Mapping[int, int],
        pending: set[int],
    ) -> None:
        """Drops the partitions whose position reached their stop."""
        with errors(f"reading the positions of {self._topic}"):
            current = consumer.position([
                confluent_kafka.TopicPartition(self._topic, p) for p in pending
            ])
        for item in current:
            if item.offset >= stop[item.partition]:
                pending.discard(item.partition)

    def _watermarks(self) -> dict[int, tuple[int, int]]:
        """The oldest and next offsets of every partition.

        Raises:
            FatalError: If the topic does not exist.
        """
        consumer = self._open()
        with errors(f"reading the partitions of {self._topic}"):
            metadata = consumer.list_topics(self._topic, timeout=self._timeout)
            described = metadata.topics.get(self._topic)
            if described is None or described.error is not None:
                raise contract.FatalError(
                    f"Topic {self._topic!r} does not exist:"
                    f" {described.error if described else 'unknown'}"
                )
            return {
                partition: consumer.get_watermark_offsets(
                    confluent_kafka.TopicPartition(self._topic, partition),
                    timeout=self._timeout,
                    cached=False,
                )
                for partition in sorted(described.partitions)
            }

    def _open(self) -> confluent_kafka.Consumer:
        if self._consumer is None:
            self._consumer = confluent_kafka.Consumer(
                self._credentials.merged(self._config)
            )
        return self._consumer


class KafkaProducer:
    """Sends messages, and waits until the brokers have them.

    producer.send("orders", b'{"id": 1}', key=b"1")
    producer.flush()  # Raises if a message was not delivered.
    """

    def __init__(
        self, config: Mapping[str, object], *, timeout_seconds: float = 30.0
    ) -> None:
        self._producer = confluent_kafka.Producer(
            {**config, "enable.idempotence": True}
        )
        self._timeout = timeout_seconds
        self._failures: list[str] = []

    def send(
        self,
        topic: str,
        value: bytes | None,
        *,
        key: bytes | None = None,
        headers: Mapping[str, bytes] | None = None,
        partition: int | None = None,
    ) -> None:
        """Queues a message; `flush` waits for it."""
        options: messaging_clients.ProduceOptions = {
            "key": key,
            "on_delivery": self._delivered,
        }
        if headers:
            options["headers"] = list(headers.items())
        if partition is not None:
            options["partition"] = partition
        with errors(f"sending to {topic}"):
            self._producer.produce(topic, value, **options)
            self._producer.poll(0)

    def flush(self) -> None:
        """Waits until every message is delivered.

        Raises:
            RetryableError: If some are still queued after the timeout.
            FatalError: If the brokers refused some.
        """
        left = self._producer.flush(self._timeout)
        failures, self._failures = self._failures, []
        if failures:
            raise contract.FatalError(
                f"{len(failures)} message(s) not delivered: {failures[0]}"
            )
        if left:
            raise contract.RetryableError(
                f"{left} message(s) still queued after {self._timeout}s"
            )

    def _delivered(
        self,
        error: confluent_kafka.KafkaError | None,
        _: confluent_kafka.Message,
    ) -> None:
        if error is not None:
            self._failures.append(str(error))


class KafkaDeadLetters(contract.IDeadLetters):
    """A dead letter topic: messages as they were, with headers telling why
    (`etl.error`) and where from: `etl.message`, the id of the message, and
    for a message of Kafka `etl.topic`, `etl.partition` and `etl.offset`,
    as Kafka Connect's dead letter queues do.

    The reason is redacted (`observability.redact_text`, with `redact`),
    then cut to `_MAX_REASON` characters: the message itself is kept as it
    was, so restrict who reads the topic and how long it keeps them as for
    the source topic.

    Attributes:
        topic: The dead letter topic.
        redact: More secret names hidden in the reasons.
    """

    def __init__(
        self,
        producer: KafkaProducer,
        topic: str,
        *,
        redact: Collection[str] = (),
    ) -> None:
        self.topic = topic
        self.redact = tuple(redact)
        self._producer = producer

    def send(self, message: contract.Message, reason: str) -> None:
        """Sends the message, and waits until the brokers have it: its
        position may be committed right after."""
        origin = _origin(message.id)
        self._producer.send(
            self.topic,
            message.value,
            key=message.key,
            headers={
                **message.headers,
                "etl.error": _reason(reason, self.redact).encode(),
                "etl.message": message.id.encode(),
                **origin,
            },
        )
        self._producer.flush()
        metrics.meter().add(
            "etl.messages.dead_lettered",
            1,
            {_TOPIC_HEADER: origin.get(_TOPIC_HEADER, b"").decode()},
            unit="{message}",
            description="Messages sent to a dead letter topic",
        )


def _reason(reason: str, redact: Collection[str]) -> str:
    """Why a message is a dead letter: redacted, at most `_MAX_REASON`
    characters."""
    # Redacted whole (linear time), then cut: a cut first may split a secret
    # out of its pattern, and masks shrink the text, bringing it in front.
    reason = observability.redact_text(reason, redact)
    if len(reason) <= _MAX_REASON:
        return reason
    return reason[: _MAX_REASON - 1] + "…"


def _offsets(position: contract.StateValue) -> dict[int, int]:
    """The offsets of a position, by partition.

    Raises:
        FatalError: If it is not a position of a topic.
    """
    if position is None:
        return {}
    if not isinstance(position, dict):
        raise contract.FatalError(f"Not a position of a topic: {position!r}")
    try:
        return {
            int(partition): _offset(offset)
            for partition, offset in position.items()
        }
    except (TypeError, ValueError):
        raise contract.FatalError(
            f"Not a position of a topic: {position!r}"
        ) from None


def _offset(value: contract.StateValue) -> int:
    if not isinstance(value, (str, int, float)):
        raise TypeError(f"not an offset: {value!r}")
    return int(value)


def _coordinates(message: confluent_kafka.Message) -> tuple[int, int]:
    partition, offset = message.partition(), message.offset()
    if partition is None or offset is None:
        raise contract.FatalError("A Kafka message without an offset")
    return partition, offset


def _message(
    message: confluent_kafka.Message,
    topic: str,
    partition: int,
    offset: int,
    positions: Mapping[int, int],
) -> contract.Message:
    kind, milliseconds = message.timestamp()
    return contract.Message(
        id=f"{topic}[{partition}]@{offset}",
        key=message.key(),
        value=message.value(),
        headers=_headers(message.headers()),
        timestamp=(
            datetime.datetime.fromtimestamp(milliseconds / 1000, datetime.UTC)
            if kind != confluent_kafka.TIMESTAMP_NOT_AVAILABLE
            else None
        ),
        position={str(p): after for p, after in sorted(positions.items())},
    )


def _origin(message_id: str) -> dict[str, bytes]:
    """The Kafka Connect style headers of where a dead letter came from:
    `etl.topic`, `etl.partition`, `etl.offset`; none for a message that
    did not come from Kafka."""
    found = _MESSAGE_ID.fullmatch(message_id)
    if found is None:
        return {}
    topic, partition, offset = found.groups()
    return {
        _TOPIC_HEADER: topic.encode(),
        "etl.partition": partition.encode(),
        "etl.offset": offset.encode(),
    }


def _headers(raw: _Headers | None) -> dict[str, bytes]:
    """The headers of a message, as `{name: value}`; a name sent twice keeps
    its last value."""
    items = raw.items() if isinstance(raw, dict) else raw or ()
    return {
        str(name): data if isinstance(data, bytes) else str(data or "").encode()
        for name, data in items
    }
