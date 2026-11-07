"""Kafka dead letters: why a message is one, redacted and short."""

from __future__ import annotations

from typing import Any

import confluent_kafka
import pytest

from core import contract
from infra.messaging import kafka


class _Producer:
    """Records what is produced; everything is delivered at once."""

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.produced: list[tuple[str, bytes | None, dict[str, Any]]] = []

    def produce(self, topic: str, value: bytes | None, **options: Any) -> None:
        self.produced.append((topic, value, options))

    def poll(self, timeout: float) -> int:
        del timeout
        return 0

    def flush(self, timeout: float) -> int:
        del timeout
        return 0


@pytest.fixture
def producer(monkeypatch: pytest.MonkeyPatch) -> list[_Producer]:
    built: list[_Producer] = []

    def build(config: dict[str, Any]) -> _Producer:
        built.append(_Producer(config))
        return built[-1]

    monkeypatch.setattr(confluent_kafka, "Producer", build)
    return built


def _message() -> contract.Message:
    return contract.Message(
        id="legacy.CRM.CUSTOMER[2]@41",
        key=b'{"ID": 1}',
        value=b"not json",
        headers={"origin": b"connect"},
    )


def _headers(built: list[_Producer]) -> dict[str, bytes]:
    ((_, _, options),) = built[0].produced
    return dict(options["headers"])


def test_the_reason_is_redacted(producer: list[_Producer]) -> None:
    letters = kafka.KafkaCluster({"bootstrap.servers": "k:9092"}).dead_letters(
        "dlq"
    )

    letters.send(
        _message(),
        "bad value: postgresql://etl:S3cr3t@db/crm password=hunter2;"
        " Key (email)=(jane@example.com) already exists",
    )

    headers = _headers(producer)
    error = headers["etl.error"].decode()
    for secret in ("S3cr3t", "hunter2", "jane@example.com"):
        assert secret not in error
    assert error.startswith("bad value: postgresql://etl:***@db/crm")
    assert headers["etl.topic"] == b"legacy.CRM.CUSTOMER"
    assert (headers["etl.partition"], headers["etl.offset"]) == (b"2", b"41")
    assert headers["etl.message"] == b"legacy.CRM.CUSTOMER[2]@41"
    assert headers["origin"] == b"connect"


def test_a_message_of_another_broker_keeps_its_id(
    producer: list[_Producer],
) -> None:
    letters = kafka.KafkaCluster({"bootstrap.servers": "k:9092"}).dead_letters(
        "dlq"
    )

    letters.send(contract.Message("4f1c-sqs-id", None, b"not json"), "bad")

    headers = _headers(producer)
    assert headers["etl.message"] == b"4f1c-sqs-id"
    assert not {"etl.topic", "etl.partition", "etl.offset"} & headers.keys()


def test_a_position_covers_the_messages_read_before_it() -> None:
    topic = kafka.KafkaCluster({"bootstrap.servers": "k:9092"}).topic("t")
    first = contract.Message("t[0]@4", None, b"", position={"0": 5, "1": 2})
    later = contract.Message("t[1]@2", None, b"", position={"0": 5, "1": 3})

    assert topic.covers({"0": 5, "1": 2}, first)
    assert not topic.covers({"0": 5, "1": 2}, later)
    assert topic.covers({"0": 7, "1": 3}, later)
    assert not topic.covers(None, first)


def test_a_long_reason_is_cut(producer: list[_Producer]) -> None:
    letters = kafka.KafkaCluster({"bootstrap.servers": "k:9092"}).dead_letters(
        "dlq"
    )

    letters.send(_message(), "x" * 100_000)

    error = _headers(producer)["etl.error"].decode()
    assert len(error) == 1000
    assert error.endswith("…")
