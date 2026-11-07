"""Kafka credentials: given to the clients, kept out of the cluster's config."""

from __future__ import annotations

from typing import Any

import confluent_kafka
import pytest

from infra.messaging import kafka

_CONFIG = {
    "bootstrap.servers": "kafka:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "etl",
    "sasl.password": "S3cr3t",
    "ssl.key.password": "K3y",
}


class _Client:
    """Records the properties of the clients built."""

    built: list[dict[str, Any]] = []

    def __init__(self, config: dict[str, Any]) -> None:
        self.built.append(config)


@pytest.fixture
def built(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    _Client.built = []
    monkeypatch.setattr(confluent_kafka, "Producer", _Client)
    monkeypatch.setattr(confluent_kafka, "Consumer", _Client)
    return _Client.built


def test_secrets_are_not_in_the_cluster_config() -> None:
    cluster = kafka.KafkaCluster(
        _CONFIG, credentials={"sasl.oauthbearer.client.secret": "c"}
    )
    topic = cluster.topic("orders")

    assert cluster.config == {
        "bootstrap.servers": "kafka:9092",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "etl",
    }
    shown = repr(vars(cluster)) + repr(vars(topic))
    for secret in ("S3cr3t", "K3y", "'c'"):
        assert secret not in shown


def test_clients_get_the_secrets(built: list[dict[str, Any]]) -> None:
    cluster = kafka.KafkaCluster(_CONFIG, credentials={"sasl.password": "N3w"})

    cluster.producer()
    cluster.topic("orders")._open()  # pylint: disable=protected-access

    producer, consumer = built
    assert producer["sasl.password"] == consumer["sasl.password"] == "N3w"
    assert producer["ssl.key.password"] == consumer["ssl.key.password"] == "K3y"
    assert consumer["group.id"] == "etl"
