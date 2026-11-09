"""Message logs: Kafka topics, read run after run, and Debezium changes.

from infra.messaging import kafka

cluster = kafka.KafkaCluster({"bootstrap.servers": "kafka:9092"})
changes = messaging.DebeziumChangeLog(cluster, prefix="legacy")
"""

from infra.messaging.debezium import DebeziumChangeLog
from infra.messaging.debezium import DebeziumChangeSource
from infra.messaging.debezium import decode_json
from infra.messaging.types import Decoder

__all__ = [
    "DebeziumChangeLog",
    "DebeziumChangeSource",
    "Decoder",
    "decode_json",
]
