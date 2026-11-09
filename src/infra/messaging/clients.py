"""What the messaging adapters pass to confluent-kafka's objects."""

from __future__ import annotations

from collections.abc import Callable
from typing import TypedDict

import confluent_kafka

__all__ = ["ProduceOptions"]


class ProduceOptions(TypedDict, total=False):
    """The keyword arguments of `Producer.produce` the adapters give."""

    key: bytes | None
    on_delivery: Callable[
        [confluent_kafka.KafkaError | None, confluent_kafka.Message], None
    ]
    headers: list[tuple[str, str | bytes | None]]
    partition: int
