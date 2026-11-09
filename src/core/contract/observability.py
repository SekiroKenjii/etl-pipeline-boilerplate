"""What the metrics and the traces record with each value: its attributes,
which the exporters (`infra.observability`) send as they are."""

from __future__ import annotations

from collections.abc import Mapping

type Attribute = str | bool | int | float
type Attributes = Mapping[str, Attribute]
