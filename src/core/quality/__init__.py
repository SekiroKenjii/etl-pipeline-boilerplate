"""Data quality: a gate that audits the ETL phases at their checkpoints.

The ETL job depends on `contract.IQualityGate` only, never on this package.
"""

from core.quality.checks import BatchCheck
from core.quality.checks import RecordCheck
from core.quality.checks import unique_by
from core.quality.gate import CheckSet
from core.quality.gate import QualityGate
from core.quality.parking import InMemoryParkingLot
from core.quality.parking import ParkingQuarantine
from core.quality.sinks import InMemoryIssueSink
from core.quality.sinks import InMemoryQuarantine
from core.quality.sinks import JsonLinesIssueSink
from core.quality.sinks import JsonLinesQuarantine

__all__ = [
    "BatchCheck",
    "CheckSet",
    "InMemoryIssueSink",
    "InMemoryParkingLot",
    "InMemoryQuarantine",
    "JsonLinesIssueSink",
    "JsonLinesQuarantine",
    "ParkingQuarantine",
    "QualityGate",
    "RecordCheck",
    "unique_by",
]
