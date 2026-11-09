"""Reusable ETL operators, built on the ports of `core.contract` only.

They carry the ETL semantics (incremental state, acknowledgement, lineage,
run parameters) once, whatever adapter is plugged in.
"""

from core.operators.api import ApiExtractor
from core.operators.changes import ChangeExtractor
from core.operators.changes import ChangeLoader
from core.operators.changes import Deletes
from core.operators.changes import Snapshot
from core.operators.corrections import Corrections
from core.operators.corrections import CorrectionsTransformer
from core.operators.dedupe import DropDuplicates
from core.operators.messages import MessageExtractor
from core.operators.objects import ObjectRecord
from core.operators.objects import ObjectStoreExtractor
from core.operators.parking import ParkedRecordExtractor
from core.operators.partitions import KeyPartitions
from core.operators.partitions import KeyRange
from core.operators.partitions import PartitionExtractor
from core.operators.partitions import split
from core.operators.passthrough import Passthrough
from core.operators.quarantine import QuarantineReplay
from core.operators.reconciliation import MissingSourceRows
from core.operators.tables import TableExtractor
from core.operators.tables import TableLoader
from core.operators.types import Fetch
from core.operators.types import Params
from core.operators.types import SourceRecord

__all__ = [
    "ApiExtractor",
    "ChangeExtractor",
    "ChangeLoader",
    "Corrections",
    "CorrectionsTransformer",
    "Deletes",
    "DropDuplicates",
    "Fetch",
    "KeyPartitions",
    "KeyRange",
    "MessageExtractor",
    "MissingSourceRows",
    "ObjectRecord",
    "ObjectStoreExtractor",
    "Params",
    "ParkedRecordExtractor",
    "PartitionExtractor",
    "Passthrough",
    "QuarantineReplay",
    "Snapshot",
    "SourceRecord",
    "TableExtractor",
    "TableLoader",
    "split",
]
