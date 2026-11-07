"""Mapping-driven pipelines: a business module from a mapping workbook.

A module that migrates tables through compiled mappings (`core.mapping`)
writes three things: its settings (a subclass of
`config.MappingModuleSettings`: its databases, its compiled workbook), its
custom transforms (a `mapping.TransformRegistry`) and a `create` function
returning a `MappingModule`. It then has five pipelines, one step per
mapping, in the order the references of the mappings need:

    full_load   rows changed since the last run, by watermark; by key
                ranges in parallel with `partitions`
    sync        a snapshot, then the source's change log, deletes included
    parked      the orphans parked until their parent was loaded
    replay      the quarantined rows, once their cause is fixed
    deletes     the rows gone from the source, from the key registry

Each step corrects, maps, checks and loads its rows, registering their
surrogate keys, and is retried on transient failures. Another pipeline is a
`MappingPipeline` whose `job` builds a step from the same parts.
"""

from core.mapping_pipelines.base import MappingPipeline
from core.mapping_pipelines.kinds import Deletes
from core.mapping_pipelines.kinds import factories
from core.mapping_pipelines.kinds import FullLoad
from core.mapping_pipelines.kinds import Parked
from core.mapping_pipelines.kinds import Replay
from core.mapping_pipelines.kinds import Sync
from core.mapping_pipelines.mappings import Mappings
from core.mapping_pipelines.module import MappingModule
from core.mapping_pipelines.reconciliation import reconciliations
from core.mapping_pipelines.settings import LookupTable
from core.mapping_pipelines.settings import MappingSettings

__all__ = [
    "Deletes",
    "FullLoad",
    "LookupTable",
    "MappingModule",
    "MappingPipeline",
    "MappingSettings",
    "Mappings",
    "Parked",
    "Replay",
    "Sync",
    "factories",
    "reconciliations",
]
