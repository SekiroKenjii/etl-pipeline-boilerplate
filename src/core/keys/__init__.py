"""Surrogate keys derived from natural keys, and the crosswalk of loaded keys.

The crosswalk (key registry) serves referential integrity checks and lookups
between natural and surrogate keys; database-backed registries are database
adapters (`infra.database`), behind a `TieredKeyCache`.
`discover_relationships` suggests the keys and foreign keys of a legacy
schema.
"""

from core.keys.generation import canonical
from core.keys.generation import canonical_component
from core.keys.generation import canonical_text
from core.keys.generation import key_namespace
from core.keys.generation import KeyMaker
from core.keys.generation import MissingKeyComponentError
from core.keys.generation import named_namespace
from core.keys.loader import RegisteringLoader
from core.keys.registry import InMemoryKeyRegistry
from core.keys.registry import JsonLinesKeyRegistry
from core.keys.relationships import CandidateKey
from core.keys.relationships import discover_relationships
from core.keys.relationships import Evidence
from core.keys.relationships import Relationship
from core.keys.relationships import RelationshipReport
from core.keys.tiered import TieredKeyCache

__all__ = [
    "CandidateKey",
    "Evidence",
    "InMemoryKeyRegistry",
    "JsonLinesKeyRegistry",
    "KeyMaker",
    "MissingKeyComponentError",
    "RegisteringLoader",
    "Relationship",
    "RelationshipReport",
    "TieredKeyCache",
    "canonical",
    "canonical_component",
    "canonical_text",
    "discover_relationships",
    "key_namespace",
    "named_namespace",
]
