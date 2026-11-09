"""Source-to-target mappings: defined in spreadsheets, applied as transformers.

Specs are format-neutral (`core.contract`); readers of authored files, like
xlsx, live in `infra.mapping`; the compiled JSON form is in `catalog`.
"""

from core.mapping.casts import CastError
from core.mapping.casts import parse_type
from core.mapping.casts import SUPPORTED_TYPE_NAMES
from core.mapping.casts import TargetType
from core.mapping.catalog import load_catalog
from core.mapping.catalog import save_catalog
from core.mapping.compiled import CompiledMapping
from core.mapping.compiler import catalog_warnings
from core.mapping.compiler import compile_catalog
from core.mapping.compiler import DEFAULT_LABELS
from core.mapping.compiler import key_fingerprint
from core.mapping.compiler import key_namespace
from core.mapping.compiler import mapping_dependencies
from core.mapping.compiler import skipped_mappings
from core.mapping.compiler import validate_catalog
from core.mapping.diff import CatalogChanges
from core.mapping.diff import compare as compare_catalogs
from core.mapping.diff import describe as describe_changes
from core.mapping.lookups import LookupSource
from core.mapping.lookups import read_lookup
from core.mapping.lookups import with_lookups
from core.mapping.orphans import decode_record
from core.mapping.orphans import encode_record
from core.mapping.orphans import InferredParent
from core.mapping.orphans import InferringLoader
from core.mapping.orphans import record_identity
from core.mapping.privacy import anonymizer
from core.mapping.privacy import KINDS as PERSONAL_KINDS
from core.mapping.quality import MappingIssuesRule
from core.mapping.quality import reference_rule_name
from core.mapping.quality import reference_rules
from core.mapping.quality import ReferenceRule
from core.mapping.results import MappedRecord
from core.mapping.results import MappingIssue
from core.mapping.results import Reference
from core.mapping.transformer import MappingTransformer
from core.mapping.transforms import Transform
from core.mapping.transforms import TransformRegistry
from core.mapping.transforms import UnmappedValueError

__all__ = [
    "CatalogChanges",
    "LookupSource",
    "PERSONAL_KINDS",
    "DEFAULT_LABELS",
    "SUPPORTED_TYPE_NAMES",
    "CastError",
    "CompiledMapping",
    "InferredParent",
    "InferringLoader",
    "MappedRecord",
    "MappingIssue",
    "MappingIssuesRule",
    "MappingTransformer",
    "Reference",
    "ReferenceRule",
    "TargetType",
    "Transform",
    "TransformRegistry",
    "UnmappedValueError",
    "anonymizer",
    "catalog_warnings",
    "compare_catalogs",
    "compile_catalog",
    "describe_changes",
    "read_lookup",
    "decode_record",
    "encode_record",
    "key_fingerprint",
    "key_namespace",
    "load_catalog",
    "mapping_dependencies",
    "parse_type",
    "record_identity",
    "reference_rule_name",
    "reference_rules",
    "save_catalog",
    "skipped_mappings",
    "validate_catalog",
    "with_lookups",
]
