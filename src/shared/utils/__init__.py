"""Project-wide helpers with no domain knowledge (no ETL types).

One module per concern. Callers import the package and qualify the names:

    from shared import utils

    utils.utc_now()
"""

from shared.utils.budgets import cpu_limit
from shared.utils.budgets import Limits
from shared.utils.budgets import limits
from shared.utils.budgets import memory_limit
from shared.utils.budgets import NO_LIMIT
from shared.utils.bytesize import parse_byte_size
from shared.utils.concurrency import bounded_map
from shared.utils.datetimes import like_timezone
from shared.utils.datetimes import utc_now
from shared.utils.external_sort import Comparable
from shared.utils.external_sort import ExternalSorter
from shared.utils.external_sort import sorted_spilling
from shared.utils.external_sort import SpillFormat
from shared.utils.files import atomic_write_text
from shared.utils.jsonl import append_json_lines
from shared.utils.jsonl import JsonLinesWriter
from shared.utils.paths import get_path
from shared.utils.ratelimit import TokenBucket
from shared.utils.retrying import retry
from shared.utils.sampling import merge_reservoirs
from shared.utils.sampling import reservoir_offer
from shared.utils.serialization import from_tagged
from shared.utils.serialization import JsonObject
from shared.utils.serialization import JsonValue
from shared.utils.serialization import register_tag
from shared.utils.serialization import to_jsonable
from shared.utils.serialization import to_tagged
from shared.utils.text import one_line

__all__ = [
    "Comparable",
    "ExternalSorter",
    "JsonLinesWriter",
    "JsonObject",
    "JsonValue",
    "Limits",
    "NO_LIMIT",
    "SpillFormat",
    "append_json_lines",
    "TokenBucket",
    "atomic_write_text",
    "bounded_map",
    "cpu_limit",
    "from_tagged",
    "get_path",
    "like_timezone",
    "limits",
    "memory_limit",
    "merge_reservoirs",
    "one_line",
    "parse_byte_size",
    "register_tag",
    "reservoir_offer",
    "retry",
    "sorted_spilling",
    "to_jsonable",
    "to_tagged",
    "utc_now",
]
