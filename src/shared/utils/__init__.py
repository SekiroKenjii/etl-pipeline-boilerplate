"""Project-wide helpers with no domain knowledge (no ETL types).

One module per concern. Callers import the package and qualify the names:

    from shared import utils

    utils.utc_now()
"""

from shared.utils.concurrency import bounded_map
from shared.utils.datetimes import like_timezone
from shared.utils.datetimes import utc_now
from shared.utils.files import atomic_write_text
from shared.utils.jsonl import append_json_lines
from shared.utils.jsonl import JsonLinesWriter
from shared.utils.paths import get_path
from shared.utils.ratelimit import TokenBucket
from shared.utils.retrying import retry
from shared.utils.sampling import merge_reservoirs
from shared.utils.sampling import reservoir_offer
from shared.utils.serialization import from_tagged
from shared.utils.serialization import register_tag
from shared.utils.serialization import to_jsonable
from shared.utils.serialization import to_tagged

__all__ = [
    "JsonLinesWriter",
    "append_json_lines",
    "TokenBucket",
    "atomic_write_text",
    "bounded_map",
    "from_tagged",
    "get_path",
    "like_timezone",
    "merge_reservoirs",
    "register_tag",
    "reservoir_offer",
    "retry",
    "to_jsonable",
    "to_tagged",
    "utc_now",
]
