"""Cutover: does the target hold what the source says?

Per entity: the source rows, those the mapping rejects (and why), the target
rows expected, and those of the target missing, extra or different, compared
row by row across databases. `LoadedRowsCheck` verifies each batch as it is
loaded. Rejected rows fail the report unless accepted
(`AcceptedRejections`).
"""

from core.cutover.acceptance import AcceptedRejections
from core.cutover.checksum import canonical_value
from core.cutover.diff import count
from core.cutover.diff import Difference
from core.cutover.diff import EntityReport
from core.cutover.diff import reconcile
from core.cutover.diff import ROWS_PER_PARTITION
from core.cutover.history import compare
from core.cutover.history import Comparison
from core.cutover.history import comparison_markdown
from core.cutover.history import seal
from core.cutover.history import verify
from core.cutover.html import to_html
from core.cutover.markdown import to_markdown
from core.cutover.report import build
from core.cutover.report import CutoverReport
from core.cutover.report import last_runs
from core.cutover.report import Verdict
from core.cutover.rules import LoadedRowsCheck

__all__ = [
    "ROWS_PER_PARTITION",
    "AcceptedRejections",
    "Comparison",
    "CutoverReport",
    "Difference",
    "EntityReport",
    "LoadedRowsCheck",
    "Verdict",
    "build",
    "canonical_value",
    "compare",
    "comparison_markdown",
    "count",
    "last_runs",
    "reconcile",
    "seal",
    "to_html",
    "to_markdown",
    "verify",
]
