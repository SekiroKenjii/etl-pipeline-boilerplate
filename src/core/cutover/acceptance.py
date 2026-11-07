"""The rejected rows the BAs accept to leave behind, signed off with the
report.

A CSV in the columns of the report's rejected rows:

    entity,reason,rows
    customer,full_name: is required but empty,3
    order,*,2

Each line accepts up to `rows` rows rejected for that reason; `*` for any
reason. A row rejected for several reasons is accepted when each of them is,
or by a `*`. Rejected rows beyond what is accepted are a mismatch.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping
import csv
import dataclasses
import hashlib
import io
import pathlib
from typing import Any

ANY = "*"
_COLUMNS = ("entity", "reason", "rows")


@dataclasses.dataclass(frozen=True, slots=True)
class AcceptedRejections:
    """How many rejected rows the BAs accept, by entity and reason.

    Attributes:
        rows: Per entity, per reason (`ANY` for any), the most rows accepted.
        source: The file the decision was read from.
        digest: Its SHA-256: the report quotes it.
    """

    rows: Mapping[str, Mapping[str, int]]
    source: str = ""
    digest: str = ""

    @classmethod
    def from_csv(cls, path: pathlib.Path | str) -> AcceptedRejections:
        """Reads the decision from a CSV with `entity`, `reason`, `rows`.

        Raises:
            OSError: If the file cannot be read.
            ValueError: If it is not such a CSV.
        """
        path = pathlib.Path(path)
        content = path.read_bytes()
        reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig")))
        headers = {
            (name or "").strip().lower(): name
            for name in reader.fieldnames or ()
        }
        if not all(column in headers for column in _COLUMNS):
            raise ValueError(
                f"{path}: needs the columns {', '.join(_COLUMNS)}, has"
                f" {', '.join(reader.fieldnames or ()) or 'none'}"
            )
        rows: dict[str, dict[str, int]] = {}
        for line, record in enumerate(reader, start=2):
            entity, reason, count = (
                (record.get(headers[column]) or "").strip()
                for column in _COLUMNS
            )
            if not entity and not reason and not count:
                continue
            if not entity or not reason:
                raise ValueError(f"{path}:{line}: needs an entity and a reason")
            try:
                accepted = int(count)
            except ValueError:
                raise ValueError(
                    f"{path}:{line}: rows must be a whole number, not {count!r}"
                ) from None
            if accepted < 1:
                raise ValueError(f"{path}:{line}: rows must be at least 1")
            if reason in rows.setdefault(entity, {}):
                raise ValueError(
                    f"{path}:{line}: {entity} {reason!r} is accepted twice"
                )
            rows[entity][reason] = accepted
        return cls(rows, path.name, hashlib.sha256(content).hexdigest())

    def of(self, entity: str) -> Mapping[str, int]:
        """What is accepted of an entity: rows by reason."""
        return self.rows.get(entity, {})

    def as_dict(self) -> dict[str, Any]:
        """The decision as JSON-compatible data, for the report."""
        return {
            "source": self.source,
            "sha256": self.digest,
            "rows": {
                entity: dict(found) for entity, found in self.rows.items()
            },
        }


class Budget:
    """What is left to accept of an entity's rejected rows, row by row."""

    def __init__(self, accepted: Mapping[str, int]) -> None:
        self._left = dict(accepted)

    def accept(self, reasons: Collection[str]) -> bool:
        """Whether a row rejected for `reasons` is accepted; counts it."""
        distinct = set(reasons)
        if distinct and all(self._left.get(r, 0) > 0 for r in distinct):
            for reason in distinct:
                self._left[reason] -= 1
            return True
        if self._left.get(ANY, 0) > 0:
            self._left[ANY] -= 1
            return True
        return False
