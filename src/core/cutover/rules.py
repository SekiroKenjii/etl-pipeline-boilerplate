"""Read-after-write verification: a quality rule at AFTER_LOAD."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
import dataclasses
import itertools

from core import contract
from core.cutover import checksum

# Databases cap the values of an IN list (Oracle: 1000).
_CHUNK = 1000


@dataclasses.dataclass(frozen=True, slots=True)
class LoadedRowsCheck[T]:
    """Reads the rows of each batch back from the target, and compares them.

    Catches what a load may do silently: a trigger or a default changing a
    value, a column truncated, a time zone lost. Run at AFTER_LOAD; the
    batch is in the target already, so FAIL (stop the run) or WARN.

        quality.CheckSet(batch=[LoadedRowsCheck(reader, "customer", ...)])

    Attributes:
        target: Reads the target.
        table: The target table.
        key: The columns identifying a row; the first one is looked up by IN.
        columns: The columns compared.
        row: The target row of a record; None to skip it (a delete).
        name: The rule's name in the reports.
        on_violation: What the gate does with a row that differs.
    """

    target: contract.ITableReader
    table: str
    key: tuple[str, ...]
    columns: tuple[str, ...]
    row: Callable[[T], contract.Row | None]
    name: str = "loaded_rows_match"
    on_violation: contract.QualityAction = contract.QualityAction.FAIL

    def check_batch(
        self, records: Sequence[T], ctx: contract.ETLContext
    ) -> Iterator[contract.Violation]:
        """See `contract.BatchRule.check_batch`."""
        del ctx  # Unused.
        columns = tuple(dict.fromkeys((*self.key, *self.columns)))
        loaded = [row for row in map(self.row, records) if row is not None]
        found = self._read(loaded, columns)
        for row in loaded:
            values = checksum.canonical_row(row, columns)
            key = checksum.text(checksum.canonical_row(row, self.key))
            actual = found.get(key)
            if actual is None:
                yield contract.Violation(
                    "not found in the target after its load", record_id=key
                )
            elif actual != values:
                differing = [
                    f"{column} {want!r} became {got!r}"
                    for column, want, got in zip(columns, values, actual)
                    if want != got
                ]
                yield contract.Violation(
                    "differs in the target: " + ", ".join(differing),
                    record_id=key,
                )

    def _read(
        self, rows: Sequence[contract.Row], columns: tuple[str, ...]
    ) -> dict[str, tuple[checksum.Canonical, ...]]:
        """The canonical rows of the target with the keys of `rows`."""
        found: dict[str, tuple[checksum.Canonical, ...]] = {}
        first = self.key[0]
        values = list(dict.fromkeys(row[first] for row in rows))
        for chunk in itertools.batched(values, _CHUNK):
            query = contract.TableQuery(
                self.table,
                order_by=self.key,
                columns=columns,
                filters=(
                    contract.Condition(first, contract.Operator.IN, chunk),
                ),
            )
            for row in self.target.read(query):
                canonical = checksum.canonical_row(row, columns)
                key = checksum.text(checksum.canonical_row(row, self.key))
                found[key] = canonical
        return found
