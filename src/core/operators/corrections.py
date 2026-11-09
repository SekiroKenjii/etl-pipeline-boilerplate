"""Manual corrections of source rows: a reviewed file the BAs own.

Rows fixed neither at the source nor by a mapping rule are corrected by key
in a versioned file, applied before the mapping by every pipeline and by the
cutover's expectations:

    mapping,source_key,CRT_DT
    customer,1003,28/02/2020
"""

from __future__ import annotations

from collections.abc import Callable, Collection, Iterable, Iterator, Mapping
import csv
import dataclasses
import pathlib

from core import contract
from core import pipeline
from core.operators import types as operators_types

# Export's own columns (`_id`, `_issues`): never taken for source columns.
EXPORT_PREFIX = "_"
SCOPE, SOURCE_KEY = "mapping", "source_key"
# Formula (and DDE) injection: such cells get a leading quote (OWASP).
_FORMULA_STARTS = ("=", "+", "-", "@", "\t", "\r")
_ESCAPE = "'"


def escape_cell(value: object) -> str:
    """Returns a value as the text of a CSV cell no spreadsheet runs.

    A text starting like a formula (`=`, `+`, `-`, `@`, a tab or a carriage
    return) gets a leading `'`, and so does one starting with `'`, so that
    `unescape_cell` gives back every text as it was. None is an empty cell.
    """
    if value is None:
        return ""
    text = str(value)
    if text.startswith((*_FORMULA_STARTS, _ESCAPE)):
        return _ESCAPE + text
    return text


def unescape_cell(text: str) -> str:
    """Returns the text `escape_cell` escaped, as it was."""
    if text.startswith(_ESCAPE) and text[1:].startswith(
        (*_FORMULA_STARTS, _ESCAPE)
    ):
        return text[1:]
    return text


class Corrections:
    """The corrections of source rows, by scope (a mapping) and source key.

    Attributes:
        fixes: The corrected columns, by scope, then source key.
    """

    def __init__(self, fixes: Mapping[str, Mapping[str, contract.Row]]) -> None:
        self.fixes = {scope: dict(rows) for scope, rows in fixes.items()}

    @classmethod
    def from_csv(
        cls, path: pathlib.Path | str, ignored: Collection[str] = ()
    ) -> Corrections:
        """Reads a CSV file: `mapping`, `source_key`, then corrected columns.

        Empty cells correct nothing. Values are text, as the source holds them
        before its mapping parses them. Columns starting with `_` (those of
        `etl quarantine export`) are no source columns: they are skipped.
        Cells escaped against formulas by the export (`escape_cell`: `'=...`,
        `'-3`) are read back as they were.

        Args:
            path: The file.
            ignored: Other columns to skip.

        Raises:
            PipelineConfigError: If the file cannot be read, or lacks a
                column.
        """
        fixes: dict[str, dict[str, contract.Row]] = {}
        try:
            with pathlib.Path(path).open(
                encoding="utf-8-sig", newline=""
            ) as file:
                reader = csv.DictReader(file)
                if not {SCOPE, SOURCE_KEY} <= set(reader.fieldnames or ()):
                    raise contract.PipelineConfigError(
                        f"{path}: the columns mapping and source_key are needed"
                    )
                skipped = {SCOPE, SOURCE_KEY, *ignored}
                for number, line in enumerate(reader, start=2):
                    scope = unescape_cell(line[SCOPE] or "")
                    key = unescape_cell(line[SOURCE_KEY] or "")
                    if not scope or not key:
                        raise contract.PipelineConfigError(
                            f"{path}:{number}: no mapping or source_key"
                        )
                    fixes.setdefault(scope, {})[key] = {
                        str(column): unescape_cell(value)
                        for column, value in line.items()
                        if column is not None
                        and column not in skipped
                        and not column.startswith(EXPORT_PREFIX)
                        and value not in (None, "")
                    }
        except OSError as exc:
            raise contract.PipelineConfigError(
                f"Cannot read the corrections {path}: {exc}"
            ) from exc
        return cls(fixes)

    def __len__(self) -> int:
        return sum(len(rows) for rows in self.fixes.values())

    def apply(self, scope: str, key: str, row: contract.Row) -> contract.Row:
        """Returns the row, corrected if it has corrections."""
        fixed = self.fixes.get(scope, {}).get(key)
        return {**row, **fixed} if fixed else row

    def transformer(
        self, scope: str, key: Callable[[operators_types.SourceRecord], str]
    ) -> CorrectionsTransformer:
        """Returns a transformer correcting the rows (or changes) of `scope`."""
        return CorrectionsTransformer(self, scope, key)


class CorrectionsTransformer(
    pipeline.IDataTransformer[
        operators_types.SourceRecord, operators_types.SourceRecord
    ]
):
    """Corrects rows, or the rows of changes, before their mapping."""

    def __init__(
        self,
        corrections: Corrections,
        scope: str,
        key: Callable[[operators_types.SourceRecord], str],
    ) -> None:
        self._corrections = corrections
        self._scope = scope
        self._key = key
        self._corrected = 0

    @property
    def name(self) -> str:
        return f"{type(self).__name__}({self._scope})"

    def transform(
        self,
        records: Iterable[operators_types.SourceRecord],
        ctx: contract.ETLContext,
    ) -> Iterator[operators_types.SourceRecord]:
        if not self._corrections.fixes.get(self._scope):
            yield from records
            return
        for record in records:
            row = (
                record.row
                if isinstance(record, contract.ChangeEvent)
                else record
            )
            fixed = self._corrections.apply(self._scope, self._key(record), row)
            if fixed is row:
                yield record
                continue
            self._corrected += 1
            yield (
                dataclasses.replace(record, row=fixed)
                if isinstance(record, contract.ChangeEvent)
                else fixed
            )

    def collect_stats(self) -> contract.ETLStats:
        corrected, self._corrected = self._corrected, 0
        return contract.ETLStats(corrected=corrected)
