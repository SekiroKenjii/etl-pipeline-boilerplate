"""Reading mapping workbooks (xlsx)."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
import dataclasses
import datetime
import difflib
import pathlib
import re

import openpyxl

from core import contract
from infra.mapping import clients as mapping_clients
from infra.mapping import layout

_LIST_SEPARATORS = re.compile(r"[,;\n]")
_PARAM_SEPARATORS = frozenset(";\n")
_LABELLED_INDEX_COLUMNS = (
    "status",
    "ignored",
    "row_filter",
    "row_split",
    "key_hashing",
    "key_case",
    "key_namespace",
)

type _Problems = list[contract.MappingProblem]


def _locate(file: str, sheet: str | None = None, row: int | None = None) -> str:
    """Where something is in the workbook: `crm.xlsx › customer › row 12`."""
    parts = [file, sheet, None if row is None else f"row {row}"]
    return " › ".join(part for part in parts if part is not None)


def _at(location: str, column: str) -> str:
    """A location narrowed to a column: `... › row 12 › Transform`."""
    return f"{location} › {column}"


def _cell_text(value: object) -> str | None:
    """Returns a cell as clean text.

    Excel's 1.0 becomes "1", dates become ISO 8601, blanks become None.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    if isinstance(value, datetime.datetime):
        if value.time() == datetime.datetime.min.time():
            return value.date().isoformat()
        return value.isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()
    text = str(value).strip()
    return text or None


@dataclasses.dataclass(frozen=True, slots=True)
class _Row:
    """A row of a sheet, as clean text by canonical column key."""

    number: int
    cells: dict[str, str | None]

    def get(self, key: str) -> str | None:
        return self.cells.get(key)


class _Sheet:
    """A sheet whose header (row 1) is resolved to canonical keys.

    Attributes:
        title: The sheet name.
        file: The workbook file name.
        labels: The headers as written, by canonical key.
    """

    def __init__(
        self,
        worksheet: mapping_clients.WorksheetLike,
        file: str,
        columns: dict[str, tuple[str, ...]],
        required: Sequence[str],
        problems: _Problems,
    ) -> None:
        """Reads the header of `worksheet` and keeps its rows.

        Args:
            worksheet: The openpyxl sheet.
            file: The workbook file name, for locations.
            columns: The accepted headers, by canonical key.
            required: The columns that must be there.
            problems: Collects unknown and missing columns.
        """
        self.title: str = worksheet.title
        self.file = file
        self.labels: dict[str, str] = {}
        rows = worksheet.iter_rows(values_only=True)
        header = next(rows, ())

        aliases: dict[str, str] = {}
        for key, names in columns.items():
            for alias in names:
                aliases[layout.normalize(alias)] = key

        self._positions: dict[int, str] = {}
        for index, value in enumerate(header):
            text = _cell_text(value)
            if text is None:
                continue
            column_key = aliases.get(layout.normalize(text))
            if column_key is None:
                suggestion = _suggest_header(text, columns)
                problems.append(
                    contract.MappingProblem(
                        f"unknown column {text!r}{suggestion}",
                        self.location(1),
                    )
                )
                continue
            if column_key in self.labels:
                # Reading either copy would silently ignore the other.
                first = self.labels[column_key]
                also = f" (as {first!r} too)" if first != text else ""
                problems.append(
                    contract.MappingProblem(
                        f"column {text!r} is there twice{also}: keep one",
                        self.location(1),
                    )
                )
                continue
            self._positions[index] = column_key
            self.labels.setdefault(column_key, text)

        for key in required:
            if key not in self.labels:
                problems.append(
                    contract.MappingProblem(
                        f"missing column {columns[key][0]!r}", self.location(1)
                    )
                )

        self._rows = list(rows)

    def rows(self) -> Iterator[_Row]:
        """Yields the non-blank rows, from row 2."""
        for number, values in enumerate(self._rows, start=2):
            cells = {
                key: _cell_text(values[index]) if index < len(values) else None
                for index, key in self._positions.items()
            }
            if any(cells.values()):
                yield _Row(number, cells)

    def location(self, row: int | None = None) -> str:
        return _locate(self.file, self.title, row)

    def label(self, key: str, default: str) -> str:
        """Returns the header of a column as written, or `default`."""
        return self.labels.get(key, default)


class XlsxMappingReader:
    """Reads a mapping workbook into a catalog.

    Implements `contract.IMappingReader`. Every problem of the workbook is
    reported at once, at its cell.
    """

    def read(self, path: pathlib.Path) -> contract.MappingCatalog:
        """Reads the workbook at `path`.

        Args:
            path: The workbook.

        Returns:
            Its mappings and lookup tables.

        Raises:
            MappingDefinitionError: With every problem of the workbook.
        """
        file = path.name
        problems: _Problems = []
        workbook = openpyxl.load_workbook(path, read_only=True, data_only=True)
        try:
            sheets: dict[str, mapping_clients.WorksheetLike] = {
                layout.normalize(ws.title): ws for ws in workbook.worksheets
            }
            index = _Sheet(
                self._index_sheet(sheets, file),
                file,
                layout.INDEX_COLUMNS,
                layout.REQUIRED_INDEX_COLUMNS,
                problems,
            )
            labels = {
                key: index.labels[key]
                for key in _LABELLED_INDEX_COLUMNS
                if key in index.labels
            }
            used = {layout.normalize(index.title)}
            mappings = self._mappings(index, sheets, used, labels, problems)
            lookups = self._lookups(index, sheets, used, problems)
        finally:
            workbook.close()

        if problems:
            raise contract.MappingDefinitionError(problems)

        return contract.MappingCatalog(
            name=path.stem,
            mappings=tuple(mappings),
            lookups=lookups,
            source_file=file,
            labels=labels,
        )

    def _index_sheet(
        self, sheets: dict[str, mapping_clients.WorksheetLike], file: str
    ) -> mapping_clients.WorksheetLike:
        """Returns the sheet listing the mappings.

        Raises:
            MappingDefinitionError: If the workbook has none.
        """
        for name in layout.INDEX_SHEET:
            if layout.normalize(name) in sheets:
                return sheets[layout.normalize(name)]
        raise contract.MappingDefinitionError([
            contract.MappingProblem(
                f"no {layout.INDEX_SHEET[0]!r} sheet listing the mappings",
                _locate(file),
            )
        ])

    def _mappings(
        self,
        index: _Sheet,
        sheets: dict[str, mapping_clients.WorksheetLike],
        used: set[str],
        labels: dict[str, str],
        problems: _Problems,
    ) -> list[contract.MappingSpec]:
        """Reads every mapping listed in the index sheet."""
        mappings: list[contract.MappingSpec] = []
        for row in index.rows():
            name = row.get("mapping")
            if name is None:
                problems.append(
                    contract.MappingProblem(
                        "a mapping needs a name", index.location(row.number)
                    )
                )
                continue
            worksheet = sheets.get(layout.normalize(name))
            if worksheet is None:
                problems.append(
                    contract.MappingProblem(
                        f"no sheet named {name!r} for this mapping",
                        index.location(row.number),
                    )
                )
                continue
            used.add(layout.normalize(name))
            fields = _Sheet(
                worksheet,
                index.file,
                layout.FIELD_COLUMNS,
                layout.REQUIRED_FIELD_COLUMNS,
                problems,
            )
            for key, label in fields.labels.items():
                labels.setdefault(key, label)
            mappings.append(_mapping(row, index, fields, problems))
        return mappings

    def _lookups(
        self,
        index: _Sheet,
        sheets: dict[str, mapping_clients.WorksheetLike],
        used: set[str],
        problems: _Problems,
    ) -> dict[str, dict[str, str]]:
        """Reads the lookup sheets, and flags the sheets nothing uses."""
        lookups: dict[str, dict[str, str]] = {}
        for normalized, worksheet in sheets.items():
            if normalized.startswith(layout.LOOKUP_PREFIX):
                name = worksheet.title[len(layout.LOOKUP_PREFIX) :]
                lookups[name] = _lookup(worksheet, index.file, problems)
            elif (
                normalized not in used
                and normalized not in layout.IGNORED_SHEETS
                and not normalized.startswith("_")
            ):
                problems.append(
                    contract.MappingProblem(
                        f"sheet {worksheet.title!r} is not listed in"
                        f" {index.title!r} (rename it lookup_<name> if it is a"
                        " lookup table)",
                        _locate(index.file, worksheet.title),
                    )
                )
        return lookups


def _mapping(
    row: _Row, index: _Sheet, fields: _Sheet, problems: _Problems
) -> contract.MappingSpec:
    """Reads a mapping: its row of the index sheet and its own sheet."""
    location = index.location(row.number)

    version_text = row.get("version") or "1"
    version = int(version_text) if version_text.isdigit() else 1
    if not version_text.isdigit():
        problems.append(
            contract.MappingProblem(
                f"version {version_text!r} is not a whole number",
                _at(location, index.labels["version"]),
            )
        )

    status = _choice(
        row.get("status") or "draft",
        layout.STATUSES,
        _at(location, index.label("status", "Status")),
        problems,
    )
    policy = _choice(
        row.get("unmapped") or "error",
        layout.POLICIES,
        _at(location, index.label("unmapped", "Unmapped source fields")),
        problems,
    )
    key_version = _whole(row, "key_version", index, problems)
    # Blank: HMAC where a key secret is configured, UUIDv5 elsewhere.
    hashing_text = row.get("key_hashing")
    hashing = (
        _choice(
            hashing_text,
            layout.HASHINGS,
            _at(location, index.label("key_hashing", "Key hashing")),
            problems,
        )
        if hashing_text is not None
        else None
    )
    case = _choice(
        row.get("key_case") or "preserve",
        layout.CASES,
        _at(location, index.label("key_case", "Key case")),
        problems,
    )

    return contract.MappingSpec(
        name=row.get("mapping") or "",
        source=row.get("source") or "",
        target=row.get("target") or "",
        version=version,
        status=status or contract.MappingStatus.DRAFT,
        owner=row.get("owner"),
        unmapped_source_fields=policy or contract.UnmappedPolicy.ERROR,
        ignored_source_fields=_split(row.get("ignored")),
        row_filter=row.get("row_filter"),
        row_split=row.get("row_split"),
        key_version=key_version,
        key_hashing=hashing,
        key_case=case or contract.KeyCase.PRESERVE,
        key_namespace=row.get("key_namespace"),
        location=location,
        fields=tuple(_field(r, fields, problems) for r in fields.rows()),
    )


def _field(row: _Row, sheet: _Sheet, problems: _Problems) -> contract.FieldSpec:
    """Reads a field: a row of a mapping sheet."""
    location = sheet.location(row.number)

    required = False
    required_text = row.get("required")
    if required_text is not None:
        normalized = layout.normalize(required_text)
        if normalized in layout.YES:
            required = True
        elif normalized not in layout.NO:
            problems.append(
                contract.MappingProblem(
                    f"{required_text!r} is not Y or N",
                    _at(location, sheet.labels["required"]),
                )
            )

    target = row.get("target")
    if target is None:
        problems.append(
            contract.MappingProblem(
                "a target column is needed",
                _at(location, sheet.label("target", "Target column")),
            )
        )

    return contract.FieldSpec(
        target=target or "",
        sources=_split(row.get("sources")),
        target_type=row.get("target_type"),
        required=required,
        transform=row.get("transform"),
        params=_params(
            row.get("params"),
            _at(location, sheet.label("params", "Parameters")),
            problems,
        ),
        format=row.get("format"),
        default=row.get("default"),
        notes=row.get("notes"),
        primary_key=_primary_key(row, sheet, problems),
        references=row.get("references"),
        personal=_personal(row.get("personal")),
        location=location,
    )


def _personal(text: str | None) -> str | None:
    """The kind of personal data of a column, lowercase; checked when the
    mapping compiles."""
    return text.strip().lower() if text and text.strip() else None


def _primary_key(row: _Row, sheet: _Sheet, problems: _Problems) -> bool:
    """Reads the Key cell: PK, or blank for a column that is not the key."""
    text = row.get("key")
    if text is None:
        return False
    if layout.normalize(text) == layout.PRIMARY_KEY:
        return True
    problems.append(
        contract.MappingProblem(
            f"{text!r} is not PK (leave it empty for a column that is not the"
            " key)",
            _at(sheet.location(row.number), sheet.labels["key"]),
        )
    )
    return False


def _whole(row: _Row, key: str, sheet: _Sheet, problems: _Problems) -> int:
    """Reads a whole number >= 1; 1 when the cell is blank."""
    text = row.get(key) or "1"
    if text.isdigit() and int(text) >= 1:
        return int(text)
    problems.append(
        contract.MappingProblem(
            f"{text!r} is not a whole number >= 1",
            _at(sheet.location(row.number), sheet.labels[key]),
        )
    )
    return 1


def _lookup(
    worksheet: mapping_clients.WorksheetLike, file: str, problems: _Problems
) -> dict[str, str]:
    """Reads a lookup sheet: key in the first column, value in the second."""
    table: dict[str, str] = {}
    rows = worksheet.iter_rows(values_only=True)
    next(rows, None)  # The header.
    for number, values in enumerate(rows, start=2):
        key = _cell_text(values[0]) if values else None
        value = _cell_text(values[1]) if len(values) > 1 else None
        if key is None and value is None:
            continue
        location = _locate(file, worksheet.title, number)
        if key is None:
            problems.append(
                contract.MappingProblem("a lookup row needs a key", location)
            )
        elif key in table:
            problems.append(
                contract.MappingProblem(
                    f"key {key!r} is listed twice", location
                )
            )
        else:
            table[key] = value or ""
    return table


def _split(text: str | None) -> tuple[str, ...]:
    """Splits a list cell on commas, semicolons and new lines."""
    if text is None:
        return ()
    return tuple(
        part.strip() for part in _LIST_SEPARATORS.split(text) if part.strip()
    )


def _params(
    text: str | None, location: str, problems: _Problems
) -> dict[str, str]:
    """Reads a parameters cell: `separator=", "; skip=true`.

    Parameters are separated by `;` or new lines. Values are trimmed, unless
    quoted: a quoted value keeps its spaces and may hold `;` (`separator=";"`),
    and `""` in it is one quote.
    """
    params: dict[str, str] = {}
    if text is None:
        return params

    items, unclosed = _param_items(text)
    if unclosed:
        problems.append(
            contract.MappingProblem(
                "a quote is not closed: write values with quotes as"
                ' name="value"',
                location,
            )
        )
    for item, value, trailing in items:
        key, equals, _ = item.partition("=")
        key = key.strip()
        if not equals or not key:
            problems.append(
                contract.MappingProblem(
                    f"{item.strip()!r} should be written name=value", location
                )
            )
            continue
        if trailing:
            problems.append(
                contract.MappingProblem(
                    f"{item.strip()!r} has text after its closing quote: put"
                    ' it inside the quotes, or "" for a quote',
                    location,
                )
            )
        if key in params:
            problems.append(
                contract.MappingProblem(
                    f"parameter {key!r} is given twice", location
                )
            )
        params[key] = value
    return params


type _ParamItem = tuple[str, str, bool]


def _param_items(text: str) -> tuple[list[_ParamItem], bool]:
    """Splits a parameters cell into its items, outside quotes.

    Returns:
        Each item as written, with its value (unquoted, or trimmed) and
        whether text follows its closing quote; and whether a quote is left
        open.
    """
    items: list[_ParamItem] = []
    raw: list[str] = []  # The item as written.
    value: list[str] = []  # Its value, once `=` is met.
    in_value = quoted = was_quoted = trailing = False
    position = 0
    while position < len(text):
        char = text[position]
        position += 1
        if quoted:
            raw.append(char)
            if char == '"' and text[position : position + 1] == '"':
                raw.append(char)
                value.append(char)
                position += 1
            elif char == '"':
                quoted = False
            else:
                value.append(char)
            continue
        if char in _PARAM_SEPARATORS:
            _add_item(items, raw, value, was_quoted, trailing)
            raw, value, in_value, was_quoted = [], [], False, False
            trailing = False
            continue
        raw.append(char)
        if not in_value:
            in_value = char == "="
        elif char == '"' and not "".join(value).strip() and not was_quoted:
            quoted = was_quoted = True
            value = []
        elif was_quoted:
            trailing = trailing or not char.isspace()
        else:
            value.append(char)
    _add_item(items, raw, value, was_quoted, trailing)
    return items, quoted


def _add_item(
    items: list[_ParamItem],
    raw: list[str],
    value: list[str],
    was_quoted: bool,
    trailing: bool,
) -> None:
    """Adds an item to `items`, unless it is blank."""
    item = "".join(raw)
    if item.strip():
        text = "".join(value)
        items.append((item, text if was_quoted else text.strip(), trailing))


def _choice[T](
    text: str,
    choices: dict[str, T],
    location: str,
    problems: _Problems,
) -> T | None:
    """Reads a cell that only accepts some values."""
    value = choices.get(layout.normalize(text))
    if value is None:
        accepted = ", ".join(sorted(choices))
        problems.append(
            contract.MappingProblem(
                f"{text!r} is not one of: {accepted}", location
            )
        )
    return value


def _suggest_header(text: str, columns: dict[str, tuple[str, ...]]) -> str:
    names: list[str] = []
    for spellings in columns.values():
        names.extend(spellings[:2])
    match = difflib.get_close_matches(text, names, n=1, cutoff=0.6)
    return f". Did you mean {match[0]!r}?" if match else ""
