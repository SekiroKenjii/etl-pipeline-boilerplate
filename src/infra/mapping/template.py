"""Writing a mapping workbook to start from."""

from __future__ import annotations

from collections.abc import Sequence
import dataclasses
import pathlib

import openpyxl
from openpyxl import styles
from openpyxl.worksheet import datavalidation
from openpyxl.worksheet import worksheet as openpyxl_worksheet

from core import contract
from infra.mapping import layout

_README: list[str] = [
    "How to fill this workbook",
    (
        "1. List each mapping in the Mappings sheet; give each one a sheet"
        " named after it."
    ),
    (
        "2. One row per target column. Pick Required, Transform and Status"
        " from the lists."
    ),
    (
        "3. Several source columns: separate them with commas, and pick a"
        " transform. A field of a nested document: address.city."
    ),
    (
        "4. Parameters: name=value; name=value (put a value in quotes to"
        " keep its spaces)."
    ),
    "5. Format: how dates are written in the source, e.g. dd/MM/yyyy HH:mm.",
    (
        "6. Lookup tables: sheets named lookup_<name>, key in column A,"
        " value in column B."
    ),
    (
        "7. Rules that no transform covers: describe them in Notes; a"
        " developer adds a custom transform (domain.name) that you then"
        " pick in Transform."
    ),
    (
        "8. Row filter / Row split (Mappings sheet): drop some source rows,"
        " or turn one source row into several, with a rule a developer"
        " registered. explode(addresses) makes one row per element of an"
        " array (documents); addresses# is the element's position."
    ),
    (
        "9. Keys: mark the surrogate key column PK (its source columns are"
        " the natural key); a column pointing to another mapping names it"
        " in References."
    ),
    (
        "10. Key case (Mappings sheet): if the source ignores case (SQL"
        " Server, MySQL), pick upper or lower, so that c001 and C001 stay"
        " one key."
    ),
    (
        "11. Personal data: pick the kind of the personal columns (name,"
        " email, phone...). Environments that must not hold personal"
        " data (UAT) get fake values instead."
    ),
    (
        "12. Key hashing (Mappings sheet): leave it empty for hmac where a"
        " key secret is configured; uuid5 only for natural keys holding no"
        " personal data."
    ),
    (
        "13. Key namespace (Mappings sheet): empty, keys are derived from"
        " the source and target tables, and renaming either changes every"
        " key. Before renaming one, write the old <source>:<target> here."
    ),
]
_TITLES: dict[str, str] = {
    "readme": "README",
    "transforms": "Transforms",
    "types": "Target types",
    "pick": "Pick a value from the list",
}
_LOOKUP_HEADERS = ["Key", "Value"]

# Their targets are prefixed with the module's name: a target names the
# entity of the surrogate keys, shared by every module of the key registry.
_EXAMPLE_INDEX: list[dict[str, str]] = [
    {
        "mapping": "customer",
        "source": "legacy.CUST_MASTER",
        "target": "customer",
        "ignored": "LEGACY_FLAG",
    },
    {"mapping": "order", "source": "legacy.ORD_MASTER", "target": "order"},
]
_EXAMPLE_FIELDS: dict[str, list[dict[str, str]]] = {
    "customer": [
        {
            "target": "customer_id",
            "target_type": "NUMBER(10)",
            "required": "Y",
            "sources": "CUST_NO",
        },
        {
            "target": "full_name",
            "target_type": "VARCHAR(200)",
            "required": "Y",
            "sources": "FIRST_NM, LAST_NM",
            "transform": "concat",
            "params": 'separator=" "',
        },
        {
            "target": "status",
            "target_type": "VARCHAR(20)",
            "required": "N",
            "sources": "STAT_CD",
            "transform": "value_map",
            "params": "A=active; I=inactive",
            "default": "unknown",
        },
        {
            "target": "created_at",
            "target_type": "DATE",
            "required": "N",
            "sources": "CRT_DT",
            "format": "dd/MM/yyyy",
        },
        {
            "target": "country",
            "target_type": "VARCHAR(2)",
            "required": "N",
            "sources": "CTRY",
            "transform": "lookup",
            "params": "table=countries",
        },
        {
            "target": "customer_key",
            "target_type": "UUID",
            "sources": "CUST_NO",
            "key": "PK",
        },
    ],
    "order": [
        {
            "target": "order_key",
            "target_type": "UUID",
            "sources": "ORD_NO",
            "key": "PK",
        },
        {
            "target": "customer_key",
            "target_type": "UUID",
            "required": "Y",
            "sources": "CUST_NO",
            "references": "customer",
        },
        {
            "target": "total",
            "target_type": "NUMBER(12,2)",
            "required": "Y",
            "sources": "AMT",
        },
        {
            "target": "ordered_at",
            "target_type": "DATE",
            "required": "N",
            "sources": "ORD_DT",
            "format": "dd/MM/yyyy",
        },
    ],
}

_HEADER_FONT = styles.Font(bold=True, color="FFFFFF")
_HEADER_FILL = styles.PatternFill("solid", fgColor="305496")


@dataclasses.dataclass(frozen=True, slots=True)
class _Lists:
    """The values of the drop-down lists, one column each in `_lists`."""

    transforms: list[str]
    yes_no: list[str]
    statuses: list[str]
    policies: list[str]
    row_filters: list[str]
    row_splitters: list[str]
    key_marks: list[str]
    hashings: list[str]
    cases: list[str]
    personal: list[str]

    def columns(self) -> list[list[str]]:
        """Returns the lists in the order of the `_lists` columns (A, B...)."""
        return [
            self.transforms,
            self.yes_no,
            self.statuses,
            self.policies,
            self.row_filters,
            self.row_splitters,
            self.key_marks,
            self.hashings,
            self.cases,
            self.personal,
        ]


def write_template(
    path: pathlib.Path,
    *,
    transforms: Sequence[tuple[str, str]],
    type_names: Sequence[str],
    row_filters: Sequence[str] = (),
    row_splitters: Sequence[str] = (),
    personal_kinds: Sequence[str] = (),
    module: str = "crm",
) -> None:
    """Writes a workbook to start from.

    Instructions, example mappings that validate as they are, a lookup table,
    and drop-down lists for the cells that only accept names.

    Args:
        path: Where to write the workbook.
        transforms: The (name, description) of every transform, built-in or
            custom.
        type_names: The supported target types, listed in the instructions.
        row_filters: The registered row filters.
        row_splitters: The registered row splitters.
        personal_kinds: The kinds of personal data, for the Personal data
            column.
        module: The module the workbook is for: the example mappings
            target `<module>.customer` and `<module>.order`.
    """
    lists = _Lists(
        transforms=[name for name, _ in transforms],
        yes_no=["Y", "N"],
        statuses=["draft", "approved"],
        policies=["error", "ignore"],
        row_filters=list(row_filters),
        row_splitters=list(row_splitters),
        key_marks=["PK"],
        hashings=[hashing.value for hashing in contract.KeyHashing],
        cases=["preserve", "upper", "lower"],
        personal=list(personal_kinds),
    )

    workbook = openpyxl.Workbook()
    _write_readme(workbook, transforms, type_names)
    _write_lists(workbook, lists)
    index = _write_index(workbook, lists, module)
    _write_field_sheets(workbook, index.title, lists)
    _write_lookup(workbook)

    path.parent.mkdir(parents=True, exist_ok=True)
    workbook.save(path)


def _write_readme(
    workbook: openpyxl.Workbook,
    transforms: Sequence[tuple[str, str]],
    type_names: Sequence[str],
) -> None:
    """Writes the instructions, with the transforms and types available."""
    readme = workbook.active
    if readme is None:
        readme = workbook.create_sheet()
    readme.title = _TITLES["readme"]
    title, *steps = _README
    readme.append([title])
    readme["A1"].font = styles.Font(bold=True, size=14)
    for step in steps:
        readme.append([step])
    readme.append([])
    readme.append([_TITLES["transforms"]])
    for name, description in transforms:
        readme.append([name, description])
    readme.append([])
    readme.append([_TITLES["types"], ", ".join(type_names)])
    readme.column_dimensions["A"].width = 90


def _write_lists(workbook: openpyxl.Workbook, lists: _Lists) -> None:
    """Writes the hidden sheet the drop-down lists read their values from."""
    sheet = workbook.create_sheet("_lists")
    sheet.sheet_state = "hidden"
    for column, values in enumerate(lists.columns(), start=1):
        for row, value in enumerate(values, start=1):
            sheet.cell(row=row, column=column, value=value)


def _write_index(
    workbook: openpyxl.Workbook, lists: _Lists, module: str
) -> openpyxl_worksheet.Worksheet:
    """Writes the sheet listing the mappings, with the example mappings."""
    sheet: openpyxl_worksheet.Worksheet = workbook.create_sheet(
        layout.INDEX_SHEET[0]
    )
    keys = list(layout.INDEX_COLUMNS)
    _header(sheet, layout.headers(layout.INDEX_COLUMNS))

    defaults = {
        "version": "1",
        "status": lists.statuses[0],
        "unmapped": lists.policies[0],
        "key_version": "1",
        "key_hashing": "",
        "key_case": lists.cases[0],
    }
    for example in _EXAMPLE_INDEX:
        cells = {
            **defaults,
            **example,
            "target": f"{module}.{example['target']}",
        }
        sheet.append([cells.get(key, "") for key in keys])

    pick = _TITLES["pick"]
    _list_dropdown(sheet, "E2:E200", "C", len(lists.statuses), pick)
    _list_dropdown(sheet, "G2:G200", "D", len(lists.policies), pick)
    if lists.row_filters:
        _list_dropdown(sheet, "I2:I200", "E", len(lists.row_filters), pick)
    if lists.row_splitters:
        _list_dropdown(sheet, "J2:J200", "F", len(lists.row_splitters), pick)
    _list_dropdown(sheet, "L2:L200", "H", len(lists.hashings), pick)
    _list_dropdown(sheet, "M2:M200", "I", len(lists.cases), pick)
    return sheet


def _write_field_sheets(
    workbook: openpyxl.Workbook,
    index_title: str,
    lists: _Lists,
) -> None:
    """Writes one sheet per example mapping, with its fields."""
    keys = list(layout.FIELD_COLUMNS)
    pick = _TITLES["pick"]
    for name, fields in _EXAMPLE_FIELDS.items():
        sheet = workbook.create_sheet(name)
        _header(sheet, layout.headers(layout.FIELD_COLUMNS))
        for field in fields:
            sheet.append([field.get(key, "") for key in keys])

        _list_dropdown(sheet, "C2:C1000", "B", len(lists.yes_no), pick)
        _list_dropdown(
            sheet, "E2:E1000", "A", max(1, len(lists.transforms)), pick
        )
        _list_dropdown(sheet, "J2:J1000", "G", len(lists.key_marks), pick)
        if lists.personal:
            _list_dropdown(sheet, "L2:L1000", "J", len(lists.personal), pick)
        _dropdown(sheet, "K2:K1000", f"='{index_title}'!$A$2:$A$200", pick)


def _write_lookup(workbook: openpyxl.Workbook) -> None:
    sheet = workbook.create_sheet(f"{layout.LOOKUP_PREFIX}countries")
    _header(sheet, _LOOKUP_HEADERS)
    for key, value in [("VN", "VN"), ("VIETNAM", "VN"), ("US", "US")]:
        sheet.append([key, value])


def _header(sheet: openpyxl_worksheet.Worksheet, labels: list[str]) -> None:
    """Writes a styled, frozen header row, and sizes the columns."""
    sheet.append(labels)
    for cell in sheet[1]:
        cell.font = _HEADER_FONT
        cell.fill = _HEADER_FILL
    sheet.freeze_panes = "A2"
    for index, label in enumerate(labels):
        letter = chr(ord("A") + index)
        sheet.column_dimensions[letter].width = max(16, len(label) + 4)


def _list_dropdown(
    sheet: openpyxl_worksheet.Worksheet,
    cells: str,
    letter: str,
    count: int,
    error: str,
) -> None:
    """Restricts `cells` to the values of column `letter` of `_lists`."""
    _dropdown(sheet, cells, f"=_lists!${letter}$1:${letter}${count}", error)


def _dropdown(
    sheet: openpyxl_worksheet.Worksheet, cells: str, source: str, error: str
) -> None:
    validation = datavalidation.DataValidation(
        type="list", formula1=source, allow_blank=True
    )
    validation.error = error
    sheet.add_data_validation(validation)
    validation.add(cells)
