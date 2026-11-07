from __future__ import annotations

from collections.abc import Mapping, Sequence
import datetime
import pathlib

import openpyxl
import pytest

from core import contract
from core import mapping
from infra import mapping as infra_mapping

INDEX_HEADER = ["Mapping", "Source table", "Target table", "Version", "Status"]
FIELD_HEADER = [
    "Target column",
    "Target type",
    "Required",
    "Source columns",
    "Transform",
    "Parameters",
    "Format",
    "Default",
]


def _workbook(
    path: pathlib.Path, sheets: Mapping[str, Sequence[Sequence[object]]]
) -> pathlib.Path:
    workbook = openpyxl.Workbook()
    workbook.remove(workbook.active)  # type: ignore[arg-type]
    for title, rows in sheets.items():
        sheet = workbook.create_sheet(title)
        for row in rows:
            sheet.append(row)
    workbook.save(path)
    return path


def _problems(path: pathlib.Path) -> list[str]:
    reader = infra_mapping.XlsxMappingReader()
    with pytest.raises(contract.MappingDefinitionError) as error:
        reader.read(path)
    return [str(problem) for problem in error.value.problems]


def _transforms() -> list[tuple[str, str]]:
    return [
        (t.name, t.description)
        for t in mapping.TransformRegistry().transforms()
    ]


def test_the_template_reads_back_and_validates(
    tmp_path: pathlib.Path,
) -> None:
    path = tmp_path / "crm.xlsx"
    infra_mapping.write_template(
        path,
        transforms=_transforms(),
        type_names=mapping.SUPPORTED_TYPE_NAMES,
    )

    catalog = infra_mapping.XlsxMappingReader().read(path)

    assert [m.name for m in catalog.mappings] == ["customer", "order"]
    (reference,) = [f for f in catalog.get("order").fields if f.references]
    assert (reference.target, reference.references) == (
        "customer_key",
        "customer",
    )
    assert catalog.lookups == {
        "countries": {"VN": "VN", "VIETNAM": "VN", "US": "US"}
    }
    assert mapping.validate_catalog(catalog, mapping.TransformRegistry()) == []


def test_the_template_offers_drop_down_lists(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "crm.xlsx"
    infra_mapping.write_template(
        path, transforms=_transforms(), type_names=mapping.SUPPORTED_TYPE_NAMES
    )

    sheet = openpyxl.load_workbook(path)["customer"]
    ranges = {
        str(v.sqref): v.formula1 for v in sheet.data_validations.dataValidation
    }

    assert ranges["E2:E1000"].startswith("=_lists!$A$1")  # Transform
    assert ranges["C2:C1000"].startswith("=_lists!$B$1")  # Required


def test_excel_quirks_are_normalized(tmp_path: pathlib.Path) -> None:
    path = _workbook(
        tmp_path / "crm.xlsx",
        {
            "Mappings": [
                [
                    "Mapping",
                    "Source table",
                    "Target table",
                    "  VERSION ",
                    "Status",
                    "  Ignored  SOURCE fields ",
                ],
                [
                    "customer",
                    "legacy.CUST",
                    "crm.customer",
                    3.0,
                    "Approved",
                    "FLAG; OLD_ID",
                ],
            ],
            "customer": [
                [
                    "Target column",
                    "Target type",
                    "Required",
                    "source column",
                    "Transform",
                    "Parameters",
                    "Format",
                    "Default",
                ],
                ["id", "NUMBER(10)", "x", "CUST_NO", None, None, None, None],
                [None, None, None, None, None, None, None, None],  # blank row
                [
                    "name",
                    "VARCHAR(200)",
                    "yes",
                    "FIRST_NM,\nLAST_NM",
                    " concat ",
                    'separator=" "',
                    None,
                    None,
                ],
                [
                    "since",
                    "DATE",
                    "N",
                    "SINCE",
                    None,
                    None,
                    "dd/MM/yyyy",
                    datetime.datetime(2000, 1, 1),
                ],
                ["score", "NUMBER(3)", None, "SCORE", None, None, None, 0.0],
            ],
        },
    )

    catalog = infra_mapping.XlsxMappingReader().read(path)
    (spec,) = catalog.mappings

    assert (spec.version, spec.status, spec.ignored_source_fields) == (
        3,
        contract.MappingStatus.APPROVED,
        ("FLAG", "OLD_ID"),
    )
    assert (
        spec.unmapped_source_fields is contract.UnmappedPolicy.ERROR
    )  # the safe default
    by_target = {f.target: f for f in spec.fields}
    assert list(by_target) == ["id", "name", "since", "score"]
    assert by_target["id"].required
    assert by_target["name"].required
    assert by_target["name"].sources == ("FIRST_NM", "LAST_NM")
    assert (by_target["name"].transform, by_target["name"].params) == (
        "concat",
        {"separator": " "},
    )
    assert by_target["since"].default == "2000-01-01"
    assert by_target["score"].default == "0"
    assert by_target["name"].location == "crm.xlsx › customer › row 4"
    # Problems name a column as the workbook does.
    assert catalog.labels["sources"] == "source column"


def test_every_structural_problem_is_reported_at_once(
    tmp_path: pathlib.Path,
) -> None:
    path = _workbook(
        tmp_path / "crm.xlsx",
        {
            "Mappings": [
                INDEX_HEADER,
                ["customer", "legacy.CUST", "crm.customer", "v2", "done"],
                ["orders", "legacy.ORD", "crm.order", 1, "draft"],
            ],
            "customer": [
                [*FIELD_HEADER, "Tranform"],
                [
                    "id",
                    "NUMBER(10)",
                    "maybe",
                    "CUST_NO",
                    None,
                    "separator",
                    None,
                    None,
                ],
            ],
            "lookup_countries": [
                ["Key", "Value"],
                ["VN", "VN"],
                ["VN", "Viet Nam"],
            ],
            "Sheet1": [["leftover"]],
        },
    )

    assert _problems(path) == [
        (
            "crm.xlsx › customer › row 1: unknown column 'Tranform'. Did you"
            " mean 'Transform'?"
        ),
        (
            "crm.xlsx › Mappings › row 2 › Version: version 'v2' is not a whole"
            " number"
        ),
        (
            "crm.xlsx › Mappings › row 2 › Status: 'done' is not one of:"
            " approved, draft"
        ),
        "crm.xlsx › customer › row 2 › Required: 'maybe' is not Y or N",
        (
            "crm.xlsx › customer › row 2 › Parameters: 'separator' should be"
            " written name=value"
        ),
        "crm.xlsx › Mappings › row 3: no sheet named 'orders' for this mapping",
        "crm.xlsx › lookup_countries › row 3: key 'VN' is listed twice",
        (
            "crm.xlsx › Sheet1: sheet 'Sheet1' is not listed in 'Mappings'"
            " (rename it lookup_<name> if it is a lookup table)"
        ),
    ]


def test_a_workbook_needs_its_index_sheet(tmp_path: pathlib.Path) -> None:
    path = _workbook(tmp_path / "crm.xlsx", {"customer": [FIELD_HEADER]})

    assert _problems(path) == [
        "crm.xlsx: no 'Mappings' sheet listing the mappings"
    ]


def test_required_columns_are_checked(tmp_path: pathlib.Path) -> None:
    path = _workbook(
        tmp_path / "crm.xlsx",
        {
            "Mappings": [
                ["Mapping", "Source table"],
                ["customer", "legacy.CUST"],
            ],
            "customer": [FIELD_HEADER],
        },
    )

    assert _problems(path) == [
        "crm.xlsx › Mappings › row 1: missing column 'Target table'"
    ]


def test_row_hooks_are_read_from_the_index_sheet(
    tmp_path: pathlib.Path,
) -> None:
    path = _workbook(
        tmp_path / "crm.xlsx",
        {
            "Mappings": [
                [
                    "Mapping",
                    "Source table",
                    "Target table",
                    "row FILTER",
                    "Row split",
                ],
                [
                    "phone",
                    "legacy.CUST",
                    "crm.phone",
                    "customer.not_deleted",
                    "customer.phones",
                ],
            ],
            "phone": [
                ["Target column", "Source columns"],
                ["number", "NUMBER"],
            ],
        },
    )

    catalog = infra_mapping.XlsxMappingReader().read(path)
    (spec,) = catalog.mappings

    assert (spec.row_filter, spec.row_split) == (
        "customer.not_deleted",
        "customer.phones",
    )
    assert catalog.labels["row_filter"] == "row FILTER"


def test_the_template_lists_the_registered_row_hooks(
    tmp_path: pathlib.Path,
) -> None:
    path = tmp_path / "crm.xlsx"
    infra_mapping.write_template(
        path,
        transforms=_transforms(),
        type_names=mapping.SUPPORTED_TYPE_NAMES,
        row_filters=["customer.not_deleted"],
        row_splitters=["customer.phones"],
    )

    book = openpyxl.load_workbook(path)
    ranges = {
        str(v.sqref): v.formula1
        for v in book["Mappings"].data_validations.dataValidation
    }

    assert ranges["I2:I200"] == "=_lists!$E$1:$E$1"
    assert ranges["J2:J200"] == "=_lists!$F$1:$F$1"
    assert (book["_lists"]["E1"].value, book["_lists"]["F1"].value) == (
        "customer.not_deleted",
        "customer.phones",
    )


def test_key_columns_are_read(tmp_path: pathlib.Path) -> None:
    path = _workbook(
        tmp_path / "crm.xlsx",
        {
            "Mappings": [
                [
                    "Mapping",
                    "Source table",
                    "Target table",
                    "Key version",
                    "Key hashing",
                    "Key case",
                ],
                [
                    "customer",
                    "legacy.CUST",
                    "crm.customer",
                    2.0,
                    "HMAC",
                    "Upper",
                ],
                ["order", "legacy.ORD", "crm.order", None, None, None],
            ],
            "customer": [
                ["Target column", "Source columns", "Key"],
                ["customer_key", "CUST_NO", "pk"],
            ],
            "order": [
                ["Target column", "Source columns", "Key", "References"],
                ["order_key", "ORD_NO", "PK", None],
                ["customer_key", "CUST_NO", None, "customer"],
            ],
        },
    )

    catalog = infra_mapping.XlsxMappingReader().read(path)
    customer, order = catalog.get("customer"), catalog.get("order")

    assert (
        customer.key_version,
        customer.key_hashing,
        customer.key_case,
    ) == (2, contract.KeyHashing.HMAC, contract.KeyCase.UPPER)
    # Blank: the default, HMAC where a key secret is configured.
    assert (order.key_version, order.key_hashing, order.key_case) == (
        1,
        None,
        contract.KeyCase.PRESERVE,
    )
    assert customer.fields[0].primary_key
    assert [(f.target, f.primary_key, f.references) for f in order.fields] == [
        ("order_key", True, None),
        ("customer_key", False, "customer"),
    ]


def test_invalid_key_cells(tmp_path: pathlib.Path) -> None:
    path = _workbook(
        tmp_path / "crm.xlsx",
        {
            "Mappings": [
                [*INDEX_HEADER, "Key version", "Key hashing", "Key case"],
                [
                    "customer",
                    "legacy.CUST",
                    "crm.customer",
                    1,
                    "draft",
                    0,
                    "sha1",
                    "title",
                ],
            ],
            "customer": [
                ["Target column", "Source columns", "Key"],
                ["id", "CUST_NO", "Y"],
            ],
        },
    )

    assert _problems(path) == [
        (
            "crm.xlsx › Mappings › row 2 › Key version: '0' is not a whole"
            " number >= 1"
        ),
        (
            "crm.xlsx › Mappings › row 2 › Key hashing: 'sha1' is not one of:"
            " hmac, uuid5"
        ),
        (
            "crm.xlsx › Mappings › row 2 › Key case: 'title' is not one of:"
            " lower, preserve, upper"
        ),
        (
            "crm.xlsx › customer › row 2 › Key: 'Y' is not PK (leave it empty"
            " for a column that is not the key)"
        ),
    ]
