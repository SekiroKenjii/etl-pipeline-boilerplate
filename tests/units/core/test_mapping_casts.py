from __future__ import annotations

import datetime
import decimal

import pytest

from core import mapping
from core.mapping import casts


@pytest.mark.parametrize(
    ("raw", "kind", "length", "precision", "scale"),
    [
        ("VARCHAR(200)", casts.TypeKind.STRING, 200, None, None),
        ("nvarchar2 ( 50 )", casts.TypeKind.STRING, 50, None, None),
        ("TEXT", casts.TypeKind.STRING, None, None, None),
        ("NUMBER(10)", casts.TypeKind.INTEGER, None, 10, None),
        ("NUMBER(10,0)", casts.TypeKind.INTEGER, None, 10, None),
        ("NUMBER(10,2)", casts.TypeKind.DECIMAL, None, 10, 2),
        ("NUMBER", casts.TypeKind.DECIMAL, None, None, None),
        ("BIGINT", casts.TypeKind.INTEGER, None, None, None),
        ("DATE", casts.TypeKind.DATE, None, None, None),
        ("timestamp", casts.TypeKind.DATETIME, None, None, None),
        ("BIT", casts.TypeKind.BOOLEAN, None, None, None),
    ],
)
def test_parse_db_types(
    raw: str,
    kind: casts.TypeKind,
    length: int | None,
    precision: int | None,
    scale: int | None,
) -> None:
    parsed = mapping.parse_type(raw)

    assert (parsed.kind, parsed.max_length, parsed.precision, parsed.scale) == (
        kind,
        length,
        precision,
        scale,
    )


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        ("VARCHAR(10,2)", "one size"),
        ("MONEY", "unknown type"),
        ("NUMBER(2,5)", "scale cannot exceed"),
        ("DATE(3)", "takes no size"),
        ("VARCHAR(", "not a type"),
    ],
)
def test_reject_invalid_types(raw: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        mapping.parse_type(raw)


class TestCast:

    def test_text_longer_than_the_column(self) -> None:
        text = mapping.parse_type("VARCHAR(5)")

        with pytest.raises(
            mapping.CastError, match="6 characters, more than the 5"
        ):
            text.cast("abcdef")

    def test_integers(self) -> None:
        number = mapping.parse_type("NUMBER(3)")

        assert number.cast("042") == 42
        assert number.cast(7.0) == 7
        with pytest.raises(mapping.CastError, match="not a whole number"):
            number.cast("4.5")
        with pytest.raises(mapping.CastError, match="more than 3 digits"):
            number.cast("1000")

    def test_decimals_are_never_rounded_silently(self) -> None:
        money = mapping.parse_type("NUMBER(10,2)")

        assert money.cast("10.5") == decimal.Decimal("10.50")
        assert money.cast("10.5000") == decimal.Decimal("10.50")
        with pytest.raises(mapping.CastError, match="would be rounded"):
            money.cast("10.555")
        with pytest.raises(mapping.CastError, match="does not fit"):
            money.cast("123456789")  # 9 integer digits > 10 - 2

    @pytest.mark.parametrize("value", ["abc", "NaN", "Infinity", True])
    def test_not_numbers(self, value: object) -> None:
        money = mapping.parse_type("NUMBER(10,2)")

        with pytest.raises(mapping.CastError, match="not a number"):
            money.cast(value)

    def test_dates_in_the_format_bas_write(self) -> None:
        column = mapping.parse_type("DATE")

        assert column.cast("31/12/2020", "dd/MM/yyyy") == datetime.date(
            2020, 12, 31
        )
        assert column.cast("2020-12-31") == datetime.date(2020, 12, 31)
        with pytest.raises(
            mapping.CastError, match="does not match the format dd/MM/yyyy"
        ):
            column.cast("31/02/2020", "dd/MM/yyyy")

    def test_timestamps(self) -> None:
        column = mapping.parse_type("TIMESTAMP")

        assert column.cast(
            "01/02/2026 13:45", "dd/MM/yyyy HH:mm"
        ) == datetime.datetime(2026, 2, 1, 13, 45)

    def test_booleans(self) -> None:
        column = mapping.parse_type("BOOLEAN")

        assert [column.cast(v) for v in ("Y", "no", "1", "false")] == [
            True,
            False,
            True,
            False,
        ]
        with pytest.raises(mapping.CastError):
            column.cast("maybe")


def test_excel_style_formats_become_strftime() -> None:
    assert casts.to_strftime("dd/MM/yyyy HH:mm:ss") == "%d/%m/%Y %H:%M:%S"
    assert casts.to_strftime("yy-MM-dd") == "%y-%m-%d"
    assert casts.to_strftime("%d.%m.%Y") == "%d.%m.%Y"


@pytest.mark.parametrize(
    ("source_format", "expected"),
    [
        ("yyyyMMdd", "%Y%m%d"),
        ("yyyy-MM-dd'T'HH:mm", "%Y-%m-%dT%H:%M"),
        ("yyy", "%yy"),
        ("dd MMM yyyy", "%d %mM %Y"),
        ("EEE, dd", "EEE, %d"),
        ("Myyyy", "Myyyy"),
    ],
)
def test_letters_are_tokens_only_where_a_token_starts(
    source_format: str, expected: str
) -> None:
    assert casts.to_strftime(source_format) == expected
