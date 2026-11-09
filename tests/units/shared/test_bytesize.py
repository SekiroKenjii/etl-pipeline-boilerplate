"""`parse_byte_size`: byte counts with a unit, as the settings hold them."""

from __future__ import annotations

import pytest

from shared import utils


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("256MiB", 256 * 1024**2),
        ("2GiB", 2 * 1024**3),
        ("512KB", 512_000),
        ("1MB", 1_000_000),
        ("1TB", 10**12),
        ("1TiB", 1024**4),
        ("1.5KiB", 1536),
        ("1 GiB", 1024**3),
        ("64mib", 64 * 1024**2),
        ("3M", 3 * 1024**2),
        ("2k", 2048),
        ("10B", 10),
        (" 10 ", 10),
        ("0", 0),
        ("0.5", 0),
        (1024, 1024),
        (0, 0),
    ],
)
def test_byte_counts_with_units(value: str | int, expected: int) -> None:
    assert utils.parse_byte_size(value) == expected


@pytest.mark.parametrize(
    "text",
    [
        "",
        " ",
        "MiB",
        "1e9",
        "-1",
        "+1",
        "1.",
        ".5",
        "1 2",
        "1GiBs",
        "1 Gi B",
        "1,000",
        "1_000",
        "0x10",
        "1 bit",
    ],
)
def test_nonsense_is_refused(text: str) -> None:
    with pytest.raises(ValueError, match="byte count|unit"):
        utils.parse_byte_size(text)


@pytest.mark.parametrize("value", [True, False, 1.5, None, b"1", [1]])
def test_other_types_are_refused(value: object) -> None:
    with pytest.raises(ValueError, match="not a byte count"):
        utils.parse_byte_size(value)  # type: ignore[arg-type]


def test_a_negative_count_is_refused() -> None:
    with pytest.raises(ValueError, match="not negative"):
        utils.parse_byte_size(-1)
