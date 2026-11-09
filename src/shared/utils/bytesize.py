"""Byte counts written with a unit, as settings hold them: `"256MiB"`."""

from __future__ import annotations

import decimal
import re

_UNITS: dict[str, int] = {
    "b": 1,
    "kb": 1000,
    "mb": 1000**2,
    "gb": 1000**3,
    "tb": 1000**4,
    "kib": 1024,
    "mib": 1024**2,
    "gib": 1024**3,
    "tib": 1024**4,
    "k": 1024,
    "m": 1024**2,
    "g": 1024**3,
    "t": 1024**4,
}
_SIZE = re.compile(r"^(\d+(?:\.\d+)?)\s*([a-zA-Z]*)$")


def parse_byte_size(value: str | int) -> int:
    """Returns the bytes `value` means.

    An integer is a count of bytes. A string is a number, whole or decimal,
    then a unit: `B`; `KB`, `MB`, `GB`, `TB` (powers of 1,000); `KiB`,
    `MiB`, `GiB`, `TiB` (powers of 1,024); `K`, `M`, `G`, `T` (powers of
    1,024, as `ls -h` and `ulimit` read them). The letters' case does not
    matter, nor does a space before the unit. A fraction of a byte is
    dropped: `"1.5KiB"` is 1,536 bytes, `"0.5"` is 0.

    Refused: a sign, an exponent (`"1e9"`), a bool, a float, an unknown or
    missing number.

    Raises:
        ValueError: If `value` is not a byte count.
    """
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ValueError(f"not a byte count: {value!r}")
    if isinstance(value, int):
        if value < 0:
            raise ValueError(f"a byte count is not negative: {value}")
        return value
    found = _SIZE.match(value.strip())
    if found is None:
        raise ValueError(f"not a byte count: {value!r} (e.g. 256MiB, 2GiB)")
    number, unit = found.groups()
    scale = _UNITS.get(unit.lower() or "b")
    if scale is None:
        raise ValueError(
            f"unknown unit {unit!r} in {value!r}: use B, KB, MB, GB, TB, KiB,"
            " MiB, GiB or TiB"
        )
    return int(decimal.Decimal(number) * scale)
