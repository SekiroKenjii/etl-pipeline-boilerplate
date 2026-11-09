"""Text from anywhere (data, exceptions) shown inside a line of a document."""

from __future__ import annotations

import unicodedata

# Kept: a tab breaks no line, and no document format chokes on it.
_KEPT = frozenset("\t")


def one_line(text: str) -> str:
    """`text` with its line breaks and control characters escaped.

    `a\\nb` becomes `a\\\\nb`, `\\x1b` becomes `\\\\x1b`: text that cannot
    start a line of a Markdown report (a heading, a table row), and that a
    workbook can hold.
    """
    if text.isprintable():
        return text
    return "".join(_escaped(char) if _breaks(char) else char for char in text)


def _breaks(char: str) -> bool:
    if char in _KEPT:
        return False
    return unicodedata.category(char) in ("Cc", "Zl", "Zp")


def _escaped(char: str) -> str:
    return char.encode("unicode_escape").decode("ascii")
