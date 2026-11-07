"""Record formats shared by every adapter: storage objects, queue payloads."""

from infra.formats.text import CsvFormat
from infra.formats.text import JsonLinesFormat

__all__ = [
    "CsvFormat",
    "JsonLinesFormat",
]
