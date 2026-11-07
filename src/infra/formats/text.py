"""Text record formats, with the standard library only.

They implement `contract.IRecordFormat`.
"""

from __future__ import annotations

from collections.abc import Iterator
import csv
import dataclasses
import io
import json
from typing import BinaryIO

from core import contract


@dataclasses.dataclass(frozen=True, slots=True)
class CsvFormat:
    """CSV with a header line; every value is text.

    Types are the transformer's job.

    Attributes:
        delimiter: The field separator.
        quotechar: The quote character.
        encoding: The text encoding; `utf-8-sig` also strips the byte order
            mark Excel likes to add.
    """

    delimiter: str = ","
    quotechar: str = '"'
    encoding: str = "utf-8-sig"

    def read(self, stream: BinaryIO) -> Iterator[contract.Row]:
        """See `contract.IRecordFormat.read`."""
        text = io.TextIOWrapper(stream, encoding=self.encoding, newline="")
        try:
            reader = csv.DictReader(
                text, delimiter=self.delimiter, quotechar=self.quotechar
            )
            for row in reader:
                yield dict(row)
        finally:
            # The caller owns `stream`: do not let the wrapper close it.
            text.detach()


@dataclasses.dataclass(frozen=True, slots=True)
class JsonLinesFormat:
    """One JSON object per line (https://jsonlines.org); blank lines skipped.

    Attributes:
        encoding: The text encoding.
    """

    encoding: str = "utf-8"

    def read(self, stream: BinaryIO) -> Iterator[contract.Row]:
        """See `contract.IRecordFormat.read`.

        Raises:
            FatalError: If a line is not a JSON object.
        """
        text = io.TextIOWrapper(stream, encoding=self.encoding)
        try:
            for number, line in enumerate(text, start=1):
                if not line.strip():
                    continue
                value = json.loads(line)
                if not isinstance(value, dict):
                    raise contract.FatalError(
                        f"line {number} is not a JSON object"
                    )
                yield value
        finally:
            text.detach()
