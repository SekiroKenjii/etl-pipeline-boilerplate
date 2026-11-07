"""Writing JSON Lines files (https://jsonlines.org)."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
import json
import pathlib
from typing import TextIO

from shared.utils import serialization


def append_json_lines(
    path: pathlib.Path, rows: Iterable[Mapping[str, object]]
) -> None:
    """Appends `rows` to `path`, one line each, and closes it.

    The file and its parent directories are created if needed; values JSON
    cannot encode go through `serialization.to_jsonable`. For occasional
    writes: nothing stays open between them.
    """
    writer = JsonLinesWriter()
    writer.open(path)
    try:
        writer.write(rows)
    finally:
        writer.close()


class JsonLinesWriter:
    """Appends JSON objects to a file, one per line."""

    def __init__(self) -> None:
        self._file: TextIO | None = None

    @property
    def is_open(self) -> bool:
        return self._file is not None

    def open(self, path: pathlib.Path) -> None:
        """Opens `path` for appending; its parent directories are created."""
        path.parent.mkdir(parents=True, exist_ok=True)
        self._file = path.open("a", encoding="utf-8")

    def write(self, rows: Iterable[Mapping[str, object]]) -> None:
        """Appends `rows`, then flushes.

        Values JSON cannot encode go through `serialization.to_jsonable`.

        Args:
            rows: The objects to append, one line each.

        Raises:
            RuntimeError: If the writer is not open.
        """
        if self._file is None:
            raise RuntimeError("JsonLinesWriter.write() called before open()")

        for row in rows:
            line = json.dumps(
                row, default=serialization.to_jsonable, ensure_ascii=False
            )
            self._file.write(line + "\n")
        self._file.flush()

    def close(self) -> None:
        """Closes the file; closing a closed writer does nothing."""
        if self._file is not None:
            self._file.close()
            self._file = None
