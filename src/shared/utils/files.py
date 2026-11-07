"""File system helpers."""

from __future__ import annotations

import os
import pathlib
import stat
import uuid


def atomic_write_text(
    path: pathlib.Path, text: str, encoding: str = "utf-8"
) -> None:
    """Replaces the content of a file atomically.

    Readers see the old content or the new one, never a half-written file,
    even if the process dies while writing.

    Args:
        path: The file to write; its parent directories are created.
        text: The new content.
        encoding: The text encoding.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    # Keeps an existing file's mode; a new one gets the umask's.
    try:
        mode: int | None = stat.S_IMODE(path.stat().st_mode)
    except FileNotFoundError:
        mode = None
    tmp = path.parent / f".{path.name}.{uuid.uuid4().hex}.tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o666)
    try:
        with os.fdopen(fd, "w", encoding=encoding) as file:
            file.write(text)
            file.flush()
            os.fsync(file.fileno())
        if mode is not None:
            os.chmod(tmp, mode)
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
