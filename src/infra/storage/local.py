"""A local directory seen as an object store."""

from __future__ import annotations

from collections.abc import Iterator
import datetime
import pathlib
from typing import BinaryIO

from core import contract


class LocalObjectStore(contract.IObjectStore):
    """A local directory seen as an `IObjectStore`.

    Keys are POSIX paths relative to the root (`2026/10/orders.csv`), whatever
    the operating system. The version of a file is its modification time and
    size.

    Attributes:
        root: The directory.
    """

    def __init__(self, root: pathlib.Path | str) -> None:
        self.root = pathlib.Path(root)

    def list(self, prefix: str = "") -> Iterator[contract.ObjectInfo]:
        """See `contract.IObjectStore.list`.

        Raises:
            FileNotFoundError: If the root is not a directory.
        """
        if not self.root.is_dir():
            raise FileNotFoundError(f"{self.root} is not a directory")

        objects = []
        for path in self.root.rglob("*"):
            key = path.relative_to(self.root).as_posix()
            if path.is_file() and key.startswith(prefix):
                stat = path.stat()
                objects.append(
                    contract.ObjectInfo(
                        key=key,
                        size=stat.st_size,
                        updated_at=datetime.datetime.fromtimestamp(
                            stat.st_mtime, datetime.timezone.utc
                        ),
                        version=f"{stat.st_mtime_ns}-{stat.st_size}",
                    )
                )

        yield from sorted(objects, key=lambda obj: obj.key)

    def open(self, obj: contract.ObjectInfo) -> BinaryIO:
        return (self.root / obj.key).open("rb")
