"""The port of file and blob stores: local directory, GCS, S3, a CDN..."""

from __future__ import annotations

import abc
from collections.abc import Iterator
import dataclasses
import datetime
from typing import BinaryIO


@dataclasses.dataclass(frozen=True, slots=True)
class ObjectInfo:
    """An object (a file, a blob) in a store.

    Attributes:
        key: The path of the object in the store.
        size: Its size in bytes.
        updated_at: When it was last written.
        version: Changes whenever the content changes (an etag, a GCS
            generation, mtime and size...): it tells an object already
            processed from a new one.
    """

    key: str
    size: int
    updated_at: datetime.datetime
    version: str


class IObjectStore(abc.ABC):
    """The minimal surface of a file or blob store.

    A store may hold clients, opened on first use and released by `close`.
    """

    @abc.abstractmethod
    def list(self, prefix: str = "") -> Iterator[ObjectInfo]:
        """Yields the objects whose key starts with `prefix`, by key order."""

    @abc.abstractmethod
    def open(self, obj: ObjectInfo) -> BinaryIO:
        """Returns a binary stream on the content; the caller closes it."""

    def close(self) -> None:
        """Releases the clients of the store, if any; the next use opens
        others. Does nothing by default."""
