"""What the storage adapters use of the cloud SDKs' objects: Google Cloud
Storage's client, buckets and blobs, faked in the tests."""

from __future__ import annotations

from collections.abc import Callable, Iterator
import datetime
from typing import BinaryIO, Protocol

__all__ = [
    "GcsBlobLike",
    "GcsBucketLike",
    "GcsClient",
    "GcsClientFactory",
    "GcsListedBlobLike",
]


class GcsListedBlobLike(Protocol):
    """The part of a listed `storage.Blob` this adapter uses."""

    @property
    def name(self) -> str:
        ...

    @property
    def size(self) -> int | None:
        ...

    @property
    def generation(self) -> int | None:
        ...

    @property
    def updated(self) -> datetime.datetime | None:
        ...


class GcsBlobLike(Protocol):
    """The part of a `storage.Blob` read this adapter uses."""

    def reload(self) -> None:
        """Fetches its metadata: raises NotFound if it does not exist."""
        ...

    def open(self, mode: str, /) -> BinaryIO:
        """Opens it."""
        ...


class GcsBucketLike(Protocol):
    """The part of a `storage.Bucket` this adapter uses."""

    def blob(
        self, blob_name: str, /, *, generation: int | None = ...
    ) -> GcsBlobLike:
        """Returns a handle on a blob, at a generation."""
        ...


class GcsClient(Protocol):
    """The part of `storage.Client` this adapter uses."""

    def list_blobs(
        self, bucket_or_name: str, *, prefix: str | None = ...
    ) -> Iterator[GcsListedBlobLike]:
        """Yields the blobs of a bucket, by name."""
        ...

    def bucket(self, bucket_name: str) -> GcsBucketLike:
        """Returns a handle on a bucket."""
        ...

    def close(self) -> None:
        """Releases the connections of the client."""
        ...


type GcsClientFactory = Callable[[], GcsClient]
