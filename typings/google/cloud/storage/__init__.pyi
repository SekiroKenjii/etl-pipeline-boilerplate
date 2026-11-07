"""Stubs for the part of google-cloud-storage this project uses.

The library ships no type information. These stubs give type checkers (mypy,
pyright / Pylance) the signatures `infra.storage.gcs` relies on; extend them
when an adapter starts using more of the library.
"""

from collections.abc import Iterator
import datetime
from typing import Any, BinaryIO

class Blob:
    name: str
    size: int | None
    generation: int | None
    updated: datetime.datetime | None
    def reload(self) -> None: ...
    def open(self, mode: str = ..., **kwargs: Any) -> BinaryIO: ...

class Bucket:
    name: str
    def blob(
        self,
        blob_name: str,
        chunk_size: int | None = ...,
        encryption_key: bytes | None = ...,
        kms_key_name: str | None = ...,
        generation: int | None = ...,
    ) -> Blob: ...

class Client:
    def __init__(
        self,
        project: str | None = ...,
        credentials: Any = ...,
        **kwargs: Any,
    ) -> None: ...
    @classmethod
    def create_anonymous_client(cls) -> Client: ...
    def list_blobs(
        self,
        bucket_or_name: str | Bucket,
        max_results: int | None = ...,
        page_token: str | None = ...,
        prefix: str | None = ...,
        delimiter: str | None = ...,
        **kwargs: Any,
    ) -> Iterator[Blob]: ...
    def bucket(
        self, bucket_name: str, user_project: str | None = ...
    ) -> Bucket: ...
    def close(self) -> None: ...

__version__: str
