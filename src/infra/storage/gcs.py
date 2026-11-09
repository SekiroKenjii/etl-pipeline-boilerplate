"""A Google Cloud Storage bucket seen as an object store.

Credentials come from Application Default Credentials: `gcloud auth
application-default login` locally, the service account on GCP.
"""

from __future__ import annotations

from collections.abc import Iterator
import threading
from typing import BinaryIO

from google.api_core import exceptions as google_exceptions
from google.cloud import storage

from core import contract
from infra.storage import clients as storage_clients


def default_client_factory(
    project: str | None = None,
) -> storage_clients.GcsClientFactory:
    """Returns a factory of `storage.Client`s for `project`."""

    def create() -> storage_clients.GcsClient:
        return storage.Client(project=project)

    return create


class GcsObjectStore(contract.IObjectStore):
    """A Google Cloud Storage bucket seen as an `IObjectStore`.

    Keys are blob names; the version is the generation, which every write
    changes, so an overwritten object is processed again. `open` reads exactly
    the listed generation, and raises `RetryableError` if it was overwritten or
    deleted since. Each thread gets its own client
    (`ObjectStoreExtractor(concurrency=N)`).

    Attributes:
        bucket: The bucket name.
    """

    def __init__(
        self,
        bucket: str,
        *,
        project: str | None = None,
        client_factory: storage_clients.GcsClientFactory | None = None,
    ) -> None:
        """Initializes the store; clients are created on first use.

        Args:
            bucket: The bucket name.
            project: The Google Cloud project of the default clients.
            client_factory: Makes the clients, to control the project,
                credentials, retries, an emulator...
        """
        self.bucket = bucket
        self._factory = client_factory or default_client_factory(project)
        self._local = threading.local()
        self._clients: list[storage_clients.GcsClient] = []
        self._lock = threading.Lock()

    @property
    def name(self) -> str:
        return f"{type(self).__name__}(gs://{self.bucket})"

    def close(self) -> None:
        """Closes the clients of every thread; the next use opens others."""
        with self._lock:
            clients, self._clients = self._clients, []
        self._local = threading.local()
        for client in clients:
            client.close()

    def list(self, prefix: str = "") -> Iterator[contract.ObjectInfo]:
        # GCS lists names in lexicographic order, the order the port wants.
        for blob in self._client().list_blobs(
            self.bucket, prefix=prefix or None
        ):
            if blob.name.endswith("/"):
                # A "folder" placeholder, created by the console.
                continue
            if blob.updated is None:
                raise contract.FatalError(
                    f"gs://{self.bucket}/{blob.name} is listed without its"
                    " update time"
                )
            yield contract.ObjectInfo(
                key=blob.name,
                size=int(blob.size or 0),
                updated_at=blob.updated,
                version=str(blob.generation),
            )

    def open(self, obj: contract.ObjectInfo) -> BinaryIO:
        """See `contract.IObjectStore.open`.

        Raises:
            RetryableError: If the listed generation no longer exists.
        """
        blob = (
            self._client()
            .bucket(self.bucket)
            .blob(obj.key, generation=int(obj.version))
        )
        try:
            # Downloads are lazy: check the generation exists now, to say why.
            blob.reload()
        except google_exceptions.NotFound as exc:
            raise contract.RetryableError(
                f"gs://{self.bucket}/{obj.key} generation {obj.version} is"
                " gone: it changed while the run was reading it"
            ) from exc
        return blob.open("rb")

    def _client(self) -> storage_clients.GcsClient:
        """Returns the client of the current thread, creating it if needed."""
        client: storage_clients.GcsClient | None = getattr(
            self._local, "client", None
        )
        if client is None:
            client = self._factory()
            self._local.client = client
            with self._lock:
                self._clients.append(client)
        return client
