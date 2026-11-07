"""File and blob store adapters, implementing `contract.IObjectStore`.

`local` needs the standard library only. `gcs` needs the `gcs` extra, so it is
not imported here: import it explicitly, `from infra.storage import gcs`.
"""

from infra.storage.local import LocalObjectStore

__all__ = [
    "LocalObjectStore",
]
