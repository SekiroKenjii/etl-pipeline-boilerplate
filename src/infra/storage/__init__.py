"""File and blob store adapters, implementing `contract.IObjectStore`.

`local` is here; `gcs` is imported as its module,
`from infra.storage import gcs`.
"""

from infra.storage.local import LocalObjectStore

__all__ = [
    "LocalObjectStore",
]
