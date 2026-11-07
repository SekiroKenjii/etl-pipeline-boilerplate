"""Stores of the state kept between runs.

Extractors depend on `contract.IStateStore` only, never on this package.
"""

from core.state.stores import InMemoryStateStore
from core.state.stores import JsonFileStateStore

__all__ = [
    "InMemoryStateStore",
    "JsonFileStateStore",
]
