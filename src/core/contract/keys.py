"""The port and data types of keys.

Surrogate keys derive from natural keys (`core.keys`); the key registry
(crosswalk) records which keys were loaded.
"""

from __future__ import annotations

import abc
from collections.abc import Collection, Iterator, Sequence
import dataclasses
import enum


class KeyHashing(enum.StrEnum):
    """How surrogate keys are derived from natural keys."""

    UUID5 = "uuid5"
    HMAC = "hmac"


class KeyCase(enum.StrEnum):
    """How the letter case of natural keys is normalized.

    A source with a case-insensitive collation (SQL Server, MySQL by default)
    holds `c001` and `C001` as the same key: normalize both to one case, or
    they get two surrogate keys. Changing it changes the keys: bump the key
    version with it.
    """

    PRESERVE = "preserve"
    UPPER = "upper"
    LOWER = "lower"

    def apply(self, text: str) -> str:
        """Returns `text` in this case."""
        if self is KeyCase.UPPER:
            return text.upper()
        if self is KeyCase.LOWER:
            return text.lower()
        return text


@dataclasses.dataclass(frozen=True, slots=True)
class KeyEntry:
    """A loaded entity: its surrogate key and the natural key behind it.

    Attributes:
        entity: The target entity, e.g. `crm.customer`.
        surrogate_key: The key in the target.
        natural_key: The canonical natural key it was derived from.
    """

    entity: str
    surrogate_key: str
    natural_key: tuple[str, ...]


class IKeyRegistry(abc.ABC):
    """The crosswalk of loaded keys.

    Register keys only once their rows are loaded, so that a child row never
    passes an integrity check against a parent that failed to load.
    Implementations must be thread-safe, and `register` idempotent.
    """

    @abc.abstractmethod
    def register(self, entries: Sequence[KeyEntry]) -> None:
        """Records loaded keys; registering a known key does nothing."""

    @abc.abstractmethod
    def known(self, entity: str, surrogate_keys: Collection[str]) -> set[str]:
        """Returns the keys of `surrogate_keys` registered for `entity`."""

    @abc.abstractmethod
    def entries(self, entity: str) -> Iterator[KeyEntry]:
        """Yields every key registered for `entity`, for reconciliations."""

    @abc.abstractmethod
    def natural_key(
        self, entity: str, surrogate_key: str
    ) -> tuple[str, ...] | None:
        """Returns the natural key behind a surrogate key, if registered."""

    @abc.abstractmethod
    def forget(self, entity: str, natural_key: Sequence[str]) -> list[str]:
        """Forgets the keys of a natural key, e.g. for a right to erasure.

        Args:
            entity: The entity.
            natural_key: Its canonical natural key.

        Returns:
            The surrogate keys forgotten; none when it was not registered.
        """
