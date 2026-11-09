"""Deterministic surrogate keys derived from natural keys.

    key = UUIDv5(namespace(source system, entity, version), canonical(natural))

- canonical: every component normalized (trimmed, Unicode NFC, numbers without
  trailing zeros, dates in ISO, text in one letter case for case-insensitive
  sources), then the list encoded as JSON, so that composite keys never
  collide (`("A|B", "C")` and `("A", "B|C")`);
- namespace: per source system and entity, so that the same number in two
  legacy systems gives two keys; versioned, so that changing the canonical
  form is explicit;
- HMAC: a UUIDv5 of a natural key holding personal data (national ID, phone
  number) can be brute-forced back from a small value space; an HMAC keyed
  with a secret cannot. The result is a UUIDv8.

The same natural key gives the same key on every run, environment and
machine: children compute their foreign keys without any lookup, in parallel.
"""

from __future__ import annotations

from collections.abc import Sequence
import dataclasses
import datetime
import decimal
import hashlib
import hmac
import json
import unicodedata
import uuid

from core import contract

_NAMESPACE_ROOT = uuid.uuid5(
    uuid.NAMESPACE_URL, "urn:etl-pipeline-boilerplate:keys"
)


class MissingKeyComponentError(ValueError):
    """A natural key component is empty: no key can be derived from it."""


def canonical_component(
    value: object, case: contract.KeyCase = contract.KeyCase.PRESERVE
) -> str:
    """Returns the canonical text of one natural key component.

    Args:
        value: The component, as read from the source.
        case: The letter case of text components.

    Returns:
        Its canonical text: trimmed NFC text, numbers without trailing zeros,
        dates and times in ISO 8601, booleans as `true` or `false`.

    Raises:
        MissingKeyComponentError: If the component is None or blank.
    """
    if value is None:
        raise MissingKeyComponentError("empty")
    if not isinstance(value, str):
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, int):
            return str(value)
        if isinstance(value, float):
            if not value.is_integer():
                return repr(value)
            return str(int(value))
        if isinstance(value, decimal.Decimal):
            normalized = value.normalize()
            if normalized == normalized.to_integral():
                return str(int(normalized))
            return format(normalized, "f")
        if isinstance(value, (datetime.datetime, datetime.date)):
            return value.isoformat()

    text = unicodedata.normalize("NFC", str(value)).strip()
    if not text:
        raise MissingKeyComponentError("empty")
    return case.apply(text)


def canonical(
    components: Sequence[object],
    case: contract.KeyCase = contract.KeyCase.PRESERVE,
) -> tuple[str, ...]:
    """Returns the canonical form of a natural key, component by component.

    Raises:
        MissingKeyComponentError: If a component is None or blank.
    """
    return tuple(
        canonical_component(component, case) for component in components
    )


def canonical_text(
    components: Sequence[object],
    case: contract.KeyCase = contract.KeyCase.PRESERVE,
) -> str:
    """Returns a natural key as one text, its canonical components joined by
    `|`: how a source row is identified in lineage, in the quarantine and in
    corrections files.

    `1003`, `1003.0` and `Decimal("1003")` give `1003`, whatever the driver
    returned; an empty component gives an empty text, not an error.
    """
    parts: list[str] = []
    for component in components:
        try:
            parts.append(canonical_component(component, case))
        except MissingKeyComponentError:
            parts.append("")
    return "|".join(parts)


def key_namespace(
    source_system: str, entity: str, version: int = 1
) -> uuid.UUID:
    """Returns the namespace of the keys of one entity of one source system.

    Args:
        source_system: Where the natural keys come from, e.g.
            `legacy.CUST_MASTER`.
        entity: The target entity, e.g. `crm.customer`.
        version: Bumped only to change every key of the entity on purpose.

    Returns:
        A UUID namespace: `named_namespace(f"{source_system}:{entity}")`.
    """
    return named_namespace(f"{source_system}:{entity}", version)


def named_namespace(name: str, version: int = 1) -> uuid.UUID:
    """Returns the namespace of the keys of the entity called `name`.

    The name is chosen once, and kept: renaming the tables the entity is read
    from or written to must not change its keys.

    Args:
        name: The name of the namespace, e.g. `legacy.CUST_MASTER:crm.customer`
            (what `key_namespace` derives).
        version: Bumped only to change every key of the entity on purpose.

    Returns:
        A UUID namespace.
    """
    return uuid.uuid5(_NAMESPACE_ROOT, f"v{version}:{name}")


@dataclasses.dataclass(frozen=True, slots=True)
class KeyMaker:
    """Derives the surrogate keys of one entity from its natural keys.

    Attributes:
        namespace: The namespace of the entity (see `key_namespace`).
        secret: Keys an HMAC instead of a public UUIDv5, when set.
        case: The letter case of the text of its natural keys.
    """

    namespace: uuid.UUID
    secret: bytes | None = None
    case: contract.KeyCase = contract.KeyCase.PRESERVE

    def canonical(self, components: Sequence[object]) -> tuple[str, ...]:
        """Returns the canonical natural key, in the entity's letter case.

        Raises:
            MissingKeyComponentError: If a component is None or blank.
        """
        return canonical(components, self.case)

    def key(self, natural_key: Sequence[str]) -> str:
        """Returns the surrogate key of a canonical natural key.

        Args:
            natural_key: The canonical components (see `canonical`).

        Returns:
            A UUIDv5, or a UUIDv8 when a secret is set, as text.
        """
        encoded = encoded_natural_key(natural_key)
        if self.secret is None:
            return str(uuid.uuid5(self.namespace, encoded))

        digest = hmac.new(
            self.secret, self.namespace.bytes + encoded.encode(), hashlib.sha256
        )
        return str(uuid.UUID(bytes=digest.digest()[:16], version=8))


def encoded_natural_key(natural_key: Sequence[str]) -> str:
    """Returns the text a key is derived from: the canonical natural key as
    compact JSON, `["C001","2"]`, since the first release.

    Built without the JSON encoder when no component needs escaping: a
    component with a quote, a backslash or a character JSON would escape
    goes through `json.dumps`, which gives the same text.
    """
    if not natural_key:
        return "[]"
    for component in natural_key:
        if '"' in component or "\\" in component or not component.isprintable():
            return json.dumps(
                list(natural_key), ensure_ascii=False, separators=(",", ":")
            )
    return '["' + '","'.join(natural_key) + '"]'
