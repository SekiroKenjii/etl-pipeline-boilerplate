"""Anonymizing personal data, for the environments that must not hold it.

A mapping marks its personal columns with a kind (`Personal data` in the
workbook); compiled with an anonymization secret, they get fake values
instead of the real ones. Values are derived from the real one by HMAC:

- the same value always gives the same fake one, so joins on them still
  match, and the kinds made unique (`email`, `phone`, `hash`) keep unique
  columns unique: they take enough bits of the HMAC for a collision to be
  negligible (below 1e-3 for 10 million phone numbers, the smallest space);
- without the secret, a fake value tells nothing of the real one.

Kinds:
    name: a full name, of at most 15 characters (not unique).
    email: `user.<20 hex digits>@example.invalid`, 41 characters.
    phone: a phone number of the fictional 555 range, `+1555` and 15
        digits, 20 characters.
    address: a street address, of at most 29 characters (not unique).
    date: the date (or time) shifted by 1 to 180 days, either way. The shift
        is one per record, derived from its natural key: the dates of a
        record keep their order and the days between them (a start stays
        before its end), while two records get unrelated shifts.
    mask: the first and last characters kept, the others `*` (same length).
    hash: 16 hex digits.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
import datetime
import hashlib
import hmac
import json
from typing import Protocol


class Anonymizer(Protocol):
    """Replaces a personal value by a fake one."""

    def __call__(self, value: object, record: Sequence[str] = ()) -> object:
        """Returns the fake value of `value`.

        Args:
            value: The value, cast to its target type; None stays None.
            record: Identifies the record, e.g. its canonical natural key:
                the `date` kind shifts every date of a record by the same
                number of days.
        """
        ...


_FAMILY = (
    "Smith", "Johnson", "Brown", "Garcia", "Miller", "Davis", "Martin",
    "Lopez", "Wilson", "Anderson", "Taylor", "Moore", "Jackson", "Lee",
    "Walker", "Young",
)  # fmt: skip
_GIVEN = (
    "Alex", "Blair", "Casey", "Dana", "Eden", "Finley", "Gray", "Harper",
    "Jordan", "Kai", "Logan", "Morgan", "Noel", "Parker", "Quinn", "Reese",
    "Riley", "Sage", "Taylor", "Avery", "Robin", "Jamie",
)  # fmt: skip
_STREETS = ("Oak", "Maple", "Pine", "Cedar", "Elm", "Lake", "Hill", "Park")
_MAX_SHIFT_DAYS = 180


def _digest(secret: bytes, kind: str, value: object) -> bytes:
    text = value.isoformat() if isinstance(value, datetime.date) else value
    message = f"{kind}\0{text}".encode()
    return hmac.new(secret, message, hashlib.sha256).digest()


_EMAIL_HEX_DIGITS = 20  # 80 bits.
_PHONE_DIGITS = 15  # 10^15 numbers, from 64 bits.
_HASH_HEX_DIGITS = 16


def _number(digest: bytes) -> int:
    return int.from_bytes(digest[:8], "big")


def _name(digest: bytes, value: object) -> object:
    del value  # Unused.
    n = _number(digest)
    return f"{_GIVEN[n % len(_GIVEN)]} {_FAMILY[n // 32 % len(_FAMILY)]}"


def _email(digest: bytes, value: object) -> object:
    del value  # Unused.
    return f"user.{digest.hex()[:_EMAIL_HEX_DIGITS]}@example.invalid"


def _phone(digest: bytes, value: object) -> object:
    del value  # Unused.
    number = _number(digest) % 10**_PHONE_DIGITS
    return f"+1555{number:0{_PHONE_DIGITS}d}"


def _address(digest: bytes, value: object) -> object:
    del value  # Unused.
    n = _number(digest)
    street = _STREETS[n // 512 % len(_STREETS)]
    return f"{1 + n % 300} {street} Street, District {1 + n // 300 % 12}"


def _date(digest: bytes, value: object) -> object:
    """Masks a value that is no date; dates are shifted by `_shifted`."""
    return _mask(digest, value)


def _shift_days(secret: bytes, record: Sequence[str]) -> int:
    """The shift of the dates of a record: 1 to 180 days, either way."""
    message = "date\0" + json.dumps(list(record), ensure_ascii=False)
    digest = hmac.new(secret, message.encode(), hashlib.sha256).digest()
    days = _number(digest) % (2 * _MAX_SHIFT_DAYS) - _MAX_SHIFT_DAYS
    return days if days < 0 else days + 1


def _mask(digest: bytes, value: object) -> object:
    del digest  # Unused.
    text = str(value)
    if len(text) <= 2:
        return "*" * len(text)
    return text[0] + "*" * (len(text) - 2) + text[-1]


def _hash(digest: bytes, value: object) -> object:
    del value  # Unused.
    return digest.hex()[:_HASH_HEX_DIGITS]


_KINDS: dict[str, Callable[[bytes, object], object]] = {
    "name": _name,
    "email": _email,
    "phone": _phone,
    "address": _address,
    "date": _date,
    "mask": _mask,
    "hash": _hash,
}
KINDS = frozenset(_KINDS)
# The target column must hold the longest fake of its kind.
TEXT_LENGTHS: dict[str, int] = {
    "name": max(len(g) for g in _GIVEN) + 1 + max(len(f) for f in _FAMILY),
    "email": len(f"user.{'0' * _EMAIL_HEX_DIGITS}@example.invalid"),
    "phone": len("+1555") + _PHONE_DIGITS,
    "address": len(f"300 {max(_STREETS, key=len)} Street, District 12"),
    "hash": _HASH_HEX_DIGITS,
}
SAME_LENGTH_KINDS = frozenset({"mask"})
TEMPORAL_KINDS = frozenset({"date"})


def anonymizer(kind: str, secret: bytes) -> Anonymizer:
    """Returns the function anonymizing values of `kind`.

    Raises:
        ValueError: If the kind is unknown, or the secret empty.
    """
    if kind not in _KINDS:
        raise ValueError(
            f"unknown personal data kind {kind!r}: {sorted(KINDS)}"
        )
    if not secret:
        raise ValueError("anonymizing needs a secret")
    fake = _KINDS[kind]

    def anonymize(value: object, record: Sequence[str] = ()) -> object:
        if value is None:
            return None
        if kind in TEMPORAL_KINDS and isinstance(value, datetime.date):
            days = _shift_days(secret, record)
            return value + datetime.timedelta(days=days)
        return fake(_digest(secret, kind, value), value)

    return anonymize
