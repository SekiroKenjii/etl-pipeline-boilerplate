"""Anonymizing personal data: deterministic, keyed, kind by kind."""

from __future__ import annotations

import datetime
import re

import pytest

from core import contract
from core import mapping
from tests.factories import mapping_specs

SECRET = b"uat-secret"


@pytest.mark.parametrize(
    ("kind", "value", "pattern"),
    [
        ("name", "José Núñez", r"^\w+ \w+$"),
        (
            "email",
            "anna@acme.example",
            r"^user\.[0-9a-f]{20}@example\.invalid$",
        ),
        ("phone", "+44 20 7946 0000", r"^\+1555\d{15}$"),
        ("address", "12 Baker Street", r"^\d+ \w+ Street, District \d+$"),
        ("mask", "0912345678", r"^0\*{8}8$"),
        ("hash", "0912345678", r"^[0-9a-f]{16}$"),
    ],
)
def test_kinds(kind: str, value: str, pattern: str) -> None:
    anonymize = mapping.anonymizer(kind, SECRET)

    fake = anonymize(value)

    assert isinstance(fake, str)
    assert fake != value
    assert re.match(pattern, fake)
    # The same value, the same fake one; another secret, another one.
    assert anonymize(value) == fake
    if kind != "mask":
        assert mapping.anonymizer(kind, b"other")(value) != fake


def test_dates_shift_but_keep_their_type() -> None:
    anonymize = mapping.anonymizer("date", SECRET)
    born = datetime.date(1990, 5, 17)
    seen = datetime.datetime(2026, 1, 1, 8, 30)

    shifted, moved = anonymize(born), anonymize(seen)

    assert isinstance(shifted, datetime.date)
    assert shifted != born
    assert abs((shifted - born).days) <= 180
    assert isinstance(moved, datetime.datetime)
    assert moved.time() == seen.time()
    assert anonymize("31/12/1990") == "3********0"  # Text: masked.


def test_nothing_stays_nothing_and_bad_kinds_fail() -> None:
    assert mapping.anonymizer("name", SECRET)(None) is None
    with pytest.raises(ValueError, match="unknown personal data kind"):
        mapping.anonymizer("ssn", SECRET)
    with pytest.raises(ValueError, match="needs a secret"):
        mapping.anonymizer("name", b"")


def _catalog(**personal: str) -> contract.MappingCatalog:
    """customer: a key, a name and an email, some marked personal."""
    return mapping_specs.catalog(
        mapping_specs.mapping(
            mapping_specs.field(
                "customer_key",
                "CUST_NO",
                primary_key=True,
                personal=personal.get("customer_key"),
            ),
            mapping_specs.field(
                "full_name", "NAME", personal=personal.get("full_name")
            ),
            mapping_specs.field(
                "email", "MAIL", personal=personal.get("email")
            ),
        )
    )


ROW = {"CUST_NO": "C001", "NAME": "José Núñez", "MAIL": "jose@acme.example"}


def test_compiled_mappings_anonymize_only_when_asked() -> None:
    catalog = _catalog(full_name="name", email="email")
    registry = mapping_specs.registry

    (real,) = mapping.compile_catalog(catalog, registry, key_secret=b"k")[
        "customer"
    ].map(ROW)
    (fake,) = mapping.compile_catalog(
        catalog, registry, key_secret=b"k", anonymize=SECRET
    )["customer"].map(ROW)

    assert real.data["full_name"] == "José Núñez"
    assert fake.data["full_name"] != "José Núñez"
    assert str(fake.data["email"]).endswith("@example.invalid")
    # Keys do not change: the surrogate key hides the natural key already.
    assert fake.key == real.key
    assert fake.data["customer_key"] == real.data["customer_key"]


def test_keys_and_unknown_kinds_are_refused() -> None:
    registry = mapping_specs.registry

    keyed = mapping.validate_catalog(_catalog(customer_key="name"), registry)
    unknown = mapping.validate_catalog(_catalog(email="ssn"), registry)

    assert any("a key is not personal data" in p.message for p in keyed)
    assert any("'ssn' is unknown" in p.message for p in unknown)
