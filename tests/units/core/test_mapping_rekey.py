"""Surrogate keys across workbook changes: renames, key namespaces, secrets.

A change that makes other keys is reported (`etl-mappings diff`), and shows
in the `keys` fingerprint the kernel compares between runs.
"""

from __future__ import annotations

import dataclasses
import pathlib

import pytest

from core import contract
from core import keys as core_keys
from core import mapping
from core.mapping import catalog as mapping_catalog
from modules.crm import module as crm_module
from modules.crm import settings as crm_settings
from tests.factories import mapping_specs

field = mapping_specs.field
SECRET = b"key-secret"

CUSTOMER = mapping_specs.mapping(
    field("customer_key", "CUST_NO", primary_key=True),
    field("name", "NAME"),
    key_hashing=contract.KeyHashing.UUID5,
)
ORDER = mapping_specs.mapping(
    field("order_key", "ORD_NO", primary_key=True),
    field("customer_key", "CUST_NO", references="customer"),
    name="order",
    source="legacy.ORD",
    target="crm.order",
    key_hashing=contract.KeyHashing.HMAC,
)


def _key(spec: contract.MappingSpec, secret: bytes | None = None) -> object:
    """The surrogate key the compiled `spec` gives the natural key 1003."""
    compiled = mapping.compile_catalog(
        mapping_specs.catalog(spec), mapping_specs.registry, key_secret=secret
    )[spec.name]
    return compiled.apply({"CUST_NO": "1003", "NAME": "x"}).key


@pytest.mark.parametrize(
    ("source", "target"),
    [
        ("LEGACY.CUST_MASTER", "crm.customer"),
        ("legacy.CUST_MASTER", "crm.customers"),
    ],
)
def test_renaming_a_table_is_reported_as_a_rekey(
    source: str, target: str
) -> None:
    renamed = dataclasses.replace(CUSTOMER, source=source, target=target)
    old = mapping_specs.catalog(CUSTOMER, ORDER)
    new = mapping_specs.catalog(renamed, ORDER)

    changes = mapping.compare_catalogs(old, new)
    text = mapping.describe_changes(changes)

    assert changes.rekeyed == ["customer"]
    assert "Surrogate keys change for: customer" in text
    # How to keep them.
    assert "set its Key namespace to 'legacy.CUST_MASTER:crm.customer'" in text
    assert _key(CUSTOMER) != _key(renamed)


def test_a_key_namespace_keeps_the_keys_across_renames() -> None:
    pinned = dataclasses.replace(
        CUSTOMER, key_namespace="legacy.CUST_MASTER:crm.customer"
    )
    renamed = dataclasses.replace(
        pinned, source="LEGACY.CUSTOMERS", target="crm.customers"
    )

    changes = mapping.compare_catalogs(
        mapping_specs.catalog(pinned), mapping_specs.catalog(renamed)
    )

    # The default namespace is the one written: no key changes.
    assert _key(CUSTOMER) == _key(pinned) == _key(renamed)
    assert changes.rekeyed == []
    assert changes.changed == ["customer"]
    # The keys are those derived as before key namespaces existed.
    maker = core_keys.KeyMaker(
        core_keys.key_namespace("legacy.CUST_MASTER", "crm.customer")
    )
    assert _key(renamed) == maker.key(("1003",))


def test_changing_the_key_namespace_is_a_rekey() -> None:
    pinned = dataclasses.replace(CUSTOMER, key_namespace="crm:customer")

    changes = mapping.compare_catalogs(
        mapping_specs.catalog(CUSTOMER), mapping_specs.catalog(pinned)
    )

    assert changes.rekeyed == ["customer"]
    assert _key(pinned) != _key(CUSTOMER)


def test_the_key_namespace_and_default_hashing_are_compiled_to_json(
    tmp_path: pathlib.Path,
) -> None:
    catalog = mapping_specs.catalog(
        dataclasses.replace(CUSTOMER, key_namespace="crm:customer"),
        dataclasses.replace(ORDER, key_hashing=None),
    )
    path = tmp_path / "crm.json"

    mapping_catalog.save_catalog(path, catalog)
    loaded = mapping_catalog.load_catalog(path)

    assert loaded.get("customer").key_namespace == "crm:customer"
    assert loaded.get("order").key_hashing is None
    # Files compiled before the default existed always named uuid5.
    assert (
        mapping_catalog.loads(
            path.read_text(encoding="utf-8").replace('"key_hashing": null,', "")
        )
        .get("order")
        .key_hashing
        is contract.KeyHashing.UUID5
    )


class TestDefaultHashing:

    def test_hmac_with_a_key_secret_uuid5_without(self) -> None:
        default = dataclasses.replace(CUSTOMER, key_hashing=None)

        assert _key(default) == _key(CUSTOMER)
        assert _key(default, SECRET) == _key(
            dataclasses.replace(CUSTOMER, key_hashing=contract.KeyHashing.HMAC),
            SECRET,
        )
        assert _key(default, SECRET) != _key(default)

    def test_uuid5_is_refused_for_a_natural_key_holding_personal_data(
        self,
    ) -> None:
        national_id = mapping_specs.mapping(
            field("customer_key", "NID", primary_key=True),
            field("national_id", "NID", personal="mask"),
            key_hashing=contract.KeyHashing.UUID5,
        )

        problems = mapping.validate_catalog(
            mapping_specs.catalog(national_id), mapping_specs.registry
        )

        assert [p.message for p in problems] == [
            "the natural key of 'customer' holds personal data (NID"
            " (national_id: mask)): use hmac key hashing, as uuid5 keys can"
            " be reversed"
        ]
        # The default is HMAC: it needs the secret to run.
        default = dataclasses.replace(national_id, key_hashing=None)
        assert not mapping.validate_catalog(
            mapping_specs.catalog(default), mapping_specs.registry
        )
        catalog = mapping_specs.catalog(default)
        with pytest.raises(contract.MappingDefinitionError, match="secret"):
            mapping.compile_catalog(catalog, mapping_specs.registry)
        assert mapping.compile_catalog(
            catalog,
            mapping_specs.registry,
            key_secret=SECRET,
        )


class TestKeyFingerprint:

    def _entries(
        self, catalog: contract.MappingCatalog, secret: bytes | None
    ) -> dict[str, str]:
        text = mapping.key_fingerprint(catalog, secret)
        return dict(entry.split(":") for entry in text.split(","))

    def test_one_entry_per_mapping_that_changes_with_its_keys(self) -> None:
        catalog = mapping_specs.catalog(CUSTOMER, ORDER)
        before = self._entries(catalog, SECRET)

        rotated = self._entries(catalog, b"another secret")
        renamed = self._entries(
            mapping_specs.catalog(
                dataclasses.replace(CUSTOMER, source="LEGACY.CUSTOMERS"), ORDER
            ),
            SECRET,
        )
        bumped = self._entries(
            mapping_specs.catalog(
                CUSTOMER, dataclasses.replace(ORDER, key_version=2)
            ),
            SECRET,
        )
        cased = self._entries(
            mapping_specs.catalog(
                dataclasses.replace(CUSTOMER, key_case=contract.KeyCase.UPPER),
                ORDER,
            ),
            SECRET,
        )

        assert sorted(before) == ["customer", "order"]
        # Rotating the secret changes the HMAC keys only.
        assert rotated["customer"] == before["customer"]
        assert rotated["order"] != before["order"]
        assert renamed["customer"] != before["customer"]
        assert renamed["order"] == before["order"]
        assert bumped["order"] != before["order"]
        assert cased["customer"] != before["customer"]

    def test_unchanged_keys_give_the_same_fingerprint(self) -> None:
        catalog = mapping_specs.catalog(CUSTOMER, ORDER)
        edited = mapping_specs.catalog(
            dataclasses.replace(
                CUSTOMER, fields=(*CUSTOMER.fields, field("email", "MAIL"))
            ),
            ORDER,
        )

        assert mapping.key_fingerprint(
            catalog, SECRET
        ) == mapping.key_fingerprint(edited, SECRET)

    def test_the_module_fingerprint_has_it_and_never_the_secret(
        self, tmp_path: pathlib.Path
    ) -> None:
        path = tmp_path / "crm.json"
        mapping_catalog.save_catalog(
            path, mapping_specs.catalog(CUSTOMER, ORDER)
        )
        settings = crm_settings.CrmSettings.model_validate({"mappings": path})

        found = crm_module.Crm(settings, SECRET).fingerprint()
        rotated = crm_module.Crm(settings, b"another secret").fingerprint()

        assert found["keys"] != rotated["keys"]
        assert found["mappings"] == rotated["mappings"]
        assert SECRET.decode() not in str(found)
        assert found["keys"] == mapping.key_fingerprint(
            mapping_catalog.load_catalog(path), SECRET
        )
