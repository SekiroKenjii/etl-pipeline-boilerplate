"""The base of mapping-driven modules: what every module gets from it."""

from __future__ import annotations

import pathlib
from typing import Any

import pytest

import config
from core import mapping
from core import mapping_pipelines
from core import runtime
from core.mapping import catalog as mapping_catalog
from tests.factories import mapping_specs

field = mapping_specs.field


class _Billing(config.MappingModuleSettings):
    source: str = "legacy"
    target: str = "billing"
    mappings: pathlib.Path = pathlib.Path("mappings/compiled/billing.json")


def _billing(**settings: Any) -> _Billing:
    app = config.load_settings(
        "test",
        directory="/nonexistent",
        overrides={
            "databases": {
                "legacy": {"url": "mssql+pyodbc://db/legacy"},
                "billing": {"url": "mssql+pyodbc://db/billing"},
            },
            "modules": {"billing": {"settings": settings}},
        },
    )
    return app.module_settings("billing", _Billing)


def test_every_mapping_module_checks_its_pools() -> None:
    with pytest.raises(config.ConfigError) as caught:
        _billing(partitions=16)

    assert "databases.billing: its pool opens 15 connections" in str(
        caught.value
    )
    assert _billing(partitions=4).partitions == 4


def test_a_module_is_its_name_settings_and_transforms(
    tmp_path: pathlib.Path,
) -> None:
    path = tmp_path / "billing.json"
    mapping_catalog.save_catalog(
        path,
        mapping_specs.catalog(
            mapping_specs.mapping(
                field("invoice_key", "INV_NO", primary_key=True),
                name="invoice",
                source="legacy.INV",
                target="billing.invoice",
            )
        ),
    )
    module = mapping_pipelines.MappingModule(
        "billing",
        _Billing(mappings=path),
        mapping.TransformRegistry(),
        key_secret=b"secret",
    )

    assert module.name == "billing"
    assert sorted(module.pipelines()) == [
        "deletes",
        "full_load",
        "parked",
        "replay",
        "sync",
    ]
    assert [m.name for m in module.catalogs()[0].mappings] == ["invoice"]
    assert set(module.fingerprint()) == {"mappings", runtime.KEYS}
    assert module.fingerprint()[runtime.KEYS].startswith("invoice:")
