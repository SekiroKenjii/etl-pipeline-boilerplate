"""The base of mapping-driven modules: what every module gets from it."""

from __future__ import annotations

import datetime
import decimal
import pathlib
from typing import Any
import uuid

import pytest

import config
from core import contract
from core import mapping
from core import mapping_pipelines
from core import runtime
from core.mapping import catalog as mapping_catalog
from core.mapping_pipelines import kinds
from tests.factories import fake_ports
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


class TestReplayKeys:
    """Replay reads rejected rows by their source key, a text."""

    @pytest.mark.parametrize(
        ("family", "texts", "values"),
        [
            (contract.TypeFamily.INTEGER, ["7", "x", "1003"], [7, 1003]),
            (
                contract.TypeFamily.DECIMAL,
                ["2.25", "x"],
                [decimal.Decimal("2.25")],
            ),
            (
                contract.TypeFamily.BOOLEAN,
                ["true", "1", "false"],
                [True, False],
            ),
            (
                contract.TypeFamily.DATE,
                ["2026-01-02", "02/01/2026"],
                [datetime.date(2026, 1, 2)],
            ),
            (contract.TypeFamily.UUID, ["0" * 32, "x"], [uuid.UUID(int=0)]),
            (contract.TypeFamily.TEXT, ["C001", "7"], ["C001", "7"]),
            (contract.TypeFamily.OTHER, ["C001"], ["C001"]),
            (contract.TypeFamily.INTEGER, [], []),
        ],
    )
    def test_keys_take_the_type_of_their_column(
        self,
        family: contract.TypeFamily,
        texts: list[str],
        values: list[object],
    ) -> None:
        assert kinds.key_values(family, texts) == values

    def test_a_reader_without_a_catalog_guesses_from_the_largest_value(
        self,
    ) -> None:
        reader = fake_ports.InMemoryTableReader({
            "t": [{"k": 7}, {"k": 1003}],
            "empty": [],
        })

        assert reader.column_family("t", "k") is contract.TypeFamily.INTEGER
        assert reader.column_family("empty", "k") is contract.TypeFamily.OTHER
