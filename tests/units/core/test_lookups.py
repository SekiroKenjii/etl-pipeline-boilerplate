"""Lookup tables read from database tables."""

from __future__ import annotations

import pytest

from core import contract
from core import mapping
from tests.factories import fake_ports
from tests.factories import mapping_specs


def test_a_table_read_as_a_lookup() -> None:
    reader = fake_ports.InMemoryTableReader({
        "REF_COUNTRY": [
            {"CODE": "VIETNAM", "ISO2": "VN", "ACTIVE": 1},
            {"CODE": "704", "ISO2": "VN", "ACTIVE": 1},
            {"CODE": "OLD", "ISO2": "XX", "ACTIVE": 0},
            {"CODE": "NONE", "ISO2": None, "ACTIVE": 1},
        ]
    })
    active = contract.Condition("ACTIVE", contract.Operator.EQ, 1)

    lookup = mapping.read_lookup(
        reader, mapping.LookupSource("REF_COUNTRY", "CODE", "ISO2", [active])
    )

    assert lookup == {"VIETNAM": "VN", "704": "VN"}


def test_lookups_fill_their_sheets_only() -> None:
    catalog = mapping_specs.catalog(lookups={"countries": {}})

    filled = mapping.with_lookups(catalog, {"countries": {"VN": "VN"}})

    assert filled.lookups == {"countries": {"VN": "VN"}}
    with pytest.raises(contract.PipelineConfigError, match="no lookup_<name>"):
        mapping.with_lookups(catalog, {"branches": {}})
