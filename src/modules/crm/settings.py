"""The settings of the CRM module: `[modules.crm.settings]`.

Every value is one of `config.MappingModuleSettings`; the CRM gives its
defaults.
"""

from __future__ import annotations

import pathlib

import config


class CrmSettings(config.MappingModuleSettings):
    """Where the CRM data comes from and goes, and how it is handled.

    Attributes:
        source: The legacy database, a name of `databases`.
        target: The CRM database, a name of `databases`.
        mappings: The compiled mappings of `mappings/crm.xlsx`.
        watermark: The legacy column tracking changes.
    """

    source: str = "legacy"
    target: str = "crm"
    mappings: pathlib.Path = pathlib.Path("mappings/compiled/crm.json")
    watermark: str | None = "UPDATED_AT"
