"""The CRM module, as the composition root loads it: `modules.crm:create`."""

from __future__ import annotations

import config
from core import mapping_pipelines
from modules.crm import settings as crm_settings
from modules.crm import transforms

NAME = "crm"


class Crm(mapping_pipelines.MappingModule):
    """Customers and orders, migrated from the legacy database."""

    def __init__(
        self,
        settings: crm_settings.CrmSettings,
        key_secret: bytes | None,
        *,
        anonymize: bytes | None = None,
    ) -> None:
        super().__init__(
            NAME,
            settings,
            transforms.registry,
            key_secret=key_secret,
            anonymize=anonymize,
        )


def create(settings: config.AppSettings) -> Crm:
    """Builds the module from the application's settings.

    Raises:
        ConfigError: If its settings are invalid.
    """
    secret = settings.key_secret
    return Crm(
        settings.module_settings(NAME, crm_settings.CrmSettings),
        secret.get_secret_value().encode() if secret is not None else None,
        anonymize=settings.anonymization_secret(),
    )
