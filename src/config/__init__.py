"""Application settings: environments, databases, logging, modules, secrets.

    settings = config.load_settings("prod")
    settings.databases["crm"].url.get_secret_value()

Only the application layer (`app`, `cli`, `modules`) reads settings; `core`
and `infra` get what they need as arguments.
"""

from config.modules import LookupTable
from config.modules import MappingModuleSettings
from config.secrets import register_provider
from config.secrets import SecretError
from config.settings import AppSettings
from config.settings import BookkeepingSettings
from config.settings import ChangesSettings
from config.settings import ConfigError
from config.settings import DatabaseSettings
from config.settings import DEFAULT_ENV
from config.settings import EngineOptions
from config.settings import ENV_VARIABLE
from config.settings import is_secret_name
from config.settings import KafkaSettings
from config.settings import LineageSettings
from config.settings import load_settings
from config.settings import LoggingSettings
from config.settings import ModuleSettings
from config.settings import MongoOptions
from config.settings import OtlpSettings
from config.settings import PrivacySettings
from config.settings import RunsSettings
from config.settings import StoreSettings

__all__ = [
    "DEFAULT_ENV",
    "ENV_VARIABLE",
    "AppSettings",
    "BookkeepingSettings",
    "ChangesSettings",
    "ConfigError",
    "DatabaseSettings",
    "EngineOptions",
    "KafkaSettings",
    "LineageSettings",
    "LoggingSettings",
    "LookupTable",
    "MappingModuleSettings",
    "ModuleSettings",
    "MongoOptions",
    "OtlpSettings",
    "PrivacySettings",
    "RunsSettings",
    "SecretError",
    "StoreSettings",
    "is_secret_name",
    "load_settings",
    "register_provider",
]
