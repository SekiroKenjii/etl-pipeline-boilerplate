"""A business module that migrates tables through a mapping workbook."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import hashlib
import threading

from core import contract
from core import mapping
from core import runtime
from core.mapping_pipelines import kinds
from core.mapping_pipelines import mappings as pipelines_mappings
from core.mapping_pipelines import reconciliation
from core.mapping_pipelines import types as pipelines_types
from core.runtime import names


class MappingModule(runtime.Module):
    """A module whose pipelines are its compiled mappings, one step each.

    It has the pipelines `full_load`, `sync`, `parked`, `deletes` and
    `replay` (`core.mapping_pipelines.kinds`), its fingerprint (the
    mappings, the corrections, anonymization and the surrogate keys:
    `runtime.KEYS`), its mapping catalog, and the reconciliations of the
    cutover report. A module gives its name, its settings and its custom
    transforms:

        registry = mapping.TransformRegistry()

        def create(
            settings: config.AppSettings,
        ) -> mapping_pipelines.MappingModule:
            return mapping_pipelines.MappingModule(
                "billing",
                settings.module_settings("billing", BillingSettings),
                registry,
            )

    Subclass it to change what it builds: `pipelines` to add a pipeline (a
    `MappingPipeline`) or leave one out, `load` to compile the mappings
    otherwise.

    Attributes:
        settings: The settings of the module.
        transforms: The custom transforms of its mappings.
    """

    def __init__(
        self,
        name: str,
        settings: pipelines_types.MappingSettings,
        transforms: mapping.TransformRegistry,
        *,
        key_secret: bytes | None = None,
        anonymize: bytes | None = None,
    ) -> None:
        """Initializes the module.

        Args:
            name: The name of the module: the prefix of its pipelines.
            settings: Its settings.
            transforms: The custom transforms of its mappings.
            key_secret: The secret of the HMAC surrogate keys, if any.
            anonymize: The secret of anonymization, when personal data is
                anonymized.
        """
        self._name = name
        self.settings = settings
        self.transforms = transforms
        self._key_secret = key_secret
        self._anonymize = anonymize
        self._mappings: pipelines_mappings.Mappings | None = None
        self._lock = threading.Lock()

    @property
    def name(self) -> str:
        return self._name

    def fingerprint(self) -> Mapping[str, str]:
        """The hashes of the compiled mappings and of the corrections,
        whether personal data is anonymized, and what the surrogate keys are
        derived from (`runtime.KEYS`: per mapping, its key namespace,
        hashing, case and version, and the key secret for HMAC keys,
        hashed): when it changes, the keys loaded already no longer
        match."""
        found = {"anonymized": "yes"} if self._anonymize else {}
        for name, path in (
            ("mappings", self.settings.mappings),
            ("corrections", self.settings.corrections),
        ):
            if path is not None and path.exists():
                found[name] = hashlib.sha256(path.read_bytes()).hexdigest()[:12]
        if self.settings.mappings.exists():
            found[runtime.KEYS] = mapping.key_fingerprint(
                mapping.load_catalog(self.settings.mappings), self._key_secret
            )
        return found

    def pipelines(self) -> Mapping[str, runtime.PipelineFactory]:
        return kinds.factories(self.name, self.mappings)

    def catalogs(self) -> Sequence[contract.MappingCatalog]:
        path = self.settings.mappings
        return (mapping.load_catalog(path),) if path.exists() else ()

    def reconciliations(
        self, resources: runtime.Resources
    ) -> Sequence[contract.Reconciliation]:
        return reconciliation.reconciliations(
            resources, self.mappings(resources)
        )

    def mappings(
        self, resources: runtime.Resources
    ) -> pipelines_mappings.Mappings:
        """The compiled mappings, loaded once, on first use (with the
        resources their lookup tables are read from)."""
        with self._lock:
            if self._mappings is None:
                catalog, compiled = self.load(resources)
                self._mappings = pipelines_mappings.Mappings(
                    self.settings, catalog, compiled
                )
            return self._mappings

    def load(
        self, resources: runtime.Resources
    ) -> tuple[contract.MappingCatalog, dict[str, mapping.CompiledMapping]]:
        """Reads the mappings, fills their database lookup tables, and
        compiles them.

        Raises:
            PipelineConfigError: If the compiled file is missing, or a lookup
                table has no sheet.
        """
        path = self.settings.mappings
        if not path.exists():
            raise contract.PipelineConfigError(
                f"No compiled mappings at {path}: run `etl-mappings compile`"
            )
        catalog = mapping.load_catalog(path)
        if self.settings.lookups:
            catalog = mapping.with_lookups(catalog, self.lookups(resources))
        compiled = mapping.compile_catalog(
            catalog,
            self.transforms,
            key_secret=self._key_secret,
            anonymize=self._anonymize,
        )
        return catalog, compiled

    def lookups(
        self, resources: runtime.Resources
    ) -> dict[str, dict[str, str]]:
        """Reads the lookup tables of the settings from their databases."""
        found: dict[str, dict[str, str]] = {}
        for name, table in self.settings.lookups.items():
            reader = resources.get(
                names.reader(table.database or self.settings.source),
                contract.ITableReader,
            )
            found[name] = mapping.read_lookup(
                reader,
                mapping.LookupSource(table.table, table.key, table.value),
            )
        return found
