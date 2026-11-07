"""The compiled mappings of a module, and where each reads and writes."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from core import contract
from core import keys
from core import mapping
from core import operators
from core.mapping_pipelines import settings as pipelines_settings


class Mappings:
    """The compiled mappings, and where each reads and writes.

    Attributes:
        settings: The settings of the module.
        catalog: The mapping catalog, as compiled.
        compiled: The mappings that run, by name: drafts, and those
            referencing one, are left out where drafts do not run.
        dependencies: The mappings each one references, of those that run.
        corrections: The manual corrections of source rows.
    """

    def __init__(
        self,
        settings: pipelines_settings.MappingSettings,
        catalog: contract.MappingCatalog,
        compiled: dict[str, mapping.CompiledMapping],
    ) -> None:
        self.settings = settings
        self.catalog = catalog
        self.compiled = compiled
        self.dependencies = {
            name: parents.intersection(compiled)
            for name, parents in mapping.mapping_dependencies(catalog).items()
            if name in compiled
        }
        self.corrections = (
            operators.Corrections.from_csv(settings.corrections)
            if settings.corrections is not None
            else operators.Corrections({})
        )

    def source_table(self, name: str) -> str:
        return self.settings.source_tables.get(
            name, self.compiled[name].spec.source
        )

    def target_table(self, name: str) -> str:
        return self.settings.target_tables.get(name, self.compiled[name].entity)

    def key_field(self, name: str) -> str:
        field = self.compiled[name].key_field
        if field is None:
            raise contract.PipelineConfigError(
                f"Mapping {name!r} has no primary key"
            )
        return field

    def natural_key(self, name: str) -> tuple[str, ...]:
        """The source columns of the primary key of a mapping."""
        return next(
            f.sources for f in self.compiled[name].spec.fields if f.primary_key
        )

    def lineage(self, name: str) -> Callable[[Any], str]:
        """Identifies a source row (or change) by its natural key, as text:
        the `source_key` of what it maps to, and of its corrections.

        The text is canonical (`keys.canonical_text`), in the key case of the
        mapping: `1003` whether the driver returned 1003, 1003.0 or
        Decimal("1003"), and `C001` for `c001` on a case-insensitive source.
        """
        columns = self.natural_key(name)
        case = self.compiled[name].spec.key_case

        def lineage(record: Any) -> str:
            row = (
                record.row
                if isinstance(record, contract.ChangeEvent)
                else record
            )
            return keys.canonical_text(
                [row.get(column) for column in columns], case
            )

        return lineage
