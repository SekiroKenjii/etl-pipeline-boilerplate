"""Lookup tables read from a database table instead of the workbook.

The workbook keeps an empty `lookup_<name>` sheet, so that mappings validate
without a database; the module fills it from the table when it runs.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import dataclasses

from core import contract


@dataclasses.dataclass(frozen=True, slots=True)
class LookupSource:
    """Where a lookup table is read from.

    Attributes:
        table: The table, `schema.table`.
        key: The column of the values translated.
        value: The column of their translations.
        filters: Only the rows matching these conditions.
    """

    table: str
    key: str
    value: str
    filters: Sequence[contract.Condition] = ()


def read_lookup(
    reader: contract.ITableReader, source: LookupSource
) -> dict[str, str]:
    """Reads a lookup table: text keys, as workbooks hold them.

    Rows without a key or a value are left out.
    """
    query = contract.TableQuery(
        source.table,
        order_by=(source.key,),
        columns=(source.key, source.value),
        filters=tuple(source.filters),
    )
    return {
        str(row[source.key]): str(row[source.value])
        for row in reader.read(query)
        if row[source.key] is not None and row[source.value] is not None
    }


def with_lookups(
    catalog: contract.MappingCatalog, lookups: Mapping[str, dict[str, str]]
) -> contract.MappingCatalog:
    """The catalog, its lookup tables replaced by those given.

    Raises:
        PipelineConfigError: If a lookup has no sheet in the workbook: its
            mappings could not have been validated against it.
    """
    unknown = sorted(set(lookups) - set(catalog.lookups))
    if unknown:
        raise contract.PipelineConfigError(
            f"lookup tables {unknown} have no lookup_<name> sheet in"
            f" {catalog.source_file or catalog.name}: add one, even empty"
        )
    return dataclasses.replace(catalog, lookups={**catalog.lookups, **lookups})
