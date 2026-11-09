"""How the target of each mapping reconciles with its source, for cutover."""

from __future__ import annotations

from collections.abc import Callable

from core import contract
from core import mapping
from core import operators
from core import runtime
from core.mapping_pipelines import mappings as pipelines_mappings
from core.runtime import names


def reconciliations(
    resources: runtime.Resources, mappings: pipelines_mappings.Mappings
) -> list[contract.Reconciliation]:
    """Each mapping's target table against its source table, for the
    cutover: a source row is expected as the mapping makes it, corrected;
    soft-deleted rows are not expected, parked orphans are accounted for."""
    settings = mappings.settings
    source = resources.get(names.reader(settings.source), contract.ITableReader)
    target = resources.get(names.reader(settings.target), contract.ITableReader)
    # Soft-deleted rows left the source: they are not expected.
    live = (
        (contract.Condition("is_deleted", contract.Operator.EQ, False),)
        if settings.deletes == "soft"
        else ()
    )
    found: list[contract.Reconciliation] = []
    for name, compiled in mappings.compiled.items():
        key = mappings.key_field(name)
        found.append(
            contract.Reconciliation(
                entity=name,
                source=source,
                source_query=contract.TableQuery(
                    mappings.source_table(name),
                    order_by=mappings.natural_key(name),
                ),
                expect=_expect(
                    compiled, mappings.corrections, name, mappings.lineage(name)
                ),
                target=target,
                target_query=contract.TableQuery(
                    mappings.target_table(name), order_by=(key,), filters=live
                ),
                key=(key,),
                columns=tuple(f.target for f in compiled.spec.fields),
                parking_queue=name if settings.orphans == "park" else None,
                parked_row=_parked_row,
                source_database=settings.source,
                target_database=settings.target,
            )
        )
    return found


def _parked_row(entry: contract.ParkedEntry) -> contract.Row | None:
    """The target row a parked orphan stands for; None if it does not
    decode."""
    try:
        return mapping.decode_record(entry.payload).data
    except (KeyError, TypeError, ValueError):
        return None


def _expect(
    compiled: mapping.CompiledMapping,
    corrections: operators.Corrections,
    name: str,
    lineage: Callable[[operators.SourceRecord], str],
) -> Callable[[contract.Row], contract.Expected]:
    """What a source row becomes, corrected, through the mapping."""

    def expect(row: contract.Row) -> contract.Expected:
        records = compiled.map(corrections.apply(name, lineage(row), row))
        issues = [
            f"{issue.field}: {issue.message}"
            for record in records
            for issue in record.issues
        ]
        if issues:
            return contract.Expected(rejected=issues)
        return contract.Expected(rows=[record.data for record in records])

    return expect
