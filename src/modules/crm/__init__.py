"""The CRM migration: customers and their orders, from a legacy database.

The sample module of the boilerplate, a `core.mapping_pipelines.MappingModule`:
its settings, its custom transforms, and its mappings in `mappings/crm.xlsx`
(compiled to `mappings/compiled/crm.json`). `etl new-module` writes the
same, for another domain.

    etl run crm.full_load      # Incremental load, by watermark.
    etl run crm.sync           # Snapshot, then change data capture.
    etl run crm.parked         # Orphans parked until their parent comes.
    etl run crm.replay         # Quarantined rows, once fixed.
    etl run crm.deletes        # Deletes of a source without a change log.
"""

from modules.crm.module import create
from modules.crm.module import Crm

__all__ = [
    "Crm",
    "create",
]
