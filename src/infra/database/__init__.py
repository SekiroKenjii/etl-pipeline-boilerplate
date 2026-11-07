"""Database adapters, implementing `contract.ITableReader`, `ITableWriter`,
`ISchemaInspector` and the ETL bookkeeping ports (keys, state, parking...).

- `relational`: SQL databases through SQLAlchemy Core (`sql` extra);
- `mongodb`: MongoDB collections as tables of documents (`mongodb` extra).

Each needs its extra, so none is imported here:
`from infra.database import relational`.
"""
