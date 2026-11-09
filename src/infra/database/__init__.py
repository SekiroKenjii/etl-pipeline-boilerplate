"""Database adapters, implementing `contract.ITableReader`, `ITableWriter`,
`ISchemaInspector` and the ETL bookkeeping ports (keys, state, parking...).

- `relational`: SQL databases through SQLAlchemy Core;
- `mongodb`: MongoDB collections as tables of documents.

Import the one used: `from infra.database import relational`.
"""
