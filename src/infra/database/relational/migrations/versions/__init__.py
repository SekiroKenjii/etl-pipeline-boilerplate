"""The revisions of the ETL tables, oldest first: `migrations.REVISIONS`.

Each is written by hand and never changed once released: a change of
`bookkeeping.etl_tables` is a new revision. Alembic calls `upgrade` with the
schema and the prefix of the tables.
"""
