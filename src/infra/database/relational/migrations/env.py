"""Alembic's environment of the ETL tables.

Run by `migrations` (`etl-db upgrade`, `etl-db ddl`), which gives it the
connection, or the dialect for the SQL, the schema and the prefix; not by the
`alembic` command.
"""

from infra.database.relational import migrations

migrations.run_environment()
