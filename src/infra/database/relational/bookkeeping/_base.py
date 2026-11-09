"""What the adapters of the ETL tables share: their tables, retries of
concurrent writes, JSON text, chunks of IN lists, times read back."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
import datetime
import json

from infra.database.relational import database
from infra.database.relational import dialects
from infra.database.relational.bookkeeping import definitions
from shared import utils

_CONCURRENT_KEY_WRITE_ATTEMPTS = 8


def dumps(value: object) -> str:
    """`value` as JSON text, as the ETL tables keep it."""
    return json.dumps(value, ensure_ascii=False, default=utils.to_jsonable)


def chunks[T](items: Sequence[T], size: int) -> Iterator[Sequence[T]]:
    """`items`, `size` at a time: the values of an IN list."""
    for start in range(0, len(items), size):
        yield items[start : start + size]


class Bookkeeper(database.RelationalAdapter):
    """An adapter writing to the ETL tables.

    Attributes:
        tables: The ETL tables.
        in_chunk: The values per IN list, and the rows of a page: the
            dialect's (`Dialect.in_chunk`, `tuning.in_chunk`).
    """

    def __init__(
        self,
        db: database.Database | str,
        *,
        schema: str | None = None,
        prefix: str = definitions.DEFAULT_PREFIX,
    ) -> None:
        """Initializes the adapter.

        Args:
            db: A shared database, or a SQLAlchemy URL.
            schema: The schema of the ETL tables.
            prefix: The prefix of their names.
        """
        super().__init__(db)
        self.tables = definitions.etl_tables(schema, prefix)
        self.in_chunk = self.db.dialect.in_chunk()

    def _chunks[T](self, items: Sequence[T]) -> Iterator[Sequence[T]]:
        return chunks(items, self.in_chunk)

    def _retry_conflicts(self, attempt: Callable[[], None]) -> None:
        """Runs `attempt` again when a concurrent writer took a key first.

        An attempt that reads what exists before writing what is missing (an
        UPDATE then an INSERT, a merge's NOT EXISTS) fails on the primary key
        when a writer inserts the same key in between; the next attempt then
        sees that key.
        """
        utils.retry(
            attempt,
            when=lambda exc: dialects.is_unique_violation(exc.__cause__),
            attempts=_CONCURRENT_KEY_WRITE_ATTEMPTS,
        )


def utc(moment: datetime.datetime) -> datetime.datetime:
    """A time read back: SQLite and MySQL drop the time zone, UTC here."""
    return moment if moment.tzinfo else moment.replace(tzinfo=datetime.UTC)
