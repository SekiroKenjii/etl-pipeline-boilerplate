"""The records the quality gate rejected, and every issue it found, in the
ETL tables."""

from __future__ import annotations

from collections.abc import Callable, Collection, Iterator, Sequence
import datetime
import json

import sqlalchemy as sa

from core import contract
from infra.database.relational import database
from infra.database.relational.bookkeeping import _base
from infra.database.relational.bookkeeping import definitions
from shared import utils


class RelationalQuarantine(_base.Bookkeeper, contract.IQuarantine):
    """Rejected records, in `etl_quarantine`, with the issues that rejected
    them, as JSON text."""

    def __init__(
        self,
        db: database.Database | str,
        *,
        schema: str | None = None,
        prefix: str = definitions.DEFAULT_PREFIX,
        serialize: Callable[[object], object] | None = None,
    ) -> None:
        """Initializes the quarantine.

        Args:
            db: A shared database, or a SQLAlchemy URL.
            schema: The schema of the ETL tables.
            prefix: The prefix of their names.
            serialize: Turns a record into JSON-friendly data, e.g. to hide
                sensitive fields. By default records are stored as they are,
                through `utils.to_jsonable`.
        """
        super().__init__(db, schema=schema, prefix=prefix)
        self._serialize = serialize

    def put(
        self,
        records: Sequence[contract.RejectedRecord[object]],
        ctx: contract.ETLContext,
    ) -> None:
        """See `contract.IQuarantine.put`: a record rejected again updates
        its open entry, keeping its id.

        Safe for steps rejecting the same record at once: the open entries
        are unique (`OPEN_ENTRY_INDEX_DIALECTS`), and a writer adding one
        another just added tries again, then updates it.
        """
        if not records:
            return
        table = self.tables.quarantine
        pipeline = ctx.run.pipeline_name
        now = utils.utc_now()
        rows: dict[str, dict[str, object]] = {}
        for rejected in records:
            record = self._dump(rejected.record)
            key = contract.QuarantineEntry.key_of(
                pipeline, rejected.checkpoint.value, record
            )
            # The last one, if twice in the batch.
            rows[key] = {
                "run_id": ctx.run.run_id,
                "checkpoint": rejected.checkpoint.value,
                "record": _base.dumps(record),
                "issues": _base.dumps(
                    [issue.as_dict() for issue in rejected.issues]
                ),
                "rejected_at": now,
            }

        def attempt() -> None:
            with self.db.begin(f"quarantining {len(rows)} record(s)") as conn:
                open_ids: dict[str, int] = {}
                for chunk in self._chunks(list(rows)):
                    for key, id_ in conn.execute(
                        sa.select(table.c.record_key, table.c.id).where(
                            table.c.pipeline == pipeline,
                            table.c.status
                            == contract.QuarantineStatus.OPEN.value,
                            table.c.record_key.in_(chunk),
                        )
                    ):
                        open_ids[key] = id_
                again = [
                    {"p_id": open_ids[key], **row}
                    for key, row in rows.items()
                    if key in open_ids
                ]
                if again:
                    conn.execute(
                        table.update().where(
                            table.c.id == sa.bindparam("p_id")
                        ),
                        again,
                    )
                new = [
                    {"pipeline": pipeline, "record_key": key, **row}
                    for key, row in rows.items()
                    if key not in open_ids
                ]
                if new:
                    conn.execute(table.insert(), new)

        self._retry_conflicts(attempt)

    def _dump(self, record: object) -> object:
        return self._serialize(record) if self._serialize else record

    def entries(
        self,
        pipeline: str | None = None,
        status: contract.QuarantineStatus | None = (
            contract.QuarantineStatus.OPEN
        ),
    ) -> Iterator[contract.QuarantineEntry]:
        table = self.tables.quarantine
        statement = sa.select(table).order_by(table.c.id)
        if pipeline is not None:
            statement = statement.where(table.c.pipeline == pipeline)
        if status is not None:
            statement = statement.where(table.c.status == status.value)
        # Pages by id: no transaction stays open while the caller works.
        after = None
        while True:
            page = statement.limit(self.in_chunk)
            if after is not None:
                page = page.where(table.c.id > after)
            with self.db.connect("reading the quarantine") as conn:
                rows = conn.execute(page).mappings().all()
            for row in rows:
                yield contract.QuarantineEntry(
                    id=str(row["id"]),
                    pipeline=row["pipeline"],
                    run_id=row["run_id"],
                    checkpoint=row["checkpoint"],
                    record=json.loads(row["record"]),
                    issues=tuple(json.loads(row["issues"])),
                    rejected_at=_base.utc(row["rejected_at"]),
                    status=contract.QuarantineStatus(row["status"]),
                    resolved_at=(
                        _base.utc(row["resolved_at"])
                        if row["resolved_at"]
                        else None
                    ),
                )
            if len(rows) < self.in_chunk:
                return
            after = rows[-1]["id"]

    def resolve(
        self, ids: Collection[str], status: contract.QuarantineStatus
    ) -> int:
        table = self.tables.quarantine
        numbers = sorted({int(id_) for id_ in ids})
        changed = 0
        with self.db.begin(f"resolving {len(numbers)} record(s)") as conn:
            for chunk in self._chunks(numbers):
                changed += conn.execute(
                    table.update()
                    .where(
                        table.c.id.in_(chunk), table.c.status != status.value
                    )
                    .values(status=status.value, resolved_at=utils.utc_now())
                ).rowcount
        return changed

    def erase(self, ids: Collection[str]) -> int:
        table = self.tables.quarantine
        numbers = sorted({int(id_) for id_ in ids})
        erased = 0
        with self.db.begin(f"erasing {len(numbers)} record(s)") as conn:
            for chunk in self._chunks(numbers):
                erased += conn.execute(
                    table.delete().where(table.c.id.in_(chunk))
                ).rowcount
        return erased

    def purge(self, before: datetime.datetime) -> int:
        table = self.tables.quarantine
        with self.db.begin("purging the quarantine") as conn:
            return conn.execute(
                table.delete().where(table.c.rejected_at < before)
            ).rowcount


class RelationalIssueSink(_base.Bookkeeper, contract.IIssueSink):
    """Every quality issue, in `etl_quality_issues`."""

    def write(
        self, issues: Sequence[contract.QualityIssue], ctx: contract.ETLContext
    ) -> None:
        if not issues:
            return
        now = utils.utc_now()
        with self.db.begin(f"writing {len(issues)} quality issue(s)") as conn:
            conn.execute(
                self.tables.quality_issues.insert(),
                [
                    {
                        "pipeline": ctx.run.pipeline_name,
                        "run_id": ctx.run.run_id,
                        "rule": issue.rule,
                        "checkpoint": issue.checkpoint.value,
                        "action": issue.action.value,
                        "message": issue.message,
                        "field": issue.field,
                        "value": (
                            None
                            if issue.value is None
                            else _base.dumps(issue.value)
                        ),
                        "record_id": issue.record_id,
                        "found_at": now,
                    }
                    for issue in issues
                ],
            )

    def erase(self, record_ids: Collection[str]) -> int:
        table = self.tables.quality_issues
        wanted = sorted(set(record_ids))
        erased = 0
        with self.db.begin(
            f"erasing the issues of {len(wanted)} record(s)"
        ) as conn:
            for chunk in self._chunks(wanted):
                erased += conn.execute(
                    table.delete().where(table.c.record_id.in_(chunk))
                ).rowcount
        return erased

    def purge(self, before: datetime.datetime) -> int:
        table = self.tables.quality_issues
        with self.db.begin("purging the quality issues") as conn:
            return conn.execute(
                table.delete().where(table.c.found_at < before)
            ).rowcount
