"""The history of the runs and of their steps, and the locks of the steps
running, in the ETL tables."""

from __future__ import annotations

from collections.abc import Collection, Mapping
import dataclasses
import json
import time

import sqlalchemy as sa

from core import contract
from infra.database.relational import dialects
from infra.database.relational.bookkeeping import _base


class RelationalRunLedger(_base.Bookkeeper, contract.IRunLedger):
    """The history of the runs, in `etl_runs`, and of their steps, in
    `etl_run_steps`."""

    def start(
        self,
        run: contract.ETLRunInfo,
        host: str,
        meta: Mapping[str, str] | None = None,
    ) -> None:
        try:
            with self.db.begin(f"recording the run {run.run_id}") as conn:
                conn.execute(
                    self.tables.runs.insert().values(
                        run_id=run.run_id,
                        pipeline=run.pipeline_name,
                        status=contract.ETLStatus.IN_PROGRESS.value,
                        started_at=run.triggered_at,
                        host=host,
                        dry_run=run.dry_run,
                        meta=_base.dumps(dict(meta or {})),
                    )
                )
        except contract.FatalError as exc:
            # Idempotent: a start retried after a lost commit reply is recorded.
            if not dialects.is_unique_violation(exc.__cause__):
                raise

    def record_step(
        self, run_id: str, step: str, result: contract.ETLResult, host: str
    ) -> None:
        record = contract.StepRecord.of(step, result, host)
        table = self.tables.run_steps
        values = {
            "status": record.status.value,
            "completed_at": record.completed_at,
            "stats": _base.dumps(record.stats),
            "message": record.message,
            "host": record.host,
            "attempts": record.attempts,
        }

        def attempt() -> None:
            with self.db.begin(f"recording the step {step}") as conn:
                updated = conn.execute(
                    table.update()
                    .where(table.c.run_id == run_id, table.c.step == step)
                    .values(**values)
                ).rowcount
                if not updated:
                    conn.execute(
                        table.insert().values(
                            run_id=run_id, step=step, **values
                        )
                    )

        self._retry_conflicts(attempt)

    def finish(self, result: contract.PipelineResult) -> None:
        table = self.tables.runs
        with self.db.begin(f"recording the end of {result.run_id}") as conn:
            conn.execute(
                table.update()
                .where(table.c.run_id == result.run_id)
                .values(
                    status=result.status.value,
                    completed_at=result.completed_at,
                    stats=_base.dumps(result.stats.as_dict()),
                    message=result.message,
                )
            )

    def recent(
        self, pipeline_name: str | None, limit: int = 20
    ) -> list[contract.RunRecord]:
        table = self.tables.runs
        statement = (
            sa.select(table).order_by(table.c.started_at.desc()).limit(limit)
        )
        if pipeline_name is not None:
            statement = statement.where(table.c.pipeline == pipeline_name)
        with self.db.connect("reading the runs") as conn:
            rows = conn.execute(statement).mappings().all()
        return [_run_record(row) for row in rows]

    def get(self, run_id: str) -> contract.RunRecord | None:
        runs_table, steps = self.tables.runs, self.tables.run_steps
        with self.db.connect(f"reading the run {run_id}") as conn:
            row = (
                conn.execute(
                    sa.select(runs_table).where(runs_table.c.run_id == run_id)
                )
                .mappings()
                .first()
            )
            if row is None:
                return None
            step_rows = (
                conn.execute(sa.select(steps).where(steps.c.run_id == run_id))
                .mappings()
                .all()
            )
        return dataclasses.replace(
            _run_record(row),
            steps={
                step["step"]: contract.StepRecord(
                    step=step["step"],
                    status=contract.ETLStatus(step["status"]),
                    completed_at=_base.utc(step["completed_at"]),
                    stats=json.loads(step["stats"]) if step["stats"] else {},
                    message=step["message"],
                    host=step["host"],
                    attempts=step["attempts"],
                )
                for step in step_rows
            },
        )

    def latest(
        self,
        pipeline_prefix: str,
        statuses: Collection[contract.ETLStatus],
        meta_key: str,
    ) -> contract.RunRecord | None:
        table = self.tables.runs
        # As a key only: in a value, JSON escapes the quotes.
        key = json.dumps(meta_key, ensure_ascii=False) + ":"
        return self._newest(
            table.c.pipeline.startswith(pipeline_prefix, autoescape=True),
            table.c.status.in_([status.value for status in statuses]),
            table.c.meta.contains(key, autoescape=True),
        )

    def last_run(
        self, pipeline_name: str, label: str | None = None
    ) -> contract.RunRecord | None:
        table = self.tables.runs
        # A key and its value: in a value, JSON escapes the quotes.
        pair = '"label": ' + json.dumps(label, ensure_ascii=False)
        labelled = table.c.meta.contains(pair, autoescape=True)
        return self._newest(
            table.c.pipeline == pipeline_name,
            *([] if label is None else [labelled]),
        )

    def _newest(
        self, *where: sa.ColumnElement[bool]
    ) -> contract.RunRecord | None:
        """The newest run, not a dry run, matching `where`."""
        table = self.tables.runs
        statement = (
            sa.select(table)
            .where(table.c.dry_run == sa.false(), *where)
            .order_by(table.c.started_at.desc(), table.c.run_id)
            .limit(1)
        )
        with self.db.connect("reading the runs") as conn:
            row = conn.execute(statement).mappings().first()
        return None if row is None else _run_record(row)


def _run_record(row: sa.RowMapping) -> contract.RunRecord:
    return contract.RunRecord(
        run_id=row["run_id"],
        pipeline=row["pipeline"],
        status=contract.ETLStatus(row["status"]),
        started_at=_base.utc(row["started_at"]),
        completed_at=_base.utc(row["completed_at"])
        if row["completed_at"]
        else None,
        stats=json.loads(row["stats"]) if row["stats"] else {},
        message=row["message"],
        host=row["host"],
        dry_run=bool(row["dry_run"]),
        meta=json.loads(row["meta"]) if row["meta"] else {},
    )


class RelationalRunLock(_base.Bookkeeper, contract.IRunLock):
    """The locks of the steps running, in `etl_locks`: leases, shared by
    every host."""

    def acquire(self, name: str, owner: str, lease_seconds: float) -> bool:
        table = self.tables.locks
        now = time.time()
        lease = {"owner": owner, "expires_at": now + lease_seconds}
        with self.db.begin(f"taking the lock {name}") as conn:
            taken = conn.execute(
                table.update()
                .where(
                    table.c.name == name,
                    sa.or_(table.c.expires_at < now, table.c.owner == owner),
                )
                .values(**lease)
            ).rowcount
        if taken:
            return True
        try:
            with self.db.begin(f"taking the lock {name}") as conn:
                conn.execute(table.insert().values(name=name, **lease))
        except contract.FatalError as exc:
            if dialects.is_unique_violation(exc.__cause__):
                return False  # Held by another owner.
            raise
        return True

    def renew(self, name: str, owner: str, lease_seconds: float) -> bool:
        table = self.tables.locks
        held = sa.and_(table.c.name == name, table.c.owner == owner)
        with self.db.begin(f"renewing the lock {name}") as conn:
            return bool(
                conn.execute(
                    table.update()
                    .where(held)
                    .values(expires_at=time.time() + lease_seconds)
                ).rowcount
            )

    def release(self, name: str, owner: str) -> None:
        table = self.tables.locks
        held = sa.and_(table.c.name == name, table.c.owner == owner)
        with self.db.begin(f"releasing the lock {name}") as conn:
            conn.execute(table.delete().where(held))

    def holder(self, name: str) -> str | None:
        table = self.tables.locks
        live = sa.and_(table.c.name == name, table.c.expires_at >= time.time())
        with self.db.connect(f"reading the lock {name}") as conn:
            owner = conn.execute(sa.select(table.c.owner).where(live)).scalar()
        return None if owner is None else str(owner)
