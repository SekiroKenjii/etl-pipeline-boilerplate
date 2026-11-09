"""Run ledgers and run locks: in memory, and in files for one host."""

from __future__ import annotations

from collections.abc import Callable, Collection, Mapping
import dataclasses
import datetime
import json
import os
import pathlib
import threading
import time
import typing

from core import contract
from shared import utils

type _Clock = Callable[[], float]


def record_of(result: contract.PipelineResult) -> dict[str, object]:
    """The fields of the ledger a finished run updates."""
    return {
        "status": result.status.value,
        "completed_at": result.completed_at,
        "stats": result.stats.as_dict(),
        "message": result.message,
    }


class InMemoryRunLedger(contract.IRunLedger):
    """Keeps the runs in memory, for tests and local development."""

    def __init__(self) -> None:
        self._runs: dict[str, contract.RunRecord] = {}
        self._lock = threading.Lock()

    def start(
        self,
        run: contract.ETLRunInfo,
        host: str,
        meta: Mapping[str, str] | None = None,
    ) -> None:
        with self._lock:
            if run.run_id in self._runs:
                return
            self._runs[run.run_id] = contract.RunRecord(
                run_id=run.run_id,
                pipeline=run.pipeline_name,
                status=contract.ETLStatus.IN_PROGRESS,
                started_at=run.triggered_at,
                host=host,
                dry_run=run.dry_run,
                meta=dict(meta or {}),
            )

    def finish(self, result: contract.PipelineResult) -> None:
        with self._lock:
            started = self._runs.get(result.run_id)
            if started is None:
                return
            self._runs[result.run_id] = dataclasses.replace(
                started,
                status=result.status,
                completed_at=result.completed_at,
                stats=result.stats.as_dict(),
                message=result.message,
            )

    def record_step(
        self, run_id: str, step: str, result: contract.ETLResult, host: str
    ) -> None:
        with self._lock:
            started = self._runs.get(run_id)
            if started is not None:
                started.steps[step] = contract.StepRecord.of(step, result, host)

    def recent(
        self, pipeline_name: str | None, limit: int = 20
    ) -> list[contract.RunRecord]:
        with self._lock:
            runs = [
                dataclasses.replace(run, steps={})
                for run in self._runs.values()
                if pipeline_name is None or run.pipeline == pipeline_name
            ]
        return sorted(runs, key=lambda r: r.started_at, reverse=True)[:limit]

    def get(self, run_id: str) -> contract.RunRecord | None:
        with self._lock:
            found = self._runs.get(run_id)
            return (
                None
                if found is None
                else dataclasses.replace(found, steps=dict(found.steps))
            )

    def latest(
        self,
        pipeline_prefix: str,
        statuses: Collection[contract.ETLStatus],
        meta_key: str,
    ) -> contract.RunRecord | None:
        with self._lock:
            runs = list(self._runs.values())
        return _latest(runs, pipeline_prefix, statuses, meta_key)

    def last_run(
        self, pipeline_name: str, label: str | None = None
    ) -> contract.RunRecord | None:
        with self._lock:
            runs = list(self._runs.values())
        return _last_run(runs, pipeline_name, label)


class JsonLinesRunLedger(contract.IRunLedger):
    """Appends every start and end of a run, and of its steps, to
    `{directory}/runs.jsonl`.

    For a single host; the last line of a run tells how it went.

    Attributes:
        path: The file.
    """

    def __init__(self, directory: pathlib.Path | str) -> None:
        self.path = pathlib.Path(directory) / "runs.jsonl"
        self._lock = threading.Lock()

    def start(
        self,
        run: contract.ETLRunInfo,
        host: str,
        meta: Mapping[str, str] | None = None,
    ) -> None:
        self._append({
            "run_id": run.run_id,
            "pipeline": run.pipeline_name,
            "status": contract.ETLStatus.IN_PROGRESS.value,
            "started_at": run.triggered_at.isoformat(),
            "host": host,
            "dry_run": run.dry_run,
            "meta": dict(meta or {}),
        })

    def finish(self, result: contract.PipelineResult) -> None:
        fields = record_of(result)
        completed = result.completed_at
        self._append({
            "run_id": result.run_id,
            **fields,
            "completed_at": completed.isoformat() if completed else None,
        })

    def record_step(
        self, run_id: str, step: str, result: contract.ETLResult, host: str
    ) -> None:
        record = contract.StepRecord.of(step, result, host)
        self._append({
            "run_id": run_id,
            "step": {
                **dataclasses.asdict(record),
                "status": record.status.value,
                "completed_at": record.completed_at.isoformat(),
            },
        })

    def recent(
        self, pipeline_name: str | None, limit: int = 20
    ) -> list[contract.RunRecord]:
        records = [
            dataclasses.replace(record, steps={})
            for record in self._read()
            if pipeline_name is None or record.pipeline == pipeline_name
        ]
        return sorted(records, key=lambda r: r.started_at, reverse=True)[:limit]

    def get(self, run_id: str) -> contract.RunRecord | None:
        return next(
            (record for record in self._read() if record.run_id == run_id),
            None,
        )

    def latest(
        self,
        pipeline_prefix: str,
        statuses: Collection[contract.ETLStatus],
        meta_key: str,
    ) -> contract.RunRecord | None:
        return _latest(self._read(), pipeline_prefix, statuses, meta_key)

    def last_run(
        self, pipeline_name: str, label: str | None = None
    ) -> contract.RunRecord | None:
        return _last_run(self._read(), pipeline_name, label)

    def _read(self) -> list[contract.RunRecord]:
        """Every run, its lines merged in order."""
        runs: dict[str, dict[str, object]] = {}
        steps: dict[str, dict[str, contract.StepRecord]] = {}
        with self._lock:
            if not self.path.exists():
                return []
            lines = self.path.read_text(encoding="utf-8").splitlines()
        for line in lines:
            if not line.strip():
                continue
            item = json.loads(line)
            step = _entries(item.pop("step", None))
            if step:
                record = _step(step)
                steps.setdefault(item["run_id"], {})[record.step] = record
            elif "started_at" in item and item["run_id"] in runs:
                continue  # A start written twice: the first one counts.
            else:
                runs.setdefault(item["run_id"], {}).update(item)
        return [
            dataclasses.replace(
                _record(item), steps=steps.get(str(item["run_id"]), {})
            )
            for item in runs.values()
        ]

    def _append(self, item: dict[str, object]) -> None:
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(item, ensure_ascii=False) + "\n")


def _latest(
    runs: list[contract.RunRecord],
    pipeline_prefix: str,
    statuses: Collection[contract.ETLStatus],
    meta_key: str,
) -> contract.RunRecord | None:
    found = [
        run
        for run in runs
        if not run.dry_run
        and run.pipeline.startswith(pipeline_prefix)
        and run.status in statuses
        and meta_key in run.meta
    ]
    if not found:
        return None
    return dataclasses.replace(
        max(found, key=lambda run: run.started_at), steps={}
    )


def _last_run(
    runs: list[contract.RunRecord], pipeline_name: str, label: str | None
) -> contract.RunRecord | None:
    """`IRunLedger.last_run` over every run."""
    found = [
        run
        for run in runs
        if not run.dry_run
        and run.pipeline == pipeline_name
        and (label is None or run.meta.get("label") == label)
    ]
    if not found:
        return None
    return dataclasses.replace(
        max(found, key=lambda run: run.started_at), steps={}
    )


def _entries(value: object) -> dict[str, object]:
    """A JSON object read back; empty for anything else."""
    if not isinstance(value, dict):
        return {}
    return {
        str(k): v for k, v in typing.cast(dict[object, object], value).items()
    }


def _record(item: dict[str, object]) -> contract.RunRecord:
    completed = item.get("completed_at")
    stats = item.get("stats")
    meta = item.get("meta")
    return contract.RunRecord(
        run_id=str(item["run_id"]),
        pipeline=str(item.get("pipeline", "")),
        status=contract.ETLStatus(str(item["status"])),
        started_at=datetime.datetime.fromisoformat(str(item["started_at"])),
        completed_at=(
            datetime.datetime.fromisoformat(str(completed))
            if completed
            else None
        ),
        stats=(
            {k: int(typing.cast(int, v)) for k, v in _entries(stats).items()}
        ),
        message=str(item["message"]) if item.get("message") else None,
        host=str(item.get("host", "")),
        dry_run=bool(item.get("dry_run", False)),
        meta=({k: str(v) for k, v in _entries(meta).items()}),
    )


def _step(item: dict[str, object]) -> contract.StepRecord:
    stats = item.get("stats")
    attempts = item.get("attempts")
    return contract.StepRecord(
        step=str(item["step"]),
        status=contract.ETLStatus(str(item["status"])),
        completed_at=datetime.datetime.fromisoformat(str(item["completed_at"])),
        stats=(
            {k: int(typing.cast(int, v)) for k, v in _entries(stats).items()}
        ),
        message=str(item["message"]) if item.get("message") else None,
        host=str(item.get("host", "")),
        attempts=attempts if isinstance(attempts, int) else 1,
    )


class InMemoryRunLock(contract.IRunLock):
    """Locks in memory: for tests, and a single process."""

    def __init__(self, clock: _Clock = time.time) -> None:
        self._leases: dict[str, tuple[str, float]] = {}
        self._clock = clock
        self._lock = threading.Lock()

    def acquire(self, name: str, owner: str, lease_seconds: float) -> bool:
        with self._lock:
            held = self._leases.get(name)
            now = self._clock()
            if held is not None and held[0] != owner and held[1] > now:
                return False
            self._leases[name] = (owner, now + lease_seconds)
            return True

    def renew(self, name: str, owner: str, lease_seconds: float) -> bool:
        with self._lock:
            held = self._leases.get(name)
            if held is None or held[0] != owner:
                return False
            self._leases[name] = (owner, self._clock() + lease_seconds)
            return True

    def release(self, name: str, owner: str) -> None:
        with self._lock:
            held = self._leases.get(name)
            if held is not None and held[0] == owner:
                del self._leases[name]

    def holder(self, name: str) -> str | None:
        with self._lock:
            held = self._leases.get(name)
            return held[0] if held and held[1] > self._clock() else None


class FileRunLock(contract.IRunLock):
    """Locks as files, `{directory}/<name>.lock`: for one host.

    The file holds the owner and the end of the lease; a lease expired is
    taken over. Several hosts need a database-backed lock.

    Attributes:
        directory: Where the lock files are.
    """

    def __init__(
        self, directory: pathlib.Path | str, clock: _Clock = time.time
    ) -> None:
        self.directory = pathlib.Path(directory)
        self._clock = clock
        self._lock = threading.Lock()

    def acquire(self, name: str, owner: str, lease_seconds: float) -> bool:
        with self._lock:
            path = self._path(name)
            held = self._read(path)
            if (
                held is not None
                and held[0] != owner
                and held[1] > self._clock()
            ):
                return False
            if held is None:
                try:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    descriptor = os.open(
                        path, os.O_CREAT | os.O_EXCL | os.O_WRONLY
                    )
                except FileExistsError:
                    return False  # Another process took it just now.
                os.close(descriptor)
            self._write(path, owner, lease_seconds)
            return True

    def renew(self, name: str, owner: str, lease_seconds: float) -> bool:
        with self._lock:
            path = self._path(name)
            held = self._read(path)
            if held is None or held[0] != owner:
                return False
            self._write(path, owner, lease_seconds)
            return True

    def release(self, name: str, owner: str) -> None:
        with self._lock:
            path = self._path(name)
            held = self._read(path)
            if held is not None and held[0] == owner:
                path.unlink(missing_ok=True)

    def holder(self, name: str) -> str | None:
        with self._lock:
            held = self._read(self._path(name))
        return held[0] if held and held[1] > self._clock() else None

    def _path(self, name: str) -> pathlib.Path:
        return self.directory / f"{name.replace('/', '_')}.lock"

    def _read(self, path: pathlib.Path) -> tuple[str, float] | None:
        try:
            data = json.loads(path.read_text(encoding="utf-8") or "{}")
        except FileNotFoundError:
            return None
        except json.JSONDecodeError:
            return ("", 0.0)  # Being written: taken over once expired.
        return (str(data.get("owner", "")), float(data.get("expires_at", 0)))

    def _write(
        self, path: pathlib.Path, owner: str, lease_seconds: float
    ) -> None:
        utils.atomic_write_text(
            path,
            json.dumps(
                {"owner": owner, "expires_at": self._clock() + lease_seconds}
            ),
        )
