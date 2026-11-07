"""Quarantines and issue sinks: in memory, and JSON Lines files."""

from __future__ import annotations

from collections.abc import Callable, Collection, Iterator, Mapping, Sequence
import dataclasses
import datetime
import json
import pathlib
import threading
from typing import Any

from core import contract
from shared import utils


class InMemoryQuarantine(contract.IQuarantine):
    """Keeps rejected records in memory, for tests and local development.

    Pass it explicitly: it is never used by default.

    Attributes:
        records: Every record rejected so far.
    """

    def __init__(self) -> None:
        self.records: list[contract.RejectedRecord[Any]] = []
        self._entries: list[contract.QuarantineEntry] = []
        self._next_id = 1
        self._lock = threading.Lock()

    def put(
        self,
        records: Sequence[contract.RejectedRecord[Any]],
        ctx: contract.ETLContext,
    ) -> None:
        now = utils.utc_now()
        with self._lock:
            self.records.extend(records)
            open_index_by_key = {
                entry.key: index
                for index, entry in enumerate(self._entries)
                if entry.status is contract.QuarantineStatus.OPEN
            }
            for rejected in records:
                entry = contract.QuarantineEntry(
                    id=str(self._next_id),
                    pipeline=ctx.run.pipeline_name,
                    run_id=ctx.run.run_id,
                    checkpoint=rejected.checkpoint.value,
                    # JSON-compatible, as the other quarantines store it.
                    record=json.loads(
                        json.dumps(rejected.record, default=utils.to_jsonable)
                    ),
                    issues=tuple(issue.as_dict() for issue in rejected.issues),
                    rejected_at=now,
                )
                index = open_index_by_key.get(entry.key)
                if index is None:
                    open_index_by_key[entry.key] = len(self._entries)
                    self._entries.append(entry)
                    self._next_id += 1
                else:
                    self._entries[index] = dataclasses.replace(
                        entry, id=self._entries[index].id
                    )

    def entries(
        self,
        pipeline: str | None = None,
        status: contract.QuarantineStatus | None = (
            contract.QuarantineStatus.OPEN
        ),
    ) -> Iterator[contract.QuarantineEntry]:
        # A snapshot: entries may change while the caller iterates.
        snapshot = self._entries.copy()
        for entry in snapshot:
            if _selected(entry, pipeline, status):
                yield entry

    def resolve(
        self, ids: Collection[str], status: contract.QuarantineStatus
    ) -> int:
        wanted, changed = set(ids), 0
        now = utils.utc_now()
        with self._lock:
            for index, entry in enumerate(self._entries):
                if entry.id in wanted and entry.status != status:
                    self._entries[index] = dataclasses.replace(
                        entry, status=status, resolved_at=now
                    )
                    changed += 1
        return changed

    def erase(self, ids: Collection[str]) -> int:
        wanted = set(ids)
        with self._lock:
            kept = [e for e in self._entries if e.id not in wanted]
            erased = len(self._entries) - len(kept)
            self._entries = kept
        return erased

    def purge(self, before: datetime.datetime) -> int:
        with self._lock:
            kept = [e for e in self._entries if e.rejected_at >= before]
            purged = len(self._entries) - len(kept)
            self._entries = kept
        return purged


class InMemoryIssueSink(contract.IIssueSink):
    """Keeps every issue in memory, for tests and local development.

    Attributes:
        issues: Every issue kept, oldest first.
    """

    def __init__(self) -> None:
        self.issues: list[contract.QualityIssue] = []
        self._found_at: list[datetime.datetime] = []
        self._lock = threading.Lock()

    def write(
        self, issues: Sequence[contract.QualityIssue], ctx: contract.ETLContext
    ) -> None:
        now = utils.utc_now()
        with self._lock:
            self.issues.extend(issues)
            self._found_at.extend(now for _ in issues)

    def erase(self, record_ids: Collection[str]) -> int:
        wanted = set(record_ids)
        return self._keep(lambda issue, _: issue.record_id not in wanted)

    def purge(self, before: datetime.datetime) -> int:
        return self._keep(lambda _, found_at: found_at >= before)

    def _keep(
        self,
        kept: Callable[[contract.QualityIssue, datetime.datetime], bool],
    ) -> int:
        """Keeps the issues `kept` says; returns how many are deleted."""
        with self._lock:
            pairs = [
                pair for pair in zip(self.issues, self._found_at) if kept(*pair)
            ]
            deleted = len(self.issues) - len(pairs)
            self.issues = [issue for issue, _ in pairs]
            self._found_at = [found_at for _, found_at in pairs]
        return deleted


class _JsonLinesStore:
    """One JSON Lines file per run: `{directory}/{pipeline}/{run_id}{suffix}`.

    Each line carries the run metadata, so that files can be concatenated or
    loaded into a table as they are. Values that are not JSON types go through
    `utils.to_jsonable`. Each write appends to the file of its run, and
    closes it: nothing stays open between writes.

    Attributes:
        suffix: The end of the file names.
        directory: Where the files are written.
    """

    suffix: str

    def __init__(self, directory: pathlib.Path | str) -> None:
        self.directory = pathlib.Path(directory)
        # Steps running in parallel write and rewrite the same files.
        self._file_lock = threading.Lock()

    def path_for(self, ctx: contract.ETLContext) -> pathlib.Path:
        """Returns the file of the run of `ctx`."""
        return (
            self.directory
            / ctx.run.pipeline_name
            / f"{ctx.run.run_id}{self.suffix}"
        )

    def _write(
        self, rows: Sequence[Mapping[str, object]], ctx: contract.ETLContext
    ) -> None:
        """Appends `rows` with the run metadata to the file of the run."""
        written_at = utils.utc_now().isoformat()
        with self._file_lock:
            utils.append_json_lines(
                self.path_for(ctx),
                (self._line(row, ctx, written_at) for row in rows),
            )

    @staticmethod
    def _line(
        row: Mapping[str, object], ctx: contract.ETLContext, written_at: str
    ) -> dict[str, object]:
        """A row with the run metadata, as written."""
        return {
            "pipeline": ctx.run.pipeline_name,
            "run_id": ctx.run.run_id,
            "written_at": written_at,
            **row,
        }

    def _files(self, pipeline: str | None) -> list[pathlib.Path]:
        """The files of `pipeline`, of every pipeline when None."""
        root = self.directory / pipeline if pipeline else self.directory
        pattern = f"*{self.suffix}" if pipeline else f"*/*{self.suffix}"
        return sorted(root.glob(pattern)) if root.exists() else []


class JsonLinesIssueSink(_JsonLinesStore, contract.IIssueSink):
    """Appends every issue to `{directory}/{pipeline}/{run_id}.issues.jsonl`.

    `erase` and `purge` rewrite the files, without the lines they delete; a
    file left with none is deleted.
    """

    suffix = ".issues.jsonl"

    def write(
        self, issues: Sequence[contract.QualityIssue], ctx: contract.ETLContext
    ) -> None:
        self._write([issue.as_dict() for issue in issues], ctx)

    def erase(self, record_ids: Collection[str]) -> int:
        wanted = set(record_ids)
        return self._keep(lambda item: item.get("record_id") not in wanted)

    def purge(self, before: datetime.datetime) -> int:
        return self._keep(
            lambda item: datetime.datetime.fromisoformat(item["written_at"])
            >= before
        )

    def _keep(self, kept: Callable[[dict[str, Any]], bool]) -> int:
        """Keeps the lines `kept` says; returns how many are deleted."""
        deleted = 0
        for path in self._files(None):
            lines = [
                line
                for line in path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            live = [line for line in lines if kept(json.loads(line))]
            if len(live) == len(lines):
                continue
            deleted += len(lines) - len(live)
            if live:
                utils.atomic_write_text(path, "\n".join(live) + "\n")
            else:
                path.unlink()
        return deleted


class JsonLinesQuarantine(_JsonLinesStore, contract.IQuarantine):
    """Appends every rejected record to a JSON Lines file of the run.

    The file is `{directory}/{pipeline}/{run_id}.quarantine.jsonl`: one record
    per line, with the checkpoint and the issues that rejected it. Statuses
    are appended to `{directory}/resolutions.jsonl`.
    """

    suffix = ".quarantine.jsonl"

    def __init__(
        self,
        directory: pathlib.Path | str,
        serialize: Callable[[object], object] | None = None,
    ) -> None:
        """Initializes the quarantine.

        Args:
            directory: Where the files are written.
            serialize: Turns a record into JSON-friendly data, e.g. to hide
                sensitive fields. By default records are written as they are:
                dataclasses, dates, enums... are handled by `to_jsonable`.
        """
        super().__init__(directory)
        self._serialize = serialize
        self._lock = threading.Lock()

    def put(
        self,
        records: Sequence[contract.RejectedRecord[Any]],
        ctx: contract.ETLContext,
    ) -> None:
        """See `contract.IQuarantine.put`: a record rejected again rewrites
        the line of its open entry, in whichever run's file it is, keeping
        its id; the others are appended to the file of the run.
        """
        if not records:
            return
        # Reads every open entry of the pipeline: for small, development use.
        pipeline = ctx.run.pipeline_name
        with self._lock:
            open_ids = {entry.key: entry.id for entry in self.entries(pipeline)}
            new: dict[str, dict[str, object]] = {}
            again: dict[str, dict[str, object]] = {}
            for rejected in records:
                row: dict[str, object] = {
                    "checkpoint": rejected.checkpoint.value,
                    "record": self._dump(rejected.record),
                    "issues": [issue.as_dict() for issue in rejected.issues],
                }
                key = contract.QuarantineEntry.key_of(
                    pipeline, rejected.checkpoint.value, row["record"]
                )
                if key in open_ids:
                    again[open_ids[key]] = row
                else:
                    new[key] = row  # The last one, if twice in the batch.
            if again:
                self._rewrite(again, ctx)
            if new:
                self._write(list(new.values()), ctx)

    def _rewrite(
        self, rows: Mapping[str, Mapping[str, object]], ctx: contract.ETLContext
    ) -> None:
        """Replaces the lines of some entries, by id, with rows of this run."""
        written_at = utils.utc_now().isoformat()
        by_file: dict[str, dict[int, Mapping[str, object]]] = {}
        for id_, row in rows.items():
            name, _, line = id_.rpartition(":")
            by_file.setdefault(name, {})[int(line)] = row
        for name, numbered in by_file.items():
            path = self.directory / name
            with self._file_lock:
                lines = path.read_text(encoding="utf-8").splitlines()
                for number, row in numbered.items():
                    lines[number - 1] = json.dumps(
                        self._line(row, ctx, written_at),
                        default=utils.to_jsonable,
                        ensure_ascii=False,
                    )
                utils.atomic_write_text(path, "\n".join(lines) + "\n")

    def _dump(self, record: object) -> object:
        return self._serialize(record) if self._serialize else record

    def entries(
        self,
        pipeline: str | None = None,
        status: contract.QuarantineStatus | None = (
            contract.QuarantineStatus.OPEN
        ),
    ) -> Iterator[contract.QuarantineEntry]:
        resolutions = self._resolutions()
        found: list[contract.QuarantineEntry] = []
        for path in self._files(pipeline):
            for entry in self._read(path):
                resolved = resolutions.get(entry.id)
                if resolved is not None:
                    entry = dataclasses.replace(
                        entry, status=resolved[0], resolved_at=resolved[1]
                    )
                if _selected(entry, pipeline, status):
                    found.append(entry)
        yield from sorted(found, key=lambda e: e.rejected_at)

    def resolve(
        self, ids: Collection[str], status: contract.QuarantineStatus
    ) -> int:
        resolutions = self._resolutions()
        changed = [
            id_
            for id_ in dict.fromkeys(ids)
            if resolutions.get(id_, (contract.QuarantineStatus.OPEN,))[0]
            != status
        ]
        if changed:
            now = utils.utc_now().isoformat()
            self.directory.mkdir(parents=True, exist_ok=True)
            with self._resolutions_path().open("a", encoding="utf-8") as out:
                for id_ in changed:
                    out.write(
                        json.dumps({
                            "id": id_,
                            "status": status.value,
                            "resolved_at": now,
                        })
                        + "\n"
                    )
        return len(changed)

    def erase(self, ids: Collection[str]) -> int:
        """See `contract.IQuarantine.erase`: the lines are emptied, not
        removed, so that the ids of the others stay."""
        by_file: dict[str, set[int]] = {}
        for id_ in ids:
            name, _, line = id_.rpartition(":")
            by_file.setdefault(name, set()).add(int(line))
        erased = 0
        for name, numbers in by_file.items():
            path = self.directory / name
            if not path.exists():
                continue
            lines = path.read_text(encoding="utf-8").splitlines()
            for number in numbers:
                if 0 < number <= len(lines) and lines[number - 1].strip():
                    item = json.loads(lines[number - 1])
                    if not item.get("erased"):
                        lines[number - 1] = json.dumps({
                            "written_at": item["written_at"],
                            "erased": True,
                        })
                        erased += 1
            utils.atomic_write_text(path, "\n".join(lines) + "\n")
        return erased

    def purge(self, before: datetime.datetime) -> int:
        """See `contract.IQuarantine.purge`: old lines are emptied, as by
        `erase`; a file left with none is deleted."""
        purged = 0
        for path in self._files(None):
            lines = path.read_text(encoding="utf-8").splitlines()
            live = 0
            for index, line in enumerate(lines):
                if not line.strip():
                    continue
                item = json.loads(line)
                if item.get("erased"):
                    continue
                if datetime.datetime.fromisoformat(item["written_at"]) < before:
                    lines[index] = json.dumps(
                        {"written_at": item["written_at"], "erased": True}
                    )
                    purged += 1
                else:
                    live += 1
            if not live:
                path.unlink()
            else:
                utils.atomic_write_text(path, "\n".join(lines) + "\n")
        return purged

    def _read(self, path: pathlib.Path) -> Iterator[contract.QuarantineEntry]:
        name = path.relative_to(self.directory).as_posix()
        for number, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(), start=1
        ):
            if not line.strip():
                continue
            item = json.loads(line)
            if item.get("erased"):
                continue
            yield contract.QuarantineEntry(
                id=f"{name}:{number}",
                pipeline=item["pipeline"],
                run_id=item["run_id"],
                checkpoint=item["checkpoint"],
                record=item["record"],
                issues=tuple(item["issues"]),
                rejected_at=datetime.datetime.fromisoformat(item["written_at"]),
            )

    def _resolutions_path(self) -> pathlib.Path:
        return self.directory / "resolutions.jsonl"

    def _resolutions(
        self,
    ) -> dict[str, tuple[contract.QuarantineStatus, datetime.datetime]]:
        """The last status of every resolved record, by id."""
        path = self._resolutions_path()
        if not path.exists():
            return {}
        found: dict[
            str, tuple[contract.QuarantineStatus, datetime.datetime]
        ] = {}
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                item = json.loads(line)
                found[item["id"]] = (
                    contract.QuarantineStatus(item["status"]),
                    datetime.datetime.fromisoformat(item["resolved_at"]),
                )
        return found


def _selected(
    entry: contract.QuarantineEntry,
    pipeline: str | None,
    status: contract.QuarantineStatus | None,
) -> bool:
    """Whether an entry is of `pipeline` and in `status` (None: any)."""
    return (pipeline is None or entry.pipeline == pipeline) and (
        status is None or entry.status == status
    )
