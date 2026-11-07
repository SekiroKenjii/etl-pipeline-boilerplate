"""Issue sinks: issues erased for a right to erasure, and purged."""

from __future__ import annotations

import json
import pathlib

from core import quality
from tests.factories import issue_sinks


def test_the_memory_issue_sink() -> None:
    sink = quality.InMemoryIssueSink()

    issue_sinks.check_issue_sink(
        sink, lambda: [issue.record_id for issue in sink.issues]
    )


def test_the_file_issue_sink(tmp_path: pathlib.Path) -> None:
    sink = quality.JsonLinesIssueSink(tmp_path)

    def record_ids() -> list[str | None]:
        return [
            json.loads(line)["record_id"]
            for path in sorted(tmp_path.glob("*/*.issues.jsonl"))
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]

    issue_sinks.check_issue_sink(sink, record_ids)
    # Files left with no issue are deleted, empty ones kept as they are.
    assert all(
        not path.read_text(encoding="utf-8").strip()
        for path in tmp_path.glob("*/*.issues.jsonl")
    )
