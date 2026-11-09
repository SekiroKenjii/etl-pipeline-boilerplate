"""The cutover report as Markdown: for the BAs who sign it off."""

from __future__ import annotations

import html

from core.cutover import diff
from core.cutover import history
from core.cutover import report as cutover_report
from shared import utils

_HEADER = (
    "| Entity | Status | Source | Rejected | Accepted | Expected | Target"
    " | Matched | Missing | Parked | Extra | Different | Duplicates |"
)


def to_markdown(report: cutover_report.CutoverReport) -> str:
    """Renders the report."""
    lines = [
        f"# Cutover report: {report.module}",
        "",
        (
            f"Generated {report.generated_at:%Y-%m-%d %H:%M:%S %Z}."
            f" Verdict: **{report.verdict}**."
        ),
        "",
        _HEADER,
        "|" + "---|" * 13,
    ]
    lines.extend(_row(entity) for entity in report.entities)
    for entity in report.entities:
        lines.extend(_details(entity))
    if report.provenance:
        lines += ["", "## Provenance", ""]
        lines += [
            f"- {entry}: {history.shown(value)}"
            for entry, value in history.flat(report.provenance).items()
        ]
    if report.runs:
        lines += [
            "",
            "## Last runs",
            "",
            (
                "| Pipeline | Status | Started | Read | Created | Updated"
                " | Rejected |"
            ),
            "|---|---|---|---|---|---|---|",
        ]
        for run in report.runs:
            stats = run.stats
            lines.append(
                f"| {_text(run.pipeline)} | {run.status.value}"
                f" | {run.started_at:%Y-%m-%d %H:%M} | {stats.get('total', 0)}"
                f" | {stats.get('created', 0)} | {stats.get('updated', 0)}"
                f" | {stats.get('rejected', 0)} |"
            )
    return "\n".join(lines) + "\n"


def _row(entity: diff.EntityReport) -> str:
    if entity.counted_only:
        return (
            f"| {_text(entity.entity)} | counted | {entity.source_rows}"
            " | - | - | -"
            f" | {entity.target_rows} | - | - | {_maybe(entity.parked)} | -"
            " | - | - |"
        )
    parked = None if entity.parked is None else entity.parked_missing
    return (
        f"| {_text(entity.entity)} | {entity.status} | {entity.source_rows}"
        f" | {entity.rejected} | {entity.accepted} | {entity.expected_rows}"
        f" | {entity.target_rows} | {entity.matched} | {entity.missing}"
        f" | {_maybe(parked)} | {entity.extra} | {entity.different}"
        f" | {entity.duplicates} |"
    )


def _details(entity: diff.EntityReport) -> list[str]:
    """The rejections and differences of an entity, if any."""
    lines: list[str] = []
    if entity.status == "empty":
        lines += [
            "",
            (
                f"{entity.entity}: empty on both sides, nothing compared: check"
                " its source table and filter, or accept it empty"
                f" (`--accept-empty {entity.entity}`)."
            ),
        ]
    if entity.parked:
        lines += [
            "",
            (
                f"{entity.entity}: {entity.parked} orphan(s) parked, waiting"
                f" for their parent; {entity.parked_missing} of the missing"
                " rows among them."
            ),
        ]
    if entity.rejections:
        lines += [
            "",
            (
                f"## {entity.entity}: rejected rows ({entity.accepted} of"
                f" {entity.rejected} accepted)"
            ),
            "",
        ]
        lines += [
            f"- {count} × {_text(reason)}"
            for reason, count in entity.rejections.items()
        ]
    if entity.samples:
        lines += [
            "",
            f"## {entity.entity}: differences (first {len(entity.samples)})",
            "",
        ]
        for sample in entity.samples:
            key = _code(", ".join(str(value) for value in sample.key))
            columns = _text(
                "; ".join(
                    f"{column}: expected {want!r}, found {got!r}"
                    for column, (want, got) in sample.columns.items()
                )
            )
            lines.append(
                f"- {sample.kind} {key}" + (f": {columns}" if columns else "")
            )
    return lines


def _text(text: str) -> str:
    """Text of the data or of a module's code, as Markdown: one line, no
    HTML, no bare pipe, so that it cannot add a heading, a row, a cell or a
    tag to the report."""
    return html.escape(utils.one_line(text), quote=False).replace("|", r"\|")


def _code(text: str) -> str:
    """Text as a code span, on one line, whatever backticks it holds."""
    text = utils.one_line(text)
    if "`" not in text:
        return f"`{text}`"
    return f"`` {text} ``"


def _maybe(value: int | None) -> str:
    return "-" if value is None else str(value)
