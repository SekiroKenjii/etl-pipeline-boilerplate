"""The cutover report as one HTML page: for a browser, or an e-mail."""

from __future__ import annotations

from collections.abc import Callable
import html
import json

from core.cutover import checksum
from core.cutover import diff
from core.cutover import history
from core.cutover import report as cutover_report

_STYLE = """
body { font: 14px/1.5 system-ui, sans-serif; margin: 2rem; color: #1f2328; }
table { border-collapse: collapse; margin: 1rem 0; }
th, td { border: 1px solid #d0d7de; padding: 4px 10px; text-align: right; }
th { background: #f6f8fa; }
td:first-child, th:first-child { text-align: left; }
.match { color: #1a7f37; }
.mismatch, .empty { color: #cf222e; font-weight: 600; }
.counted, .match-with-rejects { color: #9a6700; }
.verdict { font-size: 1.2rem; font-weight: 600; }
code { font-size: 12px; }
"""
_COLUMNS: tuple[tuple[str, Callable[[diff.EntityReport], object]], ...] = (
    ("Entity", lambda e: e.entity),
    ("Status", lambda e: e.status),
    ("Source", lambda e: e.source_rows),
    ("Rejected", lambda e: e.rejected),
    ("Accepted", lambda e: e.accepted),
    ("Expected", lambda e: e.expected_rows),
    ("Target", lambda e: e.target_rows),
    ("Matched", lambda e: e.matched),
    ("Missing", lambda e: e.missing),
    ("Parked", lambda e: e.parked_missing),
    ("Extra", lambda e: e.extra),
    ("Different", lambda e: e.different),
    ("Duplicates", lambda e: e.duplicates),
)


def to_html(
    report: cutover_report.CutoverReport, digest: str | None = None
) -> str:
    """Renders the report as a self-contained page."""
    esc = html.escape
    verdict = report.verdict
    rows = "".join(
        "<tr>"
        + "".join(
            f'<td class="{esc(entity.status)}">{esc(entity.status)}</td>'
            if label == "Status"
            else f"<td>{esc(_cell(value(entity)))}</td>"
            for label, value in _COLUMNS
        )
        + "</tr>"
        for entity in report.entities
    )
    details = "".join(_details(entity) for entity in report.entities)
    sealed = f"<p>SHA-256 <code>{esc(digest)}</code></p>" if digest else ""
    provenance = "".join(
        f"<li>{esc(entry)}: <code>{esc(_plain(value))}</code></li>"
        for entry, value in history.flat(report.provenance).items()
    )
    if provenance:
        details += f"<h2>Provenance</h2><ul>{provenance}</ul>"
    return (
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        f"<title>Cutover report: {esc(report.module)}</title>"
        f"<style>{_STYLE}</style></head><body>"
        f"<h1>Cutover report: {esc(report.module)}</h1>"
        f"<p>Generated {esc(report.generated_at.isoformat())}.</p>"
        f'<p class="verdict {_VERDICT_CLASSES[verdict]}">'
        f"{verdict}</p>{sealed}"
        "<table><thead><tr>"
        + "".join(f"<th>{esc(label)}</th>" for label, _ in _COLUMNS)
        + f"</tr></thead><tbody>{rows}</tbody></table>{details}</body></html>\n"
    )


_VERDICT_CLASSES = {
    "PASSED": "match",
    "PASSED-WITH-REJECTS": "match-with-rejects",
    "FAILED": "mismatch",
    "COUNTED": "counted",
}


def _plain(value: object) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _cell(value: object) -> str:
    return "-" if value is None else str(value)


def _details(entity: diff.EntityReport) -> str:
    """The rejections and differences of an entity, if any."""
    esc = html.escape
    parts: list[str] = []
    if entity.rejections:
        items = "".join(
            f"<li>{count} × {esc(reason)}</li>"
            for reason, count in entity.rejections.items()
        )
        parts.append(f"<h2>{esc(entity.entity)}: rejected</h2><ul>{items}</ul>")
    if entity.samples:
        items = "".join(
            f"<li>{esc(sample.kind)} <code>{esc(str(_shown(sample.key)))}"
            "</code>"
            + "".join(
                f"; {esc(column)}: expected"
                f" <code>{esc(repr(checksum.shown(want)))}</code>, found"
                f" <code>{esc(repr(checksum.shown(got)))}</code>"
                for column, (want, got) in sample.columns.items()
            )
            + "</li>"
            for sample in entity.samples
        )
        parts.append(
            f"<h2>{esc(entity.entity)}: differences</h2><ul>{items}</ul>"
        )
    return "".join(parts)


def _shown(
    key: tuple[checksum.Canonical, ...],
) -> list[None | int | float | str]:
    """A key as the JSON report shows it."""
    return [checksum.shown(value) for value in key]
