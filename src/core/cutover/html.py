"""The cutover report as one HTML page: for a browser, or an e-mail."""

from __future__ import annotations

import html
import json
from typing import Any

from core.cutover import history
from core.cutover import report as cutover_report

_STYLE = """
body { font: 14px/1.5 system-ui, sans-serif; margin: 2rem; color: #1f2328; }
table { border-collapse: collapse; margin: 1rem 0; }
th, td { border: 1px solid #d0d7de; padding: 4px 10px; text-align: right; }
th { background: #f6f8fa; }
td:first-child, th:first-child { text-align: left; }
.match { color: #1a7f37; } .mismatch { color: #cf222e; font-weight: 600; }
.counted, .match-with-rejects { color: #9a6700; }
.verdict { font-size: 1.2rem; font-weight: 600; }
code { font-size: 12px; }
"""
_COLUMNS = (
    ("Entity", "entity"),
    ("Status", "status"),
    ("Source", "source_rows"),
    ("Rejected", "rejected"),
    ("Accepted", "accepted"),
    ("Expected", "expected_rows"),
    ("Target", "target_rows"),
    ("Matched", "matched"),
    ("Missing", "missing"),
    ("Parked", "parked_missing"),
    ("Extra", "extra"),
    ("Different", "different"),
    ("Duplicates", "duplicates"),
)


def to_html(
    report: cutover_report.CutoverReport, digest: str | None = None
) -> str:
    """Renders the report as a self-contained page."""
    data = report.as_dict()
    esc = html.escape
    verdict = report.verdict
    rows = "".join(
        "<tr>"
        + "".join(
            f'<td class="{esc(str(entity["status"]))}">'
            f"{esc(_cell(entity.get(key)))}</td>"
            if key == "status"
            else f"<td>{esc(_cell(entity.get(key)))}</td>"
            for _, key in _COLUMNS
        )
        + "</tr>"
        for entity in data["entities"]
    )
    details = "".join(_details(entity) for entity in data["entities"])
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
        f"<p>Generated {esc(data['generated_at'])}.</p>"
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


def _plain(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _cell(value: Any) -> str:
    return "-" if value is None else str(value)


def _details(entity: dict[str, Any]) -> str:
    """The rejections and differences of an entity, if any."""
    esc = html.escape
    parts: list[str] = []
    if entity["rejections"]:
        items = "".join(
            f"<li>{count} × {esc(reason)}</li>"
            for reason, count in entity["rejections"].items()
        )
        parts.append(
            f"<h2>{esc(entity['entity'])}: rejected</h2><ul>{items}</ul>"
        )
    if entity["samples"]:
        items = "".join(
            f"<li>{esc(sample['kind'])} <code>{esc(str(sample['key']))}</code>"
            + "".join(
                f"; {esc(column)}: expected <code>{esc(repr(v['expected']))}"
                f"</code>, found <code>{esc(repr(v['actual']))}</code>"
                for column, v in sample["columns"].items()
            )
            + "</li>"
            for sample in entity["samples"]
        )
        parts.append(
            f"<h2>{esc(entity['entity'])}: differences</h2><ul>{items}</ul>"
        )
    return "".join(parts)
