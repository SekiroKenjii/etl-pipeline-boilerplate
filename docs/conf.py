"""Sphinx configuration: `make docs` builds the site into docs/_build/html."""

from __future__ import annotations

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

project = "ETL pipeline boilerplate"
author = "Thuong Vo"
copyright = "2026, Thuong Vo"  # pylint: disable=redefined-builtin

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx_copybutton",
    "sphinxcontrib.mermaid",
    "sphinxcontrib.programoutput",
]
source_suffix = {".md": "markdown"}
exclude_patterns = ["_build"]
myst_enable_extensions = ["colon_fence", "deflist"]
myst_heading_anchors = 3
# ```mermaid fences render as diagrams, as GitHub renders them.
myst_fence_as_directive = ["mermaid"]
# The ELK layout, as editors such as Mermaid Chart use by default: Mermaid's
# own default, dagre, lays the diagrams out differently. Pinned together:
# layout-elk 0.2 needs Mermaid 11.
mermaid_version = "11.17.2"
mermaid_include_elk = True
mermaid_elk_version = "0.2.3"
mermaid_config = {"layout": "elk"}

# Google style docstrings; the order of the source.
napoleon_google_docstring = True
napoleon_numpy_docstring = False
autodoc_member_order = "bysource"
autodoc_typehints = "description"
autodoc_default_options = {"members": True, "show-inheritance": True}

html_theme = "furo"
html_title = project


def _examples_as_code(
    app: object,
    what: str,
    name: str,
    obj: object,
    options: object,
    lines: list[str],
) -> None:
    """Docstrings show examples as blocks indented after a paragraph, as in
    Markdown: reStructuredText wants `::` before them."""
    del app, what, name, obj, options  # Unused.
    index = 2
    while index < len(lines):
        current, blank, text = lines[index], lines[index - 1], lines[index - 2]
        if (
            current.startswith("    ")
            and not blank.strip()
            and text.strip()
            and not text.startswith(" ")
            and not text.rstrip().endswith("::")
        ):
            lines[index - 1 : index - 1] = ["", "::"]
            index += 2
            while index < len(lines) and (
                not lines[index].strip() or lines[index].startswith("    ")
            ):
                index += 1
        index += 1


def setup(app: object) -> None:
    """Runs the docstring fix before napoleon reads Google sections."""
    app.connect(  # type: ignore[attr-defined]
        "autodoc-process-docstring", _examples_as_code, priority=400
    )
