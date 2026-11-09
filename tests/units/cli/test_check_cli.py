"""`etl check`: a line per resource, a `WARN` line for what works but is
not as the ETL expects, exit 5 when one fails."""

from __future__ import annotations

import io
import pathlib

import pytest

from cli import run
from infra.database.relational import dialects

_WARNING = "lc_messages is 'de_DE.utf8': set the role's"


def _project(tmp_path: pathlib.Path, *, down: bool) -> pathlib.Path:
    """A config directory: a SQLite database `crm`, and an unreachable
    `legacy` one when `down`."""
    directory = tmp_path / "config"
    directory.mkdir()
    legacy = '[databases.legacy]\nurl = "sqlite:////no/such/dir/x.db"\n'
    (directory / "base.toml").write_text(
        f'[databases.crm]\nurl = "sqlite:///{tmp_path / "crm.db"}"\n'
        + (legacy if down else ""),
        encoding="utf-8",
    )
    return directory


def _check(project: pathlib.Path) -> tuple[int, list[str]]:
    out = io.StringIO()
    code = run.main(["--env", "test", "--config", str(project), "check"], out)
    return code, out.getvalue().splitlines()[1:]


@pytest.fixture
def warned(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        dialects.SqliteDialect, "session_warnings", lambda *_: [_WARNING]
    )


@pytest.mark.usefixtures("warned")
def test_a_session_warning_is_a_warn_line_after_ok(
    tmp_path: pathlib.Path,
) -> None:
    code, lines = _check(_project(tmp_path, down=False))

    assert code == 0
    assert lines == ["ok   database.crm", f"WARN database.crm: {_WARNING}"]


@pytest.mark.usefixtures("warned")
def test_a_failure_wins_over_a_warning(tmp_path: pathlib.Path) -> None:
    code, lines = _check(_project(tmp_path, down=True))

    assert code == 5
    assert lines[:2] == [
        "ok   database.crm",
        f"WARN database.crm: {_WARNING}",
    ]
    assert lines[2].startswith("FAIL database.legacy: RetryableError")
    assert not any(line.startswith("WARN database.legacy") for line in lines)


def test_no_warning_no_warn_line(tmp_path: pathlib.Path) -> None:
    code, lines = _check(_project(tmp_path, down=False))

    assert (code, lines) == (0, ["ok   database.crm"])
