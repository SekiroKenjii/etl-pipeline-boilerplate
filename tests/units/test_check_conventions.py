"""The Git-flow conventions the CI enforces: branch names, commit messages."""

from __future__ import annotations

import pathlib

import pytest
from scripts import check_conventions


@pytest.mark.parametrize(
    "name",
    [
        "main",
        "develop",
        "feature/ci-green",
        "release/v0.2",
        "release/v1.0.3",
        "hotfix/lock-wait",
        "dependabot/docker/library/python-3.15",
    ],
)
def test_git_flow_branch_names_pass(name: str) -> None:
    assert check_conventions.check_branch(name) is None


@pytest.mark.parametrize(
    "name",
    ["master", "Feature/x", "feature/", "fix-it", "release/1.0", "hotfix/Big"],
)
def test_other_branch_names_are_refused(name: str) -> None:
    assert check_conventions.check_branch(name) is not None


@pytest.mark.parametrize(
    "message",
    [
        "feat: add the tiered key cache",
        "fix(kernel): take the locks again after the wait\n",
        "ci!: run on develop",
        "build(deps): bump astral-sh/uv from 0.13.6 to 0.13.7",
        "Merge branch 'develop' into feature/x",
        "feat: one line\n# Please enter the commit message\n#\n",
    ],
)
def test_one_line_conventional_messages_pass(message: str) -> None:
    assert check_conventions.check_message(message) is None


@pytest.mark.parametrize(
    ("message", "why"),
    [
        ("Add the cache", "type(scope)"),
        ("feat: one\n\nwith a body", "one line only"),
        ("feat: " + "x" * 70, "at most 72"),
        ("", "empty"),
        ("Feat: capital type", "type(scope)"),
        ("feat:no space", "type(scope)"),
    ],
)
def test_other_messages_are_refused(message: str, why: str) -> None:
    found = check_conventions.check_message(message)

    assert found is not None
    assert why in found


def test_the_command_reads_a_message_file(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "COMMIT_EDITMSG"
    path.write_text("docs: explain the flow\n# comment\n", encoding="utf-8")

    assert check_conventions.main(["--message-file", str(path)]) == 0

    path.write_text("explain the flow\n", encoding="utf-8")

    assert check_conventions.main(["--message-file", str(path)]) == 1


def test_the_command_checks_a_branch_name() -> None:
    assert check_conventions.main(["--branch", "feature/ok"]) == 0
    assert check_conventions.main(["--branch", "Feature/No"]) == 1
