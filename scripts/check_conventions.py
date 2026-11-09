"""Checks the Git-flow conventions a personal repository's rulesets cannot:
branch names, and one-line Conventional Commit messages.

    python3 scripts/check_conventions.py --branch feature/x --range origin/develop..HEAD
    python3 scripts/check_conventions.py --message-file .git/COMMIT_EDITMSG

Branches: `main`, `develop`, `feature/<name>`, `release/vX.Y[.Z]`,
`hotfix/<name>` (and Dependabot's). A commit message is one line of at most
72 characters, `type(scope)!: subject` with a Conventional Commits type, or
a merge commit. Exit code 1 lists every violation.
"""

from __future__ import annotations

import argparse
import pathlib
import re
import subprocess  # nosec B404
import sys

BRANCH = re.compile(
    r"^(main|develop|feature/[a-z0-9][a-z0-9._-]*"
    r"|release/v[0-9]+\.[0-9]+(\.[0-9]+)?|hotfix/[a-z0-9][a-z0-9._-]*"
    r"|dependabot/.+)$"
)
SUBJECT = re.compile(
    r"^(feat|fix|docs|style|refactor|perf|test|build|ci|chore|revert)"
    r"(\([a-z0-9][a-z0-9._/-]*\))?!?: \S.*$"
)
MERGE = re.compile(r"^Merge .+$")
MAX_SUBJECT = 72


def check_branch(name: str) -> str | None:
    """Returns why `name` is not a Git-flow branch name, or None."""
    if BRANCH.match(name):
        return None
    return (
        f"branch {name!r}: use feature/<name>, release/vX.Y[.Z] or"
        " hotfix/<name> (lowercase letters, digits, . _ -)"
    )


def check_message(message: str) -> str | None:
    """Returns why `message` is not a one-line Conventional Commit, or None.

    Lines starting with `#` are the editor's comments, not the message.
    """
    lines = [
        line.rstrip()
        for line in message.splitlines()
        if not line.startswith("#")
    ]
    while lines and not lines[-1]:
        lines.pop()
    if not lines or not lines[0]:
        return "empty commit message"
    subject = lines[0]
    if len(lines) > 1:
        return f"{subject[:40]!r}: one line only, no body"
    if MERGE.match(subject):
        return None
    if len(subject) > MAX_SUBJECT:
        return (
            f"{subject[:40]!r}: {len(subject)} characters, at most"
            f" {MAX_SUBJECT}"
        )
    if not SUBJECT.match(subject):
        return (
            f"{subject[:60]!r}: write `type(scope)!: subject` with type in"
            " feat, fix, docs, style, refactor, perf, test, build, ci,"
            " chore, revert"
        )
    return None


def messages_of(revision_range: str) -> list[str]:
    """The messages of the commits of a `git log` range, oldest first."""
    found = subprocess.run(  # nosec B603 B607
        ["git", "log", "--reverse", "--format=%B%x00", revision_range],
        capture_output=True,
        text=True,
        check=True,
    )
    return [m.strip("\n") for m in found.stdout.split("\x00") if m.strip()]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--branch", help="the branch name to check")
    parser.add_argument("--range", help="a git revision range of commits")
    parser.add_argument(
        "--message-file", type=pathlib.Path, help="a commit message file"
    )
    args = parser.parse_args(argv)
    problems: list[str] = []
    if args.branch is not None and (why := check_branch(args.branch)):
        problems.append(why)
    if args.range is not None:
        problems.extend(
            why for m in messages_of(args.range) if (why := check_message(m))
        )
    if args.message_file is not None:
        why = check_message(args.message_file.read_text(encoding="utf-8"))
        if why:
            problems.append(why)
    for problem in problems:
        print(problem, file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
