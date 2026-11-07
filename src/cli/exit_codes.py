"""The exit codes of every command line: `etl`, `etl-mappings`, `etl-db`.

One table, whichever command a scheduler, a CI job or a script runs:

| Code | Name            | Meaning                                    | Retry? |
|------|-----------------|--------------------------------------------|--------|
| 0    | SUCCESS         | Done.                                      |        |
| 1    | FAILED          | A run failed (a source down, a lock lost), | yes    |
|      |                 | or a cutover report or verification did.   | no     |
| 2    | INVALID         | Bad arguments, settings, workbook or file. | no     |
| 3    | PARTIAL_SUCCESS | Done, some rows rejected.                  | no     |
| 4    | LOCKED          | Steps held by another run: nothing ran.    | later  |
| 5    | NOT_READY       | `etl check`: a resource cannot be reached; | later  |
|      |                 | `etl-db check`: the ETL tables need        |        |
|      |                 | upgrading.                                 |        |
| 6    | CHANGED         | `etl-mappings diff`, `check`: differences. | no     |
| 7    | NOT_FOUND       | Nothing matched: a key, a run, a record.   | no     |
| 130  | CANCELLED       | Stopped by a signal (SIGINT, SIGTERM).     | yes    |

The codes of a run come from the kernel (`runtime.ExitCode`), with the same
values: `of_run` converts them.
"""

from __future__ import annotations

import enum

from core import runtime


class ExitCode(enum.IntEnum):
    """What a command tells its caller: see the module's table."""

    SUCCESS = 0
    FAILED = 1
    INVALID = 2
    PARTIAL_SUCCESS = 3
    LOCKED = 4
    NOT_READY = 5
    CHANGED = 6
    NOT_FOUND = 7
    CANCELLED = 130


def of_run(code: runtime.ExitCode) -> ExitCode:
    """The exit code of a run, as the kernel tells it."""
    return ExitCode(int(code))
