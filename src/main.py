"""The application entry point: `python src/main.py ...` is `etl ...`."""

from __future__ import annotations

import sys

from cli import run

if __name__ == "__main__":
    sys.exit(run.main())
