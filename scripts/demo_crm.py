"""Creates the demo databases of the CRM module, from the settings.

uv run python scripts/demo_crm.py [--env dev]
uv run etl run crm.full_load

The same as `python -m modules.crm.demo`, which the image runs too.
"""

from __future__ import annotations

import sys

from modules.crm import demo

if __name__ == "__main__":
    sys.exit(demo.main())
