"""The custom transforms of the CRM mappings.

Pass the registry to the mapping tooling, so that BAs can pick them:
`etl-mappings --registry modules.crm.transforms:registry validate`.
"""

from __future__ import annotations

import re

from core import mapping

registry = mapping.TransformRegistry()


@registry.register("crm.phone")
def phone(value: str | None) -> str | None:
    """A Vietnamese phone number in the international format, +84..."""
    if value is None:
        return None
    digits = re.sub(r"\D", "", value)
    if digits.startswith("84"):
        digits = digits[2:]
    return f"+84{digits.lstrip('0')}" if digits else None
