"""The types the messaging adapters share, and the ones their callers pass."""

from __future__ import annotations

from collections.abc import Callable

__all__ = ["Decoder"]

type Decoder = Callable[[bytes], object]
