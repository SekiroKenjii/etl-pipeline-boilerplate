"""The types the modules of the runtime share."""

from __future__ import annotations

from collections.abc import Callable

from core import pipeline
from core.runtime import resources as runtime_resources

__all__ = ["PipelineFactory"]

type PipelineFactory = Callable[
    [runtime_resources.Resources], pipeline.BaseOrchestrator
]
