"""The ETL engine: extractors, transformers, loaders, jobs, orchestration.

This package is its public API: import it and qualify the names, e.g.
`pipeline.ETLJob`.
"""

from core.contract.pipeline import PipelineListener
from core.pipeline.async_bridge import AsyncExtractorAdapter
from core.pipeline.extractor import IAsyncDataExtractor
from core.pipeline.extractor import IDataExtractor
from core.pipeline.job import ETLJob
from core.pipeline.loader import IDataLoader
from core.pipeline.orchestrator import BaseOrchestrator
from core.pipeline.orchestrator import resolve_execution_order
from core.pipeline.orchestrator import StepCoordinator
from core.pipeline.transformer import chain
from core.pipeline.transformer import ChainedTransformer
from core.pipeline.transformer import IDataTransformer

__all__ = [
    "AsyncExtractorAdapter",
    "BaseOrchestrator",
    "ChainedTransformer",
    "ETLJob",
    "IAsyncDataExtractor",
    "IDataExtractor",
    "IDataLoader",
    "IDataTransformer",
    "PipelineListener",
    "StepCoordinator",
    "chain",
    "resolve_execution_order",
]
