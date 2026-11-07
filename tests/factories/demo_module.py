"""A small business module, loaded by the tests as a real one would be."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import pydantic

import config
from core import contract
from core import operators
from core import pipeline
from core import runtime
from core.runtime import names


class DemoSettings(pydantic.BaseModel):
    database: str = "src"
    source: str = "source"
    target: str = "target"


class Copy(pipeline.BaseOrchestrator):
    """Copies a table incrementally by key, in the same database."""

    def __init__(
        self, resources: runtime.Resources, settings: DemoSettings
    ) -> None:
        super().__init__()
        reader = resources.get(
            names.reader(settings.database), contract.ITableReader
        )
        writer = resources.get(
            names.writer(settings.database), contract.ITableWriter
        )
        state = resources.get(names.STATE, contract.IStateStore)
        self._job = pipeline.ETLJob(
            operators.TableExtractor(
                reader, settings.source, key=["id"], state=state
            ),
            operators.Passthrough[contract.Row](),
            operators.TableLoader[contract.Row](
                writer, contract.TableTarget(settings.target, key=("id",))
            ),
        )

    def steps(self) -> Sequence[contract.PipelineStep]:
        return [contract.PipelineStep("copy", self._job)]


class Demo(runtime.Module):

    def __init__(self, settings: DemoSettings) -> None:
        self.settings = settings

    @property
    def name(self) -> str:
        return "demo"

    def pipelines(self) -> Mapping[str, runtime.PipelineFactory]:
        def broken(resources: runtime.Resources) -> pipeline.BaseOrchestrator:
            raise RuntimeError("cannot build")

        return {
            "copy": lambda resources: Copy(resources, self.settings),
            "broken": broken,
        }


def create(settings: config.AppSettings) -> Demo:
    return Demo(settings.module_settings("demo", DemoSettings))
