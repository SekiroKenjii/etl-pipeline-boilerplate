"""Messages of a log (a Kafka topic), extracted run after run."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import Literal

from core import contract
from core import pipeline


class MessageExtractor(pipeline.IDataExtractor[contract.Message]):
    """Extracts the messages of a topic published since the last run.

    A run reads up to the end of the topic at its start. The position of the
    last message loaded is saved when its batch is acknowledged, and committed
    to the consumer group: delivery is at least once, so load idempotently.

        MessageExtractor(cluster.topic("orders"), state=state)

    Attributes:
        source: The topic.
    """

    def __init__(
        self,
        source: contract.IMessageSource,
        *,
        state: contract.IStateStore,
        state_key: str | None = None,
        start: Literal["beginning", "end"] = "beginning",
    ) -> None:
        """Initializes the extractor.

        Args:
            source: The topic.
            state: Saves the position, between runs.
            state_key: The state key; `messages:<topic>` by default.
            start: Where the first run starts: at the oldest message kept,
                or after the newest (only what comes next).
        """
        self.source = source
        self._state = state
        self._state_key = state_key or f"messages:{source.topic}"
        self._start = start

    @property
    def name(self) -> str:
        return f"{type(self).__name__}({self.source.topic})"

    def teardown(self, ctx: contract.ETLContext) -> None:
        self.source.close()

    def saved_position(self, ctx: contract.ETLContext) -> contract.StateValue:
        """Returns the position after the last loaded message; None at first."""
        saved = self._state.get(ctx.run.pipeline_name, self._state_key)
        return saved.get("position") if isinstance(saved, dict) else None

    def extract(self, ctx: contract.ETLContext) -> Iterator[contract.Message]:
        after = self.saved_position(ctx)
        if after is None:
            after = (
                self.source.beginning()
                if self._start == "beginning"
                else self.source.end()
            )
            if not ctx.run.dry_run:
                self._save(ctx, after)
        yield from self.source.messages(after, self.source.end())

    def acknowledge(
        self, records: Sequence[contract.Message], ctx: contract.ETLContext
    ) -> None:
        """Saves the position after the last message of the batch, and
        commits it."""
        if records and records[-1].position is not None:
            position = records[-1].position
            self._save(ctx, position)
            self.source.confirm(position)

    def _save(
        self, ctx: contract.ETLContext, position: contract.StateValue
    ) -> None:
        self._state.set(
            ctx.run.pipeline_name, self._state_key, {"position": position}
        )
