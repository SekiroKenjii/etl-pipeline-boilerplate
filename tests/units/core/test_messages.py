"""MessageExtractor: a topic, read run after run."""

from __future__ import annotations

from typing import Literal

import pytest

from core import contract
from core import operators
from core import pipeline
from core import state as core_state
from tests.factories import fake_ports
from tests.factories import fakes


def _run(
    topic: fake_ports.InMemoryTopic,
    state: contract.IStateStore,
    *,
    start: Literal["beginning", "end"] = "beginning",
    loader: fakes.CollectingLoader[contract.Message] | None = None,
    dry_run: bool = False,
) -> list[bytes | None]:
    loader = loader or fakes.CollectingLoader[contract.Message]()
    pipeline.ETLJob(
        operators.MessageExtractor(topic, state=state, start=start),
        operators.Passthrough[contract.Message](),
        loader,
        batch_size=2,
    )(fakes.make_context("events", dry_run=dry_run), {})
    return [message.value for message in loader.rows]


def test_each_run_reads_what_came_since_the_last() -> None:
    topic = fake_ports.InMemoryTopic()
    state = core_state.InMemoryStateStore()
    topic.send(b"a")
    topic.send(b"b")
    topic.send(b"c")

    assert _run(topic, state) == [b"a", b"b", b"c"]
    topic.send(b"d")
    assert _run(topic, state) == [b"d"]
    assert _run(topic, state) == []
    assert topic.confirmed == [{"0": 2}, {"0": 3}, {"0": 4}]


def test_starting_at_the_end_skips_the_past() -> None:
    topic = fake_ports.InMemoryTopic()
    state = core_state.InMemoryStateStore()
    topic.send(b"old")

    assert _run(topic, state, start="end") == []
    topic.send(b"new")
    assert _run(topic, state, start="end") == [b"new"]


def test_a_failed_batch_is_read_again() -> None:
    topic = fake_ports.InMemoryTopic()
    state = core_state.InMemoryStateStore()
    for value in (b"a", b"b", b"c", b"d"):
        topic.send(value)
    failing = fakes.CollectingLoader[contract.Message](fail_on_batch=2)

    with pytest.raises(contract.RetryableError):
        _run(topic, state, loader=failing)

    assert _run(topic, state) == [b"c", b"d"]


def test_a_dry_run_moves_nothing() -> None:
    topic = fake_ports.InMemoryTopic()
    state = core_state.InMemoryStateStore()
    topic.send(b"a")

    _run(topic, state, dry_run=True)  # Reads, loads nothing.

    assert topic.confirmed == []
    assert _run(topic, state) == [b"a"]


def test_expired_messages_fail() -> None:
    topic = fake_ports.InMemoryTopic()
    state = core_state.InMemoryStateStore()
    topic.send(b"a")
    _run(topic, state)
    topic.send(b"b")
    topic.send(b"c")
    topic.trimmed = 2

    with pytest.raises(contract.FatalError, match="expired"):
        _run(topic, state)
