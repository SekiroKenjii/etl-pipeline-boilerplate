"""Resources created in parallel: one slow factory delays only its own."""

from __future__ import annotations

import threading
import time

import pytest

from core import contract
from core import runtime


def test_a_slow_resource_does_not_delay_the_others() -> None:
    started = threading.Event()
    release = threading.Event()

    def slow(_: runtime.Resources) -> str:
        started.set()
        release.wait(30)
        return "slow"

    resources = runtime.Resources([
        runtime.ResourceSpec("slow", slow),
        runtime.ResourceSpec("fast", lambda _: "fast"),
    ])
    creating = threading.Thread(
        daemon=True, target=lambda: resources.get("slow", str)
    )
    creating.start()
    started.wait(5)

    found: list[str] = []
    getting = threading.Thread(
        daemon=True, target=lambda: found.append(resources.get("fast", str))
    )
    getting.start()
    # Under the one lock of the container, it would wait for `slow`, which
    # waits for the release: given only once `fast` came.
    getting.join(10)
    fast_came_first = found == ["fast"]
    release.set()
    creating.join(10)

    assert fast_came_first
    assert resources.get("slow", str) == "slow"


def test_threads_wait_for_the_one_creating_it() -> None:
    created: list[int] = []
    started = threading.Event()
    release = threading.Event()

    def factory(_: runtime.Resources) -> object:
        created.append(threading.get_ident())
        started.set()
        release.wait(5)
        return object()

    resources = runtime.Resources([runtime.ResourceSpec("pool", factory)])
    found: list[object] = []
    threads = [
        threading.Thread(
            daemon=True, target=lambda: found.append(resources.resolve("pool"))
        )
        for _ in range(4)
    ]
    for thread in threads:
        thread.start()
    started.wait(5)
    time.sleep(0.05)
    release.set()
    for thread in threads:
        thread.join(5)

    assert len(created) == 1
    assert len({id(item) for item in found}) == 1
    assert len(found) == 4


def test_a_failed_creation_is_tried_again_by_the_next_thread() -> None:
    attempts: list[int] = []
    started = threading.Event()
    release = threading.Event()

    def flaky(_: runtime.Resources) -> str:
        attempts.append(1)
        if len(attempts) == 1:
            started.set()
            release.wait(5)
            raise ConnectionError("refused")
        return "up"

    resources = runtime.Resources([runtime.ResourceSpec("db", flaky)])
    errors: list[BaseException] = []

    def first() -> None:
        try:
            resources.get("db", str)
        except ConnectionError as exc:
            errors.append(exc)

    failing = threading.Thread(daemon=True, target=first)
    failing.start()
    started.wait(5)
    waiting: list[str] = []
    second = threading.Thread(
        target=lambda: waiting.append(resources.get("db", str))
    )
    second.start()
    time.sleep(0.05)
    release.set()
    failing.join(5)
    second.join(5)

    assert len(errors) == 1
    assert waiting == ["up"]
    assert len(attempts) == 2


def test_resources_needing_each_other_across_threads_fail() -> None:
    # `a` needs `b` and `b` needs `a`, first asked by two threads at once:
    # each would wait for the other forever.
    both = threading.Barrier(2)
    met: list[str] = []

    def needing(other: str) -> runtime.ResourceSpec[str]:
        def factory(resources: runtime.Resources) -> str:
            if other not in met:
                met.append(other)
                both.wait(5)
            resources.get(other, str)
            return "never"

        return runtime.ResourceSpec("b" if other == "a" else "a", factory)

    resources = runtime.Resources([needing("b"), needing("a")])
    errors: list[str] = []

    def ask(name: str) -> None:
        try:
            resources.get(name, str)
        except contract.PipelineConfigError as exc:
            errors.append(str(exc))

    threads = [
        threading.Thread(daemon=True, target=ask, args=(n,)) for n in "ab"
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(5)

    assert not any(thread.is_alive() for thread in threads)
    assert len(errors) == 2
    assert all("need each other" in error for error in errors)


def test_dependencies_close_after_what_uses_them_when_made_in_parallel() -> (
    None
):
    log: list[str] = []

    def thing(name: str, *needs: str) -> runtime.ResourceSpec[str]:
        def factory(resources: runtime.Resources) -> str:
            for other in needs:
                resources.get(other, str)
            time.sleep(0.01)
            return name

        return runtime.ResourceSpec(name, factory, close=log.append)

    resources = runtime.Resources([
        thing("db"),
        thing("state", "db"),
        thing("keys", "db"),
        thing("ledger", "state"),
    ])
    threads = [
        threading.Thread(daemon=True, target=lambda n=n: resources.get(n, str))
        for n in ("ledger", "keys", "state")
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(5)
    resources.close()

    assert log[-1] == "db"
    assert log.index("ledger") < log.index("state")


def test_a_resource_created_after_closing_is_closed() -> None:
    log: list[str] = []
    started = threading.Event()
    release = threading.Event()

    def slow(_: runtime.Resources) -> str:
        started.set()
        release.wait(5)
        return "late"

    resources = runtime.Resources(
        [runtime.ResourceSpec[str]("late", slow, close=log.append)]
    )
    errors: list[BaseException] = []

    def ask() -> None:
        try:
            resources.get("late", str)
        except contract.PipelineConfigError as exc:
            errors.append(exc)

    thread = threading.Thread(daemon=True, target=ask)
    thread.start()
    started.wait(5)
    resources.close()
    release.set()
    thread.join(5)

    assert log == ["late"]
    assert len(errors) == 1
    with pytest.raises(contract.PipelineConfigError, match="closed"):
        resources.get("late", str)
