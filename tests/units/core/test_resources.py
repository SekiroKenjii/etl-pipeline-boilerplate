from __future__ import annotations

import concurrent.futures
import threading

import pytest

from core import contract
from core import runtime


class Thing:

    def __init__(self, name: str, log: list[str]) -> None:
        self.name = name
        self.log = log
        log.append(f"open {name}")

    def close(self) -> None:
        self.log.append(f"close {self.name}")


def _spec(
    name: str,
    log: list[str],
    *,
    needs: tuple[str, ...] = (),
    scope: runtime.Scope = runtime.Scope.PROCESS,
) -> runtime.ResourceSpec[Thing]:
    def factory(resources: runtime.Resources) -> Thing:
        for other in needs:
            resources.get(other, Thing)
        return Thing(name, log)

    return runtime.ResourceSpec(name, factory, scope, close=Thing.close)


def test_created_lazily_once_and_closed_in_reverse() -> None:
    log: list[str] = []
    resources = runtime.Resources([
        _spec("db", log),
        _spec("registry", log, needs=("db",)),
        _spec("unused", log),
    ])

    first = resources.get("registry", Thing)
    again = resources.get("registry", Thing)
    resources.close()

    assert first is again
    assert log == ["open db", "open registry", "close registry", "close db"]


def test_run_resources_live_in_their_run() -> None:
    log: list[str] = []
    resources = runtime.Resources([
        _spec("db", log),
        _spec("cursor", log, needs=("db",), scope=runtime.Scope.RUN),
    ])

    with resources.run_scope() as run:
        run.get("cursor", Thing)
        assert run.get("db", Thing) is resources.get("db", Thing)
    with resources.run_scope() as run:
        run.get("cursor", Thing)
    resources.close()

    assert log == [
        "open db",
        "open cursor",
        "close cursor",
        "open cursor",
        "close cursor",
        "close db",
    ]
    unscoped = runtime.Resources([_spec("c", [], scope=runtime.Scope.RUN)])
    with pytest.raises(contract.PipelineConfigError, match="run_scope"):
        unscoped.get("c", Thing)


def test_mistakes_are_explained() -> None:
    log: list[str] = []
    cyclic = runtime.Resources(
        [_spec("a", log, needs=("b",)), _spec("b", log, needs=("a",))]
    )

    with pytest.raises(contract.PipelineConfigError, match="a -> b -> a"):
        cyclic.get("a", Thing)
    with pytest.raises(contract.PipelineConfigError, match="No resource 'x'"):
        cyclic.get("x", Thing)
    single = runtime.Resources([_spec("a", log)])
    with pytest.raises(TypeError, match="not a str"):
        single.get("a", str)
    duplicates = [_spec("a", log), _spec("a", log)]
    with pytest.raises(contract.PipelineConfigError, match="Two resources"):
        runtime.Resources(duplicates)


def test_every_resource_is_closed_even_if_one_fails() -> None:
    log: list[str] = []

    def broken(thing: Thing) -> None:
        raise RuntimeError("cannot close")

    resources = runtime.Resources([
        _spec("db", log),
        runtime.ResourceSpec[Thing](
            "bad", lambda r: Thing("bad", log), close=broken
        ),
    ])
    resources.get("db", Thing)
    resources.get("bad", Thing)

    with pytest.raises(RuntimeError, match="cannot close"):
        resources.close()

    assert log[-1] == "close db"
    with pytest.raises(contract.PipelineConfigError, match="closed"):
        resources.get("db", Thing)


def test_health_reports_each_resource() -> None:
    def ping(thing: Thing) -> None:
        if thing.name == "down":
            raise ConnectionError("refused")

    resources = runtime.Resources([
        runtime.ResourceSpec[Thing](
            "up", lambda r: Thing("up", []), health=ping
        ),
        runtime.ResourceSpec[Thing](
            "down", lambda r: Thing("down", []), health=ping
        ),
        runtime.ResourceSpec("plain", lambda r: Thing("plain", [])),
    ])

    assert resources.health() == {
        "down": runtime.Health("ConnectionError: refused"),
        "up": runtime.Health("ok"),
    }


def test_health_carries_the_warnings_of_a_working_resource() -> None:
    def ping(thing: Thing) -> list[str]:
        if thing.name == "down":
            raise ConnectionError("refused")
        return [f"{thing.name} is slow"]

    resources = runtime.Resources([
        runtime.ResourceSpec[Thing](
            "up", lambda r: Thing("up", []), health=ping
        ),
        runtime.ResourceSpec[Thing](
            "down", lambda r: Thing("down", []), health=ping
        ),
    ])

    report = resources.health()

    assert report == {
        "down": runtime.Health("ConnectionError: refused"),
        "up": runtime.Health("ok", ("up is slow",)),
    }
    assert [report["up"].ok, report["down"].ok] == [True, False]


def test_threads_asking_at_once_share_one_resource() -> None:
    created = 0
    gate = threading.Barrier(8)

    def factory(resources: runtime.Resources) -> object:
        nonlocal created
        created += 1
        return object()

    resources = runtime.Resources([runtime.ResourceSpec("pool", factory)])

    def ask(_: int) -> object:
        gate.wait()
        return resources.get("pool", object)

    with concurrent.futures.ThreadPoolExecutor(8) as threads:
        found = set(map(id, threads.map(ask, range(8))))

    assert (created, len(found)) == (1, 1)
