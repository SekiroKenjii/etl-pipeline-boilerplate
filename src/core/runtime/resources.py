"""The resources of the application: created once, closed in order.

The composition root (`app.bootstrap`) declares every resource as a
`ResourceSpec`; pipelines ask for them by name and port type. Resources are
created lazily, shared within their scope (the process, or one run), and
closed in the reverse order of their creation. Steps have no scope here:
their components' `setup` and `teardown` are their lifecycle.
"""

from __future__ import annotations

from collections.abc import Callable, Generator, Sequence
import contextlib
import dataclasses
import enum
import logging
import threading
from typing import Any

from core import contract

_LOG = logging.getLogger(__name__)


class Scope(enum.StrEnum):
    """How long a resource lives."""

    PROCESS = "process"
    RUN = "run"


@dataclasses.dataclass(frozen=True, slots=True)
class ResourceSpec[T]:
    """How to create, check and close one resource.

    Attributes:
        name: The name pipelines ask for it by, e.g. `database.crm`.
        factory: Creates it; asks `Resources` for what it needs.
        scope: How long it lives.
        close: Releases it; nothing by default.
        health: Raises if it does not work, e.g. pings a database.
        description: What it is, for `etl check`.
    """

    name: str
    factory: Callable[[Resources], T]
    scope: Scope = Scope.PROCESS
    close: Callable[[T], None] | None = None
    health: Callable[[T], None] | None = None
    description: str = ""


@dataclasses.dataclass(slots=True)
class _Creation:
    """A resource being created, by one thread; the others wait for it.

    Attributes:
        thread: The thread creating it.
        done: Set once it is created, or has failed.
    """

    thread: int
    done: threading.Event = dataclasses.field(default_factory=threading.Event)


class Resources:
    """The resources of one scope; a run's scope sees the process's.

    Thread-safe: a resource asked for by parallel steps is created once, outside
    any shared lock, so a slow one delays only the threads asking for it.

        with Resources(specs) as resources:
            with resources.run_scope() as run:
                state = run.get("etl.state", contract.IStateStore)
    """

    def __init__(
        self,
        specs: Sequence[ResourceSpec[Any]],
        *,
        scope: Scope = Scope.PROCESS,
        parent: Resources | None = None,
    ) -> None:
        """Initializes the resources; nothing is created yet.

        Raises:
            PipelineConfigError: If two resources have the same name.
        """
        by_name: dict[str, ResourceSpec[Any]] = {}
        for spec in specs:
            if spec.name in by_name:
                raise contract.PipelineConfigError(
                    f"Two resources are named {spec.name!r}"
                )
            by_name[spec.name] = spec
        self._specs = by_name
        self._scope = scope
        self._parent = parent
        self._created: dict[str, object] = {}
        self._order: list[str] = []
        # Who creates and who waits for what: to detect wait cycles.
        self._creating: dict[str, _Creation] = {}
        self._waiting: dict[int, str] = {}
        # Per thread: the resources it is creating, innermost last.
        self._local = threading.local()
        self._lock = threading.Lock()
        self._closed = False

    @property
    def names(self) -> list[str]:
        """Every resource name, of this scope and the process's."""
        return sorted(self._specs)

    def spec(self, name: str) -> ResourceSpec[Any]:
        """Returns the declaration of a resource.

        Raises:
            PipelineConfigError: If there is no such resource.
        """
        try:
            return self._specs[name]
        except KeyError:
            raise contract.PipelineConfigError(
                f"No resource {name!r}; known: {self.names}"
            ) from None

    def get[T](self, name: str, kind: type[T]) -> T:
        """Returns a resource, creating it on first use.

        Args:
            name: The resource.
            kind: The type it must have, usually a port:
                `contract.IStateStore`.

        Raises:
            PipelineConfigError: If there is no such resource, a run resource
                is asked for outside a run, or resources need each other.
            TypeError: If it is not a `kind`.
        """
        found = self.resolve(name)
        if not isinstance(found, kind):
            raise TypeError(
                f"Resource {name!r} is a {type(found).__name__}, not a"
                f" {kind.__name__}"
            )
        return found

    def resolve(self, name: str) -> object:
        """Returns a resource, untyped; prefer `get`.

        Raises:
            PipelineConfigError: As `get` does.
        """
        spec = self.spec(name)
        if name in self._stack():
            cycle = " -> ".join([*self._stack(), name])
            raise contract.PipelineConfigError(
                f"Resources need each other: {cycle}"
            )
        if (
            spec.scope is Scope.PROCESS
            and self._scope is Scope.RUN
            and self._parent is not None
        ):
            return self._parent.resolve(name)
        if spec.scope is Scope.RUN and self._scope is Scope.PROCESS:
            raise contract.PipelineConfigError(
                f"Resource {name!r} lives in a run: ask for it in `run_scope()`"
            )
        while True:
            with self._lock:
                self._check_open(name)
                if name in self._created:
                    return self._created[name]
                creation = self._creating.get(name)
                if creation is None:
                    creation = _Creation(threading.get_ident())
                    self._creating[name] = creation
                    break
                self._check_cycle(name, creation)
                self._waiting[threading.get_ident()] = name
            # Another thread creates it: wait; create it if that failed.
            try:
                creation.done.wait()
            finally:
                with self._lock:
                    del self._waiting[threading.get_ident()]
        return self._create(spec, creation)

    def _create(self, spec: ResourceSpec[Any], creation: _Creation) -> object:
        """Creates a resource this thread has claimed, out of the lock."""
        stack: list[str] = self._stack()
        stack.append(spec.name)
        try:
            created = spec.factory(self)
        except BaseException:
            with self._lock:
                del self._creating[spec.name]
            creation.done.set()
            raise
        finally:
            stack.pop()
        with self._lock:
            del self._creating[spec.name]
            closed = self._closed
            if not closed:
                self._created[spec.name] = created
                self._order.append(spec.name)
        creation.done.set()
        if closed:
            # Closed meanwhile: nobody else would close it.
            if spec.close is not None:
                spec.close(created)
            self._check_open(spec.name)
        return created

    def _stack(self) -> list[str]:
        """The resources this thread is creating, innermost last."""
        stack: list[str] | None = getattr(self._local, "stack", None)
        if stack is None:
            stack = self._local.stack = []
        return stack

    def _check_open(self, name: str) -> None:
        """Raises if closed.

        Raises:
            PipelineConfigError: If the resources are closed.
        """
        if self._closed:
            raise contract.PipelineConfigError(
                f"Resources closed: cannot create {name!r}"
            )

    def _check_cycle(self, name: str, creation: _Creation) -> None:
        """Raises if waiting for `name` would wait for this thread.

        The lock is held. A thread creating `a` asks for `b`; if `b` is
        being created by a thread waiting, maybe through others, for what
        this thread creates, nobody would ever finish.

        Raises:
            PipelineConfigError: If resources need each other.
        """
        me = threading.get_ident()
        chain = [*self._stack(), name]
        owner = creation.thread
        seen: set[int] = set()
        while owner != me:
            awaited = self._waiting.get(owner)
            if (
                awaited is None
                or awaited not in self._creating
                or owner in seen
            ):
                return
            seen.add(owner)
            chain.append(awaited)
            owner = self._creating[awaited].thread
        cycle = " -> ".join(chain)
        raise contract.PipelineConfigError(
            f"Resources need each other: {cycle}"
        )

    @contextlib.contextmanager
    def run_scope(self) -> Generator[Resources]:
        """Yields the resources of one run, closed when it ends."""
        run = Resources(
            list(self._specs.values()), scope=Scope.RUN, parent=self
        )
        try:
            yield run
        finally:
            run.close()

    def health(self) -> dict[str, str]:
        """Checks every process resource that can be checked.

        Returns:
            `ok`, or what is wrong, by resource name.
        """
        report: dict[str, str] = {}
        for name, spec in sorted(self._specs.items()):
            if spec.health is None or spec.scope is not Scope.PROCESS:
                continue
            try:
                spec.health(self.resolve(name))
                report[name] = "ok"
            # A check reports what is wrong, whatever it is.
            except Exception as exc:  # pylint: disable=broad-exception-caught
                report[name] = f"{type(exc).__name__}: {exc}"
        return report

    def close(self) -> None:
        """Closes the resources of this scope, the last created first.

        Every resource is closed even if one fails: failures are logged, and
        the first one is raised at the end.

        Raises:
            Exception: The first failure of a close.
        """
        with self._lock:
            if self._closed:
                return
            self._closed = True
            order, self._order = list(reversed(self._order)), []
        first: Exception | None = None
        for name in order:
            spec = self._specs[name]
            resource = self._created.pop(name)
            if spec.close is None:
                continue
            try:
                spec.close(resource)
            # Every resource gets closed: keep going, report the first.
            except Exception as exc:  # pylint: disable=broad-exception-caught
                _LOG.exception("closing resource %s failed", name)
                first = first or exc
        if first is not None:
            raise first

    def __enter__(self) -> Resources:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()
