"""The composition root: from the settings to a kernel ready to run.

    with bootstrap.bootstrap("prod") as application:
        application.kernel.run("crm.full_load")

In order: the settings, the logging, the modules, the resources (the
settings' and the modules'), then the kernel. Closing the application
closes the resources, last created first, and flushes the logs.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
import dataclasses
import hashlib
import importlib
from importlib import metadata
import json
import os
import pathlib
import subprocess  # nosec B404
from typing import cast, TextIO

from app import factories
from app import secret_managers
import config
from core import contract
from core import runtime
from core.observability import listener as run_listener
from infra import observability
from infra.observability import lineage as observability_lineage
from infra.observability import telemetry as otel_telemetry

ENTRY_POINTS = "etl.modules"
# Read only when `git describe` fails (an image, not a clone).
BUILD_VARIABLE = "ETL_BUILD"

type _ModuleFactory = Callable[[config.AppSettings], runtime.Module]


@dataclasses.dataclass
class Application:
    """A bootstrapped application: close it to release everything.

    Attributes:
        settings: The settings.
        resources: The resources of the process.
        kernel: Runs the pipelines.
        logs: The logging set up, if it was.
        telemetry: The traces and metrics exported, if they are.
    """

    settings: config.AppSettings
    resources: runtime.Resources
    kernel: runtime.Kernel
    logs: observability.Logging | None = None
    telemetry: otel_telemetry.Telemetry | None = None

    def close(self) -> None:
        """Closes the resources, then flushes the traces, metrics and logs."""
        try:
            self.resources.close()
        finally:
            try:
                if self.telemetry is not None:
                    self.telemetry.stop()
            finally:
                if self.logs is not None:
                    self.logs.stop()

    def __enter__(self) -> Application:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


def bootstrap(
    env: str | None = None,
    *,
    directory: pathlib.Path | str = "config",
    overrides: Mapping[str, object] | None = None,
    logs: bool = True,
    log_stream: TextIO | None = None,
) -> Application:
    """Builds the application of an environment.

    Args:
        env: The environment; `$ETL_ENV`, else `dev`, by default.
        directory: Where the settings files are.
        overrides: Settings winning over every source.
        logs: Whether to set up logging (tests may not).
        log_stream: Where log lines go; stdout by default.

    Returns:
        The application, to close at exit.

    Raises:
        ConfigError: If the settings are invalid.
        PipelineConfigError: If a module cannot be loaded.
    """
    settings = load_settings(env, directory=directory, overrides=overrides)
    logging_set = setup_logging(settings, log_stream) if logs else None
    telemetry = setup_telemetry(settings) if logs else None
    try:
        modules = load_modules(settings)
        specs: list[runtime.Declaration] = [*factories.resource_specs(settings)]
        for module in modules:
            specs.extend(module.resources())
        resources = runtime.Resources(specs)
        kernel = runtime.Kernel(
            resources,
            modules,
            listeners=[
                run_listener.LoggingListener(),
                run_listener.MetricsListener(),
                *lineage_listeners(settings),
            ],
            meta=run_meta(settings),
            lease_seconds=settings.runs.lease_seconds,
            max_parallel_steps=(
                settings.tuning.effective().max_parallel_steps.value
            ),
            redact=settings.logging.redact,
        )
    except BaseException:
        if telemetry is not None:
            telemetry.stop()
        if logging_set is not None:
            logging_set.stop()
        raise
    return Application(settings, resources, kernel, logging_set, telemetry)


def load_settings(
    env: str | None = None,
    *,
    directory: pathlib.Path | str = "config",
    overrides: Mapping[str, object] | None = None,
) -> config.AppSettings:
    """Loads the settings, secret managers' placeholders resolved.

    Raises:
        ConfigError: If the settings are invalid.
    """
    secret_managers.install()
    return config.load_settings(env, directory=directory, overrides=overrides)


def run_meta(settings: config.AppSettings) -> dict[str, str]:
    """What runs, for the ledger: the environment, the version of the code,
    its build (`$ETL_BUILD`, else `git describe`), the hash of the settings
    (secrets hidden: rotating one changes nothing)."""
    try:
        version = metadata.version("etl-pipeline-boilerplate")
    except metadata.PackageNotFoundError:
        version = "unknown"
    shown = json.dumps(settings.redacted(), sort_keys=True, default=str)
    meta = {
        "env": settings.env,
        "version": version,
        "settings": hashlib.sha256(shown.encode()).hexdigest()[:12],
    }
    build = os.environ.get(BUILD_VARIABLE) or _git_describe()
    if build:
        meta["build"] = build
    return meta


def _git_describe() -> str | None:
    """The commit of the working directory, if it is a git clone.

    The only process the application starts: a fixed command, no shell.
    """
    try:
        # Fixed argv, no shell: nothing of the input reaches the command.
        found = subprocess.run(  # nosec B603 B607
            ["git", "describe", "--always", "--dirty"],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return found.stdout.strip() or None if found.returncode == 0 else None


def lineage_listeners(
    settings: config.AppSettings,
) -> list[observability_lineage.LineageListener]:
    """The OpenLineage listener of the settings, if they name an endpoint."""
    lineage = settings.lineage
    if lineage is None:
        return []
    return [
        observability_lineage.LineageListener(
            observability_lineage.http_transport(
                lineage.endpoint,
                api_key=(
                    lineage.api_key.get_secret_value()
                    if lineage.api_key
                    else None
                ),
                timeout_seconds=lineage.timeout_seconds,
            ),
            namespace=lineage.namespace or settings.service_name,
            redact=settings.logging.redact,
        )
    ]


def setup_telemetry(
    settings: config.AppSettings,
) -> otel_telemetry.Telemetry | None:
    """Exports the traces and metrics the settings ask for, if any.

    Returns:
        The telemetry, to stop at exit; None when nothing is exported.
    """
    otlp = settings.logging.otlp
    if otlp is None or not {"traces", "metrics"} & set(otlp.signals):
        return None
    return otel_telemetry.configure_telemetry(
        service=settings.service_name,
        environment=settings.env,
        endpoint=otlp.endpoint,
        headers={k: v.get_secret_value() for k, v in otlp.headers.items()},
        timeout_seconds=otlp.timeout_seconds,
        signals=[s for s in otlp.signals if s != "logs"],
        export_seconds=otlp.export_seconds,
        redact=settings.logging.redact,
    )


def setup_logging(
    settings: config.AppSettings, stream: TextIO | None = None
) -> observability.Logging:
    """Sets up the logging the settings describe."""
    options = settings.logging
    otlp = (
        observability.Otlp(
            options.otlp.endpoint,
            headers={
                name: value.get_secret_value()
                for name, value in options.otlp.headers.items()
            },
            timeout_seconds=options.otlp.timeout_seconds,
        )
        if options.otlp is not None and "logs" in options.otlp.signals
        else None
    )
    return observability.configure_logging(
        service=settings.service_name,
        environment=settings.env,
        level=options.level,
        levels=options.levels,
        format=options.format,
        otlp=otlp,
        redact=options.redact,
        queue_size=settings.tuning.queue_size,
        stream=stream,
    )


def load_modules(settings: config.AppSettings) -> list[runtime.Module]:
    """Imports the enabled modules of the settings.

    Raises:
        PipelineConfigError: If a module cannot be found, is not a `Module`,
            or is not named as in the settings.
    """
    modules = []
    for name, entry in settings.modules.items():
        if not entry.enabled:
            continue
        target = _import(entry.module) if entry.module else _entry_point(name)
        module: object = target
        if not isinstance(target, runtime.Module) and callable(target):
            create = cast(_ModuleFactory, target)
            module = create(settings)
        if not isinstance(module, runtime.Module):
            raise contract.PipelineConfigError(
                f"modules.{name}: {entry.module or name!r} is neither a Module"
                " nor a function of the settings returning one"
            )
        if module.name != name:
            raise contract.PipelineConfigError(
                f"modules.{name}: the module is named {module.name!r}"
            )
        modules.append(module)
    return modules


def _import(path: str) -> object:
    """Imports `package.module:attribute`.

    Raises:
        PipelineConfigError: If it cannot be imported.
    """
    module_name, _, attribute = path.partition(":")
    try:
        found: object = importlib.import_module(module_name)
        for part in attribute.split(".") if attribute else ():
            found = vars(found)[part]
    except (ImportError, KeyError) as exc:
        raise contract.PipelineConfigError(
            f"Cannot import {path!r}: {exc}"
        ) from exc
    return found


def _entry_point(name: str) -> object:
    """Loads the installed module `name` of the group `etl.modules`.

    Raises:
        PipelineConfigError: If no package registers it.
    """
    for entry in metadata.entry_points(group=ENTRY_POINTS):
        if entry.name == name:
            return entry.load()
    raise contract.PipelineConfigError(
        f"modules.{name}: no `module`, and no installed entry point {name!r}"
        f" in the group {ENTRY_POINTS!r}"
    )
