"""The settings of the application, validated once at start-up.

Layered, the later winning:

1. the defaults below;
2. `config/base.toml`, then `config/<env>.toml`;
3. environment variables: `ETL__DATABASES__CRM__URL` for `databases.crm.url`
   (prefix `ETL__`, `__` between keys);
4. secrets, through placeholders: `${env:NAME}`, `${file:/run/secrets/x}`.

Secret values are `SecretStr`: they print as `**********`. So do the values
of keys naming a secret (`SECRET_NAMES`: `password`, `token`...) in the
free-form sections (`options`, `connect_args`, module settings), and
`AppSettings.redacted` shows the placeholders of other values, not what they
resolve to. A module validates its own section, `modules.<name>.settings`,
with its own model (`AppSettings.module_settings`).
"""

from __future__ import annotations

from collections.abc import Collection, Mapping
import os
import pathlib
import re
from typing import Annotated, Any, ClassVar, Literal

import pydantic
import pydantic_settings

from config import secrets
from config import tuning as config_tuning
from shared import utils

ENV_VARIABLE = "ETL_ENV"
DEFAULT_ENV = "dev"

# Keep in step with `core.observability.redaction.SECRET_NAMES`.
SECRET_NAMES = (
    "password",
    "passwd",
    "pwd",
    "passphrase",
    "secret",
    "token",
    "api_key",
    "credential",
    "private_key",
    "authorization",
    "account_key",
    "signature",
    "key_pem",
)
_WHOLE_SECRET_NAMES = ("sig",)
_MASK = "**********"
_MISSING = object()
_URL_PASSWORD = re.compile(
    r"(?P<head>[a-z][a-z0-9+.\-]*://[^:/@\s]*:)[^@\s]+@", re.I
)
# Production whatever `production` says, also as a word (`prod-eu`, `eu_prd`).
PRODUCTION_ENVS = ("prod", "production", "prd")
# Besides the steps': the lock renewal and the run ledger.
BOOKKEEPING_CONNECTIONS = 2


def is_secret_name(key: str, names: Collection[str] = ()) -> bool:
    """Whether a settings key names a secret.

    Args:
        key: The key, e.g. `sasl.password` or `tlsCertificateKeyFilePassword`.
        names: More secret names, on top of `SECRET_NAMES`.
    """
    flat = _flat(key)
    if flat in _WHOLE_SECRET_NAMES:
        return True
    return any(
        part and part in flat for part in map(_flat, (*SECRET_NAMES, *names))
    )


def _flat(name: str) -> str:
    """A name lowered, without `_`, `-`, `.`: how names are compared."""
    return re.sub(r"[_\-.\s]", "", name).lower()


def _secret_values(value: object, secret: bool = False) -> object:
    """Wraps in `SecretStr` the values of the keys naming a secret, nested."""
    if isinstance(value, Mapping):
        return {
            key: _secret_values(item, secret or is_secret_name(str(key)))
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_secret_values(item, secret) for item in value]
    if secret and isinstance(value, (str, int, float)):
        return pydantic.SecretStr(str(value))
    return value


def _revealed(value: object) -> object:
    """The values of `_secret_values`, secrets revealed: for the drivers."""
    if isinstance(value, pydantic.SecretStr):
        return value.get_secret_value()
    if isinstance(value, Mapping):
        return _revealed_items(value)
    if isinstance(value, list):
        return [_revealed(item) for item in value]
    return value


def _revealed_items(mapping: Mapping[str, object]) -> dict[str, object]:
    return {key: _revealed(item) for key, item in mapping.items()}


def _hidden(
    value: utils.JsonValue, names: Collection[str], secret: bool = False
) -> utils.JsonValue:
    """Shown settings data, with the values of secret keys masked and the
    passwords of URLs hidden."""
    if isinstance(value, dict):
        return _hidden_items(value, names, secret)
    if isinstance(value, list):
        return [_hidden(item, names, secret) for item in value]
    if secret and value is not None:
        return _MASK
    if isinstance(value, str):
        return _URL_PASSWORD.sub(lambda m: f"{m['head']}{_MASK}@", value)
    return value


def _hidden_items(
    shown: utils.JsonObject, names: Collection[str], secret: bool = False
) -> utils.JsonObject:
    return {
        key: _hidden(item, names, secret or is_secret_name(key, names))
        for key, item in shown.items()
    }


def _is_mongodb(url: str) -> bool:
    return url.startswith(("mongodb://", "mongodb+srv://"))


# The values of secret keys are hidden.
type _FreeForm = Annotated[
    dict[str, object], pydantic.AfterValidator(_secret_values)
]


class ConfigError(ValueError):
    """Settings that cannot be loaded; the message lists every problem.

    Attributes:
        problems: One line per problem, `path: message`.
    """

    def __init__(self, problems: list[str]) -> None:
        super().__init__("Invalid settings:\n  " + "\n  ".join(problems))
        self.problems = problems


# A byte count: an int, or a string with a unit (`"256MiB"`).
_Bytes = Annotated[int, pydantic.BeforeValidator(utils.parse_byte_size)]


class _Model(pydantic.BaseModel):
    """A settings section: unknown keys are typos, and refused."""

    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)


class OtlpSettings(_Model):
    """Where logs, traces and metrics go by OpenTelemetry (OTLP): a
    collector, or a backend.

    They are exported over HTTP (`http/protobuf`), which every collector and
    OTLP backend takes.

    Attributes:
        endpoint: The OTLP endpoint, e.g. `http://otel-collector:4318`.
        headers: Sent with every export, e.g. an API key.
        timeout_seconds: How long an export may take.
        signals: What is exported: `logs`, `traces`, `metrics`.
        export_seconds: How often metrics are exported (and at exit).
    """

    endpoint: str
    headers: dict[str, pydantic.SecretStr] = {}
    timeout_seconds: float = 10.0
    signals: tuple[Literal["logs", "traces", "metrics"], ...] = (
        "logs",
        "traces",
        "metrics",
    )
    export_seconds: Annotated[float, pydantic.Field(gt=0)] = 15.0


class LoggingSettings(_Model):
    """How the application logs.

    Attributes:
        level: The level of every logger.
        levels: Levels by logger name, e.g. `{"sqlalchemy.engine": "INFO"}`.
        format: `json` (one object per line, for log shippers) or `console`.
        otlp: Also export to OpenTelemetry, when set.
        redact: More names whose values are hidden, wherever they appear,
            on top of the built-in ones (`observability.SECRET_NAMES`).
    """

    level: str = "INFO"
    levels: dict[str, str] = {}
    format: Literal["json", "console"] = "console"
    otlp: OtlpSettings | None = None
    redact: tuple[str, ...] = (
        "password",
        "passwd",
        "pwd",
        "secret",
        "token",
        "api_key",
        "credential",
        "private_key",
    )


class KafkaSettings(_Model):
    """A Kafka cluster.

    Attributes:
        bootstrap_servers: The brokers to start from, `host:port,...`.
        group_id: The consumer group whose offsets the ETL commits.
        options: Other librdkafka properties (`security.protocol`...); those
            naming a secret (`sasl.password`) are hidden as `credentials`.
        credentials: The librdkafka properties holding secrets
            (`sasl.password`...), hidden when shown.
        timeout_seconds: How long to wait for the brokers.
    """

    bootstrap_servers: str
    group_id: str = "etl"
    options: Annotated[
        dict[str, str | int | float | bool | pydantic.SecretStr],
        pydantic.AfterValidator(_secret_values),
    ] = {}
    credentials: dict[str, pydantic.SecretStr] = {}
    timeout_seconds: float = 30.0

    def librdkafka(self) -> dict[str, object]:
        """The librdkafka properties, secrets included."""
        return {
            "bootstrap.servers": self.bootstrap_servers,
            **_revealed_items(self.options),
            **{k: v.get_secret_value() for k, v in self.credentials.items()},
        }


class ChangesSettings(_Model):
    """Where the changes of a database are read (CDC).

    Attributes:
        kind: `native` reads the database's own log (SQL Server Change
            Tracking, Postgres logical replication, MongoDB change streams);
            `debezium` reads the topics Debezium writes them to, e.g. for
            Oracle.
        kafka: The cluster of the topics, a name of `kafka`.
        topic_prefix: The `topic.prefix` of the Debezium connector: the
            topic of table `S.T` is `<prefix>.S.T`.
        topics: The topics of tables named otherwise.
        dead_letter_topic: Where messages that are not change events go,
            instead of failing the run; on the same cluster.
    """

    kind: Literal["native", "debezium"] = "native"
    kafka: str | None = None
    topic_prefix: str | None = None
    topics: dict[str, str] = {}
    dead_letter_topic: str | None = None

    @pydantic.model_validator(mode="after")
    def _complete(self) -> ChangesSettings:
        if self.kind == "debezium":
            if self.kafka is None:
                raise ValueError("debezium changes need a kafka cluster")
            if self.topic_prefix is None and not self.topics:
                raise ValueError("debezium changes need a topic_prefix")
        return self


class _Options(_Model):
    """Options of a driver: those not given are not shown."""

    @pydantic.model_serializer(mode="wrap")
    def _given(
        self, handler: pydantic.SerializerFunctionWrapHandler
    ) -> dict[str, object]:
        shown: dict[str, object] = handler(self)
        return {k: v for k, v in shown.items() if v is not None and v != {}}


class EngineOptions(_Options):
    """The options of a SQL database's engine (`sqlalchemy.create_engine`).

    Unknown options are refused when the settings load. The options of one
    dialect go to `dialect_options`, the driver's own arguments to
    `connect_args`; the values of their secret keys are hidden when shown.

    Attributes:
        pool_size: The connections kept open; 0 for no limit.
        max_overflow: The connections opened beyond `pool_size` when all are
            in use, closed when given back; -1 for no limit.
        pool_timeout: How long to wait for a connection when all are in use,
            in seconds.
        pool_recycle: The age, in seconds, after which a connection is
            replaced: below the idle timeout of the database or firewall.
        pool_pre_ping: Whether a connection is tested before each use.
        pool_use_lifo: Whether the last connection given back is the next
            used, so that the others may time out.
        isolation_level: The isolation level of the transactions, as the
            dialect names it (`READ COMMITTED`...).
        echo: Whether the statements are logged (`sqlalchemy.engine`).
        echo_pool: Whether the pool's events are logged.
        connect_args: The arguments of the driver's `connect()`.
        execution_options: The default execution options of the
            connections.
        fast_executemany: SQL Server through pyodbc: batched inserts.
        use_setinputsizes: SQL Server and Oracle: typed parameters.
        arraysize: Oracle: the rows the driver fetches at once.
        insertmanyvalues_page_size: The rows of one multi-row INSERT.
        query_cache_size: The compiled statements cached.
        dialect_options: Other `create_engine` options, of one dialect:
            checked by SQLAlchemy when the engine is created.
    """

    pool_size: Annotated[int, pydantic.Field(ge=0)] | None = None
    max_overflow: Annotated[int, pydantic.Field(ge=-1)] | None = None
    pool_timeout: Annotated[float, pydantic.Field(gt=0)] | None = None
    pool_recycle: Annotated[int, pydantic.Field(ge=-1)] | None = None
    pool_pre_ping: bool | None = None
    pool_use_lifo: bool | None = None
    isolation_level: str | None = None
    echo: bool | None = None
    echo_pool: bool | None = None
    connect_args: _FreeForm = {}
    execution_options: dict[str, str | int | float | bool] = {}
    fast_executemany: bool | None = None
    use_setinputsizes: bool | None = None
    arraysize: Annotated[int, pydantic.Field(ge=1)] | None = None
    insertmanyvalues_page_size: Annotated[int, pydantic.Field(ge=1)] | None = (
        None
    )
    query_cache_size: Annotated[int, pydantic.Field(ge=0)] | None = None
    dialect_options: _FreeForm = {}

    DEFAULT_POOL_SIZE: ClassVar[int] = 5
    DEFAULT_MAX_OVERFLOW: ClassVar[int] = 10

    @pydantic.model_validator(mode="after")
    def _distinct(self) -> EngineOptions:
        typed = set(type(self).model_fields) & set(self.dialect_options)
        if typed:
            raise ValueError(
                f"dialect_options {sorted(typed)}: options of their own, set"
                " them outside dialect_options"
            )
        return self

    def arguments(self) -> dict[str, object]:
        """The arguments of `create_engine`, secrets revealed."""
        given = self.model_dump(exclude={"dialect_options"})
        return _revealed_items({**given, **self.dialect_options})

    def capacity(self) -> int | None:
        """The most connections the pool opens; None for no limit."""
        size = (
            self.DEFAULT_POOL_SIZE if self.pool_size is None else self.pool_size
        )
        overflow = (
            self.DEFAULT_MAX_OVERFLOW
            if self.max_overflow is None
            else self.max_overflow
        )
        if size == 0 or overflow == -1:
            return None
        return size + overflow


class MongoOptions(_Options):
    """The options of a MongoDB client (`pymongo.MongoClient`).

    Unknown options are refused when the settings load; the others of
    pymongo go to `client_options`. Names as pymongo's.

    Attributes:
        maxPoolSize: The connections opened per server; 0 for no limit.
        minPoolSize: The connections kept open per server.
        maxIdleTimeMS: How long a connection may stay idle.
        waitQueueTimeoutMS: How long to wait for a connection.
        serverSelectionTimeoutMS: How long to wait for a server.
        connectTimeoutMS: How long to wait to connect.
        socketTimeoutMS: How long to wait for an answer.
        timeoutMS: How long an operation may take.
        appname: The name the server sees.
        retryWrites: Whether writes are retried once.
        retryReads: Whether reads are retried once.
        w: The write concern.
        readPreference: Where reads go (`primary`...).
        replicaSet: The replica set.
        directConnection: Whether to connect to the one host only.
        tls: Whether connections use TLS.
        tlsCAFile: The certificate authorities.
        tlsCertificateKeyFile: The client certificate and key.
        tlsCertificateKeyFilePassword: Its password, hidden when shown.
        tlsAllowInvalidHostnames: Whether a certificate may name another
            host.
        username: The user.
        password: The password, hidden when shown.
        authSource: The database of the user.
        authMechanism: How the user authenticates.
        compressors: The compressions offered (`zstd,snappy`).
        client_options: Other options of `MongoClient`, checked when it is
            created.
    """

    # pylint: disable=invalid-name
    maxPoolSize: Annotated[int, pydantic.Field(ge=0)] | None = None
    minPoolSize: Annotated[int, pydantic.Field(ge=0)] | None = None
    maxIdleTimeMS: Annotated[int, pydantic.Field(ge=0)] | None = None
    waitQueueTimeoutMS: Annotated[int, pydantic.Field(gt=0)] | None = None
    serverSelectionTimeoutMS: Annotated[int, pydantic.Field(gt=0)] | None = None
    connectTimeoutMS: Annotated[int, pydantic.Field(ge=0)] | None = None
    socketTimeoutMS: Annotated[int, pydantic.Field(ge=0)] | None = None
    timeoutMS: Annotated[int, pydantic.Field(ge=0)] | None = None
    appname: str | None = None
    retryWrites: bool | None = None
    retryReads: bool | None = None
    w: int | str | None = None
    readPreference: str | None = None
    replicaSet: str | None = None
    directConnection: bool | None = None
    tls: bool | None = None
    tlsCAFile: str | None = None
    tlsCertificateKeyFile: str | None = None
    tlsCertificateKeyFilePassword: pydantic.SecretStr | None = None
    tlsAllowInvalidHostnames: bool | None = None
    username: str | None = None
    password: pydantic.SecretStr | None = None
    authSource: str | None = None
    authMechanism: str | None = None
    compressors: str | None = None
    client_options: _FreeForm = {}
    # pylint: enable=invalid-name

    DEFAULT_POOL_SIZE: ClassVar[int] = 100

    def arguments(self) -> dict[str, object]:
        """The arguments of `MongoClient`, secrets revealed."""
        given = self.model_dump(exclude={"client_options"})
        return _revealed_items({**given, **self.client_options})

    def capacity(self) -> int | None:
        """The most connections the pool opens; None for no limit."""
        size = (
            self.DEFAULT_POOL_SIZE
            if self.maxPoolSize is None
            else (self.maxPoolSize)
        )
        return size or None


class DatabaseSettings(_Model):
    """A database the pipelines read or write.

    Attributes:
        url: A SQLAlchemy URL, an ADO.NET, ODBC, JDBC or libpq connection
            string, or a MongoDB URI.
        dialect: The database of a connection string that does not tell it.
        options: The options of the engine (`EngineOptions`), or of the
            MongoDB client (`MongoOptions`): pool size, timeouts...
        page_size: The rows read per query: a page is held in memory, by
            every step reading the database at the same time. Keep it at the
            `batch_size` of the pipelines reading it (1000 by default, at
            most 100,000): a bigger page holds more rows, a smaller one takes
            more queries.
        replication_prefix: Postgres: the prefix of the replication slot and
            publication of each table read by logical replication.
        changes: Where its changes are read; its own log by default.
    """

    url: pydantic.SecretStr
    dialect: str | None = None
    options: EngineOptions | MongoOptions = pydantic.Field(
        default_factory=dict, validate_default=True
    )
    page_size: Annotated[int, pydantic.Field(ge=1, le=100_000)] = 1_000
    replication_prefix: str = "etl_"
    changes: ChangesSettings = ChangesSettings()

    @pydantic.field_validator("options", mode="before")
    @classmethod
    def _options(
        cls, value: object, info: pydantic.ValidationInfo
    ) -> EngineOptions | MongoOptions:
        """Validates the options as the URL's database takes them."""
        url = info.data.get("url")
        mongodb = isinstance(url, pydantic.SecretStr) and _is_mongodb(
            url.get_secret_value()
        )
        model = MongoOptions if mongodb else EngineOptions
        if isinstance(value, (EngineOptions, MongoOptions)):
            value = value.model_dump(exclude_unset=True)
        return model.model_validate(value)

    @property
    def is_mongodb(self) -> bool:
        return _is_mongodb(self.url.get_secret_value())

    def pool_capacity(self) -> int | None:
        """The most connections open at once; None for no limit (SQLite)."""
        if self.url.get_secret_value().startswith("sqlite"):
            return None
        return self.options.capacity()


class StoreSettings(_Model):
    """A file or blob store.

    Attributes:
        kind: `local` (a directory) or `gcs` (a bucket).
        path: The directory, for `local`.
        bucket: The bucket, for `gcs`.
        project: The Google Cloud project, for `gcs`.
    """

    kind: Literal["local", "gcs"]
    path: pathlib.Path | None = None
    bucket: str | None = None
    project: str | None = None

    @pydantic.model_validator(mode="after")
    def _located(self) -> StoreSettings:
        if self.kind == "local" and self.path is None:
            raise ValueError("a local store needs a path")
        if self.kind == "gcs" and self.bucket is None:
            raise ValueError("a gcs store needs a bucket")
        return self


class BookkeepingSettings(_Model):
    """Where the ETL keeps its own records: keys, state, quarantine, runs.

    Attributes:
        database: A database of `databases`; files under `directory` if
            None (one host only).
        directory: The directory of the file-based records.
        schema_name: The schema of the ETL tables.
        prefix: The prefix of their names.
        create: Whether to create the ETL tables when missing, or upgrade
            them (development and tests; production runs `etl-db upgrade`,
            or the SQL of `etl-db ddl`, in its migrations).
        retention_days: How long quarantined records are kept, for
            `etl housekeeping`: they may hold personal data.
    """

    database: str | None = None
    directory: pathlib.Path = pathlib.Path("var")
    schema_name: str | None = None
    prefix: str = "etl_"
    create: bool = False
    retention_days: Annotated[int, pydantic.Field(ge=1)] = 90


class PrivacySettings(_Model):
    """Personal data, where it must not go.

    Attributes:
        anonymize: Whether the columns the mappings mark as personal data
            get fake values: for UAT, training, development copies.
            Refused in production (`AppSettings.is_production`).
        secret: Derives the fake values, so that a value always gets the
            same one; a secret of its own, not `key_secret`.
    """

    anonymize: bool = False
    secret: pydantic.SecretStr | None = None

    @pydantic.model_validator(mode="after")
    def _keyed(self) -> PrivacySettings:
        if self.anonymize and (
            self.secret is None or not self.secret.get_secret_value()
        ):
            raise ValueError("anonymizing needs privacy.secret")
        return self


class LineageSettings(_Model):
    """Where OpenLineage events go: Marquez, DataHub, OpenMetadata...

    Attributes:
        endpoint: The OpenLineage HTTP endpoint, e.g. `http://marquez:5000`.
        namespace: The namespace of the jobs; the service name when None.
        api_key: Sent as a bearer token, if any.
        timeout_seconds: How long an event may take to send.
    """

    endpoint: str
    namespace: str | None = None
    api_key: pydantic.SecretStr | None = None
    timeout_seconds: float = 5.0


class RunsSettings(_Model):
    """How runs hold their lock.

    Attributes:
        lease_seconds: How long a run's lock outlives its process: renewed
            every third of it while the run goes on; after a crash, the next
            run waits that long at most.
    """

    lease_seconds: Annotated[float, pydantic.Field(ge=1)] = 300.0


class ModuleSettings(_Model):
    """A business module to load.

    Attributes:
        module: Where it is, `package.module:attribute`: a `Module`, or a
            function of the settings returning one. If None, the entry point
            of the same name in the group `etl.modules` (an installed
            plugin).
        enabled: Whether to load it.
        settings: Its own settings, validated by the module.
    """

    module: str | None = None
    enabled: bool = True
    settings: dict[str, object] = {}


class AppSettings(pydantic_settings.BaseSettings):
    """The settings of the whole application.

    Attributes:
        env: The environment: `dev`, `test`, `prod`...
        production: Whether the environment is production, whatever its
            name; those named after `PRODUCTION_ENVS` always are.
        service_name: The name logs and traces carry.
        logging: How the application logs.
        databases: The databases, by name.
        stores: The file and blob stores, by name.
        kafka: The Kafka clusters, by name.
        bookkeeping: Where the ETL keeps its records.
        key_secret: Keys the HMAC surrogate keys of personal data.
        privacy: Whether personal data is anonymized.
        runs: How runs hold their lock.
        tuning: The thresholds bound by the resources of the process.
        lineage: Where OpenLineage events go, if anywhere.
        modules: The business modules, by name.
    """

    model_config = pydantic_settings.SettingsConfigDict(
        env_prefix="ETL__",
        env_nested_delimiter="__",
        extra="forbid",
        frozen=True,
    )
    # Set by `load_settings`.
    toml_files: ClassVar[tuple[pathlib.Path, ...]] = ()

    env: str = DEFAULT_ENV
    production: bool = False
    service_name: str = "etl"
    logging: LoggingSettings = LoggingSettings()
    databases: dict[str, DatabaseSettings] = {}
    stores: dict[str, StoreSettings] = {}
    kafka: dict[str, KafkaSettings] = {}
    bookkeeping: BookkeepingSettings = BookkeepingSettings()
    key_secret: pydantic.SecretStr | None = None
    privacy: PrivacySettings = PrivacySettings()
    runs: RunsSettings = RunsSettings()
    tuning: config_tuning.TuningSettings = pydantic.Field(
        default_factory=config_tuning.TuningSettings
    )
    lineage: LineageSettings | None = None
    modules: dict[str, ModuleSettings] = {}
    _unresolved_placeholders_by_path: dict[tuple[str | int, ...], str] = (
        pydantic.PrivateAttr(default_factory=dict)
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[pydantic_settings.BaseSettings],
        init_settings: pydantic_settings.PydanticBaseSettingsSource,
        env_settings: pydantic_settings.PydanticBaseSettingsSource,
        dotenv_settings: pydantic_settings.PydanticBaseSettingsSource,
        file_secret_settings: pydantic_settings.PydanticBaseSettingsSource,
    ) -> tuple[pydantic_settings.PydanticBaseSettingsSource, ...]:
        """The sources, the first winning: arguments, env, then TOML files."""
        del dotenv_settings, file_secret_settings  # Placeholders do secrets.
        return (
            init_settings,
            env_settings,
            pydantic_settings.TomlConfigSettingsSource(
                settings_cls, toml_file=list(cls.toml_files), deep_merge=True
            ),
        )

    @pydantic.model_validator(mode="wrap")
    @classmethod
    def _secrets(
        cls,
        data: object,
        handler: pydantic.ModelWrapValidatorHandler[AppSettings],
    ) -> AppSettings:
        """Resolves the secret placeholders of every value, and remembers
        them to show them instead."""
        try:
            resolved = secrets.resolve_all(data)
        except secrets.SecretError as exc:
            raise ValueError(str(exc)) from None
        settings = handler(resolved)
        # pylint: disable-next=protected-access
        settings._unresolved_placeholders_by_path = secrets.placeholders(data)
        return settings

    @pydantic.model_validator(mode="after")
    def _references(self) -> AppSettings:
        """Checks that the names given refer to settings that exist, and
        that production data is never anonymized."""
        if self.privacy.anonymize and self.is_production:
            raise ValueError(
                f"privacy.anonymize is refused in production ({self.env}): it"
                " would load fake values into the target"
            )
        if (
            self.bookkeeping.database is not None
            and self.bookkeeping.database not in self.databases
        ):
            raise ValueError(
                f"bookkeeping.database {self.bookkeeping.database!r} is not"
                f" one of the databases {sorted(self.databases)}"
            )
        for name, database in self.databases.items():
            cluster = database.changes.kafka
            if cluster is not None and cluster not in self.kafka:
                raise ValueError(
                    f"databases.{name}.changes.kafka {cluster!r} is not one of"
                    f" the kafka clusters {sorted(self.kafka)}"
                )
        return self

    @property
    def is_production(self) -> bool:
        """Whether the environment is production: `production`, or a name
        holding one of `PRODUCTION_ENVS` (`prod`, `PRD`, `prod-eu`)."""
        words = re.split(r"[^a-z0-9]+", self.env.lower())
        return self.production or any(w in PRODUCTION_ENVS for w in words)

    def module_settings[T: pydantic.BaseModel](
        self, name: str, model: type[T]
    ) -> T:
        """Validates the settings of module `name` with its model.

        The model's validators get these settings as the context
        (`info.context["settings"]`): to check, say, the pools of the
        databases it uses (`check_pools`).

        Raises:
            ConfigError: If they are invalid, with paths from the root.
        """
        section = self.modules.get(name)
        try:
            return model.model_validate(
                section.settings if section else {},
                context={"settings": self},
            )
        except pydantic.ValidationError as exc:
            raise ConfigError(
                _problems(exc, prefix=("modules", name, "settings"))
            ) from None

    def anonymization_secret(self) -> bytes | None:
        """The secret of anonymization, when personal data is anonymized."""
        privacy = self.privacy
        if not privacy.anonymize or privacy.secret is None:
            return None
        return privacy.secret.get_secret_value().encode()

    def check_pools(self, needs: Mapping[str, int], why: str) -> None:
        """Checks that the pools of databases hold the connections needed.

        Args:
            needs: The connections used at the same time, by database name;
                the ETL's records add `BOOKKEEPING_CONNECTIONS` to theirs.
                Unknown names are skipped.
            why: What needs them, for the message.

        Raises:
            ValueError: If a pool is too small, saying how to grow it.
        """
        needed = dict(needs)
        book = self.bookkeeping.database
        if book is not None:
            needed[book] = needed.get(book, 0) + BOOKKEEPING_CONNECTIONS
        problems = []
        for name, count in sorted(needed.items()):
            database = self.databases.get(name)
            capacity = database.pool_capacity() if database else None
            if database is None or capacity is None or capacity >= count:
                continue
            options = database.options
            if isinstance(options, MongoOptions):
                grow = f"maxPoolSize to {count}"
            else:
                size = (
                    options.DEFAULT_POOL_SIZE
                    if options.pool_size is None
                    else options.pool_size
                )
                grow = f"max_overflow to {count - size}"
            problems.append(
                f"databases.{name}: its pool opens {capacity} connections at"
                f" most, {why} need {count}"
                f"{' with the ETL records' if name == book else ''}: set"
                f" databases.{name}.options.{grow} or more, or lower"
                " tuning.max_parallel_steps"
            )
        if problems:
            raise ValueError("; ".join(problems))

    def redacted(self) -> utils.JsonObject:
        """Returns the settings as data, secrets hidden: to show them.

        Hidden: secret values (`SecretStr`), the values of keys naming a
        secret (`is_secret_name`, with `logging.redact`) and the passwords
        of URLs; values given by placeholders show the placeholders.
        """
        shown: utils.JsonObject = self.model_dump(mode="json")
        for path, text in self._unresolved_placeholders_by_path.items():
            _replace(shown, path, text)
        return _hidden_items(shown, self.logging.redact)


def load_settings(
    env: str | None = None,
    *,
    directory: pathlib.Path | str = "config",
    overrides: Mapping[str, object] | None = None,
) -> AppSettings:
    """Loads and validates the settings of an environment.

    Args:
        env: The environment; `$ETL_ENV`, else `dev`, by default.
        directory: Where `base.toml` and `<env>.toml` are; missing files are
            skipped.
        overrides: Values winning over every source, e.g. from the command
            line.

    Returns:
        The settings.

    Raises:
        ConfigError: If they are invalid: every problem, with its path.
    """
    chosen = env or os.environ.get(ENV_VARIABLE) or DEFAULT_ENV
    root = pathlib.Path(directory)
    files = tuple(
        path
        for path in (root / "base.toml", root / f"{chosen}.toml")
        if path.exists()
    )
    loaded: type[AppSettings] = type(
        "LoadedSettings", (AppSettings,), {"toml_files": files, "__doc__": None}
    )
    # pydantic types `__init__` field by field; it validates these values.
    values: dict[str, Any] = {"env": chosen, **(overrides or {})}
    try:
        return loaded(**values)
    except pydantic.ValidationError as exc:
        raise ConfigError(_problems(exc)) from None


def _replace(
    data: utils.JsonValue, path: tuple[str | int, ...], text: str
) -> None:
    """Shows `text` at `path` of shown settings, unless nothing is there or
    a hidden secret is."""
    parent: object = data
    for part in path[:-1]:
        parent = _child(parent, part)
    if not path:
        return
    last = path[-1]
    shown = _child(parent, last)
    if not isinstance(shown, str) or shown == _MASK:
        return
    if isinstance(parent, dict):
        parent[str(last)] = text
    elif isinstance(parent, list) and isinstance(last, int):
        parent[last] = text


def _child(data: object, part: str | int) -> object:
    """The item `part` of shown settings data; `_MISSING` if none."""
    if isinstance(data, dict):
        return data.get(part, _MISSING)
    if isinstance(data, list) and isinstance(part, int) and part < len(data):
        return data[part]
    return _MISSING


def _problems(
    exc: pydantic.ValidationError, prefix: tuple[str, ...] = ()
) -> list[str]:
    """Returns one `path: message` line per validation error."""
    lines = []
    for error in exc.errors():
        path = ".".join(str(part) for part in (*prefix, *error["loc"]))
        lines.append(f"{path or '<root>'}: {error['msg']}")
    return lines
