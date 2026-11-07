"""Connection strings of other platforms, translated into SQLAlchemy URLs.

`parse` reads SQLAlchemy URLs (a missing driver gets ours), ADO.NET
(SqlClient, ODP.NET, Npgsql, Connector/NET), ODBC, OLE DB, JDBC
(`sqlserver`, `oracle:thin`, `postgresql`, `mysql`, `mariadb`) and libpq
keywords.

    spec = connection_strings.parse(
        "Server=tcp:db.corp,1433;Initial Catalog=CRM;User ID=etl;"
        "Password=...;Encrypt=True;TrustServerCertificate=False"
    )
    spec.url  # mssql+mssqlpython://etl:***@db.corp:1433/CRM?Encrypt=yes&...

Settings with no equivalent are listed in `ignored`, never dropped silently.
When nothing tells the database apart, SQL Server is assumed and `guessed`
says so: pass `dialect` to decide.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
import dataclasses
import re
from typing import Any

import sqlalchemy as sa

DEFAULT_DRIVERS: Mapping[str, str] = {
    "mssql": "mssqlpython",
    "oracle": "oracledb",
    "postgresql": "psycopg",
    "mysql": "pymysql",
    "mariadb": "pymysql",
}
_SQL_ATTR_LOGIN_TIMEOUT = 103

_SQLALCHEMY = re.compile(r"^[A-Za-z][A-Za-z0-9+.\-]*://")
_TRUE = frozenset({"true", "yes", "1", "sspi", "on"})


class ConnectionStringError(ValueError):
    """A connection string that cannot be translated."""


@dataclasses.dataclass(frozen=True, slots=True)
class ConnectionSpec:
    """A connection string, translated.

    Attributes:
        url: The SQLAlchemy URL; its password is hidden when printed.
        connect_args: Driver arguments a URL cannot hold, for
            `create_engine(url, connect_args=...)`.
        source: The format it was written in: `sqlalchemy`, `ado.net`,
            `odbc`, `oledb`, `jdbc` or `libpq`.
        guessed: Whether the database was assumed, nothing telling it.
        ignored: The settings with no equivalent, as written.
    """

    url: sa.URL
    connect_args: dict[str, Any] = dataclasses.field(default_factory=dict)
    source: str = "sqlalchemy"
    guessed: bool = False
    ignored: tuple[str, ...] = ()


def parse(
    text: str,
    *,
    dialect: str | None = None,
    drivers: Mapping[str, str] | None = None,
) -> ConnectionSpec:
    """Translates a connection string of any supported format.

    Args:
        text: The connection string.
        dialect: The database (`mssql`, `oracle`, `postgresql`, `mysql`),
            when the string does not tell it, or tells it wrong.
        drivers: The driver per database, over `DEFAULT_DRIVERS`.

    Returns:
        The translated connection.

    Raises:
        ConnectionStringError: If the format or the database is not
            recognized, or a setting cannot be honored.
    """
    text = text.strip()
    chosen = {**DEFAULT_DRIVERS, **(drivers or {})}
    if text.lower().startswith("jdbc:"):
        found, settings = _jdbc(text)
        return _build(dialect or found, settings, "jdbc", False, chosen)
    if _SQLALCHEMY.match(text):
        return _sqlalchemy(text, chosen)
    if "=" not in text:
        raise ConnectionStringError(
            "Not a connection string: expected key=value pairs, a URL or a"
            " JDBC URL"
        )
    if (
        ";" not in text
        and re.match(r"^\s*\w+\s*=\s*\S", text)
        and (" " in text.strip())
    ):
        settings = _libpq(text)
        return _build(dialect or "postgresql", settings, "libpq", False, chosen)

    settings = _pairs(text)
    source = "ado.net"
    if "driver" in settings:
        source = "odbc"
    elif "provider" in settings:
        source = "oledb"
    if dialect is not None:
        return _build(dialect, settings, source, False, chosen)
    found, guessed = _detect(settings)
    return _build(found, settings, source, guessed, chosen)


def _sqlalchemy(text: str, drivers: Mapping[str, str]) -> ConnectionSpec:
    """Keeps a SQLAlchemy URL, giving it our driver if it names none."""
    url = sa.make_url(text)
    name = url.drivername
    if name in ("postgres", "postgresql"):
        name = "postgresql"
    if "+" not in name and name in drivers:
        name = f"{name}+{drivers[name]}"
    return ConnectionSpec(url.set(drivername=name))


def _pairs(text: str) -> dict[str, str]:
    """Splits `key=value;...` the way ADO.NET and ODBC do.

    Values may be quoted (`"a;b"`, `'a;b'`, a doubled quote inside) or braced
    (ODBC: `{a;b}`, `}}` inside); keys are compared without case, spaces or
    underscores.

    Raises:
        ConnectionStringError: If a quote or a brace is not closed.
    """
    settings: dict[str, str] = {}
    i, length = 0, len(text)
    while i < length:
        while i < length and text[i] in "; \t\r\n":
            i += 1
        if i >= length:
            break
        equals = text.find("=", i)
        if equals < 0:
            raise ConnectionStringError(f"No value for {text[i:].strip()!r}")
        key = _key(text[i:equals])
        i = equals + 1
        while i < length and text[i] in " \t":
            i += 1
        value, i = _value(text, i)
        settings[key] = value
    return settings


def _value(text: str, i: int) -> tuple[str, int]:
    """Reads the value starting at `i`; returns it and where it ends."""
    if i < len(text) and text[i] in "\"'{":
        opening = text[i]
        closing = "}" if opening == "{" else opening
        chars: list[str] = []
        i += 1
        while True:
            if i >= len(text):
                raise ConnectionStringError(f"Unclosed {opening} in a value")
            if text[i] == closing:
                if text[i + 1 : i + 2] == closing:  # Doubled: a literal one.
                    chars.append(closing)
                    i += 2
                    continue
                return "".join(chars), i + 1
            chars.append(text[i])
            i += 1
    end = text.find(";", i)
    end = len(text) if end < 0 else end
    return text[i:end].strip(), end


def _libpq(text: str) -> dict[str, str]:
    """Splits libpq keywords: `host=h port=5432 dbname='my db'`."""
    settings: dict[str, str] = {}
    for match in re.finditer(r"(\w+)\s*=\s*('(?:[^'\\]|\\.)*'|\S+)", text):
        value = match.group(2)
        if value.startswith("'"):
            value = re.sub(r"\\(.)", r"\1", value[1:-1])
        settings[_key(match.group(1))] = value
    return settings


def _key(raw: str) -> str:
    return re.sub(r"[\s_]", "", raw).lower()


def _detect(settings: Mapping[str, str]) -> tuple[str, bool]:
    """Returns the database the settings are for, and whether it is a guess.

    Raises:
        ConnectionStringError: If they point to no supported database.
    """
    named = f"{settings.get('provider', '')} {settings.get('driver', '')}"
    named = named.lower()
    for marker, found in (
        ("sql server", "mssql"),
        ("sqloledb", "mssql"),
        ("msoledbsql", "mssql"),
        ("sqlncli", "mssql"),
        ("oracle", "oracle"),
        ("oraoledb", "oracle"),
        ("msdaora", "oracle"),
        ("postgres", "postgresql"),
        ("mariadb", "mysql"),
        ("mysql", "mysql"),
    ):
        if marker in named:
            return found, False
    if named.strip():
        raise ConnectionStringError(f"Unsupported provider or driver {named!r}")

    source = settings.get("datasource", "")
    if source.lstrip().startswith("(") or re.search(r":\d+/", source):
        return "oracle", False

    scores = {
        name: sum(1 for key in keys if key in settings)
        for name, keys in _PLATFORM_ONLY_KEYS.items()
    }
    port = settings.get("port", "")
    for name, default in _PORTS.items():
        if port == default or settings.get("server", "").endswith(
            f",{default}"
        ):
            scores[name] += 2
    best = max(scores, key=lambda name: scores[name])
    if scores[best] and list(scores.values()).count(scores[best]) == 1:
        return best, False
    if any(key in settings for key in (*_SERVER, "datasource")):
        return "mssql", True
    raise ConnectionStringError(
        "Cannot tell the database of this connection string: pass `dialect`"
    )


# SqlClient has no SSL mode (it has Encrypt); Npgsql and Connector/NET do.
_PLATFORM_ONLY_KEYS: dict[str, tuple[str, ...]] = {
    "mssql": (
        "initialcatalog",
        "integratedsecurity",
        "trustedconnection",
        "multipleactiveresultsets",
        "applicationintent",
        "multisubnetfailover",
        "attachdbfilename",
        "packetsize",
        "encrypt",
    ),
    "oracle": ("dbaprivilege", "statementcachesize", "proxyuserid"),
    "postgresql": (
        "host",
        "username",
        "searchpath",
        "sslmode",
        "targetsessionattributes",
    ),
    "mysql": (
        "uid",
        "sslmode",
        "charset",
        "allowuservariables",
        "convertzerodatetime",
        "allowpublickeyretrieval",
        "sslca",
    ),
}
_PORTS = {
    "mssql": "1433",
    "oracle": "1521",
    "postgresql": "5432",
    "mysql": "3306",
}

_SERVER = ("server", "datasource", "address", "addr", "networkaddress", "host")
_DATABASE = ("database", "initialcatalog", "databasename", "dbname")
_USER = ("userid", "uid", "user", "username")
_PASSWORD = ("password", "pwd")
_TIMEOUT = ("connecttimeout", "connectiontimeout", "timeout", "logintimeout")


class _Settings:
    """The settings of a connection string, consumed as they are used, so
    that what remains is what was ignored."""

    def __init__(self, settings: Mapping[str, str]) -> None:
        self._remaining = dict(settings)

    def take(self, *keys: str) -> str | None:
        """Returns the first setting of `keys` that is set, consuming all."""
        found: str | None = None
        for key in keys:
            value = self._remaining.pop(key, None)
            if found is None and value not in (None, ""):
                found = value
        return found

    def flag(self, *keys: str) -> bool | None:
        value = self.take(*keys)
        return None if value is None else value.strip().lower() in _TRUE

    def remaining(self) -> tuple[str, ...]:
        return tuple(sorted(self._remaining))


def _build(
    dialect: str,
    settings: Mapping[str, str],
    source: str,
    guessed: bool,
    drivers: Mapping[str, str],
) -> ConnectionSpec:
    """Translates the settings for `dialect`, noting what was left out.

    Raises:
        ConnectionStringError: If there is no translation for `dialect`.
    """
    builder = _BUILDERS.get(dialect)
    if builder is None:
        raise ConnectionStringError(
            f"No translation for {dialect!r}; supported: {sorted(_BUILDERS)}"
        )
    remaining = _Settings(settings)
    # Who the platform is: nothing for us to translate.
    remaining.take("provider", "driver")
    url, connect_args = builder(remaining, f"{dialect}+{drivers[dialect]}")
    return ConnectionSpec(
        url, connect_args, source, guessed, remaining.remaining()
    )


def _mssql(
    settings: _Settings, drivername: str
) -> tuple[sa.URL, dict[str, Any]]:
    """SqlClient, ODBC, OLE DB and JDBC settings, for SQL Server drivers."""
    server = settings.take(*_SERVER) or ""
    server = re.sub(r"^tcp:", "", server.strip(), flags=re.I)
    if re.match(r"^(np|lpc):", server, re.I):
        raise ConnectionStringError(
            "Named pipes and shared memory are Windows-only: connect by TCP"
        )
    host, _, port = server.partition(",")
    instance = settings.take("instancename")
    if instance:
        host = f"{host}\\{instance}"
    if host.lower() in (".", "(local)", "(localdb)"):
        host = "localhost"
    port = settings.take("port", "portnumber") or port

    query: dict[str, str] = {}
    for keys, name in (
        (("encrypt",), "Encrypt"),
        (("trustservercertificate",), "TrustServerCertificate"),
        (("multisubnetfailover",), "MultiSubnetFailover"),
    ):
        value = settings.take(*keys)
        if value is not None:
            query[name] = _odbc_bool(value)
    if settings.flag("integratedsecurity", "trustedconnection"):
        query["Trusted_Connection"] = "yes"
    for keys, name in (
        (("authentication",), "Authentication"),
        (("applicationintent",), "ApplicationIntent"),
        (("hostnameincertificate",), "HostnameInCertificate"),
        (("serverspn",), "ServerSPN"),
        (("connectretrycount",), "ConnectRetryCount"),
        (("connectretryinterval",), "ConnectRetryInterval"),
        (("packetsize",), "PacketSize"),
    ):
        value = settings.take(*keys)
        if value is not None:
            query[name] = value

    connect_args: dict[str, Any] = {}
    timeout = settings.take(*_TIMEOUT)
    if timeout is not None:
        connect_args["attrs_before"] = {_SQL_ATTR_LOGIN_TIMEOUT: int(timeout)}
    url = sa.URL.create(
        drivername,
        username=settings.take(*_USER),
        password=settings.take(*_PASSWORD),
        host=host or None,
        port=int(port) if port else None,
        database=settings.take(*_DATABASE),
        query=query,
    )
    return url, connect_args


def _odbc_bool(value: str) -> str:
    """SqlClient's True/False/Mandatory/Optional/Strict, as ODBC's."""
    lowered = value.strip().lower()
    if lowered == "strict":
        return "strict"
    if lowered in _TRUE or lowered == "mandatory":
        return "yes"
    return "no"


def _oracle(
    settings: _Settings, drivername: str
) -> tuple[sa.URL, dict[str, Any]]:
    """ODP.NET, OLE DB and JDBC settings, for python-oracledb."""
    source = (settings.take("datasource", *_SERVER) or "").strip()
    connect_args: dict[str, Any] = {}
    host: str | None = None
    port: str | None = None
    database: str | None = None
    query: dict[str, str] = {}

    ez = re.match(
        r"^(?://)?(?P<host>[^:/()\s]+)(?::(?P<port>\d+))?"
        r"/(?P<service>[^:/\s]+)",
        source,
    )
    sid = re.match(r"^(?P<host>[^:/()\s]+):(?P<port>\d+):(?P<sid>\w+)$", source)
    if source.startswith("("):
        hosts = re.findall(r"\(\s*HOST\s*=\s*([^)\s]+)\s*\)", source, re.I)
        ports = re.findall(r"\(\s*PORT\s*=\s*(\d+)\s*\)", source, re.I)
        named = re.search(r"SERVICE_NAME\s*=\s*([^)\s]+)", source, re.I)
        sid_name = re.search(r"\(\s*SID\s*=\s*([^)\s]+)", source, re.I)
        if len(hosts) == 1 and (named or sid_name):
            host, port = hosts[0], (ports[0] if ports else None)
            if named:
                query["service_name"] = named.group(1)
            elif sid_name:
                database = sid_name.group(1)
        else:
            # Failover, load balancing...: the descriptor as it is.
            connect_args["dsn"] = source
    elif sid:
        host, port, database = sid["host"], sid["port"], sid["sid"]
    elif ez:
        host, port = ez["host"], ez["port"]
        query["service_name"] = ez["service"]
    elif source:
        # A TNS alias, from tnsnames.ora (`config_dir` or $TNS_ADMIN).
        host = source
    port = settings.take("port") or port
    service = settings.take("servicename")
    if service:
        query["service_name"] = service
    timeout = settings.take(*_TIMEOUT)
    if timeout is not None:
        connect_args["tcp_connect_timeout"] = float(timeout)

    url = sa.URL.create(
        drivername,
        username=settings.take(*_USER),
        password=settings.take(*_PASSWORD),
        host=host,
        port=int(port) if port else None,
        database=database,
        query=query,
    )
    return url, connect_args


_NPGSQL_TO_LIBPQ_SSL_MODES = {
    "disable": "disable",
    "allow": "allow",
    "prefer": "prefer",
    "require": "require",
    "verifyca": "verify-ca",
    "verify-ca": "verify-ca",
    "verifyfull": "verify-full",
    "verify-full": "verify-full",
}


def _postgres(
    settings: _Settings, drivername: str
) -> tuple[sa.URL, dict[str, Any]]:
    """Npgsql, JDBC and libpq settings, for psycopg."""
    hosts = (settings.take(*_SERVER) or "").split(",")
    port = settings.take("port")
    query: dict[str, str | tuple[str, ...]] = {}
    if len(hosts) > 1:
        # libpq tries the hosts in turn (failover).
        query["host"] = tuple(
            h.strip() if ":" in h or not port else f"{h.strip()}:{port}"
            for h in hosts
        )
    ssl = settings.take("sslmode")
    if ssl is not None:
        query["sslmode"] = _NPGSQL_TO_LIBPQ_SSL_MODES.get(
            ssl.lower().replace(" ", ""), ssl
        )
    timeout = settings.take(*_TIMEOUT)
    if timeout is not None:
        query["connect_timeout"] = timeout
    for keys, name in (
        (("applicationname",), "application_name"),
        (("sslrootcert", "rootcertificate"), "sslrootcert"),
        (
            ("targetsessionattributes", "targetsessionattrs"),
            "target_session_attrs",
        ),
    ):
        value = settings.take(*keys)
        if value is not None:
            query[name] = value
    schema = settings.take("searchpath", "currentschema")
    if schema is not None:
        query["options"] = f"-csearch_path={schema}"

    url = sa.URL.create(
        drivername,
        username=settings.take(*_USER),
        password=settings.take(*_PASSWORD),
        host=hosts[0].strip() or None if len(hosts) == 1 else None,
        port=int(port) if port and len(hosts) == 1 else None,
        database=settings.take(*_DATABASE),
        query=query,
    )
    return url, {}


def _mysql(
    settings: _Settings, drivername: str
) -> tuple[sa.URL, dict[str, Any]]:
    """Connector/NET, MySqlConnector and JDBC settings, for PyMySQL."""
    host = (settings.take(*_SERVER) or "").split(",")[0].strip()
    query: dict[str, str] = {}
    charset = settings.take("charset", "characterencoding")
    if charset is not None:
        query["charset"] = charset
    timeout = settings.take(*_TIMEOUT)
    if timeout is not None:
        query["connect_timeout"] = timeout

    connect_args: dict[str, Any] = {}
    mode = (settings.take("sslmode") or "").lower().replace("_", "")
    use_ssl = settings.flag("usessl")
    if mode in ("none", "disabled") or use_ssl is False:
        connect_args["ssl_disabled"] = True
    elif mode in ("required", "require") or use_ssl:
        # Encrypted, without checking the certificate.
        connect_args["ssl"] = {"check_hostname": False}
    elif mode in ("verifyca", "verifyfull", "verifyidentity"):
        connect_args["ssl_verify_cert"] = True
        connect_args["ssl_verify_identity"] = mode != "verifyca"
    ca = settings.take("sslca", "certificatefile")
    if ca is not None:
        connect_args["ssl_ca"] = ca

    port = settings.take("port")
    url = sa.URL.create(
        drivername,
        username=settings.take(*_USER),
        password=settings.take(*_PASSWORD),
        host=host or None,
        port=int(port) if port else None,
        database=settings.take(*_DATABASE),
        query=query,
    )
    return url, connect_args


_BUILDERS: dict[
    str, Callable[[_Settings, str], tuple[sa.URL, dict[str, Any]]]
] = {
    "mssql": _mssql,
    "oracle": _oracle,
    "postgresql": _postgres,
    "mysql": _mysql,
    "mariadb": _mysql,
}


def _jdbc(text: str) -> tuple[str, dict[str, str]]:
    """Splits a JDBC URL into its database and its settings.

    Raises:
        ConnectionStringError: If the JDBC driver is not supported.
    """
    body = text[len("jdbc:") :]
    if body.lower().startswith("sqlserver://"):
        location, _, properties = body[len("sqlserver://") :].partition(";")
        settings = _pairs(properties)
        server, _, port = location.partition(":")
        server, _, instance = server.partition("\\")
        settings.setdefault("server", server)
        if port:
            settings.setdefault("port", port)
        if instance:
            settings.setdefault("instancename", instance)
        return "mssql", settings

    oracle = re.match(
        r"^oracle:thin:(?:(?P<user>[^/@]+)/(?P<password>[^@]*))?"
        r"@(?P<source>.+)$",
        body,
        re.I,
    )
    if oracle:
        settings = {"datasource": oracle["source"]}
        if oracle["user"]:
            settings["userid"] = oracle["user"]
            settings["password"] = oracle["password"]
        return "oracle", settings

    url = re.match(
        r"^(?P<kind>postgresql|mysql|mariadb)://(?P<hosts>[^/?]*)"
        r"(?:/(?P<database>[^?]*))?(?:\?(?P<query>.*))?$",
        body,
        re.I,
    )
    if url:
        settings = {
            _key(key): value
            for key, _, value in (
                part.partition("=")
                for part in (url["query"] or "").split("&")
                if part
            )
        }
        hosts = url["hosts"].split(",")
        if len(hosts) == 1 and ":" in hosts[0]:
            settings["server"], settings["port"] = hosts[0].split(":", 1)
        else:
            settings["server"] = url["hosts"]
        if url["database"]:
            settings["database"] = url["database"]
        kind = url["kind"].lower()
        return ("mysql" if kind == "mariadb" else kind), settings

    raise ConnectionStringError(f"Unsupported JDBC URL {text.split('//')[0]!r}")
