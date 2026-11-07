from __future__ import annotations

from typing import Any

import pytest
import sqlalchemy as sa

from infra.database import relational


def _url(text: str, **options: Any) -> str:
    return relational.parse_connection_string(
        text, **options
    ).url.render_as_string(hide_password=False)


class TestSqlServer:

    def test_sqlclient(self) -> None:
        spec = relational.parse_connection_string(
            "Server=tcp:db.corp,1433;Initial Catalog=CRM;User ID=etl;"
            'Password="p;ss=""1""";Encrypt=True;TrustServerCertificate=False;'
            "Connect Timeout=30;Pooling=true;MultipleActiveResultSets=True"
        )

        assert spec.url.render_as_string(hide_password=False) == (
            "mssql+mssqlpython://etl:p%3Bss%3D%221%22@db.corp:1433/CRM"
            "?Encrypt=yes&TrustServerCertificate=no"
        )
        assert spec.url.password == 'p;ss="1"'
        assert spec.connect_args == {"attrs_before": {103: 30}}
        # The engine owns pooling; MARS has no equivalent.
        assert spec.ignored == ("multipleactiveresultsets", "pooling")
        assert (spec.source, spec.guessed) == ("ado.net", False)

    def test_named_instances_and_windows_authentication(self) -> None:
        assert (
            _url(
                r"Data Source=SRV\SQL2019;Database=crm;Integrated Security=SSPI"
            )
            == r"mssql+mssqlpython://SRV\SQL2019/crm?Trusted_Connection=yes"
        )
        assert _url("Server=(local);Database=crm;Trusted_Connection=yes") == (
            "mssql+mssqlpython://localhost/crm?Trusted_Connection=yes"
        )

    def test_odbc_and_ole_db(self) -> None:
        odbc = relational.parse_connection_string(
            "Driver={ODBC Driver 18 for SQL Server};Server=db;Database=crm;"
            "UID=etl;PWD={a;b}}c};Encrypt=strict"
        )
        oledb = relational.parse_connection_string(
            "Provider=MSOLEDBSQL;Data Source=db;Initial Catalog=crm;"
            "User ID=u;Password=p"
        )

        assert (odbc.source, odbc.url.password) == ("odbc", "a;b}c")
        assert odbc.url.query == {"Encrypt": "strict"}
        assert (oledb.source, oledb.url.host) == ("oledb", "db")

    def test_jdbc(self) -> None:
        assert _url(
            r"jdbc:sqlserver://db\inst:1433;databaseName=crm;user=u;"
            "password=p;encrypt=true;trustServerCertificate=true"
        ) == (
            r"mssql+mssqlpython://u:p@db\inst:1433/crm"
            "?Encrypt=yes&TrustServerCertificate=yes"
        )

    def test_named_pipes_are_refused(self) -> None:
        with pytest.raises(relational.ConnectionStringError, match="TCP"):
            relational.parse_connection_string(
                r"Server=np:\\.\pipe\sql\query;Database=crm"
            )

    def test_another_driver(self) -> None:
        spec = relational.parse_connection_string(
            "Server=db;Initial Catalog=crm",
            drivers={"mssql": "pyodbc"},
        )

        assert spec.url.drivername == "mssql+pyodbc"


class TestOracle:

    @pytest.mark.parametrize(
        "source",
        [
            (
                "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=ora1)(PORT=1521))"
                "(CONNECT_DATA=(SERVICE_NAME=CRMPDB)))"
            ),
            "ora1:1521/CRMPDB",
            "//ora1:1521/CRMPDB",
        ],
    )
    def test_odp_net_data_sources(self, source: str) -> None:
        assert _url(f"User Id=scott;Password=tiger;Data Source={source}") == (
            "oracle+oracledb://scott:tiger@ora1:1521?service_name=CRMPDB"
        )

    def test_failover_descriptors_are_kept_whole(self) -> None:
        descriptor = (
            "(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(HOST=a)(PORT=1521))"
            "(ADDRESS=(HOST=b)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=X)))"
        )

        spec = relational.parse_connection_string(
            f"User Id=scott;Password=tiger;Data Source={descriptor}"
        )

        assert spec.connect_args == {"dsn": descriptor}
        assert spec.url.host is None

    def test_a_tns_alias_needs_the_dialect(self) -> None:
        text = "User Id=scott;Password=tiger;Data Source=PRODDB"

        guessed = relational.parse_connection_string(text)
        told = relational.parse_connection_string(text, dialect="oracle")

        # Valid for SqlClient too: a guess, said so.
        assert (guessed.url.drivername, guessed.guessed) == (
            "mssql+mssqlpython",
            True,
        )
        assert (told.url.drivername, told.url.host) == (
            "oracle+oracledb",
            "PRODDB",
        )

    def test_jdbc(self) -> None:
        assert _url("jdbc:oracle:thin:scott/tiger@//ora1:1521/CRMPDB") == (
            "oracle+oracledb://scott:tiger@ora1:1521?service_name=CRMPDB"
        )
        # host:port:SID, the SID being the database of the URL.
        assert _url("jdbc:oracle:thin:@ora1:1521:ORCL") == (
            "oracle+oracledb://ora1:1521/ORCL"
        )


class TestPostgres:

    def test_npgsql(self) -> None:
        assert _url(
            "Host=pg1;Port=5432;Database=crm;Username=etl;Password=p;"
            "SSL Mode=VerifyFull;Search Path=legacy;Application Name=etl"
        ) == (
            "postgresql+psycopg://etl:p@pg1:5432/crm?application_name=etl"
            "&options=-csearch_path%3Dlegacy&sslmode=verify-full"
        )

    def test_npgsql_failover_hosts(self) -> None:
        spec = relational.parse_connection_string(
            "Host=pg1,pg2:5433;Port=5432;Database=crm;Username=etl"
        )

        assert spec.url.query["host"] == ("pg1:5432", "pg2:5433")

    def test_libpq_keywords(self) -> None:
        spec = relational.parse_connection_string(
            "host=localhost port=5432 dbname='my db' user=etl password=s3"
            " sslmode=require"
        )

        assert spec.source == "libpq"
        assert (spec.url.database, spec.url.query["sslmode"]) == (
            "my db",
            "require",
        )

    def test_jdbc(self) -> None:
        assert (
            _url(
                "jdbc:postgresql://pg1:5432/crm?user=u&password=p"
                "&sslmode=require"
            )
            == "postgresql+psycopg://u:p@pg1:5432/crm?sslmode=require"
        )


class TestMysql:

    def test_connector_net(self) -> None:
        spec = relational.parse_connection_string(
            "Server=my1;Port=3306;Database=crm;Uid=u;Pwd=p;SslMode=Required;"
            "CharSet=utf8mb4"
        )

        assert spec.url.render_as_string(hide_password=False) == (
            "mysql+pymysql://u:p@my1:3306/crm?charset=utf8mb4"
        )
        assert spec.connect_args == {"ssl": {"check_hostname": False}}

    def test_tls_modes(self) -> None:
        def args(mode: str) -> dict[str, Any]:
            return relational.parse_connection_string(
                f"Server=my1;Uid=u;SslMode={mode}", dialect="mysql"
            ).connect_args

        assert args("None") == {"ssl_disabled": True}
        assert args("VerifyFull") == {
            "ssl_verify_cert": True,
            "ssl_verify_identity": True,
        }

    def test_jdbc(self) -> None:
        assert _url("jdbc:mariadb://my1:3306/crm?user=u&password=p") == (
            "mysql+pymysql://u:p@my1:3306/crm"
        )


class TestDetection:

    def test_sqlalchemy_urls_get_our_drivers(self) -> None:
        assert _url("mssql://u:p@h/db") == "mssql+mssqlpython://u:p@h/db"
        assert _url("postgres://u:p@h/db") == "postgresql+psycopg://u:p@h/db"
        assert _url("sqlite:///x.db") == "sqlite:///x.db"
        assert _url("mysql+mysqldb://u@h/db") == "mysql+mysqldb://u@h/db"

    def test_ambiguous_strings_are_assumed_sql_server(self) -> None:
        spec = relational.parse_connection_string(
            "Server=db;Database=crm;User Id=u;Password=p"
        )

        assert (spec.url.drivername, spec.guessed) == (
            "mssql+mssqlpython",
            True,
        )

    def test_platform_keys_tell_the_database(self) -> None:
        mysql = relational.parse_connection_string(
            "Server=db;Port=53306;Database=crm;Uid=u;Pwd=p;SslMode=None"
        )
        npgsql = relational.parse_connection_string(
            "Host=db;Database=crm;Username=u;SSL Mode=Require"
        )

        assert mysql.url.drivername == "mysql+pymysql"
        assert npgsql.url.drivername == "postgresql+psycopg"

    def test_ports_tell_the_database(self) -> None:
        spec = relational.parse_connection_string(
            "Server=db;Port=3306;Database=crm;User Id=u;Password=p"
        )

        assert (spec.url.drivername, spec.guessed) == ("mysql+pymysql", False)

    @pytest.mark.parametrize(
        ("text", "message"),
        [
            ("just text", "Not a connection string"),
            (
                "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=x.mdb",
                "Unsupported",
            ),
            ("jdbc:db2://h:50000/db", "Unsupported JDBC"),
            ('Server=db;Password="unclosed', "Unclosed"),
            ("Foo=bar", "Cannot tell"),
        ],
    )
    def test_what_cannot_be_translated(self, text: str, message: str) -> None:
        with pytest.raises(relational.ConnectionStringError, match=message):
            relational.parse_connection_string(text)

    def test_unknown_dialects(self) -> None:
        with pytest.raises(relational.ConnectionStringError, match="db2"):
            relational.parse_connection_string("Server=x", dialect="db2")


def test_passwords_are_hidden_when_printed() -> None:
    spec = relational.parse_connection_string(
        "Server=db;Database=crm;User ID=etl;Password=Secret-123"
    )

    assert "Secret-123" not in repr(spec)
    assert "Secret-123" not in str(spec.url)


def test_a_database_takes_connection_strings_as_they_are() -> None:
    db = relational.Database(
        "Server=db;Initial Catalog=crm;User ID=u;Password=p;Connect Timeout=5"
    )

    assert db.connection is not None
    assert db.engine.url.drivername == "mssql+mssqlpython"
    assert db.dialect.name == "mssql"
    assert isinstance(db.engine, sa.Engine)
