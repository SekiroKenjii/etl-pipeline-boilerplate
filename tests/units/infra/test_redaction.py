"""Secrets and row values kept out of every log sink, tracebacks included."""

from __future__ import annotations

from collections.abc import Generator
import io
import json
import logging
import time
from typing import Any

from opentelemetry.sdk import _logs as otel_logs
from opentelemetry.sdk._logs import export as otel_export
import pytest
import sqlalchemy as sa

from core import observability
from core.observability import redaction
from infra import observability as infra_observability
from infra.observability import otlp

_LOG = logging.getLogger("tests.redaction")

# Row values and a password: none of them may reach a sink. Kept out of the
# lines that raise, which tracebacks quote.
_EMAIL = "jane.doe@bank.example"
_SSN = "123-45-6789"
_PASSWORD = "S3cr3tPw"
_ROWS = [
    {"id": 1, "email": _EMAIL, "ssn": _SSN},
    {"id": 1, "email": "john.roe@bank.example", "ssn": "987-65-4321"},
]
_LEAKS = (_EMAIL, _SSN, "john.roe", "987-65-4321", _PASSWORD)


class TestText:

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            # URLs.
            (
                "postgresql+psycopg://etl:S3cr3t!@db/crm",
                "postgresql+psycopg://etl:***@db/crm",
            ),
            (
                "mongodb+srv://etl:S3cr3t@c0.x.mongodb.net/crm",
                "mongodb+srv://etl:***@c0.x.mongodb.net/crm",
            ),
            ("?password=S3cr3t&user=etl", "?password=***&user=etl"),
            ("https://x/?sv=2020&sig=abc%3D", "https://x/?sv=2020&sig=***"),
            # Connection strings: ODBC, ADO, libpq, JDBC, Azure Storage.
            (
                "Server=db;User ID=etl;Password=S3cr3t;Encrypt=yes",
                "Server=db;User ID=etl;Password=***;Encrypt=yes",
            ),
            (
                "Server=db;UID=etl;PWD=S3cr3t;Encrypt=yes",
                "Server=db;UID=etl;PWD=***;Encrypt=yes",
            ),
            ("UID=etl;PWD={S3c;r3t};X=1", "UID=etl;PWD={***};X=1"),
            (
                "host=db user=etl password='my secret' dbname=crm",
                "host=db user=etl password='***' dbname=crm",
            ),
            (
                "jdbc:sqlserver://db;user=etl;password=S3cr3t",
                "jdbc:sqlserver://db;user=etl;password=***",
            ),
            (
                "AccountKey=S3cr3t==;EndpointSuffix=core.windows.net",
                "AccountKey=***;EndpointSuffix=core.windows.net",
            ),
            (
                "SharedAccessSignature=sv=2020&sig=abc%3D",
                "SharedAccessSignature=***&sig=***",
            ),
            # Names holding a secret name, `_`, `-` and `.` included.
            ("passwd=S3cr3t", "passwd=***"),
            ("PASSWORD=S3cr3t", "PASSWORD=***"),
            ("password: S3cr3t with space", "password: *** with space"),
            ("client_secret=S3cr3t", "client_secret=***"),
            ("secret_key=S3cr3t", "secret_key=***"),
            ("KEY_SECRET=S3cr3t", "KEY_SECRET=***"),
            ("key_secret: S3cr3t", "key_secret: ***"),
            ("sasl.password=S3cr3t", "sasl.password=***"),
            ("ssl_key_password=S3cr3t", "ssl_key_password=***"),
            ("private_key=-----BEGIN", "private_key=***"),
            ("credential=S3cr3t", "credential=***"),
            ("apikey=AKIAX", "apikey=***"),
            ("api-key=AKIAX", "api-key=***"),
            ("x-api-key: AKIAX", "x-api-key: ***"),
            ("X-Vault-Token: hvs.CAESIJ", "X-Vault-Token: ***"),
            ('{"password": "S3cr3t", "n": 1}', '{"password": "***", "n": 1}'),
            # Headers and tokens.
            ("Authorization: Bearer eyJ.a.b", "Authorization: Bearer ***"),
            ("-H 'Authorization: Basic ZXRs'", "-H 'Authorization: Basic ***'"),
            ("sent Bearer eyJ.a.b to x", "sent Bearer *** to x"),
            (
                (
                    "k=-----BEGIN RSA PRIVATE KEY-----\nMIIE\n"
                    "-----END RSA PRIVATE KEY----- end"
                ),
                "k=*** end",
            ),
            # The values database errors quote.
            (
                "INSERT ...]\n[parameters: [(1, 'jane@x.example')]]\n(Back",
                "INSERT ...]\n[parameters: ***]\n(Back",
            ),
            (
                "DETAIL:  Key (email)=(jane@x.example) already exists.",
                "DETAIL:  Key (email)=(***) already exists.",
            ),
            (
                "DETAIL:  Key (id, ssn)=(1, 123-45-6789) is not present",
                "DETAIL:  Key (id, ssn)=(***) is not present",
            ),
            (
                "DETAIL:  Failing row contains (1, null, 123-45-6789).",
                "DETAIL:  Failing row contains (***).",
            ),
            (
                "The duplicate key value is (1, jane).",
                "The duplicate key value is (***).",
            ),
            (
                "Duplicate entry 'jane@x' for key 'email'",
                "Duplicate entry '***' for key 'email'",
            ),
            (
                "Incorrect integer value: 'jane' for column 'age' at row 1",
                "Incorrect integer value: '***' for column 'age' at row 1",
            ),
            (
                "Incorrect datetime value: 'it's 5'",
                "Incorrect datetime value: '***'",
            ),
            (
                (
                    "would be truncated in table 'c', column 'email'."
                    " Truncated value: 'jane@exa'."
                ),
                (
                    "would be truncated in table 'c', column 'email'."
                    " Truncated value: '***'"
                ),
            ),
            (
                "Cannot convert string 'jane' to type int",
                "Cannot convert string '***'",
            ),
            (
                'invalid input syntax for type integer: "jane"\nLINE 1: ...',
                'invalid input syntax for type integer: "***"\nLINE 1: ...',
            ),
            (
                'CONTEXT:  COPY etl_stage, line 1, column email: "jane@x"',
                'CONTEXT:  COPY etl_stage, line 1, column email: "***"',
            ),
            (
                'CONTEXT:  COPY etl_stage, line 2: "7,jane@x,555"',
                'CONTEXT:  COPY etl_stage, line 2: "***"',
            ),
        ],
    )
    def test_secrets_are_hidden(self, text: str, expected: str) -> None:
        assert observability.redact_text(text) == expected

    @pytest.mark.parametrize(
        "text",
        [
            "nothing secret here",
            "loaded 5 rows into crm.customer",
            "the token is missing",
            "Basic auth failed",
            "signal=9 design: done",
            "key=customer_id keyspace=crm etl.keys=k1",
            "author: jane status: ok",
            "https://db.example:5432/crm",
            "Key (id) is missing",
            "parameters: none",
        ],
    )
    def test_the_rest_is_left_alone(self, text: str) -> None:
        assert observability.redact_text(text) == text

    def test_configured_names_add_to_the_built_in_ones(self) -> None:
        assert observability.redact_text("ssn: 1 pwd=x", ("SSN",)) == (
            "ssn: *** pwd=***"
        )

    def test_keys_of_nested_data(self) -> None:
        value = {
            "PWD": "x",
            "client_secret": "y",
            "sig": "z",
            "signal": 9,
            "Authorization": "Bearer q",
            "etl.keys": ["k1"],
            "url": "postgresql://etl:S3cr3t@db/crm",
        }

        assert observability.redact_value(value) == {
            "PWD": "***",
            "client_secret": "***",
            "sig": "***",
            "signal": 9,
            "Authorization": "***",
            "etl.keys": ["k1"],
            "url": "postgresql://etl:***@db/crm",
        }


def _insert_duplicates() -> None:
    """Raises SQLAlchemy's IntegrityError, the rows in its parameters."""
    engine = sa.create_engine("sqlite://", poolclass=sa.pool.StaticPool)
    rows = _ROWS
    try:
        with engine.begin() as connection:
            connection.execute(
                sa.text("CREATE TABLE c (id INTEGER PRIMARY KEY, email, ssn)")
            )
            connection.execute(
                sa.text("INSERT INTO c VALUES (:id, :email, :ssn)"), rows
            )
    finally:
        engine.dispose()


def _connect() -> None:
    """Raises an error quoting a URL, chained to one quoting a password."""
    url = f"postgresql://etl:{_PASSWORD}@db/crm"
    try:
        raise ConnectionError(f"Password={_PASSWORD}; refused")
    except ConnectionError as exc:
        raise RuntimeError(f"cannot connect to {url}") from exc


def _log_failure(fail: Any) -> None:
    try:
        fail()
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.warning("step failed: %s", exc, exc_info=exc)


_FAILURES = pytest.mark.parametrize(
    "fail", [_insert_duplicates, _connect], ids=["dbapi", "url"]
)


def _configured(format: str) -> tuple[io.StringIO, infra_observability.Logging]:  # pylint: disable=redefined-builtin
    buffer = io.StringIO()
    return buffer, infra_observability.configure_logging(
        service="s", environment="test", format=format, stream=buffer
    )


class _Recorder(logging.Handler):
    """Keeps the records it handles."""

    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


class TestTracebacks:

    @_FAILURES
    def test_json(self, fail: Any) -> None:
        buffer, configured = _configured("json")
        _log_failure(fail)
        configured.stop()

        (line,) = [
            json.loads(text)
            for text in buffer.getvalue().splitlines()
            if _LOG.name in text
        ]
        assert "Traceback" in line["error.stack_trace"]
        assert line["error.type"] in ("IntegrityError", "RuntimeError")
        assert not [leak for leak in _LEAKS if leak in json.dumps(line)]

    @_FAILURES
    def test_console(self, fail: Any) -> None:
        buffer, configured = _configured("console")
        _log_failure(fail)
        configured.stop()

        output = buffer.getvalue()
        assert "Traceback" in output
        assert not [leak for leak in _LEAKS if leak in output]

    @_FAILURES
    def test_otlp(self, fail: Any) -> None:
        # The OpenTelemetry SDK leaves this constructor untyped.
        exporter: Any = otel_export.InMemoryLogRecordExporter()  # type: ignore[no-untyped-call]
        provider = otel_logs.LoggerProvider()
        provider.add_log_record_processor(
            otel_export.SimpleLogRecordProcessor(exporter)
        )
        handler = otlp.OtlpHandler(provider)
        handler.addFilter(observability.RedactingFilter(()))
        _LOG.addHandler(handler)
        try:
            _log_failure(fail)
        finally:
            _LOG.removeHandler(handler)
            handler.close()

        (exported,) = exporter.get_finished_logs()
        attributes = dict(exported.log_record.attributes or {})
        assert "Traceback" in str(attributes["exception.stacktrace"])
        assert str(attributes["exception.type"]).endswith("Error")
        shown = str(attributes) + str(exported.log_record.body)
        assert not [leak for leak in _LEAKS if leak in shown]

    @_FAILURES
    def test_any_handler_prints_the_redacted_traceback(self, fail: Any) -> None:
        handler = _Recorder()
        records = handler.records
        handler.addFilter(observability.RedactingFilter(()))
        _LOG.addHandler(handler)
        try:
            _log_failure(fail)
        finally:
            _LOG.removeHandler(handler)

        (record,) = records
        output = logging.Formatter().format(record)
        assert record.exc_info is None
        assert "Traceback" in output
        assert not [leak for leak in _LEAKS if leak in output]


@pytest.fixture
def unconfigured() -> Generator[list[logging.LogRecord]]:
    """Records of a handler without the redacting filter."""
    handler = _Recorder()
    records = handler.records
    _LOG.addHandler(handler)
    try:
        yield records
    finally:
        _LOG.removeHandler(handler)


def test_formatters_redact_built_in_names_without_the_filter(
    unconfigured: list[logging.LogRecord],
) -> None:
    _log_failure(_connect)

    (record,) = unconfigured
    line = json.loads(
        infra_observability.JsonFormatter("s", "e").format(record)
    )
    error = line["error.message"] + line["error.stack_trace"]
    assert "Traceback" in error
    assert _PASSWORD not in error


@pytest.mark.parametrize(
    "text",
    [
        "x" * 200_000,
        "a://" * 50_000,
        "password" * 25_000,
        "key (" * 40_000,
        "key (a)=(" * 22_000,
        "postgresql://u:" * 14_000,
        "Failing row contains (" * 10_000,
        "The duplicate key value is (" * 8_000,
        "Duplicate entry '" * 12_000,
        "Incorrect a value: '" * 10_000,
        "COPY t, line 1, column " * 8_000,
        "invalid input syntax for type " * 6_000,
    ],
    ids=lambda text: text[:12],
)
def test_long_lines_take_linear_time(text: str) -> None:
    # At this size, each took from 10 s to minutes while a repetition was
    # unbounded, and takes well under a second now. The best of three
    # leaves out the moments a busy machine lends the test no CPU.
    def seconds() -> float:
        started = time.perf_counter()
        redaction.redact_text(text)
        return time.perf_counter() - started

    assert min(seconds() for _ in range(3)) < 3
