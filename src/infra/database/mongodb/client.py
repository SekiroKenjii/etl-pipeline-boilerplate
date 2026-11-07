"""A MongoDB database, through a client shared by the adapters."""

from __future__ import annotations

from collections.abc import Generator
import contextlib
import datetime
import decimal
from typing import Any

from bson import binary
from bson import codec_options
from bson import decimal128
from bson import objectid
import pymongo
from pymongo import client_session
from pymongo import errors as mongo_errors

from core import contract
from shared import utils

type Document = dict[str, Any]

# A watermark on `_id` survives the state store.
utils.register_tag("objectid", objectid.ObjectId, str, objectid.ObjectId)


class _DecimalCodec(codec_options.TypeCodec):
    """Python decimals as BSON Decimal128, both ways: no float rounding."""

    @property
    def python_type(self) -> Any:
        return decimal.Decimal

    @property
    def bson_type(self) -> Any:
        return decimal128.Decimal128

    def transform_python(self, value: Any) -> Any:
        return decimal128.Decimal128(value)

    def transform_bson(self, value: Any) -> Any:
        return value.to_decimal()


class _DateCodec(codec_options.TypeEncoder):
    """Dates (BSON has none) as midnight UTC."""

    @property
    def python_type(self) -> Any:
        return datetime.date

    def transform_python(self, value: Any) -> Any:
        return datetime.datetime.combine(value, datetime.time(), datetime.UTC)


CODEC_OPTIONS: codec_options.CodecOptions[Document] = (
    codec_options.CodecOptions(
        document_class=dict,
        tz_aware=True,
        tzinfo=datetime.UTC,
        uuid_representation=binary.UuidRepresentation.STANDARD,
        type_registry=codec_options.TypeRegistry(
            [_DecimalCodec(), _DateCodec()]
        ),
    )
)


class MongoDatabase:
    """A MongoDB database, and the client (connection pool) it goes through.

    Adapters given the same `MongoDatabase` share its client; whoever created
    the database closes it (`close`) once every adapter is done. A client passed
    in belongs to the caller.

        MongoDatabase("mongodb://etl@mongo1,mongo2/crm?replicaSet=rs0")

    Values go through `CODEC_OPTIONS`: decimals as Decimal128, dates as midnight
    UTC, aware datetimes, standard UUIDs.

    Attributes:
        client: The MongoDB client.
        database: The database.
    """

    def __init__(
        self,
        client: pymongo.MongoClient[Document] | str,
        database: str | None = None,
        **options: Any,
    ) -> None:
        """Initializes the database; nothing connects before the first use.

        Args:
            client: A client, or a MongoDB URI (`mongodb://`,
                `mongodb+srv://`) to create one: .NET, Java and Python
                applications share that format.
            database: The database; the one of the URI by default.
            **options: Options of `pymongo.MongoClient`, with a URI.

        Raises:
            FatalError: If no database is given, nor in the URI.
        """
        self._owned = isinstance(client, str)
        self.client: pymongo.MongoClient[Document] = (
            pymongo.MongoClient(client, connect=False, **options)
            if isinstance(client, str)
            else client
        )
        try:
            default = self.client.get_default_database(
                codec_options=CODEC_OPTIONS
            )
        except mongo_errors.ConfigurationError:
            default = None
        if database is not None:
            self.database = self.client.get_database(
                database, codec_options=CODEC_OPTIONS
            )
        elif default is not None:
            self.database = default
        else:
            raise contract.FatalError(
                "No MongoDB database: give one, or put it in the URI"
            )
        self._transactions: bool | None = None

    @property
    def name(self) -> str:
        return self.database.name

    def close(self) -> None:
        """Closes a client created here; one passed in is the caller's."""
        if self._owned:
            self.client.close()

    def collection(self, name: str) -> Any:
        """Returns a collection of the database."""
        return self.database[name]

    @property
    def supports_transactions(self) -> bool:
        """Whether the server is a replica set or a sharded cluster.

        Raises:
            RetryableError: If the server cannot be reached.
        """
        if self._transactions is None:
            with errors("connecting to MongoDB"):
                hello = self.client.admin.command("hello")
            self._transactions = bool(
                hello.get("setName") or hello.get("msg") == "isdbgrid"
            )
        return self._transactions

    @contextlib.contextmanager
    def transaction(
        self, action: str, enabled: bool = True
    ) -> Generator[client_session.ClientSession | None]:
        """Yields a session in a transaction, committed on success.

        Without transactions (a standalone server, or `enabled` False), yields
        None: each write stands on its own.

        Args:
            action: What is being done, for the error messages.
            enabled: Whether to use a transaction where the server can.
        """
        with errors(action):
            if not (enabled and self.supports_transactions):
                yield None
                return
            with (
                self.client.start_session() as session,
                session.start_transaction(),
            ):
                yield session


@contextlib.contextmanager
def errors(action: str) -> Generator[None]:
    """Translates MongoDB errors into the exceptions of the ETL core.

    Lost connections, timeouts, elections and transient transaction errors
    are retryable; any other error (a duplicate key, a validation) is fatal.

    Args:
        action: What was being done, for the message.

    Raises:
        RetryableError: For a transient failure.
        FatalError: For any other MongoDB error.
    """
    try:
        yield
    except (
        mongo_errors.ConnectionFailure,
        mongo_errors.WTimeoutError,
        mongo_errors.ExecutionTimeout,
    ) as exc:
        raise contract.RetryableError(f"{action}: {exc}") from exc
    except mongo_errors.PyMongoError as exc:
        if exc.has_error_label("TransientTransactionError"):
            raise contract.RetryableError(f"{action}: {exc}") from exc
        raise contract.FatalError(f"{action}: {exc}") from exc


def is_duplicate_key(exc: BaseException | None) -> bool:
    """Whether an error is a duplicate key (E11000), even inside a bulk."""
    if isinstance(exc, mongo_errors.DuplicateKeyError):
        return True
    if isinstance(exc, mongo_errors.BulkWriteError):
        return any(
            error.get("code") == 11000
            for error in exc.details.get("writeErrors", [])
        )
    return False


class MongoAdapter:
    """The base of the adapters, which may share their `MongoDatabase`.

    Attributes:
        db: The database of the adapter.
    """

    def __init__(self, db: MongoDatabase | str) -> None:
        """Initializes the adapter.

        Args:
            db: A shared database, or a MongoDB URI for one of its own.
        """
        self.db = MongoDatabase(db) if isinstance(db, str) else db
