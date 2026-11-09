"""How MongoDB errors are told: their codes and key patterns, never a value
of the operation."""

from __future__ import annotations

from pymongo import errors as mongo_errors
import pytest

from core import contract
from infra.database.mongodb import client

EMAIL = "jane.doe@bank.example"
SSN = "123-45-6789"
_DUPLICATE = (
    "E11000 duplicate key error collection: etl.customers index: email_1"
    f' dup key: {{ email: "{EMAIL}" }}'
)


def _write_error(index: int) -> dict[str, object]:
    return {
        "index": index,
        "code": 11000,
        "errmsg": _DUPLICATE,
        "keyPattern": {"email": 1},
        "keyValue": {"email": EMAIL},
        "op": {"_id": index, "email": EMAIL, "ssn": SSN},
    }


def _caught(exc: Exception) -> contract.PipelineError:
    with pytest.raises(contract.PipelineError) as caught:
        with client.errors("writing 2 row(s) to customers"):
            raise exc
    return caught.value


def _clean(error: BaseException) -> str:
    message = str(error)
    assert EMAIL not in message, message
    assert SSN not in message, message
    assert len(message) < 300, message
    return message


def test_a_bulk_write_error_tells_the_count_code_and_key() -> None:
    error = _caught(
        mongo_errors.BulkWriteError({
            "writeErrors": [_write_error(1), _write_error(3)],
            "writeConcernErrors": [],
            "nInserted": 1,
        })
    )

    message = _clean(error)
    assert isinstance(error, contract.FatalError)
    assert message.startswith("writing 2 row(s) to customers: ")
    assert "2 write error(s)" in message
    assert "11000" in message
    assert "index: email_1" in message
    assert "{email: 1}" in message
    assert "ssn" not in message


def test_the_cause_prints_no_value_either_and_keeps_its_details() -> None:
    error = _caught(
        mongo_errors.BulkWriteError({
            "writeErrors": [_write_error(0)],
            "writeConcernErrors": [],
        })
    )

    cause = error.__cause__
    assert isinstance(cause, mongo_errors.BulkWriteError)
    _clean(cause)
    assert client.is_duplicate_key(cause)
    assert cause.details["writeErrors"][0]["op"]["email"] == EMAIL


def test_a_duplicate_key_error() -> None:
    error = _caught(
        mongo_errors.DuplicateKeyError(
            _DUPLICATE,
            11000,
            {
                "index": 0,
                "code": 11000,
                "errmsg": _DUPLICATE,
                "keyPattern": {"email": 1},
                "keyValue": {"email": EMAIL},
            },
        )
    )

    message = _clean(error)
    assert "11000" in message
    assert "{email: 1}" in message
    assert client.is_duplicate_key(error.__cause__)


def test_a_validation_failure_keeps_its_err_info_out() -> None:
    error = _caught(
        mongo_errors.WriteError(
            "Document failed validation",
            121,
            {
                "index": 0,
                "code": 121,
                "codeName": "DocumentValidationFailure",
                "errmsg": "Document failed validation",
                "errInfo": {
                    "failingDocumentId": 1,
                    "details": {"email": EMAIL, "ssn": SSN},
                },
                "op": {"_id": 1, "email": EMAIL, "ssn": SSN},
            },
        )
    )

    message = _clean(error)
    assert "Document failed validation" in message
    assert "121" in message
    assert "DocumentValidationFailure" in message


def test_a_document_quoted_in_the_message_is_cut() -> None:
    error = _caught(
        mongo_errors.WriteError(
            "Cannot apply $inc to a value of non-numeric type. {_id: 1,"
            f" email: \"{EMAIL}\"}} has the field 'ssn' of non-numeric type"
            " string",
            14,
            {"index": 0, "code": 14, "codeName": "TypeMismatch"},
        )
    )

    message = _clean(error)
    assert "Cannot apply $inc" in message
    assert "{...}" in message


def test_a_write_concern_timeout_is_retryable_and_clean() -> None:
    error = _caught(
        mongo_errors.WTimeoutError(
            "waiting for replication timed out",
            64,
            {
                "code": 64,
                "codeName": "WriteConcernTimeout",
                "errmsg": "waiting for replication timed out",
                "errInfo": {"wtimeout": True, "writeConcern": {"w": 2}},
            },
        )
    )

    message = _clean(error)
    assert isinstance(error, contract.RetryableError)
    assert "waiting for replication timed out" in message
    assert "WriteConcernTimeout" in message


def test_a_long_message_is_cut_short() -> None:
    error = _caught(
        mongo_errors.OperationFailure("x" * 1000, 2, {"code": 2, "errmsg": "x"})
    )

    _clean(error)


def test_errors_without_details_are_told_as_they_are() -> None:
    plain = _caught(mongo_errors.OperationFailure("not authorized on etl", 13))
    lost = _caught(mongo_errors.ConnectionFailure("connection closed"))

    assert str(plain) == (
        "writing 2 row(s) to customers: not authorized on etl (code 13)"
    )
    assert isinstance(lost, contract.RetryableError)
    assert str(lost) == "writing 2 row(s) to customers: connection closed"
