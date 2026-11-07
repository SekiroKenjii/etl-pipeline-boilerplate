"""Rejected records and issues erased and purged, the same way on every
database: the quarantine refreshes a record rejected again, the issue sink
erases the issues of a record and purges the old ones."""

from __future__ import annotations

from collections.abc import Generator
import os
import uuid

import pytest

from infra.database import mongodb
from infra.database import relational
from tests.factories import issue_sinks
from tests.factories import quarantines
from tests.integrations.databases import conftest
from tests.integrations.databases import legacy_etl_tables


@pytest.fixture(params=["created", "upgraded from 0.1.0"])
def etl(
    request: pytest.FixtureRequest, backend: conftest.Backend
) -> conftest.Backend:
    """The ETL tables with the prefix of the test: created by this release,
    or created by 0.1.0 then upgraded."""
    if request.param != "created":
        legacy_etl_tables.metadata("0001", prefix=backend.prefix).create_all(
            backend.db.engine
        )
    relational.create_etl_tables(backend.db, prefix=backend.prefix)
    return backend


@pytest.fixture
def mongo() -> Generator[mongodb.MongoDatabase]:
    url = os.environ.get("ETL_TEST_MONGODB_URL")
    if not url:
        pytest.skip("set ETL_TEST_MONGODB_URL to run the MongoDB tests")
    db = mongodb.MongoDatabase(url, database=f"t{uuid.uuid4().hex[:10]}")
    try:
        mongodb.create_etl_collections(db)
        yield db
    finally:
        db.client.drop_database(db.name)
        db.close()


def test_a_relational_quarantine_refreshes_a_record_rejected_again(
    etl: conftest.Backend,
) -> None:
    quarantines.check_rejected_again(
        relational.RelationalQuarantine(etl.db, prefix=etl.prefix)
    )


def test_a_relational_issue_sink_erases_and_purges(
    etl: conftest.Backend,
) -> None:
    table = relational.etl_tables(prefix=etl.prefix).quality_issues

    issue_sinks.check_issue_sink(
        relational.RelationalIssueSink(etl.db, prefix=etl.prefix),
        lambda: [row[0] for row in etl.rows(table, "record_id")],
    )


def test_a_mongodb_quarantine_refreshes_a_record_rejected_again(
    mongo: mongodb.MongoDatabase,
) -> None:
    quarantines.check_rejected_again(mongodb.MongoQuarantine(mongo))


def test_a_mongodb_issue_sink_erases_and_purges(
    mongo: mongodb.MongoDatabase,
) -> None:
    issue_sinks.check_issue_sink(
        mongodb.MongoIssueSink(mongo),
        lambda: [
            found.get("record_id")
            for found in mongo.collection("etl_quality_issues").find()
        ],
    )
