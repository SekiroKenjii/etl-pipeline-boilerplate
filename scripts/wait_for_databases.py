"""Waits until the test databases accept connections: Oracle takes a minute.

    uv run python scripts/wait_for_databases.py [--timeout 300]

Checks the URLs of the `ETL_TEST_*_URL` variables that are set; MongoDB is
ready once its replica set has a primary, Kafka once a broker answers.
"""

from __future__ import annotations

import argparse
import os
import socket
import sys
import time
import urllib.parse
import urllib.request

import confluent_kafka
from confluent_kafka import admin
import pymongo
import sqlalchemy as sa

VARIABLES = (
    "ETL_TEST_POSTGRES_URL",
    "ETL_TEST_MSSQL_URL",
    "ETL_TEST_ORACLE_URL",
    "ETL_TEST_MYSQL_URL",
)
MONGODB = "ETL_TEST_MONGODB_URL"
SEQ = "ETL_TEST_SEQ_URL"
KAFKA = "ETL_TEST_KAFKA_URL"
OTEL = "ETL_TEST_OTEL_URL"
VAULT = "ETL_TEST_VAULT_URL"


def main() -> int:
    """Returns 0 once every database answers, 1 on timeout."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout", type=float, default=300)
    args = parser.parse_args()
    deadline = time.monotonic() + args.timeout

    mongodb_url = os.environ.get(MONGODB)
    if mongodb_url and not _wait_for_mongodb(mongodb_url, deadline):
        return 1
    kafka_servers = os.environ.get(KAFKA)
    if kafka_servers and not _wait_for_kafka(kafka_servers, deadline):
        return 1
    for variable in (OTEL, VAULT):
        url = os.environ.get(variable)
        if url and not _wait_for_port(url, variable, deadline):
            return 1
    seq_url = os.environ.get(SEQ)
    if seq_url and not _wait_for_http(f"{seq_url}/api", SEQ, deadline):
        return 1
    for variable in VARIABLES:
        url = os.environ.get(variable)
        if not url:
            continue
        engine = sa.create_engine(url)
        while True:
            try:
                with engine.connect() as connection:
                    connection.execute(sa.select(1))
                print(f"{variable}: ready")
                break
            except sa.exc.SQLAlchemyError as exc:
                if time.monotonic() > deadline:
                    print(f"{variable}: not ready: {exc}", file=sys.stderr)
                    return 1
                time.sleep(3)
            finally:
                engine.dispose()
    return 0


def _wait_for_http(url: str, variable: str, deadline: float) -> bool:
    """Returns whether `url` answered before `deadline`."""
    if not url.startswith(("http://", "https://")):
        print(f"{variable}: not an HTTP URL", file=sys.stderr)
        return False
    while True:
        try:
            # An HTTP(S) URL, checked above.
            with urllib.request.urlopen(url, timeout=2):  # nosec B310
                print(f"{variable}: ready")
                return True
        except OSError:
            if time.monotonic() > deadline:
                print(f"{variable}: not ready", file=sys.stderr)
                return False
            time.sleep(2)


def _wait_for_port(url: str, variable: str, deadline: float) -> bool:
    """Returns whether the host and port of `url` accepted a connection."""
    parsed = urllib.parse.urlsplit(url)
    while True:
        try:
            with socket.create_connection(
                (parsed.hostname or "localhost", parsed.port or 80), timeout=2
            ):
                print(f"{variable}: ready")
                return True
        except OSError:
            if time.monotonic() > deadline:
                print(f"{variable}: not ready", file=sys.stderr)
                return False
            time.sleep(1)


def _wait_for_kafka(servers: str, deadline: float) -> bool:
    """Returns whether a broker answered before `deadline`."""
    client = admin.AdminClient({"bootstrap.servers": servers})
    while True:
        try:
            client.list_topics(timeout=3)
            print(f"{KAFKA}: ready")
            return True
        except confluent_kafka.KafkaException:
            if time.monotonic() > deadline:
                print(f"{KAFKA}: not ready", file=sys.stderr)
                return False
            time.sleep(2)


def _wait_for_mongodb(url: str, deadline: float) -> bool:
    """Returns whether the replica set elected a primary before `deadline`."""
    client: pymongo.MongoClient[dict[str, object]] = pymongo.MongoClient(
        url, serverSelectionTimeoutMS=2000
    )
    try:
        while True:
            try:
                if client.admin.command("hello").get("isWritablePrimary"):
                    print(f"{MONGODB}: ready")
                    return True
            except pymongo.errors.PyMongoError:
                pass
            if time.monotonic() > deadline:
                print(f"{MONGODB}: not ready", file=sys.stderr)
                return False
            time.sleep(3)
    finally:
        client.close()


if __name__ == "__main__":
    sys.exit(main())
