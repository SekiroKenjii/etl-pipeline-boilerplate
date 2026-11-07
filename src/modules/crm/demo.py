"""Demo data: a legacy database with dirty rows, and an empty CRM database.

    python -m modules.crm.demo --env dev    # then: etl run crm.full_load

The legacy rows hold what migrations meet: a date that does not exist, a
country no lookup knows, an order whose customer is missing.
"""

from __future__ import annotations

import argparse
import datetime
import pathlib
import sys

import sqlalchemy as sa

import config
from modules.crm import settings as crm_settings

NOW = datetime.datetime(2026, 10, 1, 8, 0)

CUSTOMERS = [
    # CUST_NO, FIRST_NM, LAST_NM, STAT_CD, CRT_DT, CTRY, LEGACY_FLAG
    ("1001", "Anna", "Müller", "A", "15/01/2020", "VIETNAM", "x"),
    ("1002", "Bruno", "Costa", "I", "02/03/2021", "VN", None),
    ("1003", "Chloé", "Martin", "A", "31/02/2020", "VN", None),  # No such date.
    ("1004", "David", "Novák", "Z", "10/10/2022", "Narnia", None),  # No lookup.
]
ORDERS = [
    # ORD_NO, CUST_NO, AMT, ORD_DT
    ("O-1", "1001", "120.50", "01/02/2024"),
    ("O-2", "1002", "75", "03/02/2024"),
    ("O-3", "1009", "10", "04/02/2024"),  # Its customer is missing.
]


def legacy_tables(metadata: sa.MetaData) -> tuple[sa.Table, sa.Table]:
    """The tables of the legacy database."""
    customers = sa.Table(
        "CUST_MASTER",
        metadata,
        sa.Column("CUST_NO", sa.Unicode(10), primary_key=True),
        sa.Column("FIRST_NM", sa.Unicode(50)),
        sa.Column("LAST_NM", sa.Unicode(50)),
        sa.Column("STAT_CD", sa.Unicode(1)),
        sa.Column("CRT_DT", sa.Unicode(10)),
        sa.Column("CTRY", sa.Unicode(20)),
        sa.Column("LEGACY_FLAG", sa.Unicode(1)),
        sa.Column("UPDATED_AT", sa.DateTime, nullable=False),
    )
    orders = sa.Table(
        "ORD_MASTER",
        metadata,
        sa.Column("ORD_NO", sa.Unicode(10), primary_key=True),
        sa.Column("CUST_NO", sa.Unicode(10)),
        sa.Column("AMT", sa.Unicode(20)),
        sa.Column("ORD_DT", sa.Unicode(10)),
        sa.Column("UPDATED_AT", sa.DateTime, nullable=False),
    )
    return customers, orders


def crm_tables(metadata: sa.MetaData) -> tuple[sa.Table, sa.Table]:
    """The tables of the CRM database: what the mappings write."""
    key = sa.Uuid().with_variant(sa.Unicode(36), "oracle", "sqlite", "mysql")
    customers = sa.Table(
        "customer",
        metadata,
        sa.Column("customer_key", key, primary_key=True),
        sa.Column("customer_id", sa.Integer, nullable=False),
        sa.Column("full_name", sa.Unicode(200), nullable=False),
        sa.Column("status", sa.Unicode(20)),
        sa.Column("created_at", sa.Date),
        sa.Column("country", sa.Unicode(2)),
        sa.Column(
            "is_deleted", sa.Boolean, nullable=False, server_default=sa.false()
        ),
    )
    orders = sa.Table(
        "order",
        metadata,
        sa.Column("order_key", key, primary_key=True),
        sa.Column(
            "customer_key",
            key,
            sa.ForeignKey(customers.c.customer_key),
            nullable=False,
        ),
        sa.Column("total", sa.Numeric(12, 2), nullable=False),
        sa.Column("ordered_at", sa.Date),
        sa.Column(
            "is_deleted", sa.Boolean, nullable=False, server_default=sa.false()
        ),
    )
    return customers, orders


def seed(legacy: sa.Engine, crm: sa.Engine) -> None:
    """Creates both databases' tables, and fills the legacy one."""
    legacy_metadata, crm_metadata = sa.MetaData(), sa.MetaData()
    customers, orders = legacy_tables(legacy_metadata)
    crm_tables(crm_metadata)
    # Drops every CRM table, the ETL's records too; legacy may be the same db.
    existing = sa.MetaData()
    existing.reflect(crm)
    existing.drop_all(crm)
    crm_metadata.create_all(crm)
    legacy_metadata.drop_all(legacy)
    legacy_metadata.create_all(legacy)
    names = [c.name for c in customers.c][:-1]
    with legacy.begin() as connection:
        connection.execute(
            customers.insert(),
            [{**dict(zip(names, row)), "UPDATED_AT": NOW} for row in CUSTOMERS],
        )
        connection.execute(
            orders.insert(),
            [
                {
                    "ORD_NO": number,
                    "CUST_NO": customer,
                    "AMT": amount,
                    "ORD_DT": day,
                    "UPDATED_AT": NOW,
                }
                for number, customer, amount, day in ORDERS
            ],
        )


def main(argv: list[str] | None = None) -> int:
    """Seeds the legacy and CRM databases of an environment's settings."""
    parser = argparse.ArgumentParser(description="Seeds the CRM demo.")
    parser.add_argument("--env", default="dev")
    args = parser.parse_args(argv)
    settings = config.load_settings(args.env)
    crm = settings.module_settings("crm", crm_settings.CrmSettings)
    engines = []
    for name in (crm.source, crm.target):
        url = sa.make_url(settings.databases[name].url.get_secret_value())
        if url.get_backend_name() == "sqlite" and url.database:
            pathlib.Path(url.database).parent.mkdir(parents=True, exist_ok=True)
        engines.append(sa.create_engine(url))
    seed(engines[0], engines[1])
    for engine in engines:
        engine.dispose()
    print(f"Demo databases ready for env {args.env!r}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
