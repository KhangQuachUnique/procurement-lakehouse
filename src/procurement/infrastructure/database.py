"""Application DB connections. No implicit dotenv load or Dagster DB fallback."""

import os

from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Engine, make_url


def application_database_url(value: str | None = None) -> URL:
    value = value if value is not None else os.environ.get("APP_DATABASE_URL")
    if not value:
        raise ValueError("APP_DATABASE_URL is required for application metadata")
    try:
        url = make_url(value)
    except Exception:  # noqa: BLE001
        raise ValueError("APP_DATABASE_URL must be a PostgreSQL URL") from None
    if url.drivername not in {"postgresql", "postgresql+psycopg2"} or not url.database:
        raise ValueError("APP_DATABASE_URL must name a PostgreSQL database")
    # Avoid relying on SQLAlchemy's default driver selection.
    return url.set(drivername="postgresql+psycopg2")


def create_application_engine(value: str | None = None) -> Engine:
    return create_engine(
        application_database_url(value),
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=5,
        hide_parameters=True,
        connect_args={"connect_timeout": 10},
    )
