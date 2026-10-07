"""Real PostgreSQL tests create/drop only a uniquely named disposable database."""

import os
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url


@pytest.fixture(scope="module")
def database():
    value = os.environ.get("TEST_METADATA_DATABASE_URL")
    if not value:
        pytest.skip("Set TEST_METADATA_DATABASE_URL to the isolated sandbox PostgreSQL")
    url = make_url(value)
    if url.host not in {"app-postgres", "127.0.0.1", "localhost"} or url.database != "procurement":
        pytest.fail("Metadata tests require the sandbox app-postgres/procurement database")
    if url.host != "app-postgres" and url.port != 25432:
        pytest.fail("Host tests require sandbox port 25432")
    url = url.set(drivername="postgresql+psycopg2")
    name = "metadata_test_" + uuid4().hex
    admin = create_engine(url, isolation_level="AUTOCOMMIT", hide_parameters=True)
    engine = None
    try:
        with admin.connect() as conn:
            conn.execute(text(f'CREATE DATABASE "{name}"'))
        engine = create_engine(url.set(database=name), hide_parameters=True)
        config_path = Path(os.environ.get("TEST_ALEMBIC_CONFIG", "alembic.ini")).resolve()
        config = Config(str(config_path))
        with engine.connect() as conn:
            config.attributes["connection"] = conn
            command.upgrade(config, "head")
        config.attributes.pop("connection", None)
        yield engine, config
    finally:
        if engine is not None:
            engine.dispose()
        with admin.connect() as conn:
            conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        admin.dispose()


@pytest.fixture
def connection(database):
    engine, _ = database
    with engine.connect() as conn:
        transaction = conn.begin()
        try:
            yield conn
        finally:
            transaction.rollback()
