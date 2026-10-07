import os
import uuid
from pathlib import Path
from uuid import uuid4

import fsspec
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from procurement.common.settings import settings
from procurement.storage import bronze
from procurement.storage.object_store import create_s3_filesystem


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


@pytest.fixture(params=["local", "s3"])
def store(request, tmp_path, monkeypatch):
    from dlt.common.runtime import run_context

    # Pacing is tested with a deterministic clock; mocked source tests need no wall-clock delays.
    monkeypatch.setattr(settings, "MUASAMCONG_REQUEST_INTERVAL_SECONDS", 0)
    monkeypatch.setattr(settings, "MUASAMCONG_MAX_RETRY_DELAY_SECONDS", 0)

    # Keep config discovery/global state away from the developer's home directory.
    monkeypatch.setattr(run_context, "global_dir", lambda: str(tmp_path / "dlt-global"))
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt-data"))
    monkeypatch.setenv("DLT_PROJECT_DIR", str(tmp_path))
    monkeypatch.setenv("RUNTIME__DLTHUB_TELEMETRY", "false")
    monkeypatch.setenv("RESTORE_FROM_DESTINATION", "false")
    monkeypatch.setattr(settings, "DLT_PIPELINES_DIR", str(tmp_path / "pipelines"))
    if request.param == "local":
        bucket = str(tmp_path / "bucket")
        monkeypatch.setattr(settings, "OBJECT_STORAGE_BUCKET", bucket)
        real_filesystem = bronze.filesystem
        # DLT's local dataset initialization uses mkdir without exist_ok; initialize once
        # through its real client before concurrent pipelines, including its init marker.
        from dlt.common.schema import Schema

        destination = real_filesystem(bucket_url=str(tmp_path / "bucket" / "bronze"))
        config = destination.spec()
        config.dataset_name = "muasamcong"
        with destination.client(Schema("muasamcong"), config) as client:
            client.initialize_storage()

        def local_destination(**kwargs):
            kwargs["bucket_url"] = str(tmp_path / "bucket" / "bronze")
            kwargs.pop("credentials")
            return real_filesystem(**kwargs)

        monkeypatch.setattr(bronze, "filesystem", local_destination)
        yield fsspec.filesystem("file", auto_mkdir=True), bucket
        return

    endpoint = os.getenv("TEST_S3_ENDPOINT")
    if not endpoint:
        pytest.skip("Set TEST_S3_ENDPOINT for an isolated S3-compatible test server")
    # Never use the developer's normal bucket or credentials.
    bucket = f"procurement-integration-{uuid.uuid4().hex}"
    monkeypatch.setattr(settings, "OBJECT_STORAGE_BUCKET", bucket)
    monkeypatch.setattr(settings, "OBJECT_STORAGE_ENDPOINT", endpoint)
    monkeypatch.setattr(settings, "OBJECT_STORAGE_ACCESS_KEY", "integration")
    monkeypatch.setattr(settings, "OBJECT_STORAGE_SECRET_KEY", "integration-test-only")
    fs = create_s3_filesystem()
    fs.mkdir(bucket)
    try:
        yield fs, bucket
    finally:
        fs.rm(bucket, recursive=True)
