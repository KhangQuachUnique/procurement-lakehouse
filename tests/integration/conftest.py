import os
import uuid

import fsspec
import pytest

from procurement.common.settings import settings
from procurement.storage import bronze
from procurement.storage.object_store import create_s3_filesystem


@pytest.fixture(params=["local", "s3"])
def store(request, tmp_path, monkeypatch):
    from dlt.common.runtime import run_context

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
