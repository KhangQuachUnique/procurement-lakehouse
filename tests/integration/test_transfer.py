import json
import os
import uuid
import zipfile

import httpx
import pytest

from procurement import transfer
from procurement.common.catalog import get_resource
from procurement.common.settings import settings
from procurement.ingestion.sources.muasamcong.client import MuasamcongClient
from procurement.jobs import ingest, runner
from procurement.ops.repositories.control import ControlRepository
from procurement.transfer import export_bundle, import_bundle, inspect_bundle
from procurement.transfer.archive import CHUNK_SIZE, copy_digest

pytestmark = pytest.mark.integration


def test_transfer_real_dlt_between_independent_buckets(store, tmp_path, monkeypatch):
    fs, source_bucket = store
    monkeypatch.setattr(settings, "MUASAMCONG_TOKEN", "test-only")
    monkeypatch.setattr(settings, "INGESTION_LOCK_DIR", str(tmp_path / "locks"))
    monkeypatch.setattr(runner, "create_s3_filesystem", lambda: fs)

    def handler(request):
        if isinstance(json.loads(request.content), list):
            return httpx.Response(200, json={"page": {
                "content": [{"id": "project-one"}], "totalElements": 1,
                "totalPages": 1, "number": 0, "size": 50,
            }})
        return httpx.Response(200, json={"id": "project-one", "projectDTO": {"version": "01"}})

    monkeypatch.setattr(runner, "MuasamcongClient",
                        lambda **kwargs: MuasamcongClient(
                            **kwargs, transport=httpx.MockTransport(handler)))
    args = ingest._parser().parse_args([
        "backfill", "--resource", "project", "--start-date", "2024-02-29",
        "--end-date", "2024-02-29",
    ])
    _, code = ingest.execute_flow(args, fs=fs)
    assert code == 0
    archive = tmp_path / "year.zip"
    # Exercise ZIP64 headers/end records through the full workflow without a huge DLT load.
    monkeypatch.setattr(zipfile, "ZIP64_LIMIT", 1024)
    exported = export_bundle(fs, year=2024, resource="project", output=archive)
    assert inspect_bundle(archive)["verified"]["records"] == 1
    is_local = "file" in fs.protocol
    target = ((tmp_path / "target-bucket").as_posix() if is_local
              else f"procurement-integration-{uuid.uuid4().hex}")
    assert target != source_bucket
    fs.mkdir(target)
    monkeypatch.setattr(settings, "OBJECT_STORAGE_BUCKET", target)
    try:
        real_copy = transfer.copy_digest

        def interrupt_upload(source, writer=None):
            if writer is not None:
                writer.write(source.read(8))
                raise KeyboardInterrupt
            return real_copy(source)

        with monkeypatch.context() as temporary_patch:
            temporary_patch.setattr(transfer, "copy_digest", interrupt_upload)
            with pytest.raises(KeyboardInterrupt):
                import_bundle(fs, archive)
        assert not fs.find(target)  # A short Parquet must not prevent the next import.
        imported = import_bundle(fs, archive)
        assert imported["destination_verified"] == exported["verified"]
        identity = get_resource("project").identity
        repository = ControlRepository(fs)
        assert len(repository.list_runs(identity)) == 1
        attempts = repository.list_attempts(identity)
        assert len(attempts) == 1
        assert attempts[0].status.value == "success"
        assert len(repository.list_pages(identity, run_id=attempts[0].run_id,
                                         source_date=attempts[0].source_date)) == 1
        args.mode = "verify"
        verified, code = ingest.execute_flow(args, fs=fs)
        assert code == 0
        assert verified["resources"][0]["verified"]["records"] == 1
        before = fs.find(target, detail=True)
        assert len(import_bundle(fs, archive)["skipped_days"]) == 1
        assert fs.find(target, detail=True) == before
    finally:
        # Only the disposable bucket created above is removed; fixture owns source cleanup.
        fs.rm(target, recursive=True)


@pytest.mark.skipif(os.getenv("RUN_ZIP64_LARGE_TEST") != "1",
                    reason="Set RUN_ZIP64_LARGE_TEST=1 to stream an actual >4 GiB ZIP member")
def test_zip64_actual_member_above_four_gib(tmp_path):
    class RepeatedReader:
        def __init__(self):
            self.remaining = 4 * 1024**3 + CHUNK_SIZE
            self.chunk = b"Z" * CHUNK_SIZE

        def read(self, size):
            size = min(size, self.remaining)
            self.remaining -= size
            return self.chunk[:size]

    path = tmp_path / "large.zip"
    with (
        zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED,
                        compresslevel=1, allowZip64=True) as archive,
        archive.open("large-object", "w", force_zip64=True) as target,
    ):
        expected = copy_digest(RepeatedReader(), target)
    with zipfile.ZipFile(path) as archive:
        assert archive.getinfo("large-object").file_size > 4 * 1024**3
        with archive.open("large-object") as source:
            assert copy_digest(source) == expected
