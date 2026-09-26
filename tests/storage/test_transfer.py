import hashlib
import io
import json
import stat
from datetime import UTC, date, datetime, timedelta
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

import fsspec
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from procurement.common.catalog import get_resource
from procurement.common.settings import settings
from procurement.ingestion.coverage import read_coverage
from procurement.ingestion.engine.metadata import calculate_content_hash
from procurement.jobs.lock import execution_lock
from procurement.models.control import DayManifest, PageManifest, RunManifest
from procurement.storage import transfer
from procurement.storage.committed import select_committed_days, verify_committed
from procurement.storage.control import (
    DayCommitUncertainError,
    read_day_manifest,
    read_run_manifest,
    write_day_manifest,
    write_page_manifest,
    write_run_manifest,
)
from procurement.storage.transfer_archive import BundleIndex, ValidatedArchive


def seed_day(fs, *, resource="project", day=date(2024, 2, 29), run_id="run-a",
             empty=False, status="success", offset=0, record_change=None):
    definition = get_resource(resource)
    started = datetime(2025, 1, 1, tzinfo=UTC) + timedelta(hours=offset)
    completed = started + timedelta(minutes=1)
    count = 0 if empty else len(definition.tables)
    run = RunManifest(
        run_id=run_id, source="muasamcong", resource=resource, start_date=day, end_date=day,
        status=status, total_dates=1, success_dates=int(status == "success"),
        failed_dates=int(status == "failed"), started_at=started,
        completed_at=None if status == "running" else completed,
    )
    manifest = DayManifest(
        run_id=run_id, source="muasamcong", resource=resource, source_date=day,
        status=status, expected_pages=1, completed_pages=1, search_items=int(not empty),
        bronze_records=count, started_at=started, completed_at=run.completed_at,
    )
    page = PageManifest(
        run_id=run_id, source_date=day, page_number=0, page_size=50, status=status,
        search_items=int(not empty), bronze_records=count, started_at=started,
        completed_at=run.completed_at,
    )
    write_run_manifest(fs, definition.identity, run)
    write_day_manifest(fs, definition.identity, manifest)
    write_page_manifest(fs, definition.identity, page)
    if not empty:
        for table in definition.tables:
            payload = {"id": table, "unicode": "Dự án"}
            record = {
                "source_id": table, "source_version": None, "run_id": run_id,
                "source_date": day, "ingested_at": started,
                "content_hash": calculate_content_hash(payload), "payload": json.dumps(payload),
            }
            record.update(record_change or {})
            key = (f"{settings.OBJECT_STORAGE_BUCKET}/bronze/muasamcong/{table}/"
                   f"source_date={day}/run_id={run_id}/part.parquet")
            with fs.open(key, "wb") as target:
                pq.write_table(pa.Table.from_pylist([record]), target)
    return manifest


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    fs = fsspec.filesystem("file", auto_mkdir=True)
    bucket = (tmp_path / "source").as_posix()
    monkeypatch.setattr(settings, "OBJECT_STORAGE_BUCKET", bucket)
    monkeypatch.setattr(settings, "INGESTION_LOCK_DIR", str(tmp_path / "locks"))
    return fs, tmp_path


def export_fixture(fs, root, **seed):
    seed_day(fs, **seed)
    path = root / "year.zip"
    transfer.export_bundle(fs, year=2024, resource=seed.get("resource", "project"), output=path)
    return path


def destination(monkeypatch, root):
    bucket = (root / "target").as_posix()
    monkeypatch.setattr(settings, "OBJECT_STORAGE_BUCKET", bucket)
    return bucket


def rewrite_archive(path, edit):
    with ZipFile(path) as archive:
        contents = {name: archive.read(name) for name in archive.namelist()}
    index = json.loads(contents["transfer.json"])
    edit(contents, index)
    contents["transfer.json"] = json.dumps(index).encode()
    with ZipFile(path, "w", compression=ZIP_DEFLATED) as archive:
        for name, value in contents.items():
            archive.writestr(name, value)


def set_object(contents, index, key, value):
    contents[f"objects/{key}"] = value
    index["objects"][key] = {"size": len(value), "sha256": hashlib.sha256(value).hexdigest()}


def test_selection_partial_leap_year_and_failed_refresh(workspace):
    fs, root = workspace
    seed_day(fs, run_id="old")
    seed_day(fs, run_id="effective", offset=1)
    seed_day(fs, run_id="failed-refresh", offset=2, status="failed")
    seed_day(fs, day=date(2023, 12, 31), run_id="outside")
    seed_day(fs, resource="contractor_result", run_id="other-resource")
    path = root / "year.zip"
    report = transfer.export_bundle(fs, year=2024, resource="project", output=path)
    assert report["selected_days"] == 1
    assert report["missing_days"] == 365
    assert report["coverage_complete"] is False
    assert "2024-02-29" not in report["missing_dates"]["project"]
    with ZipFile(path) as archive:
        bundle = ValidatedArchive(archive)
        assert bundle.index.days[0].run_id == "effective"
        assert all("effective" in key for key in bundle.index.objects)
        assert all("source" not in key.split("/")[0] for key in bundle.index.objects)


@pytest.mark.parametrize("resource,empty", [("project", True), ("project", False),
                                           ("khlcnt", False), ("notify_contractor", False)])
def test_round_trip_and_repeat_do_not_duplicate(workspace, monkeypatch, resource, empty):
    fs, root = workspace
    path = export_fixture(fs, root, resource=resource, empty=empty)
    with ZipFile(path) as archive:
        original = {name.removeprefix("objects/"): archive.read(name)
                    for name in archive.namelist() if name.startswith("objects/")}
    bucket = destination(monkeypatch, root)
    report = transfer.import_bundle(fs, path)
    count = 0 if empty else len(get_resource(resource).tables)
    assert report["destination_verified"] == {"days": 1, "files": count, "records": count}
    assert len(report["imported_days"]) == 1
    assert report["uploaded_objects"] == len(original)
    for key, expected in original.items():
        with fs.open(f"{bucket}/{key}", "rb") as actual:
            assert actual.read() == expected
    before = fs.find(bucket, detail=True)
    report = transfer.import_bundle(fs, path)
    assert len(report["skipped_days"]) == 1
    assert report["uploaded_objects"] == 0
    assert fs.find(bucket, detail=True) == before
    definition = get_resource(resource)
    selected = select_committed_days(fs, definition, date(2024, 2, 29), date(2024, 2, 29))
    assert verify_committed(fs, selected)["records"] == count


def test_keep_destination_and_only_add_missing_days(workspace, monkeypatch):
    fs, root = workspace
    seed_day(fs, day=date(2024, 3, 1), run_id="next")
    path = export_fixture(fs, root)
    destination(monkeypatch, root)
    seed_day(fs, run_id="local", offset=-1)
    report = transfer.import_bundle(fs, path)
    assert len(report["imported_days"]) == len(report["skipped_days"]) == 1
    coverage = read_coverage(fs, get_resource("project").identity,
                             date(2024, 2, 29), date(2024, 3, 1))
    assert [day.effective.run_id for day in coverage] == ["local", "next"]


def test_empty_year_and_existing_output(workspace):
    fs, root = workspace
    path = root / "year.zip"
    with pytest.raises(ValueError, match="No committed"):
        transfer.export_bundle(fs, year=2024, output=path)
    assert not path.exists()
    path.write_bytes(b"keep")
    with pytest.raises(FileExistsError):
        transfer.export_bundle(fs, year=2024, output=path)
    assert path.read_bytes() == b"keep"


def test_dry_runs_do_not_create_files_or_take_lock(workspace, monkeypatch):
    fs, root = workspace
    seed_day(fs)
    absent = root / "absent" / "year.zip"
    with execution_lock(root / "locks"):
        report = transfer.export_bundle(fs, year=2024, output=absent, dry_run=True)
    assert report["verified"] is None
    assert not absent.parent.exists()
    path = root / "year.zip"
    transfer.export_bundle(fs, year=2024, output=path)
    bucket = destination(monkeypatch, root)
    with execution_lock(root / "locks"):
        report = transfer.import_bundle(fs, path, dry_run=True)
    assert len(report["planned_days"]) == 1
    assert report["uploaded_objects"] == 0
    assert not fs.exists(bucket)


def test_actual_export_and_import_respect_ingestion_lock(workspace, monkeypatch):
    fs, root = workspace
    path = export_fixture(fs, root)
    with execution_lock(root / "locks"), pytest.raises(RuntimeError, match="lock"):
        transfer.export_bundle(fs, year=2024, output=root / "another.zip")
    bucket = destination(monkeypatch, root)
    with execution_lock(root / "locks"), pytest.raises(transfer.TransferError, match="lock"):
        transfer.import_bundle(fs, path)
    assert not fs.exists(bucket)


@pytest.mark.parametrize("conflict", ["running", "different", "extra"])
def test_preflight_checks_all_days_before_writing(workspace, monkeypatch, conflict):
    fs, root = workspace
    seed_day(fs, day=date(2024, 3, 1), run_id="next")
    path = export_fixture(fs, root)
    bucket = destination(monkeypatch, root)
    if conflict == "running":
        seed_day(fs, day=date(2024, 3, 1), run_id="active", status="running", empty=True)
    else:
        filename = "part.parquet" if conflict == "different" else "extra.parquet"
        key = (f"{bucket}/bronze/muasamcong/project_detail/"
               f"source_date=2024-03-01/run_id=next/{filename}")
        with fs.open(key, "wb") as target:
            target.write(b"conflicting")
    before = fs.find(bucket, detail=True)
    with pytest.raises(transfer.TransferError, match="preflight") as error:
        transfer.import_bundle(fs, path)
    assert error.value.report["uploaded_objects"] == 0
    assert fs.find(bucket, detail=True) == before


@pytest.mark.parametrize("after_commit", [False, True])
def test_resume_after_interrupted_commit(workspace, monkeypatch, after_commit):
    fs, root = workspace
    path = export_fixture(fs, root)
    destination(monkeypatch, root)
    real_commit = transfer.commit_day_manifest

    def interrupt(*args):
        if after_commit:
            real_commit(*args)
        raise KeyboardInterrupt

    monkeypatch.setattr(transfer, "commit_day_manifest", interrupt)
    with pytest.raises(KeyboardInterrupt):
        transfer.import_bundle(fs, path)
    day = read_day_manifest(fs, get_resource("project").identity, "run-a", date(2024, 2, 29))
    assert (day is not None) == after_commit
    monkeypatch.setattr(transfer, "commit_day_manifest", real_commit)
    report = transfer.import_bundle(fs, path)
    assert len(report["skipped_days"] if after_commit else report["imported_days"]) == 1
    if not after_commit:
        assert report["reused_objects"] == 3


def test_interrupted_upload_does_not_publish_short_object(workspace, monkeypatch):
    fs, root = workspace
    path = export_fixture(fs, root)
    bucket = destination(monkeypatch, root)
    original = transfer.copy_digest

    def interrupt(source, target=None):
        if target is not None:
            target.write(source.read(8))
            raise KeyboardInterrupt
        return original(source)

    monkeypatch.setattr(transfer, "copy_digest", interrupt)
    with pytest.raises(KeyboardInterrupt):
        transfer.import_bundle(fs, path)
    assert fs.find(bucket) == []
    monkeypatch.setattr(transfer, "copy_digest", original)
    assert len(transfer.import_bundle(fs, path)["imported_days"]) == 1


@pytest.mark.parametrize("persisted", [False, True])
def test_uncertain_commit_is_never_downgraded_and_can_be_retried(workspace, monkeypatch, persisted):
    fs, root = workspace
    path = export_fixture(fs, root)
    destination(monkeypatch, root)
    real_commit = transfer.commit_day_manifest

    def uncertain(*args):
        if persisted:
            real_commit(*args)
        raise DayCommitUncertainError("Commit acknowledgement unknown")

    monkeypatch.setattr(transfer, "commit_day_manifest", uncertain)
    with pytest.raises(transfer.TransferError, match="acknowledgement"):
        transfer.import_bundle(fs, path)
    day = read_day_manifest(fs, get_resource("project").identity, "run-a", date(2024, 2, 29))
    assert day is None or day.status.value == "success"
    monkeypatch.setattr(transfer, "commit_day_manifest", real_commit)
    report = transfer.import_bundle(fs, path)
    assert len(report["skipped_days"] if persisted else report["imported_days"]) == 1


def test_corrupt_destination_upload_cannot_commit(workspace, monkeypatch):
    fs, root = workspace
    path = export_fixture(fs, root)
    destination(monkeypatch, root)
    real_put = transfer._put_missing

    def corrupt(fs, bundle, key, report):
        real_put(fs, bundle, key, report)
        if key.endswith(".parquet"):
            with fs.open(transfer._key(key), "wb") as target:
                target.write(b"storage corruption")

    monkeypatch.setattr(transfer, "_put_missing", corrupt)
    with pytest.raises(transfer.TransferError, match="Destination checksum"):
        transfer.import_bundle(fs, path)
    assert read_day_manifest(fs, get_resource("project").identity,
                             "run-a", date(2024, 2, 29)) is None


def test_coverage_reads_scale_by_resource_not_by_day(workspace, monkeypatch):
    fs, root = workspace
    for number in range(1, 11):
        seed_day(fs, day=date(2024, 1, number), run_id=f"run-{number}", empty=True)
    path = root / "year.zip"
    transfer.export_bundle(fs, year=2024, resource="project", output=path)
    destination(monkeypatch, root)
    calls = []
    real_read = transfer.read_coverage

    def read(*args):
        calls.append(args[1])
        return real_read(*args)

    monkeypatch.setattr(transfer, "read_coverage", read)
    assert len(transfer.import_bundle(fs, path)["imported_days"]) == 10
    assert len(calls) == 2  # preflight + final coverage, independent of number of imported days


@pytest.mark.parametrize("change", ["outside-range", "unfinished-day", "schema", "pages"])
def test_invalid_source_metadata_does_not_publish_zip(workspace, change):
    fs, root = workspace
    day = seed_day(fs)
    identity = get_resource("project").identity
    run = read_run_manifest(fs, identity, "run-a")
    if change == "outside-range":
        run.end_date = date(2024, 2, 28)
    elif change == "unfinished-day":
        day.completed_at = None
        write_day_manifest(fs, identity, day)
    elif change == "schema":
        run.schema_version = 9
    else:
        day.completed_pages = 2
        write_day_manifest(fs, identity, day)
    write_run_manifest(fs, identity, run)
    with pytest.raises(ValueError):
        transfer.export_bundle(fs, year=2024, output=root / "invalid.zip")
    assert not list(root.glob("*.zip*"))


@pytest.mark.parametrize("record_change", [
    {"run_id": "wrong"}, {"source_date": date(2024, 3, 1)},
    {"payload": '{"changed":true}'},
])
def test_invalid_source_parquet_does_not_publish(workspace, record_change):
    fs, root = workspace
    seed_day(fs, record_change=record_change)
    with pytest.raises(ValueError, match="mismatch"):
        transfer.export_bundle(fs, year=2024, output=root / "bad.zip")
    assert not list(root.glob("*.zip*"))


@pytest.mark.parametrize("change", ["checksum", "missing", "extra", "version", "count",
                                   "lineage", "hash", "table", "coverage"])
def test_invalid_archive_cannot_write_any_destination_objects(workspace, monkeypatch, change):
    fs, root = workspace
    path = export_fixture(fs, root)

    def edit(contents, index):
        parquet = next(key for key in index["objects"] if key.endswith(".parquet"))
        if change == "checksum":
            data = contents[f"objects/{parquet}"]
            contents[f"objects/{parquet}"] = b"X" + data[1:]
        elif change == "missing":
            del contents[f"objects/{parquet}"]
        elif change == "extra":
            contents["objects/extra.json"] = b"{}"
        elif change == "version":
            index["format_version"] = 99
        elif change == "coverage":
            index["missing_dates"]["project"].append("2024-02-29")
        elif change == "table":
            wrong = parquet.replace("project_detail", "contractor_result_detail")
            contents[f"objects/{wrong}"] = contents.pop(f"objects/{parquet}")
            index["objects"][wrong] = index["objects"].pop(parquet)
            index["days"][0]["objects"] = [wrong if key == parquet else key
                                            for key in index["days"][0]["objects"]]
        else:
            records = pq.read_table(io.BytesIO(contents[f"objects/{parquet}"])).to_pylist()
            if change == "count":
                records += records
            elif change == "lineage":
                records[0]["run_id"] = "other"
            else:
                records[0]["content_hash"] = "0" * 64
            target = io.BytesIO()
            pq.write_table(pa.Table.from_pylist(records), target)
            set_object(contents, index, parquet, target.getvalue())

    rewrite_archive(path, edit)
    bucket = destination(monkeypatch, root)
    with pytest.raises(ValueError):
        transfer.import_bundle(fs, path)
    assert not fs.exists(bucket)


@pytest.mark.parametrize("name", ["../outside", "/absolute", "C:/absolute", "objects/../bad",
                                 "objects\\bad", "objects//bad"])
def test_unsafe_paths_rejected(workspace, name):
    fs, root = workspace
    path = export_fixture(fs, root)
    with ZipFile(path, "a") as archive:
        member = ZipInfo("placeholder")
        member.filename = name  # Avoid Windows normalizing backslashes when creating ZipInfo.
        archive.writestr(member, b"bad")
    with pytest.raises(ValueError, match="Unsafe"):
        transfer.inspect_bundle(path)


def test_duplicate_and_symlink_entries_rejected(workspace):
    fs, root = workspace
    path = export_fixture(fs, root)
    with ZipFile(path, "a") as archive, pytest.warns(UserWarning):
        archive.writestr("transfer.json", b"{}")
    with pytest.raises(ValueError, match="Duplicate ZIP"):
        transfer.inspect_bundle(path)
    path.unlink()
    transfer.export_bundle(fs, year=2024, output=path)
    link = ZipInfo("objects/link")
    link.create_system = 3
    link.external_attr = (stat.S_IFLNK | 0o777) << 16
    with ZipFile(path, "a") as archive:
        archive.writestr(link, "../secret")
    with pytest.raises(ValueError, match="links"):
        transfer.inspect_bundle(path)


def test_source_manifest_changed_during_export_is_not_published(workspace, monkeypatch):
    fs, root = workspace
    seed_day(fs)
    original = transfer._source_plan
    calls = 0

    def changed(*args):
        nonlocal calls
        calls += 1
        if calls == 2:
            seed_day(fs, run_id="refresh", offset=3)
        return original(*args)

    monkeypatch.setattr(transfer, "_source_plan", changed)
    with pytest.raises(ValueError, match="changed"):
        transfer.export_bundle(fs, year=2024, output=root / "bad.zip")
    assert not list(root.glob("*.zip*"))


def test_bundle_index_rejects_unknown_fields():
    with pytest.raises(ValueError):
        BundleIndex.model_validate({"format_version": 1, "credential": "not-allowed"})


def seed_range(fs, status="partial_failed", dates=None):
    dates = dates or [date(2024, 1, number) for number in (1, 2, 3)]
    for number, day in enumerate(dates):
        seed_day(fs, day=day, run_id="legacy-range",
                 status="success" if number < 2 else "failed")
    identity = get_resource("project").identity
    run = read_run_manifest(fs, identity, "legacy-range")
    run = run.model_copy(update={
        "start_date": min(dates), "end_date": max(dates), "total_dates": len(dates),
        "status": type(run.status)(status), "success_dates": 2,
        "failed_dates": 0 if status == "running" else 1,
        "completed_at": None if status == "running" else run.completed_at,
    })
    write_run_manifest(fs, identity, run)
    return run


@pytest.mark.parametrize("status", ["partial_failed", "running", "failed"])
@pytest.mark.parametrize("skip_first", [False, True])
def test_export_committed_days_from_old_range_runs(workspace, monkeypatch, status, skip_first):
    fs, root = workspace
    original = seed_range(fs, status)
    path = root / "range.zip"
    source_bucket = settings.OBJECT_STORAGE_BUCKET
    report = transfer.export_bundle(fs, year=2024, resource="project", output=path)
    assert report["selected_days"] == 2
    assert report["objects"] == 7  # shared run.json + two day/page/Parquet sets
    with ZipFile(path) as archive:
        bundle = ValidatedArchive(archive)
        assert bundle.index.format_version == 2
        assert len(bundle.run_manifests) == 1
        assert next(iter(bundle.run_manifests.values())) == original
    destination(monkeypatch, root)
    if skip_first:
        seed_day(fs, day=date(2024, 1, 1), run_id="keep-local")
    imported = transfer.import_bundle(fs, path)
    assert len(imported["imported_days"]) == 2 - int(skip_first)
    identity = get_resource("project").identity
    summary = read_run_manifest(fs, identity, "legacy-range")
    assert summary.status.value == "success"
    assert summary.total_dates == summary.success_dates == 2 - int(skip_first)
    assert summary.failed_dates == 0
    assert summary.start_date == date(2024, 1, 2 if skip_first else 1)
    assert summary.end_date == date(2024, 1, 2)
    coverage = read_coverage(fs, identity, date(2024, 1, 1), date(2024, 1, 3))
    assert coverage[-1].effective is None
    assert not any(day.active_run_ids for day in coverage)
    assert len(transfer.import_bundle(fs, path)["skipped_days"]) == 2
    monkeypatch.setattr(settings, "OBJECT_STORAGE_BUCKET", source_bucket)
    assert read_run_manifest(fs, identity, "legacy-range") == original


def test_range_run_resume_when_summary_was_written_before_day_commit(workspace, monkeypatch):
    fs, root = workspace
    seed_range(fs)
    path = root / "range.zip"
    transfer.export_bundle(fs, year=2024, resource="project", output=path)
    destination(monkeypatch, root)
    real_commit = transfer.commit_day_manifest

    def interrupt_second(fs, identity, manifest):
        if manifest.source_date == date(2024, 1, 2):
            raise KeyboardInterrupt
        return real_commit(fs, identity, manifest)

    monkeypatch.setattr(transfer, "commit_day_manifest", interrupt_second)
    with pytest.raises(KeyboardInterrupt):
        transfer.import_bundle(fs, path)
    identity = get_resource("project").identity
    assert read_run_manifest(fs, identity, "legacy-range").success_dates == 2
    assert read_day_manifest(fs, identity, "legacy-range", date(2024, 1, 2)) is None
    monkeypatch.setattr(transfer, "commit_day_manifest", real_commit)
    report = transfer.import_bundle(fs, path)
    assert len(report["imported_days"]) == len(report["skipped_days"]) == 1
    assert report["reused_objects"] == 3


def test_range_run_can_be_imported_from_separate_year_bundles(workspace, monkeypatch):
    fs, root = workspace
    seed_range(fs, dates=[date(2023, 12, 31), date(2024, 1, 1), date(2024, 1, 2)])
    for year in (2023, 2024):
        transfer.export_bundle(fs, year=year, resource="project", output=root / f"{year}.zip")
    destination(monkeypatch, root)
    for year in (2023, 2024):
        assert len(transfer.import_bundle(fs, root / f"{year}.zip")["imported_days"]) == 1
    summary = read_run_manifest(fs, get_resource("project").identity, "legacy-range")
    assert summary.start_date == date(2023, 12, 31)
    assert summary.end_date == date(2024, 1, 1)
    assert summary.total_dates == summary.success_dates == 2


def test_incompatible_destination_range_summary_is_not_overwritten(workspace, monkeypatch):
    fs, root = workspace
    source = seed_range(fs)
    path = root / "range.zip"
    transfer.export_bundle(fs, year=2024, resource="project", output=path)
    destination(monkeypatch, root)
    identity = get_resource("project").identity
    write_run_manifest(fs, identity, source)  # Old, unrelated history is not an import receipt.
    with pytest.raises(transfer.TransferError, match="preflight") as error:
        transfer.import_bundle(fs, path)
    assert error.value.report["conflicts"][0]["reason"] == "incompatible_destination_run_summary"
    assert read_run_manifest(fs, identity, "legacy-range") == source


def test_version_one_archive_remains_readable(workspace, monkeypatch):
    fs, root = workspace
    path = export_fixture(fs, root)
    rewrite_archive(path, lambda _contents, index: index.update(format_version=1))
    assert transfer.inspect_bundle(path)["verified"]["days"] == 1
    destination(monkeypatch, root)
    assert len(transfer.import_bundle(fs, path)["imported_days"]) == 1


@pytest.mark.parametrize("dry_run", [True, False])
def test_missing_parquet_on_success_day_is_reported_before_archive_creation(
    workspace, monkeypatch, dry_run,
):
    fs, root = workspace
    seed_day(fs)
    real_glob = fs.glob
    monkeypatch.setattr(fs, "glob", lambda pattern: [] if pattern.endswith("*.parquet")
                        else real_glob(pattern))
    with pytest.raises(ValueError, match="Committed day has no Parquet: resource=project"):
        transfer.export_bundle(fs, year=2024, output=root / "missing.zip", dry_run=dry_run)
    assert not list(root.glob("*.zip*"))


def test_resume_range_when_other_run_filled_pending_day(workspace, monkeypatch):
    fs, root = workspace
    seed_range(fs)
    seed_day(fs, day=date(2024, 1, 3), run_id="legacy-range")
    identity = get_resource("project").identity
    raw_run = read_run_manifest(fs, identity, "legacy-range")
    write_run_manifest(fs, identity, raw_run.model_copy(update={
        "start_date": date(2024, 1, 1), "total_dates": 3, "success_dates": 3,
    }))
    path = root / "range.zip"
    transfer.export_bundle(fs, year=2024, resource="project", output=path)
    destination(monkeypatch, root)
    real_commit = transfer.commit_day_manifest

    def interrupt_second(fs, identity, manifest):
        if manifest.source_date == date(2024, 1, 2):
            raise KeyboardInterrupt
        return real_commit(fs, identity, manifest)

    monkeypatch.setattr(transfer, "commit_day_manifest", interrupt_second)
    with pytest.raises(KeyboardInterrupt):
        transfer.import_bundle(fs, path)
    seed_day(fs, day=date(2024, 1, 2), run_id="new-local")
    monkeypatch.setattr(transfer, "commit_day_manifest", real_commit)
    result = transfer.import_bundle(fs, path)
    assert len(result["imported_days"]) == 1
    assert len(result["skipped_days"]) == 2
    assert read_run_manifest(fs, identity, "legacy-range").success_dates == 2
