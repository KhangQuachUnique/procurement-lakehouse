import json
from collections import Counter
from datetime import UTC, date, datetime, timedelta

import duckdb
import fsspec
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from procurement.common.catalog import get_resource
from procurement.common.settings import settings
from procurement.ingestion.bid_opening_watch import selection_for
from procurement.ingestion.coverage import read_coverage
from procurement.ingestion.engine.metadata import calculate_content_hash
from procurement.models.control import DayManifest, PageManifest, RunManifest
from procurement.ops.repositories import ControlRepository, ErrorRepository
from procurement.ops.service import OpsService
from procurement.quality.audit import frozen_selection, selection_day
from procurement.quality.files import read_json
from procurement.quality.storage import read_quality_contexts, save_quality_page
from procurement.storage import compaction
from procurement.storage.committed import (
    iter_committed_records,
    select_committed_days,
    verify_committed,
)
from procurement.storage.compact_parquet import rewrite_table, verify_multiset
from procurement.storage.control import (
    DayCommitUncertainError,
    read_day_manifest,
    write_day_manifest,
    write_page_manifest,
    write_run_manifest,
)
from procurement.storage.transfer import export_bundle, import_bundle
from procurement.tools.bronze_explorer import (
    BronzeTable,
    _create_bronze_views,
    _select_current_files,
)
from procurement.tools.count_records import build_report, count_files

DAY = date(2022, 9, 23)
START = datetime(2022, 9, 24, tzinfo=UTC)


@pytest.fixture
def env(tmp_path, monkeypatch):
    bucket = tmp_path / "bucket"
    bucket.mkdir()
    monkeypatch.setattr(settings, "OBJECT_STORAGE_BUCKET", bucket.as_posix())
    monkeypatch.setattr(settings, "INGESTION_LOCK_DIR", str(tmp_path / "locks"))
    return fsspec.filesystem("file", auto_mkdir=True, skip_instance_cache=True), tmp_path


def seed(fs, *, resource="khlcnt", run_id="baseline", started=START, files=4, status="success", day=DAY):
    definition = get_resource(resource)
    identity = definition.identity
    rows = []
    for table in definition.tables:
        for index in range(files):
            payload = {"name": "Gói thầu tiếng Việt", "number": index % 2}
            record = {"source_id": str(index % 2), "source_version": "01", "run_id": run_id,
                      "source_date": day, "ingested_at": started,
                      "content_hash": calculate_content_hash(payload),
                      "payload": json.dumps(payload, ensure_ascii=False),
                      "_dlt_id": str(index % 2), "_dlt_load_id": "original-load"}
            key = f"{compaction.parquet_prefix(identity, table, day, run_id)}/{index}.parquet"
            with fs.open(key, "wb") as file:
                pq.write_table(pa.Table.from_pylist([record, record]), file)
            rows.extend([(table, record), (table, record)])
    write_run_manifest(fs, identity, RunManifest(
        run_id=run_id, source=identity.source, resource=resource, start_date=day, end_date=day,
        total_dates=1, status=status, success_dates=int(status == "success"),
        started_at=started, completed_at=started))
    write_day_manifest(fs, identity, DayManifest(
        run_id=run_id, source=identity.source, resource=resource, source_date=day, status=status,
        expected_pages=2, completed_pages=2, search_items=files, bronze_records=len(rows),
        started_at=started, completed_at=started))
    for page, search_count in enumerate((files // 2, files - files // 2)):
        write_page_manifest(fs, identity, PageManifest(
            run_id=run_id, source_date=day, page_number=page, page_size=max(1, files), status="success",
            search_items=search_count, bronze_records=len(rows) // 2,
            started_at=started, completed_at=started))
    save_quality_page(fs, identity, run_id, day, 0, "original-contract", [{"context": {"id": "sample"}}])
    return rows


def plan_for(fs, **kwargs):
    return compaction.create_plan(fs, start=DAY, end=DAY, **{"resource": "khlcnt", **kwargs})


def selected(fs, resource="khlcnt"):
    return select_committed_days(fs, get_resource(resource), DAY, DAY)


def normalized(rows):
    return Counter((table, json.dumps({k: v for k, v in record.items() if k != "run_id"},
                                     sort_keys=True, default=str, ensure_ascii=False))
                   for table, record in rows)


def test_new_success_preserves_duplicates_unicode_and_pinned_old_reader(env):
    fs, root = env
    original = seed(fs)
    pinned = selected(fs)
    seed(fs, run_id="newer_failed", started=START + timedelta(hours=1), status="failed")
    plan = plan_for(fs)
    assert plan["days"][0]["baseline"]["run_id"] == "baseline"
    report = compaction.run_plan(fs, plan, root / "work")
    receipt = report["days"][0]
    assert report["complete"]
    assert (receipt["input_files"], receipt["output_files"]) == (8, 2)
    current = selected(fs)
    assert current[0].run_id == receipt["run_id"]
    assert normalized(iter_committed_records(fs, current, verify_hash=True)) == normalized(original)
    assert verify_committed(fs, pinned)["records"] == len(original)
    identity = get_resource("khlcnt").identity
    assert read_quality_contexts(fs, identity, receipt["run_id"], DAY) == [{"id": "sample"}]
    coverage = read_coverage(fs, identity, DAY, DAY)[0]
    assert coverage.effective.run_id == receipt["run_id"]
    assert selection_for(fs, "khlcnt", coverage.effective) == current[0]
    assert selection_day(frozen_selection(fs, "khlcnt", DAY, DAY)[0]) == current[0]
    service = OpsService(ControlRepository(fs), ErrorRepository(fs))
    assert service.get_date("khlcnt", DAY).effective_run_id == receipt["run_id"]
    tables = [BronzeTable("muasamcong", name) for name in get_resource("khlcnt").tables]
    metadata, committed, issues = {}, {}, []
    files = _select_current_files(fs, bucket=settings.OBJECT_STORAGE_BUCKET, tables=tables,
                                  start=DAY, end=DAY, file_metadata=metadata,
                                  committed_days=committed, issues=issues)
    assert sum(map(len, files.values())) == 2
    counts = count_files(fs, list(metadata), metadata, root / "counts.sqlite3", namespace="test")
    counted = build_report(files, counts, committed, issues, DAY, DAY)
    assert counted["complete"]
    assert sum(counted["totals"].values()) == len(original)
    with duckdb.connect() as con:
        _create_bronze_views(con, bucket=settings.OBJECT_STORAGE_BUCKET, tables=tables, schema="bronze",
                             files_by_table={t: [p.removeprefix("s3://") for p in paths]
                                             for t, paths in files.items()})
        for table in tables:
            assert con.execute(f'SELECT DISTINCT run_id FROM bronze."{table.table}"').fetchall() == [
                (receipt["run_id"],)]
    again = compaction.run_plan(fs, plan, root / "work")
    assert again["days"][0]["run_id"] == receipt["run_id"]
    assert plan_for(fs)["skipped"][0]["reason"] == "already_compact"


def test_compacted_khlcnt_transfer_round_trip(env, monkeypatch):
    fs, root = env
    original = seed(fs)
    receipt = compaction.run_plan(fs, plan_for(fs), root / "work")["days"][0]
    bundle = root / "compacted.zip"
    export_bundle(fs, year=2022, output=bundle, resource="khlcnt")
    target = root / "target"
    target.mkdir()
    monkeypatch.setattr(settings, "OBJECT_STORAGE_BUCKET", target.as_posix())
    import_bundle(fs, bundle)
    assert selected(fs)[0].run_id == receipt["run_id"]
    assert normalized(iter_committed_records(fs, selected(fs), verify_hash=True)) == normalized(original)


@pytest.mark.parametrize("fault", ["second_table", "upload", "verify", "interrupt"])
def test_failure_keeps_baseline_and_retry_uses_new_run(env, monkeypatch, fault):
    fs, root = env
    seed(fs)
    plan = plan_for(fs)
    with monkeypatch.context() as patch:
        if fault in {"second_table", "interrupt"}:
            original = compaction.rewrite_table
            calls = 0

            def rewrite(*args, **kwargs):
                nonlocal calls
                calls += 1
                if fault == "interrupt":
                    raise KeyboardInterrupt()
                if calls == 2:
                    raise ValueError("second table failed")
                return original(*args, **kwargs)

            patch.setattr(compaction, "rewrite_table", rewrite)
        elif fault == "upload":
            original = fs.open

            def broken_open(path, mode="rb", *args, **kwargs):
                if mode == "wb" and "/compact-" in str(path):
                    raise OSError("upload failed")
                return original(path, mode, *args, **kwargs)

            patch.setattr(fs, "open", broken_open)
        else:
            def broken_verify(*args, **kwargs):
                raise ValueError("verification failed")

            patch.setattr(compaction, "_verify_receipt", broken_verify)
        with pytest.raises(KeyboardInterrupt if fault == "interrupt" else (ValueError, OSError)):
            compaction.run_plan(fs, plan, root / "work")
    failed = read_json(root / "work/days/khlcnt-2022-09-23.json")
    assert failed["status"] == "failed"
    assert selected(fs)[0].run_id == "baseline"
    retried = compaction.run_plan(fs, plan, root / "work")["days"][0]
    assert retried["status"] == "success"
    assert retried["run_id"] != failed["run_id"]


@pytest.mark.parametrize("persist", [False, True])
def test_uncertain_commit_never_downgrades_or_duplicates(env, monkeypatch, persist):
    fs, root = env
    seed(fs)
    plan = plan_for(fs)
    original = compaction.commit_day_manifest

    def uncertain(*args, **kwargs):
        if persist:
            original(*args, **kwargs)
        raise DayCommitUncertainError("lost acknowledgement")

    with monkeypatch.context() as patch:
        patch.setattr(compaction, "commit_day_manifest", uncertain)
        with pytest.raises(DayCommitUncertainError):
            compaction.run_plan(fs, plan, root / "work", continue_on_error=True)
    checkpoint = read_json(root / "work/days/khlcnt-2022-09-23.json")
    assert checkpoint["status"] == "commit_uncertain"
    day = read_day_manifest(fs, get_resource("khlcnt").identity, checkpoint["run_id"], DAY)
    assert day.status == ("success" if persist else "running")
    if persist:
        resumed = compaction.run_plan(fs, plan, root / "work")["days"][0]
        assert resumed["run_id"] == checkpoint["run_id"]
        assert resumed["status"] == "success"
    else:
        with pytest.raises(compaction.UnconfirmedCompaction):
            compaction.run_plan(fs, plan, root / "work", continue_on_error=True)
        assert selected(fs)[0].run_id == "baseline"


def test_baseline_change_immediately_before_commit(env, monkeypatch):
    fs, root = env
    seed(fs)
    plan = plan_for(fs)
    original = compaction._verify_receipt

    def changed(*args, **kwargs):
        result = original(*args, **kwargs)
        seed(fs, run_id="replacement", started=datetime.now(UTC), files=2)
        return result

    monkeypatch.setattr(compaction, "_verify_receipt", changed)
    with pytest.raises(ValueError, match="Baseline changed"):
        compaction.run_plan(fs, plan, root / "work")
    assert selected(fs)[0].run_id == "replacement"


def test_plan_tamper_and_source_inventory_change(env):
    fs, root = env
    seed(fs)
    plan = plan_for(fs)
    plan["target_bytes"] += 1
    with pytest.raises(ValueError, match="Plan changed"):
        compaction.run_plan(fs, plan, root / "work")
    plan = plan_for(fs)
    with fs.open(plan["days"][0]["files"][0]["key"], "wb") as file:
        file.write(b"changed")
    with pytest.raises(ValueError, match="inventory changed"):
        compaction.run_plan(fs, plan, root / "work")


def test_no_benefit_keeps_baseline(env):
    fs, root = env
    seed(fs)
    plan = plan_for(fs, target_bytes=1)
    result = compaction.run_plan(fs, plan, root / "work")
    assert result["days"][0]["status"] == "no_benefit"
    assert selected(fs)[0].run_id == "baseline"
    again = compaction.run_plan(fs, plan, root / "work")
    assert again["days"][0]["run_id"] == result["days"][0]["run_id"]


def test_nullable_schema_union_and_incompatible_type(env):
    fs, root = env
    seed(fs, resource="project")
    paths = [obj["key"] for obj in plan_for(fs, resource="project")["days"][0]["files"]]
    table = pq.ParquetFile(paths[0]).read().append_column("legacy_extra", pa.array(["value", None]))
    pq.write_table(table, paths[0])
    outputs, count = rewrite_table(paths, root / "rewrite", baseline_run_id="baseline", run_id="new",
                                   source_date=DAY, target_bytes=128 * 1024**2)
    verify_multiset(paths, outputs, root / "spill")
    assert count == 8
    table = pq.ParquetFile(paths[1]).read()
    table = table.set_column(table.schema.get_field_index("source_id"), "source_id", pa.array([1, 2]))
    pq.write_table(table, paths[1])
    with pytest.raises(pa.ArrowTypeError):
        rewrite_table(paths, root / "bad", baseline_run_id="baseline", run_id="new",
                      source_date=DAY, target_bytes=128 * 1024**2)


def test_all_resources_missing_empty_and_single_file_are_skipped(env):
    fs, _ = env
    seed(fs, files=0)
    seed(fs, resource="project", files=1)
    plan = plan_for(fs, resource="all")
    reasons = {item["resource"]: item["reason"] for item in plan["skipped"]}
    assert reasons["khlcnt"] == "empty"
    assert reasons["project"] == "already_compact"
    assert reasons["bid_opening"] == "no_success"
    assert not plan["days"]


@pytest.mark.parametrize("persist", [False, True])
def test_interrupt_during_commit_stays_uncertain(env, monkeypatch, persist):
    fs, root = env
    seed(fs)
    plan = plan_for(fs)
    original = compaction.commit_day_manifest

    def interrupted(*args, **kwargs):
        if persist:
            original(*args, **kwargs)
        raise KeyboardInterrupt()

    monkeypatch.setattr(compaction, "commit_day_manifest", interrupted)
    with pytest.raises(KeyboardInterrupt):
        compaction.run_plan(fs, plan, root / "work", continue_on_error=True)
    receipt = read_json(root / "work/days/khlcnt-2022-09-23.json")
    assert receipt["status"] == "commit_uncertain"
    marker = read_day_manifest(fs, get_resource("khlcnt").identity, receipt["run_id"], DAY)
    assert marker.status == ("success" if persist else "running")


@pytest.mark.parametrize("column,value,message", [
    ("content_hash", "invalid", "content hash mismatch"),
    ("run_id", "other", "lineage mismatch"),
])
def test_invalid_source_never_commits(env, column, value, message):
    fs, root = env
    seed(fs)
    path = plan_for(fs)["days"][0]["files"][0]["key"]
    table = pq.ParquetFile(path).read()
    table = table.set_column(table.schema.get_field_index(column), column, pa.array([value, value]))
    pq.write_table(table, path)
    with pytest.raises(ValueError, match=message):
        compaction.run_plan(fs, plan_for(fs), root / "work")
    assert selected(fs)[0].run_id == "baseline"


def test_multiset_verification_detects_duplicate_loss_and_metadata_edits(env):
    fs, root = env
    seed(fs, resource="project")
    paths = [obj["key"] for obj in plan_for(fs, resource="project")["days"][0]["files"]]
    output, _ = rewrite_table(paths, root / "rewrite", baseline_run_id="baseline", run_id="new",
                              source_date=DAY, target_bytes=128 * 1024**2)
    table = pq.ParquetFile(output[0]).read()
    pq.write_table(table.slice(1), output[0])
    with pytest.raises(ValueError, match="multiset changed"):
        verify_multiset(paths, output, root / "spill")
    changed = table.set_column(table.schema.get_field_index("_dlt_load_id"), "_dlt_load_id",
                               pa.array(["changed"] * len(table)))
    pq.write_table(changed, output[0])
    with pytest.raises(ValueError, match="multiset changed"):
        verify_multiset(paths, output, root / "spill")


def test_continue_on_confirmed_error_runs_next_day(env, monkeypatch):
    fs, root = env
    seed(fs)
    next_day = DAY + timedelta(days=1)
    seed(fs, day=next_day, run_id="second")
    plan = compaction.create_plan(fs, resource="khlcnt", start=DAY, end=next_day)
    original = compaction.rewrite_table

    def failed_first(*args, **kwargs):
        if kwargs["source_date"] == DAY:
            raise ValueError("first day failed")
        return original(*args, **kwargs)

    monkeypatch.setattr(compaction, "rewrite_table", failed_first)
    report = compaction.run_plan(fs, plan, root / "work", continue_on_error=True)
    assert not report["complete"]
    assert [item["status"] for item in report["days"]] == ["failed", "success"]


def test_missing_quality_is_not_invented(env):
    fs, root = env
    seed(fs)
    from procurement.quality.storage import quality_prefix
    identity = get_resource("khlcnt").identity
    for key in fs.glob(f"{quality_prefix(identity, 'baseline', DAY)}/*.json"):
        fs.rm(key)
    receipt = compaction.run_plan(fs, plan_for(fs), root / "work")["days"][0]
    assert not fs.glob(f"{quality_prefix(identity, receipt['run_id'], DAY)}/*.json")


def test_cli_plan_run_resume(env, monkeypatch, capsys):
    from procurement.tools import compact_bronze

    fs, root = env
    seed(fs)
    monkeypatch.setattr(compact_bronze, "create_s3_filesystem", lambda: fs)
    compact_bronze.main(["plan", "--resource", "khlcnt", "--start-date", str(DAY),
                         "--end-date", str(DAY), "--output-dir", str(root / "exports")])
    path = json.loads(capsys.readouterr().out)["plan"]
    assert compact_bronze.main(["run", "--plan", path]) == 0
    run_id = selected(fs)[0].run_id
    assert compact_bronze.main(["run", "--plan", path]) == 0
    assert selected(fs)[0].run_id == run_id


def test_finish_run_failure_after_day_commit_is_resumable(env, monkeypatch):
    fs, root = env
    seed(fs)
    plan = plan_for(fs)

    def broken(*args):
        raise OSError("run finalization failed")

    with monkeypatch.context() as patch:
        patch.setattr(compaction, "_finish_run", broken)
        with pytest.raises(OSError):
            compaction.run_plan(fs, plan, root / "work")
    current = selected(fs)[0].run_id
    assert current != "baseline"
    receipt = compaction.run_plan(fs, plan, root / "work")["days"][0]
    assert receipt["run_id"] == current
    assert receipt["status"] == "success"


def test_active_attempt_prevents_publication(env):
    fs, root = env
    seed(fs)
    plan = plan_for(fs)
    seed(fs, run_id="active", started=START + timedelta(hours=1), status="running")
    assert plan_for(fs)["skipped"][0]["reason"] == "active_attempt"
    with pytest.raises(ValueError, match="another attempt is active"):
        compaction.run_plan(fs, plan, root / "work")
    assert selected(fs)[0].run_id == "baseline"


def test_corrupted_uploaded_bytes_are_rejected(env, monkeypatch):
    fs, root = env
    seed(fs)
    original = compaction._verify_receipt

    def corrupted(store, item, receipt, scratch):
        with store.open(receipt["outputs"][0]["key"], "wb") as file:
            file.write(b"corrupted")
        return original(store, item, receipt, scratch)

    monkeypatch.setattr(compaction, "_verify_receipt", corrupted)
    with pytest.raises(ValueError, match="checksum mismatch"):
        compaction.run_plan(fs, plan_for(fs), root / "work")
    assert selected(fs)[0].run_id == "baseline"
