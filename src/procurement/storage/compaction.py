"""Offline, append-only daily Bronze compaction published through ordinary SUCCESS commits."""

import hashlib
import logging
import re
from collections import Counter, defaultdict
from datetime import UTC, date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import pyarrow.parquet as pq

from procurement.common.catalog import SUPPORTED_RESOURCES, get_resource
from procurement.common.dates import today_vn, validate_closed_range
from procurement.common.settings import settings
from procurement.ingestion.coverage import read_coverage
from procurement.ingestion.engine.metadata import calculate_content_hash
from procurement.jobs.lock import execution_lock
from procurement.models.control import DayManifest, DayStatus, PageManifest, RunManifest, RunStatus
from procurement.quality.coverage import CoverageGuard
from procurement.quality.files import read_json, safe_error, write_json
from procurement.quality.storage import quality_prefix
from procurement.storage.committed import CommittedDay, verify_committed
from procurement.storage.compact_parquet import (
    copy_file,
    digest_file,
    rewrite_table,
    verify_multiset,
)
from procurement.storage.control import (
    DayCommitUncertainError,
    commit_day_manifest,
    list_page_manifests,
    read_day_manifest,
    read_run_manifest,
    write_day_manifest,
    write_page_manifest,
    write_run_manifest,
)
from procurement.storage.execution import ExecutionHeartbeat
from procurement.storage.io import write_json as write_storage_json
from procurement.storage.transfer_archive import BundleDay, validate_day_metadata

logger = logging.getLogger(__name__)
SAFE_ID = re.compile(r"[A-Za-z0-9_-]+\Z")
SAFE_FILE = re.compile(r"[A-Za-z0-9_.-]+\Z")


class UnconfirmedCompaction(RuntimeError):
    """Do not retry or mark FAILED until the existing publication has been inspected."""


def namespace():
    return calculate_content_hash([settings.OBJECT_STORAGE_ENDPOINT, settings.OBJECT_STORAGE_BUCKET])


def parquet_prefix(identity, table, day, run_id):
    return (f"{settings.OBJECT_STORAGE_BUCKET}/bronze/{identity.source}/{table}/"
            f"source_date={day}/run_id={run_id}")


def provenance_key(identity, run_id):
    return (f"{settings.OBJECT_STORAGE_BUCKET}/_ops/{identity.source}/{identity.resource}/"
            f"run_id={run_id}/compaction.json")


def object_signature(fs, key):
    info = fs.info(key)
    signature = {"size": info["size"], "markers": {
        name: str(info[name]) for name in ("ETag", "etag", "LastModified", "mtime")
        if info.get(name) is not None}}
    if not signature["markers"]:
        digest = hashlib.sha256()
        with fs.open(key, "rb") as file:
            while chunk := file.read(1024 * 1024):
                digest.update(chunk)
        signature["sha256"] = digest.hexdigest()
    return signature


def inventory(fs, definition, day, run_id):
    files = []
    for table in definition.tables:
        prefix = parquet_prefix(definition.identity, table, day, run_id)
        for key in sorted(fs.glob(f"{prefix}/*.parquet")):
            files.append({"table": table, "key": key, "signature": object_signature(fs, key)})
    return files


def quality_inventory(fs, identity, day, run_id):
    prefix = quality_prefix(identity, run_id, day)
    return [{"name": key.rsplit("/", 1)[-1], "signature": object_signature(fs, key)}
            for key in sorted(fs.glob(f"{prefix}/*.json"))]


def _metadata(identity, run, day, pages):
    if run is None:
        raise ValueError("Missing baseline RunManifest")
    entry = BundleDay(resource=identity.resource, source_date=day.source_date,
                      run_id=day.run_id, objects=[])
    validate_day_metadata(entry, run, day, pages)


def create_plan(fs, *, resource, start, end, target_bytes=128 * 1024**2):
    validate_closed_range(start, end, today=today_vn())
    if not isinstance(target_bytes, int) or target_bytes <= 0:
        raise ValueError("target_bytes must be a positive integer")
    resources = SUPPORTED_RESOURCES if resource == "all" else (get_resource(resource).identity.resource,)
    plan = {"format_version": 1, "plan_id": uuid4().hex, "storage_namespace": namespace(),
            "created_at": datetime.now(UTC).isoformat(), "start": str(start), "end": str(end),
            "target_bytes": target_bytes, "compression": "zstd", "days": [], "skipped": []}
    for name in resources:
        definition = get_resource(name)
        logger.info("compact_plan resource=%s start=%s end=%s", name, start, end)
        for covered in read_coverage(fs, definition.identity, start, end):
            base = {"resource": name, "date": str(covered.source_date)}
            if covered.effective is None or covered.active_run_ids:
                plan["skipped"].append({**base, "reason": "active_attempt" if covered.active_run_ids else "no_success"})
                continue
            day = covered.effective
            if not SAFE_ID.fullmatch(day.run_id):
                raise ValueError("Invalid baseline run ID")
            files = inventory(fs, definition, day.source_date, day.run_id)
            counts = Counter(f["table"] for f in files)
            if not day.bronze_records or all(count <= 1 for count in counts.values()):
                plan["skipped"].append({**base, "reason": "empty" if not day.bronze_records else "already_compact",
                                        "baseline_run_id": day.run_id, "input_files": len(files)})
                continue
            pages = list_page_manifests(fs, definition.identity, run_id=day.run_id, source_date=day.source_date)
            run = read_run_manifest(fs, definition.identity, day.run_id)
            _metadata(definition.identity, run, day, pages)
            plan["days"].append({**base, "baseline": day.model_dump(mode="json"),
                                 "pages": [p.model_dump(mode="json") for p in pages], "files": files,
                                 "quality": quality_inventory(fs, definition.identity, day.source_date, day.run_id),
                                 "input_bytes": sum(f["signature"]["size"] for f in files)})
    plan["plan_hash"] = calculate_content_hash(plan)
    return plan


def validate_plan(plan):
    expected = calculate_content_hash({k: v for k, v in plan.items() if k != "plan_hash"})
    if plan.get("plan_hash") != expected or plan.get("storage_namespace") != namespace():
        raise ValueError("Plan changed or belongs to another storage namespace")
    if (plan.get("format_version") != 1 or plan.get("compression") != "zstd"
            or not re.fullmatch(r"[0-9a-f]{32}", plan["plan_id"])
            or not isinstance(plan["target_bytes"], int) or plan["target_bytes"] <= 0):
        raise ValueError("Invalid compaction plan format/settings")
    start, end = date.fromisoformat(plan["start"]), date.fromisoformat(plan["end"])
    validate_closed_range(start, end, today=today_vn())
    seen = set()
    for item in plan["days"]:
        definition = get_resource(item["resource"])
        day = DayManifest.model_validate(item["baseline"])
        if (not start <= day.source_date <= end or str(day.source_date) != item["date"]
                or day.resource != item["resource"] or day.source != definition.identity.source
                or day.status is not DayStatus.SUCCESS or not SAFE_ID.fullmatch(day.run_id)
                or (item["resource"], item["date"]) in seen):
            raise ValueError("Invalid or duplicate planned day")
        seen.add((item["resource"], item["date"]))
        keys = set()
        for obj in item["files"]:
            prefix = parquet_prefix(definition.identity, obj["table"], day.source_date, day.run_id)
            name = obj["key"].removeprefix(prefix + "/")
            if (obj["table"] not in definition.tables or not obj["key"].startswith(prefix + "/")
                    or not SAFE_FILE.fullmatch(name) or not name.endswith(".parquet") or obj["key"] in keys):
                raise ValueError("Invalid or duplicate planned Parquet path")
            keys.add(obj["key"])
        if not keys:
            raise ValueError("Compaction day has no files")
        for evidence in item["quality"]:
            if not SAFE_FILE.fullmatch(evidence["name"]) or not evidence["name"].endswith(".json"):
                raise ValueError("Invalid quality evidence path")


def assert_baseline(fs, item, guard, *, own_run_id=None):
    definition = get_resource(item["resource"])
    day = DayManifest.model_validate(item["baseline"])
    current = guard.read(day.source_date)
    if (current.effective != day or set(current.active_run_ids) - {own_run_id}):
        raise ValueError("Baseline changed or another attempt is active; create a new plan")
    if inventory(fs, definition, day.source_date, day.run_id) != item["files"]:
        raise ValueError("Baseline Parquet inventory changed; create a new plan")
    if quality_inventory(fs, definition.identity, day.source_date, day.run_id) != item["quality"]:
        raise ValueError("Baseline quality evidence changed; create a new plan")
    pages = list_page_manifests(fs, definition.identity, run_id=day.run_id, source_date=day.source_date)
    if [p.model_dump(mode="json") for p in pages] != item["pages"]:
        raise ValueError("Baseline page manifests changed; create a new plan")
    _metadata(definition.identity, read_run_manifest(fs, definition.identity, day.run_id), day, pages)


def _verify_receipt(fs, item, receipt, scratch):
    """Verify uploaded bytes, hashes and lineage before commit and when recovering SUCCESS."""
    definition = get_resource(item["resource"])
    day = date.fromisoformat(item["date"])
    expected = receipt["outputs"]
    actual_keys = {(f["table"], f["key"]) for f in inventory(fs, definition, day, receipt["run_id"])}
    if actual_keys != {(f["table"], f["key"]) for f in expected}:
        raise ValueError("Compacted file inventory mismatch")
    for index, obj in enumerate(expected):
        local = Path(scratch) / f"uploaded-{index}.parquet"
        with fs.open(obj["key"], "rb") as source, local.open("wb") as target:
            actual = copy_file(source, target)
        if actual != {k: obj[k] for k in ("size", "sha256")}:
            raise ValueError("Uploaded compaction checksum mismatch")
    selected = (CommittedDay(day, receipt["run_id"], item["baseline"]["bronze_records"],
                             tuple((f["table"], f["key"]) for f in expected)),)
    return verify_committed(fs, selected)


def _finish_run(fs, identity, run_id):
    run = read_run_manifest(fs, identity, run_id)
    if run is None:
        raise UnconfirmedCompaction("Committed day has no RunManifest")
    if run.status is RunStatus.SUCCESS:
        return
    write_run_manifest(fs, identity, run.model_copy(update={
        "status": RunStatus.SUCCESS, "success_dates": 1, "failed_dates": 0,
        "completed_at": datetime.now(UTC),
    }))


def apply_day(fs, plan, item, directory, guard):
    definition = get_resource(item["resource"])
    identity = definition.identity
    baseline = DayManifest.model_validate(item["baseline"])
    day = baseline.source_date
    directory = Path(directory)
    checkpoint = directory / "days" / f"{identity.resource}-{day}.json"
    if checkpoint.exists():
        previous = read_json(checkpoint)
        if previous["plan_hash"] != plan["plan_hash"]:
            raise ValueError("Checkpoint belongs to another plan")
        persisted = read_day_manifest(fs, identity, previous["run_id"], day)
        if persisted is not None and persisted.status is DayStatus.SUCCESS:
            _metadata(identity, read_run_manifest(fs, identity, previous["run_id"]), persisted,
                      list_page_manifests(fs, identity, run_id=previous["run_id"], source_date=day))
            with TemporaryDirectory(prefix="compact-verify-", dir=directory) as scratch:
                _verify_receipt(fs, item, previous, scratch)
            _finish_run(fs, identity, previous["run_id"])
            previous["status"] = "success"
            write_json(checkpoint, previous)
            return previous
        if previous["status"] in {"running", "commit_uncertain", "success"}:
            raise UnconfirmedCompaction("Previous worker/commit is unconfirmed; inspect before retry")
        if previous["status"] == "no_benefit":
            return previous
    assert_baseline(fs, item, guard)
    started = datetime.now(UTC)
    if started <= baseline.started_at:
        raise ValueError("System time must be later than the baseline started_at")
    run_id = uuid4().hex
    receipt = {"plan_hash": plan["plan_hash"], "resource": identity.resource, "date": str(day),
               "run_id": run_id, "baseline_run_id": baseline.run_id, "status": "running",
               "target_bytes": plan["target_bytes"], "compression": "zstd",
               "input_files": len(item["files"]), "input_bytes": item["input_bytes"], "outputs": []}
    if checkpoint.exists():
        # Preserve retry history instead of losing the reference to an earlier failed attempt.
        old = read_json(checkpoint)
        write_json(directory / "attempts" / f"{old['run_id']}.json", old)
    write_json(checkpoint, receipt)
    run = RunManifest(run_id=run_id, source=identity.source, resource=identity.resource,
                      start_date=day, end_date=day, total_dates=1, status=RunStatus.RUNNING, started_at=started)
    marker = DayManifest(run_id=run_id, source=identity.source, resource=identity.resource,
                         source_date=day, status=DayStatus.RUNNING, started_at=started)
    stage = "start"
    logger.info("compact_started resource=%s date=%s baseline=%s run_id=%s files=%s",
                identity.resource, day, baseline.run_id, run_id, len(item["files"]))
    try:
        write_run_manifest(fs, identity, run)
        write_day_manifest(fs, identity, marker)
        with ExecutionHeartbeat(fs, identity, run_id), TemporaryDirectory(prefix="compact-", dir=directory) as scratch:
            scratch = Path(scratch)
            inputs = defaultdict(list)
            for index, obj in enumerate(item["files"]):
                stage = "download"
                local = scratch / f"source-{index}.parquet"
                with fs.open(obj["key"], "rb") as source, local.open("wb") as target:
                    digest = copy_file(source, target)
                if digest["size"] != obj["signature"]["size"]:
                    raise ValueError("Downloaded baseline size changed")
                inputs[obj["table"]].append(local)
            outputs, counts = {}, {}
            for table, paths in inputs.items():
                stage = "rewrite"
                outputs[table], counts[table] = rewrite_table(
                    paths, scratch / table, baseline_run_id=baseline.run_id, run_id=run_id,
                    source_date=day, target_bytes=plan["target_bytes"])
                stage = "verify_multiset"
                verify_multiset(paths, outputs[table], scratch / "spill")
            if sum(counts.values()) != baseline.bronze_records:
                raise ValueError("Source count differs from DayManifest")
            output_counts = {table: sum(pq.ParquetFile(p).metadata.num_rows for p in paths)
                             for table, paths in outputs.items()}
            if output_counts != counts:
                raise ValueError("Compacted table counts differ from source")
            receipt.update(records_by_table=counts, output_files=sum(map(len, outputs.values())),
                           output_records_by_table=output_counts,
                           input_records=sum(counts.values()), output_records=sum(output_counts.values()),
                           output_bytes=sum(p.stat().st_size for paths in outputs.values() for p in paths))
            if receipt["output_files"] >= receipt["input_files"]:
                receipt["status"] = "no_benefit"
                # An uncommitted candidate cannot replace the baseline or block later ingestion.
                write_day_manifest(fs, identity, marker.model_copy(update={
                    "status": DayStatus.FAILED, "completed_at": datetime.now(UTC)}))
                write_run_manifest(fs, identity, run.model_copy(update={
                    "status": RunStatus.FAILED, "failed_dates": 1, "completed_at": datetime.now(UTC)}))
                write_storage_json(fs, provenance_key(identity, run_id), receipt)
                write_json(checkpoint, receipt)
                return receipt
            stage = "upload"
            for table, paths in outputs.items():
                for path in paths:
                    key = f"{parquet_prefix(identity, table, day, run_id)}/{path.name}"
                    expected = digest_file(path)
                    with path.open("rb") as source, fs.open(key, "wb") as target:
                        if copy_file(source, target) != expected:
                            raise ValueError("Local compaction changed while uploading")
                    receipt["outputs"].append({"table": table, "key": key, **expected})
            stage = "copy_quality"
            for evidence in item["quality"]:
                source_key = f"{quality_prefix(identity, baseline.run_id, day)}/{evidence['name']}"
                target_key = f"{quality_prefix(identity, run_id, day)}/{evidence['name']}"
                # Copy bytes, not re-interpret old validation results as a fresh assessment.
                raw = fs.cat_file(source_key)
                with fs.open(target_key, "wb") as target:
                    target.write(raw)
                if fs.cat_file(target_key) != raw:
                    raise ValueError("Quality evidence copy mismatch")
            stage = "verify_uploaded"
            receipt["verification"] = _verify_receipt(fs, item, receipt, scratch)
            receipt["verification"].update(multiset_equal=True, table_counts_equal=True,
                                            uploaded_checksums_equal=True, hash_and_lineage_valid=True)
            completed_at = datetime.now(UTC)
            for page in item["pages"]:
                copied = PageManifest.model_validate(page).model_copy(update={
                    "run_id": run_id, "started_at": started, "completed_at": completed_at})
                write_page_manifest(fs, identity, copied)
            completed = baseline.model_copy(update={
                "run_id": run_id, "status": DayStatus.SUCCESS, "started_at": started,
                "completed_at": completed_at})
            _metadata(identity, run, completed, list_page_manifests(fs, identity, run_id=run_id, source_date=day))
            receipt["source_files"] = item["files"]
            receipt["status"] = "verified"
            write_storage_json(fs, provenance_key(identity, run_id), receipt)
            stage = "verify_baseline_before_commit"
            assert_baseline(fs, item, guard, own_run_id=run_id)
            # Write-ahead checkpoint covers a lost acknowledgement or post-commit process exit.
            receipt["status"] = "commit_uncertain"
            write_json(checkpoint, receipt)
            stage = "commit"
            commit_day_manifest(fs, identity, completed)
    except DayCommitUncertainError:
        raise
    except BaseException as exc:
        receipt.update(stage=stage, error=safe_error(exc))
        if stage == "commit":
            # Ctrl+C can interrupt the upload while the remote commit is still completing.
            # Absence/RUNNING on immediate readback is not proof of failure.
            receipt["status"] = "commit_uncertain"
            write_json(checkpoint, receipt)
            raise
        try:
            persisted = read_day_manifest(fs, identity, run_id, day)
        except Exception as check_error:
            receipt["status"] = "commit_uncertain"
            write_json(checkpoint, receipt)
            raise UnconfirmedCompaction("Cannot confirm the candidate day state") from check_error
        if persisted is not None and persisted.status is DayStatus.SUCCESS:
            receipt["status"] = "commit_uncertain"
            write_json(checkpoint, receipt)
            raise
        try:
            write_day_manifest(fs, identity, marker.model_copy(update={
                "status": DayStatus.FAILED, "error_count": 1, "completed_at": datetime.now(UTC)}))
            write_run_manifest(fs, identity, run.model_copy(update={
                "status": RunStatus.FAILED, "failed_dates": 1, "completed_at": datetime.now(UTC)}))
        except Exception as mark_error:
            receipt["status"] = "commit_uncertain"
            write_json(checkpoint, receipt)
            raise UnconfirmedCompaction("Cannot confirm failure markers") from mark_error
        receipt["status"] = "failed"
        write_json(checkpoint, receipt)
        raise
    _finish_run(fs, identity, run_id)
    receipt["status"] = "success"
    write_json(checkpoint, receipt)
    return receipt


def run_plan(fs, plan, directory, *, continue_on_error=False, lock_dir=None):
    validate_plan(plan)
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    report = {"plan_id": plan["plan_id"], "plan_hash": plan["plan_hash"], "days": [],
              "skipped": plan["skipped"], "complete": False}
    guards = {}
    with execution_lock(Path(lock_dir or settings.INGESTION_LOCK_DIR)):
        for item in plan["days"]:
            name = item["resource"]
            if name not in guards:
                guards[name] = CoverageGuard(fs, get_resource(name).identity)
            try:
                receipt = apply_day(fs, plan, item, directory, guards[name])
                report["days"].append(receipt)
                logger.info("compact_day resource=%s date=%s status=%s files=%s->%s", name, item["date"],
                            receipt["status"], receipt["input_files"], receipt.get("output_files"))
            except BaseException as exc:
                checkpoint = directory / "days" / f"{name}-{item['date']}.json"
                saved = read_json(checkpoint) if checkpoint.exists() else {}
                report["days"].append({**saved, "resource": name, "date": item["date"],
                                       "error": safe_error(exc), "status": saved.get("status", "blocked")})
                write_json(directory / "report.json", report)
                # Unknown commits and interrupts always stop, irrespective of continue-on-error.
                if (not continue_on_error or not isinstance(exc, Exception)
                        or isinstance(exc, (DayCommitUncertainError, UnconfirmedCompaction))
                        or saved.get("status") in {"running", "commit_uncertain", "success"}):
                    raise
            write_json(directory / "report.json", report)
    report["complete"] = all(row["status"] in {"success", "no_benefit"} for row in report["days"])
    write_json(directory / "report.json", report)
    return report
