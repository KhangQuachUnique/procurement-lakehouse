"""Compaction execution, Parquet rewriting, and day plan application."""

import json
import logging
from collections import defaultdict
from datetime import UTC, date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from procurement.common.catalog import get_resource
from procurement.common.settings import settings
from procurement.compaction.planner import (
    SAFE_FILE,
    SAFE_ID,
    _metadata,
    create_plan,
    inventory,
    namespace,
    object_signature,
    parquet_prefix,
    provenance_key,
    quality_inventory,
    validate_plan,
)
from procurement.compaction.recovery import UnconfirmedCompaction
from procurement.compaction.verification import (
    copy_file,
    digest_file,
    verify_multiset,
    verify_schema,
)

__all__ = [
    "SAFE_FILE",
    "SAFE_ID",
    "UnconfirmedCompaction",
    "_finish_run",
    "_metadata",
    "_verify_receipt",
    "apply_day",
    "assert_baseline",
    "copy_file",
    "create_plan",
    "digest_file",
    "inventory",
    "namespace",
    "object_signature",
    "parquet_prefix",
    "provenance_key",
    "quality_inventory",
    "rewrite_table",
    "run_plan",
    "validate_plan",
    "verify_multiset",
    "verify_schema",
]
from procurement.ingestion.engine.metadata import calculate_content_hash
from procurement.jobs.lock import execution_lock
from procurement.models.control import (
    DayManifest,
    DayStatus,
    PageManifest,
    RunManifest,
    RunStatus,
)
from procurement.quality.coverage import CoverageGuard
from procurement.quality.files import read_json, safe_error, write_json
from procurement.quality.storage import quality_prefix
from procurement.storage.committed import CommittedDay, verify_committed
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

logger = logging.getLogger(__name__)

BATCH_ROWS = 1024


def rewrite_table(
    paths: list[Path],
    directory: str | Path,
    *,
    baseline_run_id: str,
    run_id: str,
    source_date: date,
    target_bytes: int,
) -> tuple[list[Path], int]:
    """Preserve every cell except run_id. Rotate after a compressed row group reaches target."""
    dir_path = Path(directory)
    dir_path.mkdir(parents=True, exist_ok=True)
    schema = verify_schema(paths)
    writer = sink = None
    outputs: list[Path] = []
    rows = 0
    try:
        for path in paths:
            for batch in pq.ParquetFile(path).iter_batches(batch_size=BATCH_ROWS):
                for record in batch.to_pylist():
                    if (
                        record["run_id"] != baseline_run_id
                        or str(record["source_date"])[:10] != source_date.isoformat()
                    ):
                        raise ValueError("Source Bronze lineage mismatch")
                    payload = record["payload"]
                    if isinstance(payload, str):
                        payload = json.loads(payload)
                    if calculate_content_hash(payload) != record["content_hash"]:
                        raise ValueError("Source Bronze content hash mismatch")
                arrays = []
                for field in schema:
                    if field.name == "run_id":
                        column = pa.array([run_id] * batch.num_rows, type=field.type)
                    elif field.name in batch.schema.names:
                        column = batch.column(batch.schema.get_field_index(field.name)).cast(
                            field.type, safe=True
                        )
                    else:
                        column = pa.nulls(batch.num_rows, type=field.type)
                    arrays.append(column)
                if writer is None:
                    output = dir_path / f"compact-{len(outputs):06d}.parquet"
                    outputs.append(output)
                    sink = output.open("wb")
                    writer = pq.ParquetWriter(
                        sink, schema, compression="zstd", write_statistics=True
                    )
                writer.write_batch(pa.RecordBatch.from_arrays(arrays, schema=schema))
                rows += batch.num_rows
                if sink is not None and sink.tell() >= target_bytes:
                    writer.close()
                    sink.close()
                    writer = sink = None
    finally:
        if writer is not None:
            writer.close()
        if sink is not None:
            sink.close()
    if not rows and not outputs:
        # Preserve an existing empty table's schema if other tables make the day worthwhile.
        output = dir_path / "compact-000000.parquet"
        pq.write_table(pa.Table.from_batches([], schema=schema), output, compression="zstd")
        outputs.append(output)
    return outputs, rows


def assert_baseline(
    fs: Any,
    item: dict[str, Any],
    guard: CoverageGuard,
    *,
    own_run_id: str | None = None,
) -> None:
    definition = get_resource(item["resource"])
    day = DayManifest.model_validate(item["baseline"])
    current = guard.read(day.source_date)
    if current.effective != day or (set(current.active_run_ids) - {own_run_id}):
        raise ValueError("Baseline changed or another attempt is active; create a new plan")
    if inventory(fs, definition, day.source_date, day.run_id) != item["files"]:
        raise ValueError("Baseline Parquet inventory changed; create a new plan")
    if quality_inventory(fs, definition.identity, day.source_date, day.run_id) != item["quality"]:
        raise ValueError("Baseline quality evidence changed; create a new plan")
    pages = list_page_manifests(
        fs, definition.identity, run_id=day.run_id, source_date=day.source_date
    )
    if [p.model_dump(mode="json") for p in pages] != item["pages"]:
        raise ValueError("Baseline page manifests changed; create a new plan")
    _metadata(
        definition.identity,
        read_run_manifest(fs, definition.identity, day.run_id),
        day,
        pages,
    )


def _verify_receipt(
    fs: Any,
    item: dict[str, Any],
    receipt: dict[str, Any],
    scratch: Path,
) -> dict[str, Any]:
    """Verify uploaded bytes, hashes and lineage before commit and when recovering SUCCESS."""
    definition = get_resource(item["resource"])
    day = date.fromisoformat(item["date"])
    expected = receipt["outputs"]
    actual_keys = {
        (f["table"], f["key"]) for f in inventory(fs, definition, day, receipt["run_id"])
    }
    if actual_keys != {(f["table"], f["key"]) for f in expected}:
        raise ValueError("Compacted file inventory mismatch")
    for index, obj in enumerate(expected):
        local = scratch / f"uploaded-{index}.parquet"
        with fs.open(obj["key"], "rb") as source, local.open("wb") as target:
            actual = copy_file(source, target)
        if actual != {k: obj[k] for k in ("size", "sha256")}:
            raise ValueError("Uploaded compaction checksum mismatch")
    selected = (
        CommittedDay(
            day,
            receipt["run_id"],
            item["baseline"]["bronze_records"],
            tuple((f["table"], f["key"]) for f in expected),
        ),
    )
    return verify_committed(fs, selected)


def _finish_run(fs: Any, identity: Any, run_id: str) -> None:
    run = read_run_manifest(fs, identity, run_id)
    if run is None:
        raise UnconfirmedCompaction("Committed day has no RunManifest")
    if run.status is RunStatus.SUCCESS:
        return
    write_run_manifest(
        fs,
        identity,
        run.model_copy(
            update={
                "status": RunStatus.SUCCESS,
                "success_dates": 1,
                "failed_dates": 0,
                "completed_at": datetime.now(UTC),
            }
        ),
    )


def apply_day(
    fs: Any,
    plan: dict[str, Any],
    item: dict[str, Any],
    directory: Path,
    guard: CoverageGuard,
) -> dict[str, Any]:
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
            _metadata(
                identity,
                read_run_manifest(fs, identity, previous["run_id"]),
                persisted,
                list_page_manifests(fs, identity, run_id=previous["run_id"], source_date=day),
            )
            with TemporaryDirectory(prefix="compact-verify-", dir=directory) as scratch:
                _verify_receipt(fs, item, previous, Path(scratch))
            _finish_run(fs, identity, previous["run_id"])
            previous["status"] = "success"
            write_json(checkpoint, previous)
            return previous
        if previous["status"] in {"running", "commit_uncertain", "success"}:
            raise UnconfirmedCompaction(
                "Previous worker/commit is unconfirmed; inspect before retry"
            )
        if previous["status"] == "no_benefit":
            return previous
    assert_baseline(fs, item, guard)
    started = datetime.now(UTC)
    if started <= baseline.started_at:
        raise ValueError("System time must be later than the baseline started_at")
    run_id = uuid4().hex
    receipt: dict[str, Any] = {
        "plan_hash": plan["plan_hash"],
        "resource": identity.resource,
        "date": str(day),
        "run_id": run_id,
        "baseline_run_id": baseline.run_id,
        "status": "running",
        "target_bytes": plan["target_bytes"],
        "compression": "zstd",
        "input_files": len(item["files"]),
        "input_bytes": item["input_bytes"],
        "outputs": [],
    }
    if checkpoint.exists():
        old = read_json(checkpoint)
        write_json(directory / "attempts" / f"{old['run_id']}.json", old)
    write_json(checkpoint, receipt)
    run = RunManifest(
        run_id=run_id,
        source=identity.source,
        resource=identity.resource,
        start_date=day,
        end_date=day,
        total_dates=1,
        status=RunStatus.RUNNING,
        started_at=started,
    )
    marker = DayManifest(
        run_id=run_id,
        source=identity.source,
        resource=identity.resource,
        source_date=day,
        status=DayStatus.RUNNING,
        started_at=started,
    )
    stage = "start"
    logger.info(
        "compact_started resource=%s date=%s baseline=%s run_id=%s files=%s",
        identity.resource,
        day,
        baseline.run_id,
        run_id,
        len(item["files"]),
    )
    try:
        write_run_manifest(fs, identity, run)
        write_day_manifest(fs, identity, marker)
        with (
            ExecutionHeartbeat(fs, identity, run_id),
            TemporaryDirectory(prefix="compact-", dir=directory) as scratch_dir,
        ):
            scratch = Path(scratch_dir)
            inputs: dict[str, list[Path]] = defaultdict(list)
            for index, obj in enumerate(item["files"]):
                stage = "download"
                local = scratch / f"source-{index}.parquet"
                with fs.open(obj["key"], "rb") as source, local.open("wb") as target:
                    digest = copy_file(source, target)
                if digest["size"] != obj["signature"]["size"]:
                    raise ValueError("Downloaded baseline size changed")
                inputs[obj["table"]].append(local)
            outputs: dict[str, list[Path]] = {}
            counts: dict[str, int] = {}
            for table, paths in inputs.items():
                stage = "rewrite"
                outputs[table], counts[table] = rewrite_table(
                    paths,
                    scratch / table,
                    baseline_run_id=baseline.run_id,
                    run_id=run_id,
                    source_date=day,
                    target_bytes=plan["target_bytes"],
                )
                stage = "verify_multiset"
                verify_multiset(paths, outputs[table], scratch / "spill")
            if sum(counts.values()) != baseline.bronze_records:
                raise ValueError("Source count differs from DayManifest")
            output_counts = {
                table: sum(pq.ParquetFile(p).metadata.num_rows for p in p_list)
                for table, p_list in outputs.items()
            }
            if output_counts != counts:
                raise ValueError("Compacted table counts differ from source")
            receipt.update(
                records_by_table=counts,
                output_files=sum(map(len, outputs.values())),
                output_records_by_table=output_counts,
                input_records=sum(counts.values()),
                output_records=sum(output_counts.values()),
                output_bytes=sum(p.stat().st_size for paths in outputs.values() for p in paths),
            )
            if receipt["output_files"] >= receipt["input_files"]:
                receipt["status"] = "no_benefit"
                write_day_manifest(
                    fs,
                    identity,
                    marker.model_copy(
                        update={"status": DayStatus.FAILED, "completed_at": datetime.now(UTC)}
                    ),
                )
                write_run_manifest(
                    fs,
                    identity,
                    run.model_copy(
                        update={
                            "status": RunStatus.FAILED,
                            "failed_dates": 1,
                            "completed_at": datetime.now(UTC),
                        }
                    ),
                )
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
                raw = fs.cat_file(source_key)
                with fs.open(target_key, "wb") as target:
                    target.write(raw)
                if fs.cat_file(target_key) != raw:
                    raise ValueError("Quality evidence copy mismatch")
            stage = "verify_uploaded"
            receipt["verification"] = _verify_receipt(fs, item, receipt, scratch)
            receipt["verification"].update(
                multiset_equal=True,
                table_counts_equal=True,
                uploaded_checksums_equal=True,
                hash_and_lineage_valid=True,
            )
            completed_at = datetime.now(UTC)
            for page in item["pages"]:
                copied = PageManifest.model_validate(page).model_copy(
                    update={
                        "run_id": run_id,
                        "started_at": started,
                        "completed_at": completed_at,
                    }
                )
                write_page_manifest(fs, identity, copied)
            completed = baseline.model_copy(
                update={
                    "run_id": run_id,
                    "status": DayStatus.SUCCESS,
                    "started_at": started,
                    "completed_at": completed_at,
                }
            )
            _metadata(
                identity,
                run,
                completed,
                list_page_manifests(fs, identity, run_id=run_id, source_date=day),
            )
            receipt["source_files"] = item["files"]
            receipt["status"] = "verified"
            write_storage_json(fs, provenance_key(identity, run_id), receipt)
            stage = "verify_baseline_before_commit"
            assert_baseline(fs, item, guard, own_run_id=run_id)
            receipt["status"] = "commit_uncertain"
            write_json(checkpoint, receipt)
            stage = "commit"
            commit_day_manifest(fs, identity, completed)
    except DayCommitUncertainError:
        raise
    except BaseException as exc:
        receipt.update(stage=stage, error=safe_error(exc))
        if stage == "commit":
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
            write_day_manifest(
                fs,
                identity,
                marker.model_copy(
                    update={
                        "status": DayStatus.FAILED,
                        "error_count": 1,
                        "completed_at": datetime.now(UTC),
                    }
                ),
            )
            write_run_manifest(
                fs,
                identity,
                run.model_copy(
                    update={
                        "status": RunStatus.FAILED,
                        "failed_dates": 1,
                        "completed_at": datetime.now(UTC),
                    }
                ),
            )
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


def run_plan(
    fs: Any,
    plan: dict[str, Any],
    directory: Path,
    *,
    continue_on_error: bool = False,
    lock_dir: Path | str | None = None,
) -> dict[str, Any]:
    """Execute a validated compaction plan under the execution lock."""

    validate_plan(plan)
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {
        "plan_id": plan["plan_id"],
        "plan_hash": plan["plan_hash"],
        "days": [],
        "skipped": plan["skipped"],
        "complete": False,
    }
    guards: dict[str, CoverageGuard] = {}
    with execution_lock(Path(lock_dir or settings.INGESTION_LOCK_DIR)):
        for item in plan["days"]:
            name = item["resource"]
            if name not in guards:
                guards[name] = CoverageGuard(fs, get_resource(name).identity)
            try:
                receipt = apply_day(fs, plan, item, directory, guards[name])
                report["days"].append(receipt)
                logger.info(
                    "compact_day resource=%s date=%s status=%s files=%s->%s",
                    name,
                    item["date"],
                    receipt["status"],
                    receipt["input_files"],
                    receipt.get("output_files"),
                )
            except BaseException as exc:
                checkpoint = directory / "days" / f"{name}-{item['date']}.json"
                saved = read_json(checkpoint) if checkpoint.exists() else {}
                report["days"].append(
                    {
                        **saved,
                        "resource": name,
                        "date": item["date"],
                        "error": safe_error(exc),
                        "status": saved.get("status", "blocked"),
                    }
                )
                write_json(directory / "report.json", report)
                if (
                    not continue_on_error
                    or not isinstance(exc, Exception)
                    or isinstance(exc, (DayCommitUncertainError, UnconfirmedCompaction))
                    or saved.get("status") in {"running", "commit_uncertain", "success"}
                ):
                    raise
            write_json(directory / "report.json", report)
    report["complete"] = all(
        row.get("status") in {"success", "no_benefit"} for row in report["days"]
    )
    write_json(directory / "report.json", report)
    return report
