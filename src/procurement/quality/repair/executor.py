"""Execution engine for applying Bronze quality repair plans."""

import sys
import uuid
from collections import defaultdict
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from procurement.common.catalog import get_resource
from procurement.common.settings import settings
from procurement.ingestion.engine.daily_runner import _create_pipeline
from procurement.ingestion.engine.models import ResourceSpec
from procurement.jobs.lock import execution_lock
from procurement.models.control import (
    DayManifest,
    DayStatus,
    PageManifest,
    PageStatus,
    RunManifest,
    RunStatus,
)
from procurement.quality.audit import record_key
from procurement.quality.coverage import CoverageGuard
from procurement.quality.files import now, read_json, safe_error, write_json
from procurement.quality.repair.planner import validate_plan
from procurement.quality.repair.reconstruction import reconstruct
from procurement.quality.repair.verification import assert_baseline, read_baseline, verify_output
from procurement.quality.storage import quality_prefix, save_quality_page
from procurement.storage.bronze import BufferedBronzeWriter, DltBronzeWriter
from procurement.storage.control import (
    DayCommitUncertainError,
    commit_day_manifest,
    read_day_manifest,
    read_run_manifest,
    write_day_manifest,
    write_page_manifest,
    write_run_manifest,
)
from procurement.storage.execution import ExecutionHeartbeat
from procurement.storage.io import write_json as write_storage_json


def _get_save_quality_page():
    mod = sys.modules.get("procurement.quality.repair")
    if mod and "save_quality_page" in mod.__dict__:
        return mod.__dict__["save_quality_page"]
    return save_quality_page


def _get_commit_day_manifest():
    mod = sys.modules.get("procurement.quality.repair")
    if mod and "commit_day_manifest" in mod.__dict__:
        return mod.__dict__["commit_day_manifest"]
    return commit_day_manifest


def apply_day(
    fs: Any,
    client: Any,
    plan: dict[str, Any],
    planned_day: dict[str, Any],
    config: Any,
    work: Path | str,
    *,
    writer_factory: Callable[..., Any] | None = None,
    coverage_reader: Callable[..., Any] | None = None,
    detail_workers: int = 1,
) -> dict[str, Any]:
    """Execute repair on a single day partition with strict transactional verification."""
    selection = planned_day["selection"]
    identity = get_resource(plan["resource"]).identity
    day = date.fromisoformat(selection["date"])
    checkpoint = Path(work) / f"{day}.json"
    expected = {record_key(row): dict(row) for row in planned_day["records"]}
    if checkpoint.exists():
        previous = read_json(checkpoint)
        if previous["plan_hash"] != plan["plan_hash"]:
            raise ValueError("Repair checkpoint belongs to another plan")
        manifest = read_day_manifest(fs, identity, previous["run_id"], day)
        if manifest is not None and manifest.status is DayStatus.SUCCESS:
            for row in previous["outputs"]:
                expected[record_key(row)]["output_hash"] = row["output_hash"]
            verify_output(fs, identity, day, previous["run_id"], len(expected), expected, config)
            previous_run = read_run_manifest(fs, identity, previous["run_id"])
            if previous_run and previous_run.status is not RunStatus.SUCCESS:
                write_run_manifest(
                    fs,
                    identity,
                    previous_run.model_copy(
                        update={
                            "status": RunStatus.SUCCESS,
                            "success_dates": 1,
                            "failed_dates": 0,
                            "completed_at": datetime.now(UTC),
                        }
                    ),
                )
            previous["status"] = "success"
            write_json(checkpoint, previous)
            return previous
        if previous.get("status") in {"running", "commit_uncertain"}:
            raise ValueError("Previous repair worker/commit is unconfirmed; inspect before retry")
    coverage_reader = coverage_reader or CoverageGuard(fs, identity).read
    assert_baseline(fs, identity, selection, coverage_reader)
    baseline = read_baseline(fs, selection, planned_day["records"])
    run_id = uuid.uuid4().hex
    receipt: dict[str, Any] = {"plan_hash": plan["plan_hash"], "run_id": run_id, "date": str(day), "status": "running"}
    write_json(checkpoint, receipt)
    started = datetime.now(UTC)
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
    manifest = DayManifest(
        run_id=run_id,
        source=identity.source,
        resource=identity.resource,
        source_date=day,
        status=DayStatus.RUNNING,
        started_at=started,
    )
    write_run_manifest(fs, identity, run)
    write_day_manifest(fs, identity, manifest)
    stage = "fetch_replacements"
    save_sidecar_fn = _get_save_quality_page()
    commit_day_fn = _get_commit_day_manifest()
    try:
        with ExecutionHeartbeat(fs, identity, run_id):
            tables, observations, counts = reconstruct(
                client,
                baseline,
                planned_day["records"],
                config,
                run_id,
                Path(work) / "replacements",
                detail_workers=detail_workers,
            )
            # Our own RUNNING attempt is excluded; other attempts block publication.
            stage = "verify_baseline"
            coverage = coverage_reader(day)
            if (
                coverage.effective is None
                or coverage.effective.run_id != selection["run_id"]
                or set(coverage.active_run_ids) - {run_id}
            ):
                raise ValueError("Baseline changed or another worker started")
            read_baseline(fs, selection, planned_day["records"])
            spec = ResourceSpec(identity, "quality_repair", identity.source, lambda **_: {}, lambda **_: iter(()))
            writer = (
                writer_factory(run_id, day)
                if writer_factory
                else DltBronzeWriter(lambda: _create_pipeline(spec, day, run_id))
            )
            stage = "write_bronze"
            if writer_factory:
                count = writer.write_page(tables)
            else:
                buffered = BufferedBronzeWriter(writer)
                chunk: dict[str, list[Any]] = defaultdict(list)
                chunk_count = 0
                for table, records in tables.items():
                    for record in records:
                        chunk[table].append(record)
                        chunk_count += 1
                        if chunk_count >= 50:
                            buffered.write_page(dict(chunk))
                            chunk, chunk_count = defaultdict(list), 0
                if chunk_count:
                    buffered.write_page(dict(chunk))
                buffered.flush()
                count = buffered.persisted_records
            if count != len(expected):
                raise ValueError("Repair writer count mismatch")
            stage = "write_quality_sidecar"
            save_sidecar_fn(fs, identity, run_id, day, 0, config.fingerprint, observations)
            receipt.update(counts=counts, config_hash=config.fingerprint)
            receipt["outputs"] = []
            for records in tables.values():
                for record in records:
                    key = record.source_id, record.source_version
                    expected[key]["output_hash"] = record.content_hash
                    receipt["outputs"].append(
                        {"source_id": key[0], "source_version": key[1], "output_hash": record.content_hash}
                    )
            write_storage_json(fs, f"{quality_prefix(identity, run_id, day)}/repair.json", receipt)
            stage = "verify_output"
            verify_output(fs, identity, day, run_id, count, expected, config)
            write_page_manifest(
                fs,
                identity,
                PageManifest(
                    run_id=run_id,
                    source_date=day,
                    page_number=0,
                    page_size=max(1, count),
                    search_items=count,
                    bronze_records=count,
                    status=PageStatus.SUCCESS,
                    started_at=started,
                    completed_at=datetime.now(UTC),
                ),
            )
            # Verify again after writes, immediately before the commit marker.
            stage = "verify_baseline_before_commit"
            coverage = coverage_reader(day)
            if (
                coverage.effective is None
                or coverage.effective.run_id != selection["run_id"]
                or set(coverage.active_run_ids) - {run_id}
            ):
                raise ValueError("Baseline changed before commit")
            read_baseline(fs, selection, planned_day["records"])
            completed = manifest.model_copy(
                update={
                    "status": DayStatus.SUCCESS,
                    "bronze_records": count,
                    "search_items": count,
                    "expected_pages": 1,
                    "completed_pages": 1,
                    "completed_at": datetime.now(UTC),
                }
            )
            receipt["status"] = "commit_uncertain"
            write_json(checkpoint, receipt)
            stage = "commit"
            commit_day_fn(fs, identity, completed)
    except DayCommitUncertainError:
        raise  # never downgrade a commit whose acknowledgement was lost
    except BaseException as exc:
        # A failure after commit must never replace SUCCESS with FAILED.
        persisted = read_day_manifest(fs, identity, run_id, day)
        if persisted is not None and persisted.status is DayStatus.SUCCESS:
            raise
        write_day_manifest(
            fs,
            identity,
            manifest.model_copy(
                update={"status": DayStatus.FAILED, "error_count": 1, "completed_at": datetime.now(UTC)}
            ),
        )
        write_run_manifest(
            fs,
            identity,
            run.model_copy(
                update={"status": RunStatus.FAILED, "failed_dates": 1, "completed_at": datetime.now(UTC)}
            ),
        )
        receipt.update(status="failed", stage=stage, error=safe_error(exc))
        write_json(checkpoint, receipt)
        raise
    write_run_manifest(
        fs,
        identity,
        run.model_copy(
            update={"status": RunStatus.SUCCESS, "success_dates": 1, "completed_at": datetime.now(UTC)}
        ),
    )
    receipt["status"] = "success"
    write_json(checkpoint, receipt)
    return receipt


def apply_plan(
    fs: Any,
    client: Any,
    plan: dict[str, Any],
    config: Any,
    work: Path | str,
    *,
    limit: int | None = None,
    writer_factory: Callable[..., Any] | None = None,
    detail_workers: int = 1,
    dates: list[date] | None = None,
    continue_on_error: bool = False,
) -> list[dict[str, Any]]:
    """Apply plan across target days with execution lock and checkpointing."""
    validate_plan(plan, config)
    if not 1 <= detail_workers <= 32:
        raise ValueError("detail_workers must be between 1 and 32")
    receipts: list[dict[str, Any]] = []
    selected = plan["days"]
    if dates is not None:
        requested = {str(day) for day in dates}
        available = {day["selection"]["date"] for day in selected}
        if requested - available:
            raise ValueError("Requested date is absent from the repair plan")
        selected = [day for day in selected if day["selection"]["date"] in requested]
    with execution_lock(Path(settings.INGESTION_LOCK_DIR)):
        guard = CoverageGuard(fs, get_resource(plan["resource"]).identity)
        progress: dict[str, dict[str, Any]] = {}

        def compact(r: dict[str, Any]) -> dict[str, Any]:
            return {
                key: r[key] for key in ("date", "run_id", "status", "counts", "stage", "error") if key in r
            }

        def save_progress(r: dict[str, Any]) -> None:
            progress[r["date"]] = compact(r)
            completed = sum(item["status"] == "success" for item in progress.values())
            write_json(
                Path(work) / "summary.json",
                {
                    "plan_hash": plan["plan_hash"],
                    "days": [progress[key] for key in sorted(progress)],
                    "planned_days": len(plan["days"]),
                    "successful_days": completed,
                    "updated_at": now(),
                    "status": "complete" if completed == len(plan["days"]) else "partial",
                },
            )

        for day in plan["days"]:
            path = Path(work) / f"{day['selection']['date']}.json"
            if path.exists():
                previous = read_json(path)
                if previous.get("plan_hash") != plan["plan_hash"]:
                    raise ValueError("Repair checkpoint belongs to another plan")
                progress[previous["date"]] = compact(previous)
        for index, day in enumerate(selected[:limit], 1):
            previous = progress.get(day["selection"]["date"], {})
            action = (
                "verifying committed attempt (no detail fetch)"
                if previous.get("status") == "success"
                else "checking baseline and replacement cache"
            )
            print(f"repair [{index}/{len(selected[:limit])}] {day['selection']['date']}: {action}", flush=True)
            try:
                receipt = apply_day(
                    fs,
                    client,
                    plan,
                    day,
                    config,
                    work,
                    writer_factory=writer_factory,
                    coverage_reader=guard.read,
                    detail_workers=detail_workers,
                )
            except BaseException as exc:
                receipt = {"date": day["selection"]["date"], "status": "blocked", "error": safe_error(exc)}
                path = Path(work) / f"{receipt['date']}.json"
                if path.exists():
                    saved = read_json(path)
                    receipt.update({key: saved[key] for key in ("run_id", "stage") if key in saved})
                    if saved.get("status") in {"commit_uncertain", "running", "failed"}:
                        receipt["status"] = saved["status"]
                save_progress(receipt)
                if not isinstance(exc, ValueError) or not continue_on_error:
                    raise
            receipts.append(compact(receipt))
            print(f"repair {day['selection']['date']}: {receipts[-1]['status']} {receipts[-1].get('counts', {})}", flush=True)
            save_progress(receipt)
    return receipts
