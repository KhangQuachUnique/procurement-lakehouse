"""Import verified Bronze bundles into destination object store."""

import logging
import sys
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any
from zipfile import ZipFile

from procurement.common.catalog import get_resource
from procurement.common.settings import settings
from procurement.ingestion.coverage import read_coverage
from procurement.jobs.lock import execution_lock
from procurement.models.control import DayManifest, RunManifest
from procurement.storage.committed import verify_committed
from procurement.storage.control import (
    commit_day_manifest,
    list_day_manifests,
    read_day_manifest,
    read_run_manifest,
    write_run_manifest,
)
from procurement.transfer.archive import (
    BundleDay,
    ValidatedArchive,
    copy_digest,
    project_import_run,
    selection_for,
    year_dates,
)

logger = logging.getLogger(__name__)


class TransferError(RuntimeError):
    def __init__(self, message: str, report: dict[str, Any]) -> None:
        super().__init__(message)
        self.report = report


@dataclass
class ImportRun:
    source: RunManifest
    current: RunManifest | None
    days: dict[date, DayManifest]


def _bucket() -> str:
    return settings.OBJECT_STORAGE_BUCKET.replace("\\", "/").rstrip("/")


def _key(relative: str) -> str:
    return f"{_bucket()}/{relative}"


def _relative(key: str) -> str:
    prefix = _bucket() + "/"
    if not key.startswith(prefix):
        raise ValueError("Object listing returned a key outside the configured bucket")
    return key.removeprefix(prefix)


def _lock(lock_dir: Path | str | None) -> Any:
    return execution_lock(Path(lock_dir or settings.INGESTION_LOCK_DIR))


def _get_commit_day_manifest():
    for mod_name in ("procurement.storage.transfer", "procurement.transfer.importer", "procurement.transfer"):
        mod = sys.modules.get(mod_name)
        if mod and "commit_day_manifest" in mod.__dict__:
            return mod.__dict__["commit_day_manifest"]
    return commit_day_manifest


def _get_copy_digest():
    for mod_name in ("procurement.storage.transfer", "procurement.transfer.importer", "procurement.transfer"):
        mod = sys.modules.get(mod_name)
        if mod and "copy_digest" in mod.__dict__:
            return mod.__dict__["copy_digest"]
    return copy_digest


def _get_put_missing():
    for mod_name in ("procurement.storage.transfer", "procurement.transfer.importer", "procurement.transfer"):
        mod = sys.modules.get(mod_name)
        if mod and "_put_missing" in mod.__dict__:
            return mod.__dict__["_put_missing"]
    return _put_missing


def _get_read_coverage():
    for mod_name in ("procurement.storage.transfer", "procurement.transfer.importer", "procurement.transfer"):
        mod = sys.modules.get(mod_name)
        if mod and "read_coverage" in mod.__dict__:
            return mod.__dict__["read_coverage"]
    return read_coverage


def inspect_bundle(path: Path | str) -> dict[str, Any]:
    with ZipFile(path) as archive:
        return {"mode": "inspect", **ValidatedArchive(archive).report()}


def _preflight(
    fs: Any, bundle: ValidatedArchive, report: dict[str, Any]
) -> tuple[list[BundleDay], dict[str, ImportRun]]:
    index = bundle.index
    dates = year_dates(index.year)
    read_cov = _get_read_coverage()
    coverage = {
        name: {day.source_date: day for day in read_cov(fs, get_resource(name).identity, dates[0], dates[-1])}
        for name in index.resources
    }
    digest_fn = _get_copy_digest()
    planned = []
    for entry in index.days:
        day = coverage[entry.resource][entry.source_date]
        detail = {"resource": entry.resource, "date": str(entry.source_date), "run_id": entry.run_id}
        if day.effective is not None:
            report["skipped_days"].append(detail)
            continue
        if day.active_run_ids:
            report["blocked_days"].append({**detail, "active_runs": list(day.active_run_ids)})
            continue
        planned.append(entry)
        report["planned_days"].append(detail)
        expected = set(entry.objects)
        for table in get_resource(entry.resource).tables:
            prefix = _key(entry.parquet_prefix(table))
            for key in fs.glob(f"{prefix}/*.parquet"):
                relative = _relative(key)
                if relative not in expected:
                    report["conflicts"].append({**detail, "key": relative, "reason": "extra_parquet"})
        for key in entry.objects:
            if key == entry.run_key:
                continue  # Range-run summaries are projected, not copied byte-for-byte.
            if fs.exists(_key(key)):
                with fs.open(_key(key), "rb") as source:
                    if digest_fn(source) != index.objects[key]:
                        report["conflicts"].append({**detail, "key": key, "reason": "different_content"})
    runs = _preflight_runs(fs, bundle, planned, report)
    if report["blocked_days"] or report["conflicts"]:
        raise TransferError("Import preflight failed; no objects written", report)
    return planned, runs


def _preflight_runs(
    fs: Any, bundle: ValidatedArchive, planned: list[BundleDay], report: dict[str, Any]
) -> dict[str, ImportRun]:
    runs: dict[str, ImportRun] = {}
    groups: dict[str, list[BundleDay]] = {}
    for entry in planned:
        groups.setdefault(entry.run_key, []).append(entry)
    for key, entries in groups.items():
        entry = entries[0]
        source = bundle.run_manifests[key]
        identity = get_resource(entry.resource).identity
        current = read_run_manifest(fs, identity, entry.run_id)
        days = list_day_manifests(fs, identity, run_id=entry.run_id)
        reason = None
        if len({day.source_date for day in days}) != len(days):
            reason = "duplicate_destination_run_days"
        elif any(day.status.value != "success" for day in days):
            reason = "destination_run_has_uncommitted_days"
        else:
            # A crash can leave the projected summary one day ahead of day.json.
            # Accept only exact summaries of committed days, optionally plus that pending day.
            candidates = [project_import_run(source, days)] if days else []
            existing_dates = {day.source_date for day in days}
            for pending in bundle.index.days:
                if pending.run_key == key and pending.source_date not in existing_dates:
                    candidates.append(project_import_run(source, [*days, bundle.day_manifests[pending.day_key]]))
            if current is not None and current not in candidates:
                reason = "incompatible_destination_run_summary"
        if reason:
            report["conflicts"].append(
                {"resource": entry.resource, "run_id": entry.run_id, "key": key, "reason": reason}
            )
        else:
            runs[key] = ImportRun(source, current, {day.source_date: day for day in days})
    return runs


def _put_run(
    fs: Any, entry: BundleDay, day: DayManifest, state: ImportRun, report: dict[str, Any]
) -> None:
    identity = get_resource(entry.resource).identity
    days = {**state.days, day.source_date: day}
    projected = project_import_run(state.source, list(days.values()))
    if read_run_manifest(fs, identity, entry.run_id) != state.current:
        raise ValueError(f"Destination run changed since preflight: {entry.run_id}")
    if projected == state.current:
        report["reused_objects"] += 1
    else:
        write_run_manifest(fs, identity, projected)
        if read_run_manifest(fs, identity, entry.run_id) != projected:
            raise ValueError(f"Destination run readback mismatch: {entry.run_id}")
        report["uploaded_objects"] += 1
        report["uploaded_bytes"] += fs.info(_key(entry.run_key))["size"]
    state.current, state.days = projected, days


def _put_missing(fs: Any, bundle: ValidatedArchive, key: str, report: dict[str, Any]) -> None:
    digest_fn = _get_copy_digest()
    if fs.exists(_key(key)):
        with fs.open(_key(key), "rb") as source:
            if digest_fn(source) != bundle.index.objects[key]:
                raise ValueError(f"Object changed since preflight: {key}")
        report["reused_objects"] += 1
        return
    # Do not publish a short object when interruption closes a partially written stream.
    target = fs.open(_key(key), "wb", autocommit=False)
    try:
        with bundle.archive.open(f"objects/{key}") as source:
            digest = digest_fn(source, target)
        if digest != bundle.index.objects[key]:
            raise ValueError(f"Archive changed during import: {key}")
        target.close()
        target.commit()
    except BaseException:
        try:
            try:
                target.close()
            finally:
                target.discard()
        except Exception:  # noqa: BLE001 -- preserve the original upload/interruption error
            logger.warning("Could not discard unpublished upload")
        raise
    report["uploaded_objects"] += 1
    report["uploaded_bytes"] += digest.size


def _check_objects(fs: Any, bundle: ValidatedArchive, keys: list[str]) -> None:
    digest_fn = _get_copy_digest()
    for key in keys:
        with fs.open(_key(key), "rb") as source:
            if digest_fn(source) != bundle.index.objects[key]:
                raise ValueError(f"Destination checksum mismatch: {key}")


def _confirm_coverage(fs: Any, planned: list[BundleDay], year: int) -> None:
    """One scan per resource, avoiding quadratic S3 manifest reads for a full year."""
    dates = year_dates(year)
    read_cov = _get_read_coverage()
    for resource in sorted({entry.resource for entry in planned}):
        coverage = {
            day.source_date: day.effective
            for day in read_cov(fs, get_resource(resource).identity, dates[0], dates[-1])
        }
        for entry in planned:
            if entry.resource == resource:
                effective = coverage[entry.source_date]
                if effective is None or effective.run_id != entry.run_id:
                    raise ValueError("Destination effective attempt changed during import")


def import_bundle(
    fs: Any, path: Path | str, *, dry_run: bool = False, lock_dir: Path | str | None = None
) -> dict[str, Any]:
    """Import missing committed days from a validated ZIP bundle."""
    with ZipFile(path) as archive:
        bundle = ValidatedArchive(archive)  # Validate all days, including ones later skipped.
        report: dict[str, Any] = {
            "mode": "import",
            "dry_run": dry_run,
            **bundle.report(),
            "planned_days": [],
            "skipped_days": [],
            "blocked_days": [],
            "conflicts": [],
            "imported_days": [],
            "uploaded_objects": 0,
            "uploaded_bytes": 0,
            "reused_objects": 0,
            "destination_verified": {"days": 0, "files": 0, "records": 0},
        }
        put_missing_fn = _get_put_missing()
        commit_day_fn = _get_commit_day_manifest()
        try:
            with nullcontext() if dry_run else _lock(lock_dir):
                planned, runs = _preflight(fs, bundle, report)
                if dry_run:
                    return report
                for entry in planned:
                    logger.info("import resource=%s date=%s", entry.resource, entry.source_date)
                    day = bundle.day_manifests[entry.day_key]
                    parquet_keys = [key for _, key in entry.parquet_files()]
                    for key in parquet_keys:
                        put_missing_fn(fs, bundle, key, report)
                    _check_objects(fs, bundle, parquet_keys)
                    verify_committed(fs, selection_for(entry, day, prefix=_bucket() + "/"))
                    metadata = [
                        key
                        for key in entry.objects
                        if key.startswith("_control/") and key not in {entry.day_key, entry.run_key}
                    ]
                    for key in metadata:
                        put_missing_fn(fs, bundle, key, report)
                    _check_objects(fs, bundle, metadata)
                    _put_run(fs, entry, day, runs[entry.run_key], report)
                    definition = get_resource(entry.resource)
                    commit_day_fn(fs, definition.identity, day)
                    if read_day_manifest(fs, definition.identity, entry.run_id, entry.source_date) != day:
                        raise ValueError("Destination day commit readback mismatch")
                    report["uploaded_objects"] += 1
                    report["uploaded_bytes"] += fs.info(_key(entry.day_key))["size"]
                    verified = verify_committed(fs, selection_for(entry, day, prefix=_bucket() + "/"))
                    for key in verified:
                        report["destination_verified"][key] += verified[key]
                    report["imported_days"].append(
                        {"resource": entry.resource, "date": str(entry.source_date), "run_id": entry.run_id}
                    )
                _confirm_coverage(fs, planned, bundle.index.year)
                return report
        except TransferError:
            raise
        except KeyboardInterrupt:
            # Retain progress for CLI diagnostics while preserving the interruption exit code.
            logger.warning("Import interrupted; committed_days=%s", len(report["imported_days"]))
            raise
        except Exception as exc:
            raise TransferError(str(exc), report) from exc
