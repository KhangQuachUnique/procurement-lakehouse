"""Export/import committed Bronze days without changing attempt identities."""

import logging
import os
import tempfile
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from procurement.common.catalog import SUPPORTED_RESOURCES, get_resource
from procurement.common.settings import settings
from procurement.ingestion.coverage import read_coverage
from procurement.jobs.lock import execution_lock
from procurement.models.control import DayManifest, RunManifest
from procurement.storage.committed import verify_committed
from procurement.storage.control import (
    commit_day_manifest,
    list_day_manifests,
    list_page_manifests,
    read_day_manifest,
    read_run_manifest,
    write_run_manifest,
)
from procurement.storage.transfer_archive import (
    BundleDay,
    BundleIndex,
    ValidatedArchive,
    copy_digest,
    project_import_run,
    selection_for,
    validate_day_metadata,
    year_dates,
)

logger = logging.getLogger(__name__)


class TransferError(RuntimeError):
    def __init__(self, message, report):
        super().__init__(message)
        self.report = report


@dataclass
class ImportRun:
    source: RunManifest
    current: RunManifest | None
    days: dict[date, DayManifest]


def _bucket():
    return settings.OBJECT_STORAGE_BUCKET.replace("\\", "/").rstrip("/")


def _key(relative):
    return f"{_bucket()}/{relative}"


def _relative(key):
    prefix = _bucket() + "/"
    if not key.startswith(prefix):
        raise ValueError("Object listing returned a key outside the configured bucket")
    return key.removeprefix(prefix)


def _lock(lock_dir):
    return execution_lock(Path(lock_dir or settings.INGESTION_LOCK_DIR))


def _source_plan(fs, year, resource):
    dates = year_dates(year)
    resources = SUPPORTED_RESOURCES if resource == "all" else (resource,)
    entries, missing, manifests = [], {}, {}
    runs = {}
    for name in resources:
        definition = get_resource(name)
        logger.info("select committed days resource=%s year=%s", name, year)
        coverage = read_coverage(fs, definition.identity, dates[0], dates[-1])
        logger.info("selected resource=%s committed_days=%s", name,
                    sum(day.effective is not None for day in coverage))
        missing[name] = [day.source_date for day in coverage if day.effective is None]
        for covered in coverage:
            day = covered.effective
            if day is None:
                continue
            entry = BundleDay(resource=name, source_date=day.source_date,
                              run_id=day.run_id, objects=[])
            if (name, day.run_id) not in runs:
                runs[name, day.run_id] = read_run_manifest(fs, definition.identity, day.run_id)
            run = runs[name, day.run_id]
            if run is None:
                raise ValueError(f"Missing run: {day.run_id}")
            pages = list_page_manifests(fs, definition.identity, run_id=day.run_id,
                                       source_date=day.source_date)
            validate_day_metadata(entry, run, day, pages)
            objects = [entry.run_key, entry.day_key]
            objects.extend(entry.day_key.removesuffix("day.json")
                           + f"pages/page-{page.page_number:06d}.json" for page in pages)
            for table in definition.tables:
                prefix = _key(entry.parquet_prefix(table))
                objects.extend(_relative(key)
                               for key in sorted(fs.glob(f"{prefix}/*.parquet")))
            if day.bronze_records and not any(key.startswith("bronze/") for key in objects):
                raise ValueError(
                    f"Committed day has no Parquet: resource={name} date={day.source_date} "
                    f"run_id={day.run_id} expected_records={day.bronze_records}; "
                    "restore its files or repair this day with --refresh before export"
                )
            entry.objects = objects
            entries.append(entry)
            manifests[entry.day_key] = day
    if not entries:
        raise ValueError("No committed days available for export")
    return entries, missing, manifests


def _publish(partial, output):
    # Windows rename refuses existing destinations. POSIX rename would overwrite them.
    if os.name == "nt":
        os.rename(partial, output)
    else:
        os.link(partial, output)
        partial.unlink()


def export_bundle(fs, *, year, output, resource="all", dry_run=False, lock_dir=None):
    output = Path(output)
    if output.exists():
        raise FileExistsError("Output already exists; choose a new archive filename")
    with nullcontext() if dry_run else _lock(lock_dir):
        entries, missing, manifests = _source_plan(fs, year, resource)
        keys = sorted({key for entry in entries for key in entry.objects})
        if dry_run:
            logger.info("estimate source size objects=%s", len(keys))
            with ThreadPoolExecutor(max_workers=8, thread_name_prefix="transfer-stat") as pool:
                total_bytes = sum(info["size"] for info in pool.map(
                    fs.info, (_key(key) for key in keys)))
            return {
                "mode": "export", "dry_run": True, "year": year,
                "selected_days": len(entries), "objects": len(keys),
                "bytes": total_bytes,
                "missing_dates": {name: [str(day) for day in days]
                                  for name, days in missing.items()},
                "missing_days": sum(map(len, missing.values())),
                "coverage_complete": not any(missing.values()), "verified": None,
            }
        output.parent.mkdir(parents=True, exist_ok=True)
        descriptor, name = tempfile.mkstemp(prefix=output.name + ".", suffix=".part",
                                            dir=output.parent)
        os.close(descriptor)
        partial = Path(name)
        try:
            objects = {}
            with ZipFile(partial, "w", compression=ZIP_DEFLATED,
                         compresslevel=1, allowZip64=True) as archive:
                for entry in entries:
                    logger.info("export resource=%s date=%s", entry.resource, entry.source_date)
                    for key in entry.objects:
                        if key in objects:
                            continue  # Older range runs share one original run.json across days.
                        with (
                            fs.open(_key(key), "rb") as source,
                            archive.open(f"objects/{key}", "w", force_zip64=True) as target,
                        ):
                            objects[key] = copy_digest(source, target)
                index = BundleIndex(
                    bundle_id=uuid.uuid4().hex, created_at=datetime.now(UTC), year=year,
                    resources=list(missing), missing_dates=missing, days=entries, objects=objects,
                )
                archive.writestr("transfer.json", index.model_dump_json())
            with ZipFile(partial) as archive:
                checked = ValidatedArchive(archive)
                for key, day in manifests.items():
                    if checked.day_manifests[key] != day:
                        raise ValueError("Source day changed during export")
                report = checked.report()
            # Detect refreshes, file-list changes and changed control bytes before publication.
            again, _, _ = _source_plan(fs, year, resource)
            if again != entries:
                raise ValueError("Source selection changed during export")
            for key, digest in objects.items():
                if key.startswith("_control/"):
                    with fs.open(_key(key), "rb") as source:
                        if copy_digest(source) != digest:
                            raise ValueError("Source manifests changed during export")
            _publish(partial, output)
            return {"mode": "export", "archive": str(output), **report}
        finally:
            partial.unlink(missing_ok=True)


def inspect_bundle(path):
    with ZipFile(path) as archive:
        return {"mode": "inspect", **ValidatedArchive(archive).report()}


def _preflight(fs, bundle, report):
    index = bundle.index
    dates = year_dates(index.year)
    coverage = {
        name: {day.source_date: day for day in read_coverage(
            fs, get_resource(name).identity, dates[0], dates[-1])}
        for name in index.resources
    }
    planned = []
    for entry in index.days:
        day = coverage[entry.resource][entry.source_date]
        detail = {"resource": entry.resource, "date": str(entry.source_date),
                  "run_id": entry.run_id}
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
                    report["conflicts"].append({**detail, "key": relative,
                                                "reason": "extra_parquet"})
        for key in entry.objects:
            if key == entry.run_key:
                continue  # Range-run summaries are projected, not copied byte-for-byte.
            if fs.exists(_key(key)):
                with fs.open(_key(key), "rb") as source:
                    if copy_digest(source) != index.objects[key]:
                        report["conflicts"].append({**detail, "key": key,
                                                    "reason": "different_content"})
    runs = _preflight_runs(fs, bundle, planned, report)
    if report["blocked_days"] or report["conflicts"]:
        raise TransferError("Import preflight failed; no objects written", report)
    return planned, runs


def _preflight_runs(fs, bundle, planned, report):
    runs = {}
    groups = {}
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
                    candidates.append(project_import_run(
                        source, [*days, bundle.day_manifests[pending.day_key]]))
            if current is not None and current not in candidates:
                reason = "incompatible_destination_run_summary"
        if reason:
            report["conflicts"].append({"resource": entry.resource, "run_id": entry.run_id,
                                        "key": key, "reason": reason})
        else:
            runs[key] = ImportRun(source, current, {day.source_date: day for day in days})
    return runs


def _put_run(fs, entry, day, state, report):
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


def _put_missing(fs, bundle, key, report):
    if fs.exists(_key(key)):
        with fs.open(_key(key), "rb") as source:
            if copy_digest(source) != bundle.index.objects[key]:
                raise ValueError(f"Object changed since preflight: {key}")
        report["reused_objects"] += 1
        return
    # Do not publish a short object when interruption closes a partially written stream.
    target = fs.open(_key(key), "wb", autocommit=False)
    try:
        with bundle.archive.open(f"objects/{key}") as source:
            digest = copy_digest(source, target)
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


def _check_objects(fs, bundle, keys):
    for key in keys:
        with fs.open(_key(key), "rb") as source:
            if copy_digest(source) != bundle.index.objects[key]:
                raise ValueError(f"Destination checksum mismatch: {key}")


def _confirm_coverage(fs, planned, year):
    """One scan per resource, avoiding quadratic S3 manifest reads for a full year."""
    dates = year_dates(year)
    for resource in sorted({entry.resource for entry in planned}):
        coverage = {day.source_date: day.effective for day in read_coverage(
            fs, get_resource(resource).identity, dates[0], dates[-1])}
        for entry in planned:
            if entry.resource == resource:
                effective = coverage[entry.source_date]
                if effective is None or effective.run_id != entry.run_id:
                    raise ValueError("Destination effective attempt changed during import")


def import_bundle(fs, path, *, dry_run=False, lock_dir=None):
    with ZipFile(path) as archive:
        bundle = ValidatedArchive(archive)  # Validate all days, including ones later skipped.
        report = {
            "mode": "import", "dry_run": dry_run, **bundle.report(),
            "planned_days": [], "skipped_days": [], "blocked_days": [], "conflicts": [],
            "imported_days": [], "uploaded_objects": 0, "uploaded_bytes": 0,
            "reused_objects": 0, "destination_verified": {"days": 0, "files": 0, "records": 0},
        }
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
                        _put_missing(fs, bundle, key, report)
                    _check_objects(fs, bundle, parquet_keys)
                    verify_committed(fs, selection_for(
                        entry, day, prefix=_bucket() + "/"))
                    metadata = [key for key in entry.objects if key.startswith("_control/")
                                and key not in {entry.day_key, entry.run_key}]
                    for key in metadata:
                        _put_missing(fs, bundle, key, report)
                    _check_objects(fs, bundle, metadata)
                    _put_run(fs, entry, day, runs[entry.run_key], report)
                    definition = get_resource(entry.resource)
                    commit_day_manifest(fs, definition.identity, day)
                    if read_day_manifest(fs, definition.identity, entry.run_id,
                                         entry.source_date) != day:
                        raise ValueError("Destination day commit readback mismatch")
                    report["uploaded_objects"] += 1
                    report["uploaded_bytes"] += fs.info(_key(entry.day_key))["size"]
                    verified = verify_committed(fs, selection_for(entry, day, prefix=_bucket() + "/"))
                    for key in verified:
                        report["destination_verified"][key] += verified[key]
                    report["imported_days"].append({"resource": entry.resource,
                                                    "date": str(entry.source_date),
                                                    "run_id": entry.run_id})
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
