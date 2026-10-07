"""Export committed Bronze days into portable bundles."""

import logging
import os
import sys
import tempfile
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zipfile import ZIP_DEFLATED, ZipFile

from procurement.common.catalog import SUPPORTED_RESOURCES, get_resource
from procurement.common.settings import settings
from procurement.ingestion.coverage import read_coverage
from procurement.jobs.lock import execution_lock
from procurement.models.control import DayManifest, RunManifest
from procurement.storage.control import list_page_manifests, read_run_manifest
from procurement.transfer.archive import (
    BundleDay,
    BundleIndex,
    ValidatedArchive,
    copy_digest,
    validate_day_metadata,
    year_dates,
)

logger = logging.getLogger(__name__)


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


def _source_plan(
    fs: Any, year: int, resource: str
) -> tuple[list[BundleDay], dict[str, list[Any]], dict[str, DayManifest]]:
    dates = year_dates(year)
    resources = SUPPORTED_RESOURCES if resource == "all" else (resource,)
    entries: list[BundleDay] = []
    missing: dict[str, list[Any]] = {}
    manifests: dict[str, DayManifest] = {}
    runs: dict[tuple[str, str], RunManifest | None] = {}
    for name in resources:
        definition = get_resource(name)
        logger.info("select committed days resource=%s year=%s", name, year)
        coverage = read_coverage(fs, definition.identity, dates[0], dates[-1])
        logger.info(
            "selected resource=%s committed_days=%s",
            name,
            sum(day.effective is not None for day in coverage),
        )
        missing[name] = [day.source_date for day in coverage if day.effective is None]
        for covered in coverage:
            day = covered.effective
            if day is None:
                continue
            entry = BundleDay(resource=name, source_date=day.source_date, run_id=day.run_id, objects=[])
            if (name, day.run_id) not in runs:
                runs[name, day.run_id] = read_run_manifest(fs, definition.identity, day.run_id)
            run = runs[name, day.run_id]
            if run is None:
                raise ValueError(f"Missing run: {day.run_id}")
            pages = list_page_manifests(fs, definition.identity, run_id=day.run_id, source_date=day.source_date)
            validate_day_metadata(entry, run, day, pages)
            objects = [entry.run_key, entry.day_key]
            objects.extend(
                entry.day_key.removesuffix("day.json") + f"pages/page-{page.page_number:06d}.json" for page in pages
            )
            for table in definition.tables:
                prefix = _key(entry.parquet_prefix(table))
                objects.extend(_relative(key) for key in sorted(fs.glob(f"{prefix}/*.parquet")))
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


def _get_source_plan_fn():
    for mod_name in ("procurement.storage.transfer", "procurement.transfer.export", "procurement.transfer"):
        mod = sys.modules.get(mod_name)
        if mod and "_source_plan" in mod.__dict__:
            return mod.__dict__["_source_plan"]
    return _source_plan


def _publish(partial: Path, output: Path) -> None:
    # Windows rename refuses existing destinations. POSIX rename would overwrite them.
    if os.name == "nt":
        os.rename(partial, output)
    else:
        os.link(partial, output)
        partial.unlink()


def export_bundle(
    fs: Any,
    *,
    year: int,
    output: Path | str,
    resource: str = "all",
    dry_run: bool = False,
    lock_dir: Path | str | None = None,
) -> dict[str, Any]:
    """Export effective SUCCESS days for one calendar year into a ZIP bundle."""
    output = Path(output)
    if output.exists():
        raise FileExistsError("Output already exists; choose a new archive filename")
    plan_fn = _get_source_plan_fn()
    with nullcontext() if dry_run else _lock(lock_dir):
        entries, missing, manifests = plan_fn(fs, year, resource)
        keys = sorted({key for entry in entries for key in entry.objects})
        if dry_run:
            logger.info("estimate source size objects=%s", len(keys))
            with ThreadPoolExecutor(max_workers=8, thread_name_prefix="transfer-stat") as pool:
                total_bytes = sum(info["size"] for info in pool.map(fs.info, (_key(key) for key in keys)))
            return {
                "mode": "export",
                "dry_run": True,
                "year": year,
                "selected_days": len(entries),
                "objects": len(keys),
                "bytes": total_bytes,
                "missing_dates": {name: [str(day) for day in days] for name, days in missing.items()},
                "missing_days": sum(map(len, missing.values())),
                "coverage_complete": not any(missing.values()),
                "verified": None,
            }
        output.parent.mkdir(parents=True, exist_ok=True)
        descriptor, name = tempfile.mkstemp(prefix=output.name + ".", suffix=".part", dir=output.parent)
        os.close(descriptor)
        partial = Path(name)
        try:
            objects = {}
            with ZipFile(partial, "w", compression=ZIP_DEFLATED, compresslevel=1, allowZip64=True) as archive:
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
                    bundle_id=uuid.uuid4().hex,
                    created_at=datetime.now(UTC),
                    year=year,
                    resources=list(missing),
                    missing_dates=missing,
                    days=entries,
                    objects=objects,
                )
                archive.writestr("transfer.json", index.model_dump_json())
            with ZipFile(partial) as archive:
                checked = ValidatedArchive(archive)
                for key, day in manifests.items():
                    if checked.day_manifests[key] != day:
                        raise ValueError("Source day changed during export")
                report = checked.report()
            # Detect refreshes, file-list changes and changed control bytes before publication.
            again, _, _ = plan_fn(fs, year, resource)
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
