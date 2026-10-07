"""Portable, versioned Bronze bundles. Paths are bucket-relative, never local paths."""

import hashlib
import json
import logging
import re
import stat
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from tempfile import TemporaryFile
from typing import Any, Literal
from zipfile import ZipFile

from pydantic import BaseModel, ConfigDict, Field

from procurement.common.catalog import DEFAULT_SOURCE, get_resource
from procurement.common.dates import today_vn
from procurement.models.control import DayManifest, PageManifest, RunManifest
from procurement.storage.committed import CommittedDay, verify_committed

CHUNK_SIZE = 1024 * 1024
MAX_JSON_BYTES = 16 * 1024 * 1024
MAX_INDEX_BYTES = 256 * 1024 * 1024
logger = logging.getLogger(__name__)


class BundleModel(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)


class ObjectDigest(BundleModel):
    size: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class BundleDay(BundleModel):
    resource: str
    source_date: date
    run_id: str = Field(pattern=r"^[A-Za-z0-9_-]+$")
    objects: list[str]

    @property
    def control_prefix(self) -> str:
        return f"_control/{DEFAULT_SOURCE}/{self.resource}/run_id={self.run_id}"

    @property
    def day_key(self) -> str:
        return f"{self.control_prefix}/source_date={self.source_date}/day.json"

    @property
    def run_key(self) -> str:
        return f"{self.control_prefix}/run.json"

    def parquet_prefix(self, table: str) -> str:
        return f"bronze/{DEFAULT_SOURCE}/{table}/source_date={self.source_date}/run_id={self.run_id}"

    def parquet_files(self) -> tuple[tuple[str, str], ...]:
        return tuple((key.split("/")[2], key) for key in self.objects if key.startswith("bronze/"))


class BundleIndex(BundleModel):
    format_version: Literal[1, 2] = 2
    bundle_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    created_at: datetime
    year: int
    resources: list[str] = Field(min_length=1)
    missing_dates: dict[str, list[date]]
    days: list[BundleDay] = Field(min_length=1)
    objects: dict[str, ObjectDigest]


def year_dates(year: int) -> list[date]:
    if not 1 <= year < today_vn().year:
        raise ValueError("--year must be a fully closed calendar year")
    start, end = date(year, 1, 1), date(year, 12, 31)
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def copy_digest(reader: Any, writer: Any = None) -> ObjectDigest:
    digest, size = hashlib.sha256(), 0
    while chunk := reader.read(CHUNK_SIZE):
        digest.update(chunk)
        size += len(chunk)
        if writer is not None:
            writer.write(chunk)
    return ObjectDigest(size=size, sha256=digest.hexdigest())


def safe_key(key: str) -> None:
    if not key or "\\" in key or ":" in key or "\x00" in key or any(part in {"", ".", ".."} for part in key.split("/")):
        raise ValueError("Unsafe archive object path")


def _unique_json(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON key in archive")
        result[key] = value
    return result


def read_json_member(archive: ZipFile, name: str) -> Any:
    limit = MAX_INDEX_BYTES if name == "transfer.json" else MAX_JSON_BYTES
    if archive.getinfo(name).file_size > limit:
        raise ValueError("Archive JSON exceeds size limit")
    return json.loads(archive.read(name), object_pairs_hook=_unique_json)


class ArchiveFilesystem:
    """Expose a seekable Parquet at a time; never extract archive paths to disk."""

    def __init__(self, archive: ZipFile) -> None:
        self.archive = archive

    @contextmanager
    def open(self, key: str, mode: str = "rb"):
        if mode != "rb":
            raise ValueError("Archive is read-only")
        with TemporaryFile() as staged:
            with self.archive.open(f"objects/{key}") as source:
                copy_digest(source, staged)
            staged.seek(0)
            yield staged


def selection_for(entry: BundleDay, day: DayManifest, prefix: str = "") -> tuple[CommittedDay, ...]:
    return (
        CommittedDay(
            entry.source_date,
            entry.run_id,
            day.bronze_records,
            tuple((table, prefix + key) for table, key in entry.parquet_files()),
        ),
    )


def _validate_index(index: BundleIndex) -> None:
    calendar = set(year_dates(index.year))
    if len(index.resources) != len(set(index.resources)) or set(index.missing_dates) != set(index.resources):
        raise ValueError("Invalid resource inventory")
    for resource in index.resources:
        get_resource(resource)
    selected: set[tuple[str, date]] = set()
    runs: set[tuple[str, str]] = set()
    objects: set[str] = set()
    for entry in index.days:
        pair = (entry.resource, entry.source_date)
        run = (entry.resource, entry.run_id)
        if (
            entry.resource not in index.resources
            or entry.source_date not in calendar
            or pair in selected
            or (index.format_version == 1 and run in runs)
        ):
            raise ValueError("Duplicate or out-of-range day/run in archive")
        selected.add(pair)
        runs.add(run)
        if len(entry.objects) != len(set(entry.objects)):
            raise ValueError("Duplicate object in day inventory")
        for key in entry.objects:
            safe_key(key)
            if key in objects and not (index.format_version == 2 and key == entry.run_key):
                raise ValueError("Duplicate object ownership")
            objects.add(key)
    if objects != set(index.objects):
        raise ValueError("Object inventory does not match selected days")
    for resource in index.resources:
        missing = index.missing_dates[resource]
        present = {day for res, day in selected if res == resource}
        if len(missing) != len(set(missing)) or set(missing) != calendar - present:
            raise ValueError("Missing dates do not match coverage")


def _completed(manifest: Any) -> None:
    label = f"{type(manifest).__name__} run_id={manifest.run_id}"
    if manifest.schema_version != 1:
        raise ValueError(f"Unsupported schema_version={manifest.schema_version}: {label}")
    if manifest.status.value != "success" or manifest.completed_at is None:
        raise ValueError(f"Unfinished manifest status={manifest.status.value}: {label}")
    if manifest.started_at.tzinfo is None or manifest.completed_at.tzinfo is None or manifest.completed_at < manifest.started_at:
        raise ValueError(f"Invalid manifest timestamps: {label}")


def validate_day_metadata(entry: BundleDay, run: RunManifest, day: DayManifest, pages: list[PageManifest], *, legacy_bundle: bool = False) -> None:
    """Day SUCCESS commits data even if an older range run failed or stopped later."""
    if run.schema_version != 1 or run.started_at.tzinfo is None or (
        run.completed_at is not None and (run.completed_at.tzinfo is None or run.completed_at < run.started_at)
    ):
        raise ValueError(f"Unsupported run schema or invalid timestamps: {entry.run_id}")
    if legacy_bundle:
        _completed(run)
        if (
            run.start_date != entry.source_date
            or run.end_date != entry.source_date
            or run.total_dates != 1
            or run.success_dates != 1
            or run.failed_dates != 0
        ):
            raise ValueError(f"Unsupported multi-day run in v1 bundle: {entry.run_id}")
    for manifest in (day, *pages):
        _completed(manifest)
        if manifest.run_id != entry.run_id:
            raise ValueError(f"Manifest run mismatch: {entry.run_id}")
    if (
        run.run_id != entry.run_id
        or run.source != DEFAULT_SOURCE
        or day.source != DEFAULT_SOURCE
        or run.resource != entry.resource
        or day.resource != entry.resource
        or not run.start_date <= entry.source_date <= run.end_date
        or day.source_date != entry.source_date
        or day.error_count != 0
    ):
        raise ValueError(f"Inconsistent run/day metadata: {entry.run_id}")
    if (
        day.expected_pages is None
        or day.expected_pages != len(pages)
        or day.completed_pages != len(pages)
        or [page.page_number for page in pages] != list(range(len(pages)))
        or any(page.source_date != entry.source_date or page.error_count for page in pages)
        or sum(page.bronze_records for page in pages) != day.bronze_records
        or sum(page.search_items for page in pages) != day.search_items
    ):
        raise ValueError(f"Incomplete/inconsistent page manifests: {entry.run_id}")


def project_import_run(source: RunManifest, days: list[DayManifest]) -> RunManifest:
    """Summarize only transferred SUCCESS days; keep original run metadata in the ZIP."""
    for day in days:
        _completed(day)
        if day.run_id != source.run_id or day.resource != source.resource or day.source != source.source:
            raise ValueError(f"Destination run/day mismatch: {source.run_id}")
    if not days or len({day.source_date for day in days}) != len(days):
        raise ValueError("Cannot summarize empty or duplicate days")
    if (
        len(days) == 1
        and source.start_date == source.end_date == days[0].source_date
        and source.status.value == "success"
        and source.completed_at is not None
        and source.total_dates == source.success_dates == 1
        and source.failed_dates == 0
    ):
        return source  # Preserve the original single-day manifest exactly.
    return source.model_copy(
        update={
            "start_date": min(day.source_date for day in days),
            "end_date": max(day.source_date for day in days),
            "status": type(source.status).SUCCESS,
            "total_dates": len(days),
            "success_dates": len(days),
            "failed_dates": 0,
            "completed_at": max(day.completed_at for day in days if day.completed_at is not None),
        }
    )


class ValidatedArchive:
    """Validation uses the same open ZIP handle later consumed by import."""

    def __init__(self, archive: ZipFile) -> None:
        self.archive = archive
        names = [info.filename for info in archive.infolist()]
        if len(names) != len(set(names)) or "transfer.json" not in names:
            raise ValueError("Duplicate ZIP entries or missing transfer.json")
        for info in archive.infolist():
            safe_key(info.orig_filename)
            safe_key(info.filename)
            kind = stat.S_IFMT(info.external_attr >> 16)
            if info.is_dir() or kind not in {0, stat.S_IFREG} or info.flag_bits & 1:
                raise ValueError("Directories, links and encrypted entries are unsupported")
        self.index = BundleIndex.model_validate(read_json_member(archive, "transfer.json"))
        _validate_index(self.index)
        expected = {"transfer.json", *(f"objects/{key}" for key in self.index.objects)}
        if set(names) != expected:
            raise ValueError("Missing or extra ZIP entries")
        logger.info("validate archive objects=%s", len(self.index.objects))
        for number, (key, expected_digest) in enumerate(self.index.objects.items(), start=1):
            name = f"objects/{key}"
            if archive.getinfo(name).file_size != expected_digest.size:
                raise ValueError(f"Object size mismatch: {key}")
            with archive.open(name) as source:
                if copy_digest(source) != expected_digest:
                    raise ValueError(f"Object checksum mismatch: {key}")
            if number % 100 == 0:
                logger.info("checksum objects=%s/%s", number, len(self.index.objects))
        self.day_manifests: dict[str, DayManifest] = {}
        self.run_manifests: dict[str, RunManifest] = {}
        self.verified: dict[str, int] = {"days": 0, "files": 0, "records": 0}
        for entry in self.index.days:
            logger.info("verify resource=%s date=%s", entry.resource, entry.source_date)
            day = self._validate_day(entry)
            self.day_manifests[entry.day_key] = day
            verified = verify_committed(ArchiveFilesystem(archive), selection_for(entry, day))
            for key in self.verified:
                self.verified[key] += verified[key]

    def _model(self, key: str, model: Any) -> Any:
        return model.model_validate(read_json_member(self.archive, f"objects/{key}"))

    def _validate_day(self, entry: BundleDay) -> DayManifest:
        if entry.run_key not in entry.objects or entry.day_key not in entry.objects:
            raise ValueError("Missing run/day manifest")
        if entry.run_key not in self.run_manifests:
            self.run_manifests[entry.run_key] = self._model(entry.run_key, RunManifest)
        run = self.run_manifests[entry.run_key]
        day = self._model(entry.day_key, DayManifest)
        page_prefix = entry.day_key.removesuffix("day.json") + "pages/"
        page_keys = sorted(key for key in entry.objects if key.startswith(page_prefix))
        pages = [self._model(key, PageManifest) for key in page_keys]
        validate_day_metadata(entry, run, day, pages, legacy_bundle=self.index.format_version == 1)
        allowed = {entry.run_key, entry.day_key}
        allowed.update(f"{page_prefix}page-{page.page_number:06d}.json" for page in pages)
        tables = get_resource(entry.resource).tables
        for _, key in entry.parquet_files():
            if not any(key.startswith(entry.parquet_prefix(table) + "/") for table in tables):
                raise ValueError("Parquet path does not match resource/day/run")
            filename = key.rsplit("/", 1)[1]
            if not re.fullmatch(r"[A-Za-z0-9_.-]+\.parquet", filename) or key.count("/") != 5:
                raise ValueError("Invalid Parquet filename")
            allowed.add(key)
        if set(entry.objects) != allowed:
            raise ValueError("Unexpected object in selected day")
        return day

    def report(self) -> dict[str, Any]:
        return {
            "bundle_id": self.index.bundle_id,
            "year": self.index.year,
            "resources": self.index.resources,
            "selected_days": len(self.index.days),
            "missing_dates": {key: [str(day) for day in days] for key, days in self.index.missing_dates.items()},
            "missing_days": sum(map(len, self.index.missing_dates.values())),
            "coverage_complete": not any(self.index.missing_dates.values()),
            "objects": len(self.index.objects),
            "bytes": sum(item.size for item in self.index.objects.values()),
            "verified": self.verified,
        }
