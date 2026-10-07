"""Compaction planning, inventory inspection, and plan validation."""

import hashlib
import logging
import re
from collections import Counter
from datetime import UTC, date, datetime
from typing import Any
from uuid import uuid4

from procurement.common.catalog import SUPPORTED_RESOURCES, get_resource
from procurement.common.dates import today_vn, validate_closed_range
from procurement.common.settings import settings
from procurement.ingestion.coverage import read_coverage
from procurement.ingestion.engine.metadata import calculate_content_hash
from procurement.models.control import DayManifest, DayStatus
from procurement.quality.storage import quality_prefix
from procurement.storage.control import list_page_manifests, read_run_manifest
from procurement.transfer import BundleDay, validate_day_metadata

logger = logging.getLogger(__name__)

SAFE_ID = re.compile(r"[A-Za-z0-9_-]+\Z")
SAFE_FILE = re.compile(r"[A-Za-z0-9_.-]+\Z")


def namespace() -> str:
    return calculate_content_hash(
        [settings.OBJECT_STORAGE_ENDPOINT, settings.OBJECT_STORAGE_BUCKET]
    )


def parquet_prefix(identity: Any, table: str, day: date | str, run_id: str) -> str:
    return (
        f"{settings.OBJECT_STORAGE_BUCKET}/bronze/{identity.source}/{table}/"
        f"source_date={day}/run_id={run_id}"
    )


def provenance_key(identity: Any, run_id: str) -> str:
    return (
        f"{settings.OBJECT_STORAGE_BUCKET}/_ops/{identity.source}/{identity.resource}/"
        f"run_id={run_id}/compaction.json"
    )


def object_signature(fs: Any, key: str) -> dict[str, Any]:
    info = fs.info(key)
    signature: dict[str, Any] = {
        "size": info["size"],
        "markers": {
            name: str(info[name])
            for name in ("ETag", "etag", "LastModified", "mtime")
            if info.get(name) is not None
        },
    }
    if not signature["markers"]:
        digest = hashlib.sha256()
        with fs.open(key, "rb") as file:
            while chunk := file.read(1024 * 1024):
                if isinstance(chunk, str):
                    chunk = chunk.encode("utf-8")
                digest.update(chunk)
        signature["sha256"] = digest.hexdigest()
    return signature


def inventory(fs: Any, definition: Any, day: date, run_id: str) -> list[dict[str, Any]]:
    files: list[dict[str, Any]] = []
    for table in definition.tables:
        prefix = parquet_prefix(definition.identity, table, day, run_id)
        for key in sorted(fs.glob(f"{prefix}/*.parquet")):
            str_key = str(key)
            files.append(
                {"table": table, "key": str_key, "signature": object_signature(fs, str_key)}
            )
    return files


def quality_inventory(fs: Any, identity: Any, day: date, run_id: str) -> list[dict[str, Any]]:
    prefix = quality_prefix(identity, run_id, day)
    return [
        {"name": str(key).rsplit("/", 1)[-1], "signature": object_signature(fs, str(key))}
        for key in sorted(fs.glob(f"{prefix}/*.json"))
    ]


def _metadata(identity: Any, run: Any, day: Any, pages: list[Any]) -> None:
    if run is None:
        raise ValueError("Missing baseline RunManifest")
    entry = BundleDay(
        resource=identity.resource,
        source_date=day.source_date,
        run_id=day.run_id,
        objects=[],
    )
    validate_day_metadata(entry, run, day, pages)


def create_plan(
    fs: Any,
    *,
    resource: str,
    start: date,
    end: date,
    target_bytes: int = 128 * 1024**2,
) -> dict[str, Any]:
    """Inspect committed days and create an immutable compaction plan."""
    validate_closed_range(start, end, today=today_vn())
    if not isinstance(target_bytes, int) or target_bytes <= 0:
        raise ValueError("target_bytes must be a positive integer")
    resources = (
        SUPPORTED_RESOURCES if resource == "all" else (get_resource(resource).identity.resource,)
    )
    plan: dict[str, Any] = {
        "format_version": 1,
        "plan_id": uuid4().hex,
        "storage_namespace": namespace(),
        "created_at": datetime.now(UTC).isoformat(),
        "start": str(start),
        "end": str(end),
        "target_bytes": target_bytes,
        "compression": "zstd",
        "days": [],
        "skipped": [],
    }
    for name in resources:
        definition = get_resource(name)
        logger.info("compact_plan resource=%s start=%s end=%s", name, start, end)
        for covered in read_coverage(fs, definition.identity, start, end):
            base = {"resource": name, "date": str(covered.source_date)}
            if covered.effective is None or covered.active_run_ids:
                plan["skipped"].append(
                    {
                        **base,
                        "reason": "active_attempt" if covered.active_run_ids else "no_success",
                    }
                )
                continue
            day = covered.effective
            if not SAFE_ID.fullmatch(day.run_id):
                raise ValueError("Invalid baseline run ID")
            files = inventory(fs, definition, day.source_date, day.run_id)
            counts = Counter(f["table"] for f in files)
            if not day.bronze_records or all(count <= 1 for count in counts.values()):
                plan["skipped"].append(
                    {
                        **base,
                        "reason": "empty" if not day.bronze_records else "already_compact",
                        "baseline_run_id": day.run_id,
                        "input_files": len(files),
                    }
                )
                continue
            pages = list_page_manifests(
                fs, definition.identity, run_id=day.run_id, source_date=day.source_date
            )
            run = read_run_manifest(fs, definition.identity, day.run_id)
            _metadata(definition.identity, run, day, pages)
            plan["days"].append(
                {
                    **base,
                    "baseline": day.model_dump(mode="json"),
                    "pages": [p.model_dump(mode="json") for p in pages],
                    "files": files,
                    "quality": quality_inventory(
                        fs, definition.identity, day.source_date, day.run_id
                    ),
                    "input_bytes": sum(f["signature"]["size"] for f in files),
                }
            )
    plan["plan_hash"] = calculate_content_hash(plan)
    return plan


def validate_plan(plan: dict[str, Any]) -> None:
    """Validate plan signature, namespace, and planned day integrity."""
    expected = calculate_content_hash({k: v for k, v in plan.items() if k != "plan_hash"})
    if plan.get("plan_hash") != expected or plan.get("storage_namespace") != namespace():
        raise ValueError("Plan changed or belongs to another storage namespace")
    if (
        plan.get("format_version") != 1
        or plan.get("compression") != "zstd"
        or not re.fullmatch(r"[0-9a-f]{32}", str(plan.get("plan_id", "")))
        or not isinstance(plan.get("target_bytes"), int)
        or plan["target_bytes"] <= 0
    ):
        raise ValueError("Invalid compaction plan format/settings")
    start = date.fromisoformat(plan["start"])
    end = date.fromisoformat(plan["end"])
    validate_closed_range(start, end, today=today_vn())
    seen = set()
    for item in plan["days"]:
        definition = get_resource(item["resource"])
        day = DayManifest.model_validate(item["baseline"])
        if (
            not start <= day.source_date <= end
            or str(day.source_date) != item["date"]
            or day.resource != item["resource"]
            or day.source != definition.identity.source
            or day.status is not DayStatus.SUCCESS
            or not SAFE_ID.fullmatch(day.run_id)
            or (item["resource"], item["date"]) in seen
        ):
            raise ValueError("Invalid or duplicate planned day")
        seen.add((item["resource"], item["date"]))
        keys = set()
        for obj in item["files"]:
            prefix = parquet_prefix(definition.identity, obj["table"], day.source_date, day.run_id)
            name = obj["key"].removeprefix(prefix + "/")
            if (
                obj["table"] not in definition.tables
                or not obj["key"].startswith(prefix + "/")
                or not SAFE_FILE.fullmatch(name)
                or not name.endswith(".parquet")
                or obj["key"] in keys
            ):
                raise ValueError("Invalid or duplicate planned Parquet path")
            keys.add(obj["key"])
        if not keys:
            raise ValueError("Compaction day has no files")
        for evidence in item["quality"]:
            if not SAFE_FILE.fullmatch(evidence["name"]) or not evidence["name"].endswith(".json"):
                raise ValueError("Invalid quality evidence path")
