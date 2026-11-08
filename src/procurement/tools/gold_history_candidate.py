"""Resumable canonical Bronze -> local Silver acceptance; no remote clients.

All resources/dates come from immutable transfer manifests, including the approved
supplement. Each checkpoint binds source controls/files, mapper code and all local
outputs. This stage is NOT an Iceberg readiness attestation or Gold acceptance.
"""

import argparse
import hashlib
import json
import os
import resource
import shutil
import time
import zipfile
from contextlib import ExitStack
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import fsspec
from fsspec.implementations.zip import ZipFileSystem

from procurement.common.file_lock import exclusive_file_lock
from procurement.silver.adapters.driven.bronze.arrow_adapter import ArrowBronzeRepositoryAdapter
from procurement.silver.adapters.driven.storage.s3_candidate_adapter import S3SilverCandidateAdapter
from procurement.silver.application.process_silver_service import ProcessSilverService
from procurement.silver.application.readiness import RESOURCES
from procurement.silver.application.resources import get_silver_resource


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def file_hash(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024**2), b""):
            value.update(chunk)
    return value.hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def save(path, value):
    raw = canonical(value).encode()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    with temporary.open("wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def mapper_fingerprint():
    package = Path(__file__).resolve().parents[1]
    paths = [
        p
        for directory in ("silver", "common", "models")
        for p in (package / directory).rglob("*.py")
    ]
    paths.append(Path(__file__).resolve())
    return digest(
        canonical({str(p.relative_to(package)): file_hash(p) for p in sorted(paths)}).encode()
    )


def inventory(archives, start, end):
    selected, identities = {}, []
    for path in archives:
        with zipfile.ZipFile(path) as archive:
            raw = archive.read("transfer.json")
            transfer = json.loads(raw)
            if transfer.get("format_version") != 2:
                raise ValueError("Unsupported transfer version")
            identities.append({"path": str(path.resolve()), "transfer_sha256": digest(raw)})
            for entry in transfer["days"]:
                name, day = entry["resource"], entry["source_date"]
                if name not in RESOURCES or not str(start) <= day <= str(end):
                    continue
                key = name + "/" + day
                if key in selected:
                    raise ValueError("Duplicate resource/day, not an explicit supplement: " + key)
                controls = [k for k in entry["objects"] if k.endswith("/day.json")]
                if len(controls) != 1:
                    raise ValueError("Ambiguous day control: " + key)
                raw = archive.read("objects/" + controls[0])
                if digest(raw) != transfer["objects"][controls[0]]["sha256"]:
                    raise ValueError("Control checksum mismatch: " + key)
                control = json.loads(raw)
                if (
                    control.get("status") != "success"
                    or control.get("error_count", 0)
                    or control.get("completed_pages") != control.get("expected_pages")
                    or control.get("run_id") != entry["run_id"]
                    or control.get("source_date") != day
                    or control.get("resource") != name
                ):
                    raise ValueError("Incomplete/mismatched source control: " + key)
                allowed = set(get_silver_resource(name).select_tables())
                files = []
                for object_key in entry["objects"]:
                    if object_key.startswith("bronze/") and object_key.endswith(".parquet"):
                        table = object_key.split("/muasamcong/", 1)[1].split("/", 1)[0]
                        if table not in allowed:
                            raise ValueError("Wrong table in source day: " + key)
                        proof = transfer["objects"][object_key]
                        files.append(
                            {
                                "table": table,
                                "key": "objects/" + object_key,
                                "sha256": proof["sha256"],
                            }
                        )
                if control["bronze_records"] and not files:
                    raise ValueError("Missing source files: " + key)
                selected[key] = {
                    "archive": str(path.resolve()),
                    "partition": {
                        "resource": name,
                        "source_date": day,
                        "run_id": entry["run_id"],
                        "expected_records": control["bronze_records"],
                        "manifest": control,
                        "files": files,
                    },
                }
    day = start
    while day <= end:
        for name in RESOURCES:
            if name + "/" + str(day) not in selected:
                raise ValueError("Missing source day: " + name + "/" + str(day))
        day += timedelta(days=1)
    return selected, identities


def verify_checkpoint(path, identity):
    receipt = json.loads(path.read_text())
    if receipt["identity"] != identity:
        raise ValueError("Checkpoint identity changed")
    for item in receipt["files"]:
        p = Path(item["path"])
        if not p.is_file() or p.stat().st_size != item["bytes"] or file_hash(p) != item["sha256"]:
            raise ValueError("Checkpoint output corrupted: " + str(p))
    return receipt


def run(
    archives,
    start,
    end,
    output,
    *,
    max_seconds=21600,
    max_bytes=50 * 1024**3,
    max_day_records=20000,
    on_progress=None,
):
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    with exclusive_file_lock(output / "writer.lock"):
        selected, sources = inventory(archives, start, end)
        oversized = [
            (k, v["partition"]["expected_records"])
            for k, v in selected.items()
            if v["partition"]["expected_records"] > max_day_records
        ]
        if oversized:
            raise ValueError(
                "Source day exceeds preflight row budget: "
                + str(max(oversized, key=lambda x: x[1]))
            )
        plan = {
            "version": 1,
            "start": str(start),
            "end": str(end),
            "sources": sources,
            "mapper_code": mapper_fingerprint(),
            "max_seconds": max_seconds,
            "max_bytes": max_bytes,
            "max_day_records": max_day_records,
        }
        plan_path = output / "plan.json"
        if plan_path.exists():
            if json.loads(plan_path.read_text()) != plan:
                raise ValueError("Immutable full-history plan differs")
        else:
            save(plan_path, plan)
        marker = output / "job.json"
        state = (
            json.loads(marker.read_text())
            if marker.exists()
            else {"started_at": datetime.now(UTC).isoformat(), "elapsed_active_seconds": 0.0}
        )
        prior_seconds = state["elapsed_active_seconds"]
        started = time.monotonic()
        state.update(
            status="RUNNING",
            pid=os.getpid(),
            expected_resource_days=len(selected),
            completed_resource_days=0,
            reviewed_resource_days=0,
            raw_records=0,
            output_bytes=0,
            production_written=False,
            iceberg_published=False,
            historical_silver_ready=False,
            full_gold_accepted=False,
            memory_limit_external="operator must apply process/cgroup bound",
        )
        save(marker, state)
        with ExitStack() as stack:
            filesystems = {
                str(path.resolve()): stack.enter_context(_zip_fs(path)) for path in archives
            }
            # Chronological order is scheduling only, not an entity/grain partition.
            ordered = sorted(selected, key=lambda k: (selected[k]["partition"]["source_date"], k))
            for key in ordered:
                state["current"] = key
                try:
                    if prior_seconds + time.monotonic() - started >= max_seconds:
                        raise RuntimeError("Active wall-time budget reached; checkpoint retained")
                    if (
                        state["output_bytes"] >= max_bytes
                        or shutil.disk_usage(output).free < 5 * 1024**3
                    ):
                        raise RuntimeError("Disk budget/free-space guard reached")
                    item = selected[key]
                    day_root = output / "days" / key
                    receipt_path = day_root / "accepted.json"
                    identity = digest(canonical({"plan": plan, "source": item}).encode())
                    if receipt_path.exists():
                        receipt = verify_checkpoint(receipt_path, identity)
                    else:
                        p = item["partition"]
                        bronze = ArrowBronzeRepositoryAdapter(
                            filesystems[item["archive"]],
                            freeze_fn=lambda *_args, p=p: [p],
                            verify_files=True,
                        )
                        service = ProcessSilverService(
                            bronze,
                            S3SilverCandidateAdapter(fsspec.filesystem("file", auto_mkdir=True)),
                            str(day_root),
                            resource_name=p["resource"],
                            iceberg_config=None,
                        )
                        report = service.execute(
                            date.fromisoformat(p["source_date"]),
                            date.fromisoformat(p["source_date"]),
                        )
                        entry = report["days"][0]
                        if entry["status"] not in ("completed", "needs_review"):
                            raise RuntimeError(
                                "Canonical Silver failed: " + str(entry.get("error_type"))
                            )
                        files = [
                            {
                                "path": str(Path(path).resolve()),
                                "sha256": file_hash(path),
                                "bytes": Path(path).stat().st_size,
                            }
                            for path in entry["files"].values()
                        ]
                        report_path = day_root / "canonical-report.json"
                        save(report_path, report)
                        files.append(
                            {
                                "path": str(report_path),
                                "sha256": file_hash(report_path),
                                "bytes": report_path.stat().st_size,
                            }
                        )
                        receipt = {
                            "identity": identity,
                            "status": entry["status"],
                            "raw_records": p["expected_records"],
                            "quality": entry["quality"],
                            "mapping_version": report["mapping_version"],
                            "files": files,
                        }
                        save(receipt_path, receipt)
                    state["completed_resource_days"] += 1
                    state["reviewed_resource_days"] += receipt["status"] == "needs_review"
                    state["raw_records"] += receipt["raw_records"]
                    state["output_bytes"] += sum(f["bytes"] for f in receipt["files"])
                    state["elapsed_active_seconds"] = prior_seconds + time.monotonic() - started
                    state["peak_rss_kib"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                    state["updated_at"] = datetime.now(UTC).isoformat()
                    save(marker, state)
                    if on_progress:
                        on_progress(state)
                except BaseException as exc:
                    state.update(
                        status="FAILED",
                        error_type=type(exc).__name__,
                        elapsed_active_seconds=prior_seconds + time.monotonic() - started,
                    )
                    save(marker, state)
                    raise
        state.update(
            status="NEEDS_REVIEW" if state["reviewed_resource_days"] else "LOCAL_SILVER_VALIDATED",
            finished_at=datetime.now(UTC).isoformat(),
        )
        save(marker, state)
        return state


def _zip_fs(path):
    from contextlib import closing

    return closing(ZipFileSystem(fo=str(path)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archives", type=Path, nargs="+", required=True)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--execute-isolated", action="store_true")
    args = parser.parse_args()
    if not args.execute_isolated or args.start > args.end:
        raise SystemExit("Explicit isolated opt-in and ordered scope required")
    result = run(
        args.archives,
        args.start,
        args.end,
        args.output,
        on_progress=lambda s: (
            print(canonical(s), flush=True) if s["completed_resource_days"] % 10 == 0 else None
        ),
    )
    print(canonical(result), flush=True)


if __name__ == "__main__":
    main()
