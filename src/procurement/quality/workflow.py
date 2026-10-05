"""One resumable job for snapshot, audit, selective repair and final comparison."""

from pathlib import Path

from procurement.common.file_lock import exclusive_file_lock
from procurement.common.settings import settings
from procurement.ingestion.engine.metadata import calculate_content_hash
from procurement.quality.adapters import QUALITY_RESOURCES, search_api
from procurement.quality.audit import run_audit
from procurement.quality.comparison import compare_audits
from procurement.quality.contracts import load_config
from procurement.quality.evidence import load_snapshot, snapshot
from procurement.quality.files import now, read_json, safe_error, write_json
from procurement.quality.repair import apply_plan, create_plan, validate_plan


def initialize_job(directory, year, config_path=None, *, existing=None, resource="notify_contractor"):
    """Existing artifacts can be adopted explicitly without copying or changing them."""
    directory = Path(directory).resolve()
    path = directory / "job.json"
    if path.exists():
        return read_json(path)
    selected = config_path or (settings.NOTIFY_QUALITY_CONFIG if resource == "notify_contractor" else None)
    config_path = Path(selected).resolve() if selected else Path(__file__).with_name("bid_opening.toml" if resource == "bid_opening" else "notify.toml")
    config = load_config(config_path)
    if config.resource not in QUALITY_RESOURCES:
        raise ValueError("Unsupported automatic repair resource")
    paths = {key: str(directory / value) for key, value in {
        "snapshot": "search", "before": "before", "plan": "repair-plan.json", "repair": "repair",
    }.items()}
    for key, value in (existing or {}).items():
        if key not in paths:
            raise ValueError(f"Unknown artifact: {key}")
        paths[key] = str(Path(value).resolve())
    job = {"schema_version": 1, "year": year, "resource": config.resource,
           "config": str(config_path), "config_hash": config.fingerprint,
           "storage_namespace": calculate_content_hash([
               settings.OBJECT_STORAGE_ENDPOINT, settings.OBJECT_STORAGE_BUCKET,
           ]), "paths": paths, "status": "pending", "stage": "snapshot", "created_at": now()}
    if Path(paths["plan"]).exists():
        plan = read_json(paths["plan"])
        if plan["year"] != year:
            raise ValueError("Existing plan year differs")
        validate_plan(plan, config)
        job["plan_hash"] = plan["plan_hash"]
    write_json(path, job)
    return job


def run_workflow(fs, client, *, year, directory, config_path=None, detail_workers=3,
                 resource="notify_contractor"):
    directory = Path(directory).resolve()
    with exclusive_file_lock(directory / "job.lock"):
        job = initialize_job(directory, year, config_path, resource=resource)
        config = load_config(config_path or job["config"])
        namespace = calculate_content_hash([settings.OBJECT_STORAGE_ENDPOINT, settings.OBJECT_STORAGE_BUCKET])
        if (job["year"] != year or job["config_hash"] != config.fingerprint
                or job["resource"] != config.resource or job["resource"] != resource
                or job["storage_namespace"] != namespace):
            raise ValueError("Job year/config/storage changed; use a different work directory")
        paths = {key: Path(value) for key, value in job["paths"].items()}

        def stage(name):
            job.update(status="running", stage=name, updated_at=now())
            job.pop("error", None)
            write_json(directory / "job.json", job)
            print(f"quality job {year}: {name}", flush=True)

        try:
            stage("snapshot")
            index_path = paths["snapshot"] / "index.json"
            if not index_path.exists() or read_json(index_path)["status"] != "complete":
                snapshot(search_api(client, job["resource"]), year, paths["snapshot"], resume=index_path.exists())
            index, days = load_snapshot(paths["snapshot"])
            if index["year"] != year:
                raise ValueError("Snapshot year differs from job")
            snapshot_hash = calculate_content_hash(index)
            if job.get("snapshot_hash", snapshot_hash) != snapshot_hash:
                raise ValueError("Job snapshot changed")
            job["snapshot_hash"] = snapshot_hash
            stage("before_audit")
            if not paths["plan"].exists():
                run_audit(fs, resource=job["resource"], year=year, config=config,
                          output=paths["before"], search_days=days, snapshot_hash=snapshot_hash,
                          resume=(paths["before"] / "selection.json").exists())
                stage("repair_plan")
                create_plan(paths["before"], config, paths["plan"])
            plan = read_json(paths["plan"])
            if plan["year"] != year or job.get("plan_hash", plan["plan_hash"]) != plan["plan_hash"]:
                raise ValueError("Job repair plan changed")
            job["plan_hash"] = plan["plan_hash"]
            stage("repair")
            results = apply_plan(fs, client, plan, config, paths["repair"],
                                 detail_workers=detail_workers, continue_on_error=True)
            # If a previously blocked day is repaired on retry, freeze a fresh after audit.
            generation = calculate_content_hash([
                {key: result.get(key) for key in ("date", "run_id", "status")} for result in results
            ])[:20]
            after = directory / "after" / generation
            report = directory / "reports" / generation
            job.update(after=str(after), report=str(report))
            stage("after_audit")
            run_audit(fs, resource=job["resource"], year=year, config=config, output=after,
                      search_days=days, snapshot_hash=snapshot_hash,
                      resume=(after / "selection.json").exists())
            stage("comparison")
            if report.exists():
                comparison = read_json(report / "comparison.json")
                if comparison["plan_hash"] != plan["plan_hash"]:
                    raise ValueError("Comparison belongs to another plan")
            else:
                comparison = compare_audits(paths["before"], after, plan, paths["repair"], report)
            job.update(status="complete" if comparison["fully_verified"] else "needs_attention",
                       stage="finished", completed_at=now(), counts=comparison["counts"],
                       repair_days=comparison["repair_days"])
            write_json(directory / "job.json", job)
            print(f"Report: {report / 'comparison.md'}", flush=True)
            return job
        except BaseException as exc:
            job.update(status="interrupted" if isinstance(exc, KeyboardInterrupt) else "failed",
                       error=safe_error(exc), updated_at=now())
            write_json(directory / "job.json", job)
            raise
