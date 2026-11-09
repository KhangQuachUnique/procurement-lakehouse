"""Initial bulk INSERT + set-based suffix update; canonical source stays immutable."""

import argparse
import json
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from uuid import uuid4

from procurement.common.file_lock import exclusive_file_lock
from procurement.gold.domain.models import logic_version
from procurement.readiness_with_exception import seal_with_null_policy, validate_report
from procurement.silver.application.readiness import _ancestry, save_bundle
from procurement.source_quality import NULL_POLICY_DIGEST
from procurement.storage.catalog_scope import NamespaceCatalog
from procurement.tools.gold_history_candidate import file_hash, mapper_fingerprint, save
from procurement.tools.silver_bootstrap_views import create_views
from procurement.tools.silver_bootstrap_writer import apply_delta, load_empty


def code_hashes():
    root = Path(__file__).parent
    return {
        n: file_hash(root / n)
        for n in [
            "gold_history_bootstrap.py",
            "silver_bootstrap_views.py",
            "silver_bootstrap_writer.py",
        ]
    }


def publish_reports(reports, pins, namespace, run_id, scope):
    vector = {
        name: {k: pin[k] for k in ["table_uuid", "snapshot_id", "operation"]}
        for name, pin in pins.items()
    }
    return [
        report
        | {
            "run_id": run_id + "_" + report["resource"],
            "iceberg_enabled": True,
            "iceberg_namespace": namespace,
            "days": [
                report["days"][0]
                | {
                    "iceberg": vector,
                    "publication_bulk": {
                        "scope": scope,
                        "shared_snapshot": True,
                        "not_individual_daily_snapshot": True,
                    },
                }
            ],
        }
        for report in reports
    ]


def run(
    source, output, db, catalog, *, budget_used=0, max_seconds=21600, prefix_end=date(2024, 12, 31)
):
    source = source.resolve()
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    with exclusive_file_lock(output / "writer.lock"):
        state_source = json.loads((source / "job.json").read_text())
        source_plan = json.loads((source / "plan.json").read_text())
        if (
            state_source["status"] not in ["LOCAL_SILVER_VALIDATED", "NEEDS_REVIEW"]
            or state_source["completed_resource_days"] != state_source["expected_resource_days"]
        ):
            raise ValueError("Canonical source incomplete")
        if source_plan["mapper_code"] != mapper_fingerprint():
            raise ValueError("Canonical mapping changed")
        identity = {
            "source_plan_sha256": file_hash(source / "plan.json"),
            "code": code_hashes(),
            "gold_logic": logic_version(),
            "policy": NULL_POLICY_DIGEST,
            "prefix_end": str(prefix_end),
            "max_seconds": max_seconds,
            "budget_used_before_bootstrap": budget_used,
        }
        path = output / "plan.json"
        if path.exists():
            plan = json.loads(path.read_text())
            if any(plan[k] != v for k, v in identity.items()):
                raise ValueError("Frozen bootstrap inputs/strategy changed")
        else:
            plan = identity | {
                "namespace": "test_silver_v2_bulk_history_" + uuid4().hex[:12],
                "run_id": "bulk_" + uuid4().hex,
            }
            save(path, plan)
        namespace = plan["namespace"]
        catalog = NamespaceCatalog(catalog, namespace)
        marker = output / "job.json"
        old = json.loads(marker.read_text()) if marker.exists() else {}
        prior = max(budget_used, old.get("elapsed_active_seconds", 0))
        started = time.monotonic()
        state = {
            "status": "RUNNING",
            "current": "verify_source",
            "namespace": namespace,
            "production_written": False,
            "elapsed_active_seconds": prior,
            "steps": {},
        }
        save(marker, state)

        def progress(phase, table=None):
            state.update(
                current=phase,
                elapsed_active_seconds=prior + time.monotonic() - started,
                updated_at=datetime.now(UTC).isoformat(),
            )
            if table:
                state["steps"].setdefault(phase, []).append(table)
            save(marker, state)
            print(json.dumps(state), flush=True)
            if state["elapsed_active_seconds"] >= max_seconds:
                raise RuntimeError("Original cumulative Silver budget reached")

        try:
            from procurement.tools.gold_history_candidate import verify_checkpoint

            reports = []
            paths = sorted(
                source.glob("days/*/*/accepted.json"),
                key=lambda p: (p.parent.name, p.parent.parent.name),
            )
            if len(paths) != state_source["expected_resource_days"]:
                raise ValueError("Canonical checkpoint inventory changed")
            for i, checkpoint in enumerate(paths):
                accepted = json.loads(checkpoint.read_text())
                verify_checkpoint(checkpoint, accepted["identity"])
                report = json.loads((checkpoint.parent / "canonical-report.json").read_text())
                validate_report(report, null_policy=True)
                reports.append(report)
                if i % 300 == 0:
                    progress("verify_source")
            actual = [(r["resource"], r["days"][0]["date"]) for r in reports]
            day = date.fromisoformat(source_plan["start"])
            expected_scope = set()
            while day <= date.fromisoformat(source_plan["end"]):
                expected_scope.update(
                    (r, str(day)) for r in ("khlcnt", "notify_contractor", "contractor_result")
                )
                day += timedelta(days=1)
            if len(set(actual)) != len(actual) or set(actual) != expected_scope:
                raise ValueError("Canonical source calendar/resource coverage differs")
            pre = [
                (i, r["days"][0]["files"])
                for i, r in enumerate(reports)
                if date.fromisoformat(r["days"][0]["date"]) <= prefix_end
            ]
            suffix = [
                (i, r["days"][0]["files"])
                for i, r in enumerate(reports)
                if date.fromisoformat(r["days"][0]["date"]) > prefix_end
            ]
            if not pre or not suffix:
                raise ValueError("Full workflow requires real prefix AND suffix")
            prefix_phase = output / "prefix-phase.json"
            if prefix_phase.exists():
                base = json.loads(prefix_phase.read_text())
                for name, pin in base.items():
                    meta = catalog.table(namespace, name)["metadata"]
                    current = meta.get("current-snapshot-id")
                    current = current if current is not None and current >= 0 else None
                    if meta["table-uuid"] != pin["table_uuid"] or not _ancestry(
                        meta, pin["snapshot_id"], current
                    ):
                        raise ValueError("Frozen prefix is no longer retained")
            else:
                progress("prepare_prefix_key_indexes")
                prepared = create_views(db, pre, prefix="prefix")
                progress("bulk_insert_prefix")
                base = load_empty(
                    db,
                    catalog,
                    namespace,
                    prepared["views"],
                    output / "prefix-tables",
                    plan["source_plan_sha256"] + ":prefix",
                    after_commit=lambda t: progress("bulk_insert_prefix", t),
                )
                save(prefix_phase, base)
            prefix_reports = [
                r for r in reports if date.fromisoformat(r["days"][0]["date"]) <= prefix_end
            ]
            prefix_bundle = output / f"silver-ready-through-{prefix_end}.json"
            if not prefix_bundle.exists():
                progress("seal_prefix")
                bundle = seal_with_null_policy(
                    publish_reports(
                        prefix_reports,
                        base,
                        namespace,
                        plan["run_id"],
                        [source_plan["start"], str(prefix_end)],
                    ),
                    start=date.fromisoformat(source_plan["start"]),
                    end=prefix_end,
                    namespace=namespace,
                    catalog=catalog,
                    count_rows=lambda t, s: (
                        db.execute(
                            f"SELECT count(*) FROM lake.{namespace}.{t} AT (VERSION => {s})"
                        ).fetchone()[0]
                        if s is not None
                        else 0
                    ),
                )
                save_bundle(bundle, prefix_bundle)
            progress("prepare_suffix_and_full_key_indexes")
            delta = create_views(db, suffix, prefix="suffix")
            full = create_views(
                db, [(i, r["days"][0]["files"]) for i, r in enumerate(reports)], prefix="full"
            )
            progress("merge_suffix")
            final = apply_delta(
                db,
                catalog,
                namespace,
                delta,
                full["views"],
                base,
                output / "suffix-tables",
                plan["source_plan_sha256"] + ":suffix",
                after_commit=lambda t: progress("merge_suffix", t),
            )
            progress("verify_full_counts")
            counts = {}
            for table, view in full["views"].items():
                expected = db.execute(f"SELECT count(*) FROM {view}").fetchone()[0]
                sid = final[table]["snapshot_id"]
                actual = (
                    db.execute(
                        f"SELECT count(*) FROM lake.{namespace}.{table} AT (VERSION => {sid})"
                    ).fetchone()[0]
                    if sid is not None
                    else 0
                )
                if expected != actual:
                    raise ValueError("Bootstrap count mismatch: " + table)
                counts[table] = actual
                progress("verify_full_counts", table)
            final_path = output / "silver-ready.json"
            completed_at = (
                datetime.fromisoformat(
                    json.loads(final_path.read_text())["receipt"]["completed_at"]
                )
                if final_path.exists()
                else None
            )
            bundle = seal_with_null_policy(
                publish_reports(
                    reports,
                    final,
                    namespace,
                    plan["run_id"],
                    [source_plan["start"], source_plan["end"]],
                ),
                start=date.fromisoformat(source_plan["start"]),
                end=date.fromisoformat(source_plan["end"]),
                namespace=namespace,
                catalog=catalog,
                completed_at=completed_at,
                count_rows=lambda t, s: (
                    db.execute(
                        f"SELECT count(*) FROM lake.{namespace}.{t} AT (VERSION => {s})"
                    ).fetchone()[0]
                    if s is not None
                    else 0
                ),
            )
            save_bundle(bundle, final_path)
            state.update(
                status="SILVER_READY",
                current="complete",
                completed_resource_days=len(reports),
                expected_resource_days=len(reports),
                approved_exception_count=bundle["receipt"]["quarantined"],
                quality_status=bundle["receipt"]["quality_status"],
                source_rows=counts,
                input_id=bundle["receipt"]["input_id"],
                elapsed_active_seconds=prior + time.monotonic() - started,
            )
            save(marker, state)
            return state
        except BaseException as exc:
            state.update(
                status="FAILED",
                error_type=type(exc).__name__,
                elapsed_active_seconds=prior + time.monotonic() - started,
            )
            save(marker, state)
            raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--budget-used", type=float, required=True)
    parser.add_argument("--execute-isolated", action="store_true")
    args = parser.parse_args()
    if not args.execute_isolated:
        raise SystemExit("Explicit isolated execution required")
    from dotenv import load_dotenv

    from procurement.common.settings import Settings
    from procurement.storage.iceberg import RestCatalog, connect, literal

    load_dotenv(args.env_file)
    cfg = Settings.from_environment().iceberg_config()
    spill = args.output.resolve() / "spill"
    spill.mkdir(parents=True, exist_ok=True)
    with connect(cfg, max_memory="1GB", threads=1) as db, RestCatalog(cfg) as catalog:
        db.execute("SET temp_directory=" + literal(str(spill)))
        db.execute("SET max_temp_directory_size='20GiB'")
        print(
            json.dumps(run(args.source, args.output, db, catalog, budget_used=args.budget_used)),
            flush=True,
        )


if __name__ == "__main__":
    main()
