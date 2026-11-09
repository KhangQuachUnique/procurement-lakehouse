"""Managed Silver sync -> shared Gold runtime. Never ingest Bronze or delete history.

Explicit one-shot publication consent; same schema/business/NULL policy and bulk
selection algorithm already verified. All plans and per-table ACKs are resumable.
"""

import argparse
import json
import resource
import shutil
import time
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv

from procurement.common.dates import today_vn
from procurement.common.file_lock import exclusive_file_lock
from procurement.common.settings import Settings
from procurement.gold.application.runtime import execute_run
from procurement.gold.application.state import StateStore
from procurement.gold.domain.models import RunPlan, SilverReadyInput, logic_version
from procurement.gold.domain.policies import ReferencePolicy
from procurement.readiness_with_exception import seal_with_null_policy, validate_report
from procurement.silver.application.readiness import save_bundle
from procurement.source_quality import NULL_POLICY_DIGEST
from procurement.storage.iceberg import RestCatalog, connect, literal
from procurement.tools.gold_history_bootstrap import publish_reports
from procurement.tools.gold_history_candidate import (
    file_hash,
    mapper_fingerprint,
    save,
    verify_checkpoint,
)
from procurement.tools.silver_bootstrap_views import create_views
from procurement.tools.silver_bootstrap_writer import load_empty

SOURCE_NAMESPACE = "silver_v2_main_2022_2025"
TARGET_NAMESPACE = "gold_v2_main_2022_2025"
START, END = date(2022, 1, 1), date(2025, 12, 31)


def source_reports(source, progress):
    job = json.loads((source / "job.json").read_text())
    plan = json.loads((source / "plan.json").read_text())
    if (
        job["status"] not in ("NEEDS_REVIEW", "LOCAL_SILVER_VALIDATED")
        or job["completed_resource_days"] != 4383
        or job["expected_resource_days"] != 4383
        or plan["start"] != str(START)
        or plan["end"] != str(END)
        or plan["mapper_code"] != mapper_fingerprint()
    ):
        raise ValueError("Canonical 2022-2025 source incomplete or mapper changed")
    paths = sorted(
        source.glob("days/*/*/accepted.json"), key=lambda p: (p.parent.name, p.parent.parent.name)
    )
    if len(paths) != 4383:
        raise ValueError("Source checkpoint inventory differs")
    reports = []
    for i, p in enumerate(paths):
        accepted = json.loads(p.read_text())
        verify_checkpoint(p, accepted["identity"])
        report = json.loads((p.parent / "canonical-report.json").read_text())
        validate_report(report, null_policy=True)
        reports.append(report)
        if (i + 1) % 300 == 0:
            progress("verify_source", {"checked": i + 1, "total": 4383})
    actual = [(r["resource"], r["days"][0]["date"]) for r in reports]
    expected = set()
    day = START
    while day <= END:
        expected.update(
            (name, str(day)) for name in ("khlcnt", "notify_contractor", "contractor_result")
        )
        day += timedelta(days=1)
    if len(set(actual)) != len(actual) or set(actual) != expected:
        raise ValueError("Full calendar/resource coverage differs before publication")
    # The sealer later requires every day/resource, rejects duplicates, and
    # checks all UUIDs/retained snapshots, lineage and approved source exceptions.
    progress("verify_source", {"checked": 4383, "total": 4383})
    return reports


def run(source, output, *, execute_managed=False, profile_first_partition=False):
    if not execute_managed:
        raise ValueError("Managed publication requires explicit opt-in")
    source, output = Path(source).resolve(), Path(output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(output).free < 40 * 1024**3:
        raise ValueError("Less than 40GiB free for state/spill; refusing heavy work")
    identity = {
        "source_plan_sha256": file_hash(source / "plan.json"),
        "gold_logic": logic_version(),
        "null_policy": NULL_POLICY_DIGEST,
        "source_namespace": SOURCE_NAMESPACE,
        "gold_target": TARGET_NAMESPACE,
        "scope": [str(START), str(END)],
        "observation_day": str(today_vn()),
        "writer_sha256": file_hash(Path(__file__).with_name("silver_bootstrap_writer.py")),
        "partition_writer_sha256": file_hash(
            Path(__file__).with_name("silver_partitioned_insert.py")
        ),
        "views_sha256": file_hash(Path(__file__).with_name("silver_bootstrap_views.py")),
        "runner_sha256": file_hash(Path(__file__)),
    }
    started = time.monotonic()
    with exclusive_file_lock(output / "pipeline.lock"):
        frozen = output / "pipeline-plan.json"
        if frozen.exists():
            old = json.loads(frozen.read_text())
            # Observation day is frozen even when resumed on another day.
            identity["observation_day"] = old["observation_day"]
            if identity != old:
                raise ValueError("Immutable official pipeline changed")
        else:
            save(frozen, identity)
        state = {
            "status": "RUNNING",
            "scope": identity["scope"],
            "source_namespace": SOURCE_NAMESPACE,
            "gold_target": TARGET_NAMESPACE,
            "production_written": False,
            "steps": {},
        }

        def progress(phase, evidence):
            state.update(
                current=phase,
                elapsed_seconds=time.monotonic() - started,
                peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            )
            state["steps"][phase] = evidence
            save(output / "job.json", state)
            print(json.dumps(state), flush=True)

        cfg = Settings.from_environment().iceberg_config()

        class SourceChunkGate(RuntimeError):
            pass

        def partition_progress(table, done, total):
            progress("silver_partition_insert", {"table": table, "done": done, "total": total})
            if profile_first_partition:
                group = Path("/proc/self/cgroup").read_text().strip().split(":")[-1]
                cgroup = Path("/sys/fs/cgroup") / group.lstrip("/")
                peak = int((cgroup / "memory.peak").read_text())
                if peak > 4000000000:
                    raise ValueError("Operator gate exceeded approved whole-process cap")
                save(
                    output / "partition-operator-proof.json",
                    {
                        "status": "PASS",
                        "table": table,
                        "durable_batches": done,
                        "planned_batches": total,
                        "exact_batch_multiset": True,
                        "peak_process_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                        "cgroup_peak_bytes": peak,
                        "source_full_scope": [str(START), str(END)],
                        "already_committed_tables_preserved": True,
                        "full_source_not_yet_ready": True,
                    },
                )
                raise SourceChunkGate(
                    "Controlled pause after durable exact-verified physical batch"
                )

        try:
            ready_path = output / "silver-ready.json"
            if not ready_path.exists():
                reports = source_reports(source, progress)
                with connect(cfg, max_memory="3GB", threads=1) as db, RestCatalog(cfg) as catalog:
                    # Cohort winner/order is explicit; physical INSERT order is not a contract.
                    db.execute("SET preserve_insertion_order=false")
                    spill = output / "silver-spill"
                    spill.mkdir(exist_ok=True)
                    db.execute("SET temp_directory=" + literal(str(spill)))
                    db.execute("SET max_temp_directory_size='20GiB'")
                    entries = [(i, r["days"][0]["files"]) for i, r in enumerate(reports)]
                    progress("silver_prepare", {"cohorts": 4383})
                    prepared = create_views(db, entries, prefix="main_history")
                    progress("silver_bulk_insert", {"tables_committed": 0, "total": 14})
                    completed = []

                    def after_commit(name):
                        completed.append(name)
                        progress(
                            "silver_bulk_insert", {"newly_committed": completed.copy(), "total": 14}
                        )

                    pins = load_empty(
                        db,
                        catalog,
                        SOURCE_NAMESPACE,
                        prepared["views"],
                        output / "silver-table-acks",
                        identity["source_plan_sha256"] + ":full",
                        after_commit=after_commit,
                        allow_managed_namespace=True,
                        partition_batch_size=8,
                        after_partition_chunk=partition_progress,
                    )
                    progress("silver_seal", {"tables_committed": len(pins), "total": 14})
                    published = publish_reports(
                        reports, pins, SOURCE_NAMESPACE, "main_2022_2025", identity["scope"]
                    )
                    ready = seal_with_null_policy(
                        published,
                        start=START,
                        end=END,
                        namespace=SOURCE_NAMESPACE,
                        catalog=catalog,
                        count_rows=lambda t, s: (
                            db.execute(
                                f"SELECT count(*) FROM lake.{SOURCE_NAMESPACE}.{t} AT (VERSION => {s})"
                            ).fetchone()[0]
                            if s is not None
                            else 0
                        ),
                    )
                    save_bundle(ready, ready_path)
                    progress(
                        "silver_ready",
                        {"cohorts": 4383, "quality": ready["receipt"]["quality_status"]},
                    )
                del prepared, reports, published
            ready = SilverReadyInput.model_validate(json.loads(ready_path.read_text())["receipt"])
            if (ready.source_start, ready.source_end, ready.namespace) != (
                START,
                END,
                SOURCE_NAMESPACE,
            ):
                raise ValueError("Official source receipt scope differs")
            gold_root = output / "gold-state"
            policy = ReferencePolicy()
            request = RunPlan(
                run_id="main_backfill_2022_2025",
                mode="backfill",
                logic_version=logic_version(),
                reference_version=policy.version,
                references_json="{}",
                target_namespace=TARGET_NAMESPACE,
                source=ready,
                source_cutoff=END,
                event_start=START,
                event_end=END,
                observation_day=date.fromisoformat(identity["observation_day"]),
                budget={"memory_limit": "2GB", "threads": 1, "batch_rows": 50000},
            )
            with StateStore(gold_root) as local:
                local.create(request)
            progress("gold", {"run_id": request.run_id, "state": "RESUMING"})
            result = execute_run(
                request.run_id,
                gold_root,
                allow_isolated_publication=True,
                on_phase=lambda p: progress("gold", {"phase": p.value}),
            )
            if result["phase"] != "COMMITTED":
                raise ValueError("Official Gold did not commit")
            progress("gold_readback", {"tables": 16})
            from procurement.gold.adapters.driven.iceberg.iceberg_publisher import IcebergPublisher
            from procurement.gold.application.verification import verify

            with connect(cfg, max_memory="512MB", threads=1) as remote, RestCatalog(cfg) as catalog:
                publisher = IcebergPublisher(remote, catalog)
                evidence = verify(gold_root, publisher)
                views = publisher.ensure_views(request, "gold")
            receipt = {
                "status": "COMMITTED_VERIFIED",
                "scope": identity["scope"],
                "source": ready.model_dump(mode="json"),
                "gold_target": TARGET_NAMESPACE,
                "state_root": str(gold_root),
                "release": request.run_id,
                "verification": evidence,
                "tables": views["tables"],
                "logic_version": logic_version(),
                "independent_full_vs_incremental_2022_2025": "NOT_RUN",
                "prior_reduced_scope_parity": "16 tables PASS; real multi-revision coverage pending",
                "production_written": True,
                "recurring_schedule_enabled": False,
            }
            save(output / "receipt.json", receipt)
            state.update(status="COMMITTED_VERIFIED", production_written=True)
            progress(
                "complete", {"release": request.run_id, "verified_tables": len(evidence["tables"])}
            )
            return receipt
        except SourceChunkGate:
            state.update(status="SOURCE_CHUNK_GATE")
            progress("source_chunk_gate", {"proof": "partition-operator-proof.json"})
            return state
        except BaseException as exc:
            state.update(status="FAILED", error_type=type(exc).__name__, error=str(exc))
            progress("failure", {"type": type(exc).__name__})
            raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--execute-managed", action="store_true")
    args = parser.parse_args()
    load_dotenv(args.env_file)
    run(args.source, args.output, execute_managed=args.execute_managed)


if __name__ == "__main__":
    main()
