"""Replay verified canonical Silver day files through its existing Iceberg writer.

No bulk/replacement semantics are invented here: same day ordering and exact
stage_partition inputs as ProcessSilverService. A private test_silver_v2_* namespace
and durable per-day acknowledgements are mandatory. No production publication.
"""

import argparse
import json
import time
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import uuid4

import pyarrow.parquet as pq

from procurement.common.file_lock import exclusive_file_lock
from procurement.common.silver_gold_contract import INPUT_TABLES
from procurement.readiness_with_exception import seal_with_null_policy as seal_readiness
from procurement.readiness_with_exception import validate_report
from procurement.silver.application.readiness import (
    save_bundle,
)
from procurement.silver.application.warehouse import ensure_views, stage_partition
from procurement.silver.domain.schema import to_iceberg_ddl
from procurement.source_quality import NULL_POLICY_DIGEST as APPROVAL_DIGEST
from procurement.storage.catalog_scope import NamespaceCatalog
from procurement.storage.iceberg import identifier
from procurement.tools.gold_history_candidate import (
    canonical,
    file_hash,
    mapper_fingerprint,
    save,
    verify_checkpoint,
)


def table_inputs(report):
    entry = report["days"][0]
    output = {name: pq.ParquetFile(path).read() for name, path in entry["files"].items()}
    selected = {name: value for name, value in output.items() if len(value)}
    if report["resource"] == "contractor_result":
        for name in ("award_lot", "award_contractor"):
            selected[name] = output[name]
    if len(output["observation"]):
        for name in ("quality_issue", "quarantine"):
            selected[name] = output[name]
    return selected


def publish(
    source,
    output,
    connection,
    catalog,
    *,
    namespace=None,
    max_seconds=21600,
    on_progress=None,
    batch_days=1,
    batch_rows=250000,
    batch_bytes=256 * 1024**2,
):
    source, output = source.resolve(), output.resolve()
    local_state = json.loads((source / "job.json").read_text())
    plan = json.loads((source / "plan.json").read_text())
    if (
        local_state["status"] not in ("LOCAL_SILVER_VALIDATED", "NEEDS_REVIEW")
        or local_state["completed_resource_days"] != local_state["expected_resource_days"]
    ):
        raise ValueError("Canonical full-period Silver is not accepted locally")
    if plan["mapper_code"] != mapper_fingerprint():
        raise ValueError("Canonical mapper fingerprint changed since local validation")
    # Validate every checkpoint before any remote mutation, including exact exception proof.
    paths = sorted(
        source.glob("days/*/*/accepted.json"), key=lambda p: (p.parent.name, p.parent.parent.name)
    )
    if len(paths) != local_state["expected_resource_days"]:
        raise ValueError("Checkpoint inventory differs")
    exception_count = 0
    reviewed_count = 0
    for checkpoint in paths:
        accepted = json.loads(checkpoint.read_text())
        verify_checkpoint(checkpoint, accepted["identity"])
        original = json.loads((checkpoint.parent / "canonical-report.json").read_text())
        proofs = validate_report(original, null_policy=True)
        exception_count += len(proofs)
        reviewed_count += bool(proofs)
    if reviewed_count != local_state["reviewed_resource_days"]:
        raise ValueError("Reviewed cohorts do not match exact approved exception")
    approval = APPROVAL_DIGEST
    output.mkdir(parents=True, exist_ok=True)
    with exclusive_file_lock(output / "writer.lock"):
        path = output / "plan.json"
        if path.exists():
            publication = json.loads(path.read_text())
            if publication.get("approval_digest") != approval:
                raise ValueError("Publication source-exception policy differs")
            if publication["source_plan_hash"] != file_hash(source / "plan.json"):
                raise ValueError("Publication source plan changed")
            if namespace is not None and publication["namespace"] != namespace:
                raise ValueError("Publication namespace changed")
        else:
            publication = {
                "namespace": namespace or "test_silver_v2_history_" + uuid4().hex[:12],
                "source_plan_hash": file_hash(source / "plan.json"),
                "run_id": "r" + uuid4().hex,
                "max_seconds": max_seconds,
                "approval_digest": approval,
            }
            save(path, publication)
        namespace = identifier(publication["namespace"])
        if not namespace.startswith("test_silver_v2_"):
            raise ValueError("Historical publisher requires private test_silver_v2_* namespace")
        strategy_path = output / "batch-strategy-v1.json"
        if batch_days > 1 or strategy_path.exists():
            from procurement.tools import gold_history_batches

            strategy = {
                "max_days": batch_days,
                "max_rows": batch_rows,
                "max_bytes": batch_bytes,
                "publisher_sha256": file_hash(Path(__file__)),
                "batch_helper_sha256": file_hash(Path(gold_history_batches.__file__)),
                "source_plan_hash": publication["source_plan_hash"],
                "semantics": "unchanged writer; conflict-free cohorts; same cumulative budget",
            }
            if strategy_path.exists() and json.loads(strategy_path.read_text()) != strategy:
                raise ValueError("Frozen publication execution strategy differs")
            save(strategy_path, strategy)
        catalog = NamespaceCatalog(catalog, namespace)
        marker = output / "job.json"
        state = (
            json.loads(marker.read_text()) if marker.exists() else {"elapsed_active_seconds": 0.0}
        )
        prior = state["elapsed_active_seconds"]
        started = time.monotonic()
        state.update(
            status="RUNNING",
            source=str(source),
            namespace=namespace,
            completed_resource_days=0,
            production_written=False,
            expected_resource_days=local_state["expected_resource_days"],
        )
        save(marker, state)
        connection.execute("CREATE SCHEMA IF NOT EXISTS lake." + namespace)
        for table in INPUT_TABLES:
            connection.execute(to_iceberg_ddl(table, namespace=namespace))
        ensure_views(connection, namespace)
        reports = []
        prefix_day = date(2024, 12, 31)
        paths = sorted(
            source.glob("days/*/*/accepted.json"),
            key=lambda p: (p.parent.name, p.parent.parent.name),
        )
        if len(paths) != state["expected_resource_days"]:
            raise ValueError("Checkpoint inventory differs from source plan")
        from procurement.tools.gold_history_batches import replay

        def before_group(group):
            state["current"] = group[0]["key"] + ".." + group[-1]["key"]
            if prior + time.monotonic() - started >= publication["max_seconds"]:
                raise RuntimeError("Publication time budget reached")

        iterator = replay(
            paths,
            output,
            publication,
            connection,
            catalog,
            table_inputs,
            stage_partition,
            max_days=batch_days,
            max_rows=batch_rows,
            max_bytes=batch_bytes,
            before_group=before_group,
        )
        while True:
            try:
                checkpoint, published = next(iterator)
            except StopIteration:
                break
            except BaseException as exc:
                state.update(
                    status="FAILED",
                    error_type=type(exc).__name__,
                    elapsed_active_seconds=prior + time.monotonic() - started,
                )
                save(marker, state)
                raise
            key = checkpoint.parent.parent.name + "/" + checkpoint.parent.name
            resource = published["resource"]
            state["current"] = key
            try:
                reports.append(published)
                if (
                    published["days"][0]["date"] == str(prefix_day)
                    and resource == "notify_contractor"
                    and date.fromisoformat(plan["start"]) <= prefix_day
                ):
                    prefix_path = output / f"silver-ready-through-{prefix_day}.json"
                    if not prefix_path.exists():
                        prefix_bundle = seal_readiness(
                            reports,
                            start=date.fromisoformat(plan["start"]),
                            end=prefix_day,
                            namespace=namespace,
                            catalog=catalog,
                            count_rows=lambda table, snap: (
                                connection.execute(
                                    f"SELECT count(*) FROM lake.{namespace}.{table} AT (VERSION => {snap})"
                                ).fetchone()[0]
                                if snap is not None
                                else 0
                            ),
                        )
                        save_bundle(prefix_bundle, prefix_path)
                state["completed_resource_days"] += 1
                state["elapsed_active_seconds"] = prior + time.monotonic() - started
                state["updated_at"] = datetime.now(UTC).isoformat()
                save(marker, state)
                if on_progress:
                    on_progress(state)
            except BaseException as exc:
                state.update(
                    status="FAILED",
                    error_type=type(exc).__name__,
                    elapsed_active_seconds=prior + time.monotonic() - started,
                )
                save(marker, state)
                raise
        ready_path = output / "silver-ready.json"
        completed_at = (
            datetime.fromisoformat(json.loads(ready_path.read_text())["receipt"]["completed_at"])
            if ready_path.exists()
            else None
        )
        bundle = seal_readiness(
            reports,
            start=date.fromisoformat(plan["start"]),
            end=date.fromisoformat(plan["end"]),
            namespace=namespace,
            catalog=catalog,
            completed_at=completed_at,
            count_rows=lambda table, snap: (
                connection.execute(
                    f"SELECT count(*) FROM lake.{namespace}.{table} AT (VERSION => {snap})"
                ).fetchone()[0]
                if snap is not None
                else 0
            ),
        )
        save_bundle(bundle, ready_path)
        state.update(
            status="SILVER_READY",
            input_id=bundle["receipt"]["input_id"],
            quality_status=bundle["receipt"].get("quality_status", "STRICT"),
            approved_exception_count=exception_count,
            finished_at=datetime.now(UTC).isoformat(),
        )
        save(marker, state)
        return state


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--execute-isolated", action="store_true")
    parser.add_argument("--batch-days", type=int, default=1)
    args = parser.parse_args()
    if not args.execute_isolated:
        raise SystemExit("Explicit isolated publication opt-in required")
    from dotenv import load_dotenv

    load_dotenv(args.env_file)
    from procurement.common.settings import Settings
    from procurement.storage.iceberg import RestCatalog, connect

    config = Settings.from_environment().iceberg_config()
    with connect(config, max_memory="512MB", threads=1) as remote, RestCatalog(config) as catalog:
        result = publish(
            args.source,
            args.output,
            remote,
            catalog,
            batch_days=args.batch_days,
            on_progress=lambda s: (
                print(canonical(s), flush=True) if s["completed_resource_days"] % 10 == 0 else None
            ),
        )
    print(canonical(result), flush=True)


if __name__ == "__main__":
    main()
