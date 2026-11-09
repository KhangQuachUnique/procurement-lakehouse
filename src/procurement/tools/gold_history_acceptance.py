"""Isolated Gold acceptance: prefix backfill -> actual suffix -> independent full.

Requires sealed canonical Silver receipts. Does not create receipt truth from a
benchmark or query production pointers. Full and incremental share Gold runtime;
exact parity compares independently materialized 16-table generations.
"""

import argparse
import json
import resource
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from procurement.common.dates import today_vn
from procurement.common.file_lock import exclusive_file_lock
from procurement.gold.adapters.driven.iceberg.iceberg_publisher import IcebergPublisher
from procurement.gold.adapters.driven.silver.iceberg_reader import IcebergReader
from procurement.gold.application.backend import PipelineBackend
from procurement.gold.application.gold_service import GoldService
from procurement.gold.application.state import StateStore
from procurement.gold.application.verification import verify
from procurement.gold.domain.models import RunPlan, SilverReadyInput, logic_version
from procurement.gold.domain.schema import TABLES
from procurement.tools.gold_history_candidate import canonical, file_hash, save


class AcceptanceInterruption(RuntimeError):
    """Controlled exception after a durable business batch, before publication."""


def compare_generations(remote, publisher, first, second):
    publisher.ensure_views(first, "v2_incremental")
    publisher.ensure_views(second, "v2_clean")
    result = {}
    for name in TABLES:
        rows = remote.execute(
            f"SELECT count(*) FROM ((SELECT * FROM v2_incremental.{name} EXCEPT ALL "
            f"SELECT * FROM v2_clean.{name}) UNION ALL (SELECT * FROM v2_clean.{name} "
            f"EXCEPT ALL SELECT * FROM v2_incremental.{name}))"
        ).fetchone()[0]
        result[name] = rows
    if any(result.values()):
        raise ValueError("Incremental/clean generation mismatch: " + canonical(result))
    return result


def accept(
    full_bundle, prefix_bundle, output, remote, catalog, *, on_progress=None, memory_limit=None
):
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    full = SilverReadyInput.model_validate(json.loads(full_bundle.read_text())["receipt"])
    prefix = (
        SilverReadyInput.model_validate(json.loads(prefix_bundle.read_text())["receipt"])
        if prefix_bundle
        else None
    )
    if prefix and (
        prefix.approval_digest != full.approval_digest
        or prefix.namespace != full.namespace
        or prefix.source_start != full.source_start
        or prefix.source_end >= full.source_end
        or {s.table: s.table_uuid for s in prefix.tables}
        != {s.table: s.table_uuid for s in full.tables}
    ):
        raise ValueError("Prefix/full source identities or scope differ")
    with exclusive_file_lock(output / "acceptance.lock"):
        path = output / "plan.json"
        identity = {
            "full_bundle_sha256": file_hash(full_bundle),
            "prefix_bundle_sha256": file_hash(prefix_bundle) if prefix_bundle else None,
            "logic_version": logic_version(),
        }
        if path.exists():
            frozen = json.loads(path.read_text())
            if any(frozen[k] != value for k, value in identity.items()):
                raise ValueError("Immutable acceptance inputs/code differ")
            if memory_limit is not None and memory_limit != frozen["memory_limit"]:
                raise ValueError("Cannot change an existing immutable acceptance memory budget")
        else:
            nonce = uuid4().hex[:12]
            frozen = identity | {
                "observation_day": str(today_vn()),
                "stream_target": "gold_v2_history_stream_" + nonce,
                "clean_target": "gold_v2_history_clean_" + nonce,
                "prefix_run": "prefix_" + nonce,
                "suffix_run": "suffix_" + nonce,
                "clean_run": "clean_" + nonce,
                "noop_run": "noop_" + nonce,
                "memory_limit": memory_limit or "1GB",
                "threads": 1,
                "batch_rows": 50000,
            }
            save(path, frozen)
        marker = output / "job.json"
        previous_state = json.loads(marker.read_text()) if marker.exists() else {}
        prior_seconds = previous_state.get(
            "elapsed_active_seconds", previous_state.get("elapsed_seconds_this_execution", 0.0)
        )
        state = {
            "status": "RUNNING",
            "production_written": False,
            "actual_suffix_tested": prefix is not None,
            "started_at": datetime.now(UTC).isoformat(),
            "steps": {},
            "elapsed_active_seconds": prior_seconds,
        }
        started = time.monotonic()
        save(marker, state)
        publisher = IcebergPublisher(remote, catalog)
        backend = PipelineBackend(
            IcebergReader(remote, catalog, full.namespace),
            publisher,
            on_batch=lambda done, total: progress("batch", {"done": done, "total": total}),
        )

        def progress(step, evidence):
            state["current"] = step
            state["steps"][step] = evidence
            state["elapsed_seconds_this_execution"] = time.monotonic() - started
            state["elapsed_active_seconds"] = prior_seconds + time.monotonic() - started
            state["peak_rss_kib"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            state["updated_at"] = datetime.now(UTC).isoformat()
            save(marker, state)
            if state["elapsed_active_seconds"] >= 21600:
                raise RuntimeError("Cumulative acceptance time budget reached")
            if on_progress:
                on_progress(state)

        def request(ready, run_id, target, base=None):
            return RunPlan(
                run_id=run_id,
                mode="incremental" if base else "backfill",
                base_release=base,
                logic_version=frozen["logic_version"],
                reference_version="raw_v1",
                target_namespace=target,
                source=ready,
                source_cutoff=ready.source_end,
                event_start=full.source_start,
                event_end=full.source_end,
                observation_day=frozen["observation_day"],
                budget={k: frozen[k] for k in ("memory_limit", "threads", "batch_rows")},
            )

        def execute(root, plan):
            service = GoldService(root, backend)
            service.create(plan)
            progress(plan.run_id, {"state": "RESUMING"})
            recovery_path = output / "recovery-proof.json"

            def after_batch(done, total):
                progress("batch", {"done": done, "total": total})
                if not recovery_path.exists():
                    save(
                        recovery_path,
                        {
                            "run_id": plan.run_id,
                            "injected_after_batch": done,
                            "kind": "controlled exception AFTER durable batch commit",
                            "resumed": False,
                        },
                    )
                    raise AcceptanceInterruption("Exercise same-plan durable resume")

            backend.on_batch = after_batch
            try:
                result = service.resume(
                    plan.run_id,
                    on_phase=lambda phase: progress(plan.run_id, {"phase": phase.value}),
                )
            except AcceptanceInterruption:
                result = service.resume(plan.run_id)
                proof = json.loads(recovery_path.read_text())
                save(recovery_path, proof | {"resumed": True, "final_phase": result["phase"]})
            progress(plan.run_id, {"state": "COMMITTED", "run_id": result["run_id"]})
            return result

        try:
            stream_root, clean_root = output / "stream-state", output / "clean-state"
            initial = request(prefix or full, frozen["prefix_run"], frozen["stream_target"])
            # Do not recreate an old base after a later run has been accepted.
            with StateStore(stream_root) as store:
                existing = store.db.execute("SELECT run_id FROM control.runs").fetchall()
                present = {r[0] for r in existing}
            if initial.run_id not in present:
                execute(stream_root, initial)
            else:
                GoldService(stream_root, backend).resume(initial.run_id)
            final = initial
            if prefix:
                final = request(full, frozen["suffix_run"], frozen["stream_target"], initial.run_id)
                if final.run_id not in present:
                    execute(stream_root, final)
                else:
                    GoldService(stream_root, backend).resume(final.run_id)
            checks = {"stream": verify(stream_root, publisher)}
            with StateStore(stream_root) as store:
                multi_revisions = {
                    name: store.db.execute(
                        f"SELECT count(*) FROM (SELECT entity_id FROM frozen.{name} "
                        "GROUP BY entity_id HAVING count(DISTINCT revision_id)>1)"
                    ).fetchone()[0]
                    for name in ("package_revision", "notice_revision", "result_revision")
                }
                public_release = store.db.execute(
                    "SELECT release_id FROM control.accepted"
                ).fetchone()[0]
            progress("stream_verified", {"tables": len(checks["stream"]["tables"])})
            clean = request(full, frozen["clean_run"], frozen["clean_target"])
            execute(clean_root, clean)
            checks["clean"] = verify(clean_root, publisher)
            parity = compare_generations(remote, publisher, final, clean)
            progress("exact_parity", parity)
            noop = request(full, frozen["noop_run"], frozen["stream_target"], public_release)
            # Durable no-op must preserve the same public generation.
            result = execute(stream_root, noop)
            with StateStore(stream_root) as store:
                result = store.status(noop.run_id)
            if result.get("equivalent_source", {}).get("release_id") != public_release:
                raise ValueError("Cross-run replay produced an unexpected release")
            if prefix is not None and not any(multi_revisions.values()):
                raise ValueError("Full-history actual multi-revision coverage not established")
            receipt = {
                "status": "PASS_WITH_EXPLICIT_SOURCE_EXCEPTION" if full.quarantined > 0 else "PASS",
                "quality_status": full.quality_status,
                "approval_digest": full.approval_digest,
                "approved_exceptions": [e.model_dump() for e in full.approved_exceptions],
                "source_quarantined": full.quarantined,
                "source_blocking_issues": full.blocking_issues,
                "scope": [str(full.source_start), str(full.source_end)],
                "actual_suffix_tested": prefix is not None,
                "prefix_end": str(prefix.source_end) if prefix else None,
                "verified_tables": len(TABLES),
                "incremental_clean_mismatches": parity,
                "cross_run_replay": "PASS",
                "actual_multi_revision_entities": multi_revisions,
                "recovery": json.loads((output / "recovery-proof.json").read_text())
                if (output / "recovery-proof.json").exists()
                else "NOT EXERCISED: no batches",
                "source_rows": {s.table: s.rows for s in full.tables},
                "stream_target": frozen["stream_target"],
                "clean_target": frozen["clean_target"],
                "production_written": False,
                "full_remote_checks": checks,
                "elapsed_seconds_this_execution": time.monotonic() - started,
                "elapsed_active_seconds": prior_seconds + time.monotonic() - started,
                "peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                "s3_bytes": "NOT MEASURED",
                "peak_spill": "NOT MEASURED",
            }
            save(output / "receipt.json", receipt)
            state.update(
                status=receipt["status"],
                quality_status=full.quality_status,
                finished_at=datetime.now(UTC).isoformat(),
            )
            save(marker, state)
            return receipt
        except BaseException as exc:
            state.update(
                status="FAILED",
                error_type=type(exc).__name__,
                elapsed_seconds_this_execution=time.monotonic() - started,
                elapsed_active_seconds=prior_seconds + time.monotonic() - started,
            )
            save(marker, state)
            raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--full-bundle", type=Path, required=True)
    parser.add_argument("--prefix-bundle", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--memory-limit", default=None)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--execute-isolated", action="store_true")
    args = parser.parse_args()
    if not args.execute_isolated:
        raise SystemExit("Explicit isolated acceptance required")
    from dotenv import load_dotenv

    load_dotenv(args.env_file)
    from procurement.common.settings import Settings
    from procurement.storage.iceberg import RestCatalog, connect

    config = Settings.from_environment().iceberg_config()
    with connect(config, max_memory="512MB", threads=1) as remote, RestCatalog(config) as catalog:
        result = accept(
            args.full_bundle,
            args.prefix_bundle,
            args.output,
            remote,
            catalog,
            on_progress=lambda s: print(canonical(s), flush=True),
            memory_limit=args.memory_limit,
        )
    print(canonical(result), flush=True)


if __name__ == "__main__":
    main()
