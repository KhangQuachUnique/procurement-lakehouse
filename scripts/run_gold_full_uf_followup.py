"""Compute only the missing independent full; compare retained incremental pins.

Source is the original clean run's captured Silver inputs, NOT incremental answers.
Never re-run ingestion/incremental, alter old plans, or activate production Gold.
"""

import argparse
import hashlib
import json
import resource
import tarfile
import time
from pathlib import Path
from uuid import uuid4

import duckdb
from dotenv import load_dotenv

from procurement.common.settings import Settings
from procurement.gold.adapters.driven.iceberg.iceberg_publisher import IcebergPublisher
from procurement.gold.adapters.driven.silver.iceberg_reader import IcebergReader
from procurement.gold.application.backend import PipelineBackend
from procurement.gold.application.gold_service import GoldService
from procurement.gold.application.state import StateStore
from procurement.gold.domain.models import INPUT_TABLES, RunPlan, SilverReadyInput, logic_version
from procurement.gold.domain.schema import TABLES
from procurement.storage.iceberg import RestCatalog, connect
from procurement.tools.gold_history_candidate import file_hash, save


class ArtifactReader:
    def __init__(self, path, ready, remote_reader):
        self.path, self.ready, self.remote_reader = path, ready, remote_reader
        self.stat = (path.stat().st_size, path.stat().st_mtime_ns)
        self.db = duckdb.connect(
            str(path), read_only=True, config={"threads": "1", "memory_limit": "256MB"}
        )
        rows = self.db.execute("SELECT run_id,phase,plan FROM control.runs").fetchall()
        if len(rows) != 1:
            raise ValueError("Artifact must be original standalone clean capture")
        run_id, phase, raw = rows[0]
        plan = RunPlan.model_validate_json(raw)
        if phase != "INPUTS_FROZEN" or plan.mode != "backfill" or plan.source != ready:
            raise ValueError("Artifact source/phase differs from attested clean inputs")
        captured = dict(
            self.db.execute(
                "SELECT table_name,rows FROM control.captures WHERE run_id=?", [run_id]
            ).fetchall()
        )
        if captured != {t.table: t.rows for t in ready.tables}:
            raise ValueError("Artifact capture receipts incomplete")

    def verify(self, ready):
        if ready != self.ready or self.stat != (
            self.path.stat().st_size,
            self.path.stat().st_mtime_ns,
        ):
            raise ValueError("Frozen source artifact changed")
        self.remote_reader.verify(ready)

    def columns(self, snapshot):
        if snapshot.table not in INPUT_TABLES:
            raise ValueError("Unexpected capture table")
        return tuple(
            (r[0], r[1]) for r in self.db.execute("DESCRIBE capture." + snapshot.table).fetchall()
        )

    def batches(self, snapshot):
        if snapshot.table not in INPUT_TABLES:
            raise ValueError("Unexpected capture table")
        yield from self.db.execute("SELECT * FROM capture." + snapshot.table).to_arrow_reader(
            batch_size=8192
        )


def business_compatibility(archive):
    current = Path("src/procurement/gold/application")
    selected = {
        "aggregation.py",
        "schema.py",
        "schema.json",
        "validation.py",
        "references.py",
        "source.py",
        "source_cache.py",
        "contracts.py",
        "publication.py",
    }
    checked = {}
    with tarfile.open(archive) as tar:
        for entry in tar.getmembers():
            prefix = "gold/v2/"
            if not entry.isfile() or not entry.name.startswith(prefix):
                continue
            name = entry.name[len(prefix) :]
            if not (
                name in selected
                or (name.startswith("compute/") and Path(name).suffix in (".py", ".sql", ".json"))
            ):
                continue
            before, after = tar.extractfile(entry).read(), (current / name).read_bytes()
            if before != after:
                raise ValueError("Business definition changed across comparison versions: " + name)
            checked[name] = hashlib.sha256(after).hexdigest()
    if not selected.issubset(checked):
        raise ValueError("Incomplete retained business-definition archive")
    return checked


def retained_parity(path, full_identity, stream_identity):
    """Only reuse a complete exact comparison bound to both immutable plans."""
    if not path.exists():
        return None
    proof = json.loads(path.read_text())
    if (proof.get("full_identity"), proof.get("stream_identity")) != (
        full_identity, stream_identity
    ):
        raise ValueError("Retained parity belongs to different plans")
    tables = proof.get("tables", {})
    if set(tables) != set(TABLES) or any(type(v) is not int or v != 0 for v in tables.values()):
        raise ValueError("Retained exact parity is incomplete or failed")
    return tables


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--execute-isolated", action="store_true")
    args = parser.parse_args()
    if not args.execute_isolated:
        raise SystemExit("Explicit isolated full follow-up required")
    base = Path("data/gold-v2-acceptance")
    root = base / "two-year-chunked-20261107-v3"
    source = root / "clean-state/state.duckdb"
    gate = json.loads((base / "union-find-v1/full-planner/proof.json").read_text())
    if gate["status"] != "PASS" or gate["planner"]["affected_nodes"] != 1790122:
        raise ValueError("Full-scale planner gate not passed")
    compatible = business_compatibility(base / "union-find-v1/before-runtime.tar.gz")
    ready_path = base / "two-year-retained-20261107-v1/full-ready.json"
    ready = SilverReadyInput.model_validate(json.loads(ready_path.read_text())["receipt"])
    with duckdb.connect(
        str(root / "stream-state/state.duckdb"),
        read_only=True,
        config={"threads": "1", "memory_limit": "128MB"},
    ) as old:
        raw = old.execute(
            "SELECT plan FROM control.runs WHERE run_id=(SELECT release_id FROM control.accepted)"
        ).fetchone()[0]
    stream = RunPlan.model_validate_json(raw)
    if stream.source != ready:
        raise ValueError("Retained stream and full source vectors differ")
    args.output.mkdir(parents=True, exist_ok=True)
    path = args.output / "plan.json"
    identity = {
        "logic": logic_version(),
        "ready_sha256": file_hash(ready_path),
        "stream_identity": stream.identity,
        "source_stat": [source.stat().st_size, source.stat().st_mtime_ns],
        "business": compatible,
    }
    if path.exists():
        frozen = json.loads(path.read_text())
        if frozen["identity"] != identity:
            raise ValueError("Immutable follow-up definition differs")
    else:
        nonce = uuid4().hex[:12]
        frozen = {
            "identity": identity,
            "run_id": "full_uf_" + nonce,
            "noop_id": "noop_uf_" + nonce,
            "target": "gold_v2_full_uf_" + nonce,
        }
        save(path, frozen)
    request = RunPlan.model_validate(
        stream.model_dump()
        | {
            "run_id": frozen["run_id"],
            "mode": "backfill",
            "base_release": None,
            "logic_version": logic_version(),
            "target_namespace": frozen["target"],
            "budget": {"memory_limit": "2GB", "threads": 1, "batch_rows": 50000},
        }
    )
    # Recover the last table-level evidence after a supervisor timeout. Neither
    # output is mutable here: service phase and remote active pins are checked
    # again below before the comparison is reused.
    parity_path = args.output / "exact-parity-proof.json"
    marker = args.output / "job.json"
    if marker.exists() and not parity_path.exists():
        old_state = json.loads(marker.read_text())
        steps = old_state.get("steps", {})
        values = steps.get("exact_parity", {})
        if (
            old_state.get("scope") == [str(ready.source_start), str(ready.source_end)]
            and old_state.get("incremental_reexecuted") is False
            and steps.get("full", {}).get("phase") == "COMMITTED"
            and set(values) == set(TABLES)
            and all(type(v) is int and v == 0 for v in values.values())
        ):
            with StateStore(args.output / "state") as previous:
                if previous.phase(request.run_id).value != "COMMITTED":
                    raise ValueError("Retained full comparison is not committed")
                if previous.plan(request.run_id).identity != request.identity:
                    raise ValueError("Retained full plan changed")
            save(args.output / "interrupted-job.json", old_state)
            save(parity_path, {
                "full_identity": request.identity,
                "stream_identity": stream.identity,
                "tables": values,
                "origin": "durable per-table comparison before supervisor timeout",
            })
    retained = retained_parity(parity_path, request.identity, stream.identity)
    state = {
        "status": "RUNNING",
        "production_written": False,
        "scope": [str(ready.source_start), str(ready.source_end)],
        "current": "full",
        "steps": {},
        "incremental_reexecuted": False,
    }
    started = time.monotonic()

    def progress(phase, evidence):
        state.update(
            current=phase,
            elapsed_seconds=time.monotonic() - started,
            peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        )
        state["steps"][phase] = evidence
        save(args.output / "job.json", state)
        print(json.dumps(state), flush=True)

    class RecoveryInterruption(RuntimeError):
        pass

    load_dotenv(args.env_file)
    cfg = Settings.from_environment().iceberg_config()
    try:
        with connect(cfg, max_memory="512MB", threads=1) as remote, RestCatalog(cfg) as catalog:
            reader = ArtifactReader(source, ready, IcebergReader(remote, catalog, ready.namespace))
            publisher = IcebergPublisher(remote, catalog)

            def batch(done, total):
                progress("batch", {"done": done, "total": total})
                proof = args.output / "recovery-proof.json"
                if not proof.exists():
                    save(proof, {"after_batch": done, "resumed": False})
                    raise RecoveryInterruption("After durable full business batch")

            service = GoldService(
                args.output / "state", PipelineBackend(reader, publisher, on_batch=batch)
            )
            service.create(request)
            progress("full", {"state": "RESUMING", "target": request.target_namespace})
            try:
                service.resume(
                    request.run_id, on_phase=lambda p: progress("full", {"phase": p.value})
                )
            except RecoveryInterruption:
                service.resume(
                    request.run_id, on_phase=lambda p: progress("full", {"phase": p.value})
                )
                proof = json.loads((args.output / "recovery-proof.json").read_text())
                save(
                    args.output / "recovery-proof.json",
                    proof | {"resumed": True, "final_phase": "COMMITTED"},
                )
            # Independent full output exists BEFORE the retained incremental is read.
            if not publisher.is_active(stream) or not publisher.is_active(request):
                raise ValueError("A retained/current comparison generation changed")
            publisher.ensure_views(stream, "uf_incremental")
            publisher.ensure_views(request, "uf_clean")
            parity = retained or {}
            for name, table in TABLES.items():
                if name in parity:
                    continue
                key = table.key
                for side in ("uf_incremental", "uf_clean"):
                    if remote.execute(
                        f"SELECT EXISTS(SELECT 1 FROM {side}.{name} WHERE {key} IS NULL)"
                    ).fetchone()[0]:
                        raise ValueError("Null public comparison key")
                mismatches = 0
                for bucket in range(16):
                    where = f"hash({key})%16={bucket}"
                    mismatches += remote.execute(
                        f"SELECT count(*) FROM ((SELECT * FROM uf_incremental.{name} WHERE {where} EXCEPT ALL SELECT * FROM uf_clean.{name} WHERE {where}) UNION ALL (SELECT * FROM uf_clean.{name} WHERE {where} EXCEPT ALL SELECT * FROM uf_incremental.{name} WHERE {where}))"
                    ).fetchone()[0]
                parity[name] = mismatches
                progress("exact_parity", parity.copy())
                if mismatches:
                    raise ValueError("Independent full/incremental row mismatch: " + name)
            save(parity_path, {
                "full_identity": request.identity,
                "stream_identity": stream.identity,
                "tables": parity,
                "origin": "exact comparison; both active pinned generations revalidated",
            })
            progress("exact_parity", parity.copy())
            noop = RunPlan.model_validate(
                request.model_dump()
                | {
                    "run_id": frozen["noop_id"],
                    "mode": "incremental",
                    "base_release": request.run_id,
                }
            )
            service.create(noop)
            progress("noop", {"state": "RESUMING", "run_id": noop.run_id})
            replay = service.resume(
                noop.run_id, on_phase=lambda p: progress("noop", {"phase": p.value})
            )
            if replay.get("equivalent_source", {}).get("release_id") != request.run_id:
                raise ValueError("Equivalent-input replay changed public release")
            with StateStore(args.output / "state") as local:
                local.db.execute("SET memory_limit='2GB'")
                coverage = {
                    n: local.db.execute(
                        f"SELECT count(*) FROM (SELECT entity_id FROM frozen.{n} GROUP BY entity_id HAVING count(DISTINCT revision_id)>1)"
                    ).fetchone()[0]
                    for n in ("package_revision", "notice_revision", "result_revision")
                }
            complete = any(coverage.values())
            receipt = {
                "status": ("PASS_WITH_EXPLICIT_SOURCE_EXCEPTION" if ready.quarantined else "PASS")
                if complete
                else "PARITY_PASS_COVERAGE_PENDING",
                "scope": state["scope"],
                "verified_tables": 16,
                "incremental_clean_mismatches": parity,
                "full_target": request.target_namespace,
                "retained_incremental_target": stream.target_namespace,
                "old_logic": stream.logic_version,
                "new_logic": request.logic_version,
                "business_files_unchanged": compatible,
                "source": "original clean Silver captures, not incremental outputs",
                "cross_run_replay": "PASS",
                "recovery": json.loads((args.output / "recovery-proof.json").read_text()),
                "actual_multi_revision_entities": coverage,
                "quality_status": ready.quality_status,
                "source_quarantined": ready.quarantined,
                "production_written": False,
                "incremental_reexecuted": False,
            }
            save(args.output / "receipt.json", receipt)
            state.update(status=receipt["status"])
            progress("complete", receipt)
            reader.db.close()
    except BaseException as exc:
        state.update(status="FAILED", error_type=type(exc).__name__, error=str(exc))
        progress("failure", {"type": type(exc).__name__, "message": str(exc)})
        raise


if __name__ == "__main__":
    main()
