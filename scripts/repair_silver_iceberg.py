"""Recover the three result tables without catalog renames or a Bronze rebuild.

Run `stage`, then `repair`, then `verify`. Every destructive metadata change is
preceded by a durable source copy and a saved catalog record. No physical cleanup
is performed here; cleanup requires a separate cross-catalog reference audit.
"""

import json
import sys
from datetime import UTC, datetime
from pathlib import Path

from procurement.common.file_lock import exclusive_file_lock
from procurement.common.settings import Settings
from procurement.silver.domain.schema import get_primary_key, metadata
from procurement.storage.iceberg import RestCatalog, connect, literal

ROOT = Path("data/silver-iceberg-repair-20261106")
DOMAIN = ("result_revision", "award_lot", "award_contractor")
ARCHIVE = "silver_archive_20261106_023309"


def save(name, data):
    ROOT.mkdir(parents=True, exist_ok=True)
    p = ROOT / name
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2, default=str))
    tmp.replace(p)


def log(message):
    print(datetime.now(UTC).isoformat(), message, flush=True)


def columns(name):
    return ",".join('"' + c.name + '"' for c in metadata.tables["silver." + name].columns)


def fingerprint(db, relation, name):
    cols = columns(name)
    pk = get_primary_key(name)
    row = db.execute(
        f'SELECT count(*),count(DISTINCT "{pk}"),count(*) FILTER (WHERE "{pk}" IS NULL),'
        f"sum(hash({cols})::HUGEINT),bit_xor(hash({cols})) FROM {relation}"
    ).fetchone()
    return dict(zip(("rows", "distinct_keys", "null_keys", "hash_sum", "hash_xor"), row))


def stage(cfg):
    manifest = json.loads(Path("data/manual-result-history/parquet.json").read_text())
    if manifest["status"] != "completed":
        raise ValueError("Parquet source is not complete")
    with (
        connect(cfg, database=str(ROOT / "expected.duckdb"), max_memory="1GB", threads=1) as db,
        RestCatalog(cfg) as cat,
    ):
        db.execute("SET http_retries=2")
        if not (ROOT / "catalog-before.json").exists():
            save(
                "catalog-before.json",
                {
                    "live": {n: cat.table("silver", n) for n in DOMAIN},
                    "archive": {n: cat.table(ARCHIVE, n) for n in DOMAIN},
                },
            )
        tables = {r[0] for r in db.execute("SHOW TABLES").fetchall()}
        if "preserved_ids" not in tables:
            log("Identifying out-of-scope rows to preserve")
            db.execute(
                "CREATE TEMP TABLE scope_obs AS SELECT observation_id FROM lake.silver.observation WHERE resource='contractor_result' AND source_date BETWEEN DATE '2022-01-01' AND DATE '2025-12-31'"
            )
            db.execute(
                f"CREATE TABLE preserved_ids AS SELECT revision_id FROM lake.{ARCHIVE}.result_revision WHERE observation_id IS NULL OR observation_id NOT IN (SELECT observation_id FROM scope_obs)"
            )
        receipt = {"parquet_run_id": manifest["run_id"], "tables": {}}
        for name in DOMAIN:
            expected = "expected_" + name
            if expected not in tables:
                log("Staging complete source + preserved rows: " + name)
                files = (
                    "["
                    + ",".join(
                        literal(
                            f"s3://procurement-lakehouse/silver/{name}/source_date={day}/data.parquet"
                        )
                        for day in manifest["days"]
                    )
                    + "]"
                )
                predicate = "revision_id IN (SELECT revision_id FROM preserved_ids)"
                db.execute(
                    f"CREATE TABLE {expected} AS SELECT {columns(name)} FROM read_parquet({files},hive_partitioning=false) UNION ALL SELECT {columns(name)} FROM lake.{ARCHIVE}.{name} WHERE {predicate}"
                )
            fp = fingerprint(db, expected, name)
            if fp["rows"] != fp["distinct_keys"] or fp["null_keys"]:
                raise ValueError(f"Invalid source primary keys: {name}: {fp}")
            receipt["tables"][name] = fp
            log(f"Staged {name}: {fp['rows']} rows")
            save("source-progress.json", receipt)
        # Children must resolve to the complete parent source, including preserved rows.
        for name in DOMAIN[1:]:
            missing = db.execute(
                f"SELECT count(*) FROM expected_{name} c ANTI JOIN expected_result_revision p USING(revision_id)"
            ).fetchone()[0]
            if missing:
                raise ValueError(f"Orphan children: {name}: {missing}")
        db.execute("CHECKPOINT")
        receipt["status"] = "verified_source"
        save("source.json", receipt)


def repair(cfg):
    source = json.loads((ROOT / "source.json").read_text())
    assert source["status"] == "verified_source"
    progress = (
        json.loads((ROOT / "repair.json").read_text())
        if (ROOT / "repair.json").exists()
        else {"tables": {}}
    )
    for name in DOMAIN:
        if progress["tables"].get(name, {}).get("verified"):
            continue
        entry = progress["tables"].setdefault(name, {})
        if not entry.get("reset"):
            with RestCatalog(cfg) as cat:
                m = cat.table("silver", name)["metadata"]
                original = json.loads((ROOT / "catalog-before.json").read_text())["live"][name][
                    "metadata"
                ]
                if m["current-snapshot-id"] != original["current-snapshot-id"]:
                    raise ValueError("Concurrent writer or unrecorded reset: " + name)
                updates = [{"action": "set-location", "location": f"s3://warehouse/silver/{name}"}]
                updates += [
                    {"action": "remove-snapshot-ref", "ref-name": r} for r in m.get("refs", {})
                ]
                updates += [
                    {
                        "action": "remove-snapshots",
                        "snapshot-ids": [s["snapshot-id"] for s in m.get("snapshots", [])],
                    }
                ]
                cat.request(
                    "POST",
                    f"namespaces/silver/tables/{name}",
                    json={
                        "requirements": [
                            {"type": "assert-table-uuid", "uuid": m["table-uuid"]},
                            {
                                "type": "assert-ref-snapshot-id",
                                "ref": "main",
                                "snapshot-id": m["current-snapshot-id"],
                            },
                        ],
                        "updates": updates,
                    },
                )
                entry["reset"] = True
                save("repair.json", progress)
                log("Repointed and cleared broken snapshots (no files deleted): " + name)
        with connect(cfg, max_memory="2GB", threads=1) as db:
            db.execute(
                f"ATTACH {literal(str((ROOT / 'expected.duckdb').resolve()))} AS recovery (READ_ONLY)"
            )
            # Silver result is partitioned by public_date. A single four-year
            # INSERT opens too many partition writers, so commit month buckets.
            # Each bucket is retry-safe, including a lost acknowledgement.
            if name == "result_revision":
                buckets = db.execute(
                    "SELECT DISTINCT date_trunc('month',public_date)::DATE FROM recovery.expected_result_revision ORDER BY 1 NULLS LAST"
                ).fetchall()
            else:
                buckets = [("all",)]
            done = entry.setdefault("batches", [])
            for (month,) in buckets:
                label = str(month)
                if label in done:
                    continue
                predicate = (
                    "TRUE"
                    if month == "all"
                    else "public_date IS NULL"
                    if month is None
                    else f"public_date >= DATE '{month}' AND public_date < DATE '{month}' + INTERVAL 1 MONTH"
                )
                existing = db.execute(
                    f"SELECT count(*) FROM lake.silver.{name} WHERE {predicate}"
                ).fetchone()[0]
                if existing:
                    actual = fingerprint(
                        db, f"(SELECT * FROM lake.silver.{name} WHERE {predicate})", name
                    )
                    expected = fingerprint(
                        db, f"(SELECT * FROM recovery.expected_{name} WHERE {predicate})", name
                    )
                    if actual != expected:
                        raise ValueError(
                            "Unexpected partial bucket, refusing duplicate append: "
                            + name
                            + ":"
                            + label
                        )
                else:
                    log("Writing restored bucket: " + name + ":" + label)
                    db.execute(
                        f"INSERT INTO lake.silver.{name} ({columns(name)}) SELECT {columns(name)} FROM recovery.expected_{name} WHERE {predicate}"
                    )
                done.append(label)
                save("repair.json", progress)
                log(f"Committed {name} bucket {len(done)}/{len(buckets)}: {label}")
        # Never trust the writer connection or summary-only counts.
        with connect(cfg, max_memory="1GB", threads=1) as db, RestCatalog(cfg) as cat:
            m = cat.table("silver", name)["metadata"]
            snap = m["current-snapshot-id"]
            actual = fingerprint(db, f"lake.silver.{name} AT (VERSION => {snap})", name)
            if actual != source["tables"][name]:
                raise ValueError("Fresh connection content mismatch: " + name)
            entry.update(
                verified=True, snapshot_id=snap, fingerprint=actual, location=m["location"]
            )
            save("repair.json", progress)
            log(f"Fresh pinned read verified {name}: {actual['rows']} rows")
    progress["status"] = "completed"
    save("repair.json", progress)


def verify(cfg):
    source = json.loads((ROOT / "source.json").read_text())
    result = {"tables": {}}
    with connect(cfg, max_memory="1GB", threads=1) as db, RestCatalog(cfg) as cat:
        for name in DOMAIN:
            m = cat.table("silver", name)["metadata"]
            actual = fingerprint(
                db, f"lake.silver.{name} AT (VERSION => {m['current-snapshot-id']})", name
            )
            assert actual == source["tables"][name], name
            result["tables"][name] = actual
        for name in DOMAIN[1:]:
            missing = db.execute(
                f"SELECT count(*) FROM lake.silver.{name} c ANTI JOIN lake.silver.result_revision p USING(revision_id)"
            ).fetchone()[0]
            assert not missing, (name, missing)
        result["status"] = "verified"
        save("acceptance.json", result)
        sync_path = Path("data/manual-result-history/sync.json")
        synced = json.loads(sync_path.read_text())
        if synced.get("parquet_run_id") != source["parquet_run_id"]:
            raise ValueError("Sync source changed during recovery")
        synced["recovery_readback"] = {
            "verified_at": datetime.now(UTC).isoformat(),
            "receipt": str(ROOT / "acceptance.json"),
            "tables": result["tables"],
        }
        tmp = sync_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(synced, indent=2))
        tmp.replace(sync_path)
        log("All restored tables independently verified")


if __name__ == "__main__":
    ROOT.mkdir(parents=True, exist_ok=True)
    with exclusive_file_lock(Path("data/manual-result-history/workflow.lock")):
        {"stage": stage, "repair": repair, "verify": verify}[sys.argv[1]](
            Settings.from_environment().iceberg_config()
        )
