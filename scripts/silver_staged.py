"""Manual result-history workflow: reset, Parquet only, then monthly Iceberg sync.

Scope is committed contractor_result source days in 2022–2025. Never crawls Bronze.
"""

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import typer
from dotenv import load_dotenv

from procurement.cli.silver import execute_resource, get_storage
from procurement.common.file_lock import exclusive_file_lock
from procurement.common.settings import Settings
from procurement.silver.application.resources import get_silver_resource
from procurement.silver.domain.schema import metadata, to_iceberg_ddl
from procurement.storage.iceberg import RestCatalog, connect, literal

load_dotenv()
app = typer.Typer(pretty_exceptions_show_locals=False)
ROOT = Path("data/manual-result-history")
DOMAIN = ("result_revision", "award_lot", "award_contractor")
GOV = ("observation", "quality_issue", "quarantine")
START, END = date(2022, 1, 1), date(2025, 12, 31)


def save(name, value):
    ROOT.mkdir(parents=True, exist_ok=True)
    path = ROOT / name
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps(value, indent=2, default=str))
    temp.replace(path)


def columns(name):
    return ",".join('"' + c.name + '"' for c in metadata.tables["silver." + name].columns)


def rename(cat, old_ns, new_ns, name):
    cat.request(
        "POST",
        "tables/rename",
        json={
            "source": {"namespace": [old_ns], "name": name},
            "destination": {"namespace": [new_ns], "name": name},
        },
    )


@app.command()
def reset():
    """Remove incorrect history from live domain tables, keeping a recoverable catalog archive."""
    with exclusive_file_lock(ROOT / "workflow.lock"):
        if (ROOT / "reset.json").exists():
            raise typer.BadParameter(
                "Reset already recorded; inspect reset.json instead of resetting again"
            )
        cfg = Settings.from_environment().iceberg_config()
        stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        archive, replacement = "silver_archive_" + stamp, "silver_clean_" + stamp
        state = {
            "status": "preparing",
            "archive": archive,
            "replacement": replacement,
            "scope": ["2022-01-01", "2025-12-31"],
            "tables": {},
        }
        with connect(cfg, max_memory="3GB", threads=1) as db, RestCatalog(cfg) as cat:
            db.execute("CREATE SCHEMA IF NOT EXISTS lake." + archive)
            db.execute("CREATE SCHEMA IF NOT EXISTS lake." + replacement)
            db.execute(
                "CREATE TEMP TABLE scoped_obs AS SELECT observation_id FROM lake.silver.observation WHERE resource='contractor_result' AND source_date BETWEEN DATE '2022-01-01' AND DATE '2025-12-31'"
            )
            db.execute(
                "CREATE TEMP TABLE preserved_rev AS SELECT revision_id FROM lake.silver.result_revision WHERE observation_id NOT IN (SELECT observation_id FROM scoped_obs) OR observation_id IS NULL"
            )
            # Prepare every replacement BEFORE moving any live catalog identity.
            for name in DOMAIN:
                source = "lake.silver." + name
                snapshot = cat.table("silver", name)["metadata"].get("current-snapshot-id")
                predicate = (
                    "observation_id NOT IN (SELECT observation_id FROM scoped_obs) OR observation_id IS NULL"
                    if name == "result_revision"
                    else "revision_id IN (SELECT revision_id FROM preserved_rev)"
                )
                db.execute(to_iceberg_ddl(name, namespace=replacement))
                cols = columns(name)
                db.execute(
                    f"INSERT INTO lake.{replacement}.{name} ({cols}) SELECT {cols} FROM {source} WHERE {predicate}"
                )
                expected = db.execute(
                    f"SELECT count(*) FROM {source} WHERE {predicate}"
                ).fetchone()[0]
                actual = db.execute(f"SELECT count(*) FROM lake.{replacement}.{name}").fetchone()[0]
                if expected != actual:
                    raise RuntimeError("Out-of-scope copy count mismatch: " + name)
                state["tables"][name] = {
                    "previous_snapshot": snapshot,
                    "preserved_rows": actual,
                    "switched": False,
                }
                save("reset.json", state)
            state["status"] = "switching"
            save("reset.json", state)
            for name in DOMAIN:
                # REST rename preserves metadata, tags and physical files in the archive.
                if (
                    cat.table("silver", name)["metadata"].get("current-snapshot-id")
                    != state["tables"][name]["previous_snapshot"]
                ):
                    raise RuntimeError("Concurrent writer detected: " + name)
                rename(cat, "silver", archive, name)
                try:
                    rename(cat, replacement, "silver", name)
                except BaseException:
                    rename(cat, archive, "silver", name)
                    raise
                state["tables"][name]["switched"] = True
                save("reset.json", state)
            # Governance is shared. Only discard issues belonging to this exact cohort.
            for name in ("quality_issue", "quarantine"):
                db.execute(
                    f"DELETE FROM lake.silver.{name} WHERE observation_id IN (SELECT observation_id FROM scoped_obs)"
                )
            state["status"] = "completed"
            save("reset.json", state)
        typer.echo(json.dumps(state, indent=2))


@app.command()
def parquet():
    """Run current Silver logic only on committed source days; never publish Iceberg."""
    from procurement.infrastructure.database import create_application_engine
    from procurement.silver.adapters.driven.bronze.arrow_adapter import ArrowBronzeRepositoryAdapter

    with exclusive_file_lock(ROOT / "workflow.lock"):
        fs, bucket = get_storage()
        engine = create_application_engine()
        try:
            repo = ArrowBronzeRepositoryAdapter(fs=fs, db_engine=engine)
            parts = repo.select_committed_partitions(["contractor_result"], START, END)
            days = sorted({date.fromisoformat(p.source_date) for p in parts})
        finally:
            engine.dispose()
        if not days:
            raise RuntimeError("No committed Bronze partitions")
        expected = {START + timedelta(days=i) for i in range((END - START).days + 1)}
        state = {
            "status": "running",
            "run_id": datetime.now(UTC).strftime("%Y%m%d_%H%M%S_%f"),
            "days": list(map(str, days)),
            "missing_bronze_days": sorted(map(str, expected - set(days))),
            "reports": [],
        }
        save("sync.json", {"status": "not_started", "reason": "Parquet is being regenerated"})
        save("parquet.json", state)
        # Group contiguous days, respecting known gaps instead of recrawling them.
        groups = []
        for day in days:
            if groups and day == groups[-1][1] + timedelta(days=1):
                groups[-1][1] = day
            else:
                groups.append([day, day])
        for first, last in groups:
            report = execute_resource(
                fs,
                bucket,
                "contractor_result",
                first,
                last,
                iceberg=False,
                workers=2,
                skip_existing=False,
            )
            state["reports"].append(report)
            save("parquet.json", state)
            if report["status"] != "completed":
                raise RuntimeError("Parquet processing did not complete; inspect parquet.json")
        state["status"] = "completed"
        save("parquet.json", state)
        typer.echo("Parquet complete. Missing Bronze days: " + str(state["missing_bronze_days"]))


def sync_month(db):
    # Scope both old and new revision IDs, clearing obsolete children on replay.
    db.execute(
        "CREATE OR REPLACE TEMP TABLE month_obs AS SELECT observation_id FROM src_observation"
    )
    db.execute(
        "CREATE OR REPLACE TEMP TABLE month_rev AS SELECT revision_id FROM src_result_revision UNION SELECT revision_id FROM lake.silver.result_revision WHERE observation_id IN (SELECT observation_id FROM month_obs)"
    )
    db.execute("BEGIN")
    try:
        for name in (
            "award_contractor",
            "award_lot",
            "result_revision",
            "quality_issue",
            "quarantine",
        ):
            key, scope = (
                ("revision_id", "month_rev")
                if name in ("award_contractor", "award_lot")
                else ("observation_id", "month_obs")
            )
            db.execute(f"DELETE FROM lake.silver.{name} WHERE {key} IN (SELECT {key} FROM {scope})")
        # Observation is immutable source lineage, not replaced by mapper changes.
        cols = columns("observation")
        db.execute(
            f"INSERT INTO lake.silver.observation ({cols}) SELECT {cols} FROM src_observation s WHERE NOT EXISTS (SELECT 1 FROM lake.silver.observation o WHERE o.observation_id=s.observation_id)"
        )
        for name in (*DOMAIN, "quality_issue", "quarantine"):
            cols = columns(name)
            db.execute(f"INSERT INTO lake.silver.{name} ({cols}) SELECT {cols} FROM src_{name}")
        db.execute("COMMIT")
    except BaseException:
        db.execute("ROLLBACK")
        raise


def verify_synced_tables(cfg):
    """Read actual files through a new catalog connection, not summary counts.

    This deliberately hashes all domain columns: COUNT(*) alone can be answered
    from manifest summaries even when a data object is missing. Pinned reads also
    exercise the paths that downstream Gold uses.
    """
    verified = {}
    with connect(cfg, max_memory="1GB", threads=1) as db, RestCatalog(cfg) as cat:
        for name in DOMAIN:
            m = cat.table("silver", name)["metadata"]
            snap = m.get("current-snapshot-id")
            relation = f"lake.silver.{name}"
            if snap is not None and snap >= 0:
                relation += f" AT (VERSION => {snap})"
            rows, digest = db.execute(
                f"SELECT count(*),bit_xor(hash({columns(name)})) FROM {relation}"
            ).fetchone()
            if cat.table("silver", name)["metadata"].get("current-snapshot-id") != snap:
                raise RuntimeError("Silver changed during independent verification: " + name)
            verified[name] = {"snapshot_id": snap, "rows": rows, "hash_xor": digest}
    return verified


@app.command("sync-iceberg")
def sync():
    """Publish prepared Parquet in monthly transactions, without reprocessing Bronze."""
    with exclusive_file_lock(ROOT / "workflow.lock"):
        manifest = json.loads((ROOT / "parquet.json").read_text())
        if manifest["status"] != "completed":
            raise RuntimeError("Complete parquet stage first")
        reset_state = json.loads((ROOT / "reset.json").read_text())
        if reset_state["status"] != "completed":
            raise RuntimeError("Reset was interrupted; inspect reset.json first")
        fs, bucket = get_storage()
        paths = {
            name: prefix
            for prefix, names in get_silver_resource("result").candidate_groups(bucket + "/silver")
            for name in names
        }
        available = {
            name: set(fs.glob(f"{prefix}/{name}/source_date=*/data.parquet"))
            for name, prefix in paths.items()
        }
        months = sorted({day[:7] for day in manifest["days"]})
        state = {
            "status": "running",
            "parquet_run_id": manifest["run_id"],
            "months_completed": [],
            "missing_bronze_days": manifest["missing_bronze_days"],
        }
        save("sync.json", state)
        cfg = Settings.from_environment().iceberg_config()
        with connect(cfg, max_memory="3GB", threads=1) as db:
            db.execute("SET http_retries=8")
            db.execute("SET http_timeout=120")
            for month in months:
                days = [day for day in manifest["days"] if day.startswith(month)]
                for name in (*GOV, *DOMAIN):
                    files = [f"{paths[name]}/{name}/source_date={day}/data.parquet" for day in days]
                    missing = [p for p in files if p not in available[name]]
                    if missing:
                        raise RuntimeError("Missing prepared Parquet: " + missing[0])
                    sql_files = "[" + ",".join(literal("s3://" + p) for p in files) + "]"
                    db.execute(
                        f"CREATE OR REPLACE TEMP VIEW src_{name} AS SELECT {columns(name)} FROM read_parquet({sql_files}, hive_partitioning=false)"
                    )
                sync_month(db)
                state["months_completed"].append(month)
                save("sync.json", state)
                typer.echo(f"{month}: committed ({len(state['months_completed'])}/{len(months)})")
            from procurement.silver.application.warehouse import ensure_views

            ensure_views(db, "silver")
        state["status"] = "verifying"
        save("sync.json", state)
        state["readback"] = verify_synced_tables(cfg)
        state["status"] = "completed"
        save("sync.json", state)
        typer.echo("Iceberg sync complete; fresh pinned full-column readback passed.")


if __name__ == "__main__":
    app()
