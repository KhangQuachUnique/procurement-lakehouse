"""Raw accepted source cache and authoritative receipt replay, not a public Gold mart."""

import duckdb

from procurement.gold.domain.models import INPUT_TABLES, RunPlan


def previous_source(db):
    row = db.execute(
        "SELECT r.plan FROM control.accepted a JOIN control.runs r ON a.run_id=r.run_id"
    ).fetchone()
    return RunPlan.model_validate_json(row[0]).source if row else None


def accept_cache(db, plan):
    db.execute("CREATE SCHEMA IF NOT EXISTS source_cache")
    for name in INPUT_TABLES:
        db.execute(f"CREATE OR REPLACE TABLE source_cache.{name} AS SELECT * FROM capture.{name}")


def capture_from_journal(db, plan, snapshot):
    if not plan.change_journal:
        return None
    previous = previous_source(db)
    if previous is None or (
        previous.namespace,
        previous.mapping_version,
        previous.projection_version,
    ) != (plan.source.namespace, plan.source.mapping_version, plan.source.projection_version):
        return None
    old = next(t for t in previous.tables if t.table == snapshot.table)
    if old.table_uuid != snapshot.table_uuid:
        return None
    from procurement.silver.application.change_journal import chain, replay

    try:
        events = chain(
            plan.change_journal,
            plan.source.namespace,
            snapshot.table,
            snapshot.table_uuid,
            old.snapshot_id,
            snapshot.snapshot_id,
        )
        db.execute("BEGIN")
        try:
            changes = replay(
                db, "source_cache." + snapshot.table, events, "capture." + snapshot.table
            )
            count = db.execute("SELECT count(*) FROM capture." + snapshot.table).fetchone()[0]
            if count != snapshot.rows:
                raise ValueError("Journal replay count differs from producer")
            db.execute(
                "INSERT INTO control.captures(run_id,table_name,rows,strategy) VALUES (?,?,?,?)",
                [plan.run_id, snapshot.table, count, "journal"],
            )
            db.execute("COMMIT")
        except BaseException:
            db.execute("ROLLBACK")
            raise
        return {"strategy": "journal", "edges": len(events), "changed_rows": changes}
    except (OSError, ValueError, KeyError, duckdb.Error):
        # Missing/corrupt/noncontiguous receipts are not "no changes". Caller
        # performs the complete pinned capture with explicit fallback evidence.
        return {"strategy": "full_pin_fallback"}
