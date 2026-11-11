"""Read-only snapshot input boundary with per-table durable local captures."""

from typing import Protocol

from procurement.gold.domain.models import INPUT_TABLES, Snapshot, identifier
from .source_cache import capture_from_journal


class SnapshotReader(Protocol):
    def verify(self, ready) -> None: ...
    def columns(self, snapshot: Snapshot) -> tuple[tuple[str, str], ...]: ...
    def batches(self, snapshot: Snapshot): ...


def freeze(db, plan, reader: SnapshotReader):
    reader.verify(plan.source)
    db.execute("CREATE SCHEMA IF NOT EXISTS capture")
    db.execute("CREATE SCHEMA IF NOT EXISTS control")
    db.execute(
        "CREATE TABLE IF NOT EXISTS control.captures(run_id VARCHAR,table_name VARCHAR,rows BIGINT,PRIMARY KEY(run_id,table_name))"
    )
    db.execute(
        "ALTER TABLE control.captures ADD COLUMN IF NOT EXISTS strategy VARCHAR DEFAULT 'full_pin'"
    )
    for snapshot in plan.source.tables:
        if db.execute(
            "SELECT count(*) FROM control.captures WHERE run_id=? AND table_name=?",
            [plan.run_id, snapshot.table],
        ).fetchone()[0]:
            continue
        outcome = capture_from_journal(db, plan, snapshot)
        if outcome and outcome["strategy"] == "journal":
            continue
        strategy = "full_pin_fallback" if outcome else "full_pin"
        columns = reader.columns(snapshot)
        for name, _ in columns:
            identifier(name)
        # SQL type strings are from trusted engine DESCRIBE, never receipt JSON.
        db.execute("BEGIN")
        try:
            ddl = ",".join(f"{n} {t}" for n, t in columns)
            db.execute(f"CREATE OR REPLACE TABLE capture.{snapshot.table} ({ddl})")
            for batch in reader.batches(snapshot):
                db.register("_capture_batch", batch)
                try:
                    db.execute(
                        f"INSERT INTO capture.{snapshot.table} BY NAME SELECT * FROM _capture_batch"
                    )
                finally:
                    db.unregister("_capture_batch")
            rows = db.execute(f"SELECT count(*) FROM capture.{snapshot.table}").fetchone()[0]
            if rows != snapshot.rows:
                raise ValueError("Input count differs from producer receipt: " + snapshot.table)
            db.execute(
                "INSERT INTO control.captures(run_id,table_name,rows,strategy) VALUES (?,?,?,?)",
                [plan.run_id, snapshot.table, rows, strategy],
            )
            db.execute("COMMIT")
        except BaseException:
            db.execute("ROLLBACK")
            raise
    # Verify the complete attested vector before accepting capture completion.
    reader.verify(plan.source)
    db.execute("BEGIN")
    try:
        db.execute("DROP SCHEMA IF EXISTS frozen CASCADE")
        db.execute("CREATE SCHEMA frozen")
        db.execute(
            "CREATE TABLE frozen.observation AS SELECT * FROM capture.observation WHERE source_date<=?",
            [plan.source_cutoff],
        )
        for name in (
            "package_revision",
            "notice_revision",
            "result_revision",
            "plan_revision",
            "opening_revision",
            "contract_revision",
            "contract_performance_revision",
        ):
            bad = db.execute(
                f"SELECT count(*) FROM capture.{name} r WHERE r.observation_id IS NULL OR NOT EXISTS (SELECT 1 FROM capture.observation o WHERE o.observation_id=r.observation_id)"
            ).fetchone()[0]
            if bad:
                raise ValueError("Missing observation lineage: " + name)
            db.execute(
                f"CREATE TABLE frozen.{name} AS SELECT r.* FROM capture.{name} r JOIN frozen.observation o USING(observation_id)"
            )
        for name, parent in (
            ("package_location", "package_revision"),
            ("award_lot", "result_revision"),
            ("award_contractor", "result_revision"),
            ("opening_submission", "opening_revision"),
        ):
            bad = db.execute(
                f"SELECT count(*) FROM capture.{name} c WHERE revision_id IS NULL OR NOT EXISTS (SELECT 1 FROM capture.{parent} r WHERE r.revision_id=c.revision_id)"
            ).fetchone()[0]
            if bad:
                raise ValueError("Missing revision lineage: " + name)
            db.execute(
                f"CREATE TABLE frozen.{name} AS SELECT c.* FROM capture.{name} c JOIN frozen.{parent} r USING(revision_id)"
            )
        if db.execute(
            "SELECT count(*) FROM frozen.award_contractor WHERE bid_result IS DISTINCT FROM 1"
        ).fetchone()[0]:
            raise ValueError("Legacy/non-winner Silver contractor input")
        db.execute("COMMIT")
    except BaseException:
        db.execute("ROLLBACK")
        raise
    return {
        "input_id": plan.source.input_id,
        "tables": len(INPUT_TABLES),
        "projection_version": plan.source.projection_version,
        "capture_strategies": dict(
            db.execute(
                "SELECT table_name,strategy FROM control.captures WHERE run_id=?", [plan.run_id]
            ).fetchall()
        ),
    }
