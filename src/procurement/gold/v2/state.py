"""One locked DuckDB state store, immutable plans and atomic phase/output markers."""

from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path

import duckdb

from procurement.common.file_lock import exclusive_file_lock

from .contracts import Phase, RunPlan, canonical


class StateStore:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.db = None
        self._lock = None

    def __enter__(self):
        self.root.mkdir(parents=True, exist_ok=True)
        self._lock = exclusive_file_lock(self.root / "writer.lock")
        self._lock.__enter__()
        try:
            self.db = duckdb.connect(
                str(self.root / "state.duckdb"), config={"threads": "1", "memory_limit": "512MB"}
            )
            self.db.execute("CREATE SCHEMA IF NOT EXISTS control")
            self.db.execute("""CREATE TABLE IF NOT EXISTS control.runs (
                run_id VARCHAR PRIMARY KEY, identity VARCHAR NOT NULL, plan VARCHAR NOT NULL,
                phase VARCHAR NOT NULL, last_error VARCHAR)""")
            self.db.execute("""CREATE TABLE IF NOT EXISTS control.checkpoints (
                run_id VARCHAR, name VARCHAR, receipt VARCHAR NOT NULL,
                PRIMARY KEY(run_id,name))""")
            self.db.execute("""CREATE TABLE IF NOT EXISTS control.accepted (
                target VARCHAR PRIMARY KEY, run_id VARCHAR, release_id VARCHAR, receipt VARCHAR)""")
        except BaseException:
            if self.db:
                self.db.close()
            self._lock.__exit__(None, None, None)
            raise
        return self

    def __exit__(self, *args):
        try:
            self.db.close()
        finally:
            self._lock.__exit__(*args)

    @contextmanager
    def transaction(self):
        self.db.execute("BEGIN")
        try:
            yield self.db
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def create(self, plan: RunPlan):
        existing = self.db.execute(
            "SELECT identity FROM control.runs WHERE run_id=?", [plan.run_id]
        ).fetchone()
        if existing:
            if existing[0] != plan.identity:
                raise ValueError("Run identity is immutable; create a new plan")
            return
        # One unresolved plan per target, even before any publication starts.
        for (raw,) in self.db.execute(
            "SELECT plan FROM control.runs WHERE phase<>?", [Phase.COMMITTED]
        ).fetchall():
            if RunPlan.model_validate_json(raw).target_namespace == plan.target_namespace:
                raise ValueError("Target has an unfinished run; resume it first")
        for (raw,) in self.db.execute("SELECT plan FROM control.runs").fetchall():
            if RunPlan.model_validate_json(raw).target_namespace != plan.target_namespace:
                raise ValueError("A state root belongs to one explicit target namespace")
        accepted = self.db.execute(
            "SELECT release_id,run_id FROM control.accepted WHERE target=?", [plan.target_namespace]
        ).fetchone()
        if (accepted[0] if accepted else None) != plan.base_release:
            raise ValueError("Run base release differs from accepted target state")
        if accepted:
            previous = self.plan(accepted[1])
            if (
                plan.event_start,
                plan.event_end,
                plan.source.namespace,
                plan.reference_version,
                plan.logic_version,
            ) != (
                previous.event_start,
                previous.event_end,
                previous.source.namespace,
                previous.reference_version,
                previous.logic_version,
            ):
                raise ValueError("Definition changed; rebuild in a new isolated target")
            if (
                plan.source.source_start > previous.source.source_end + timedelta(days=1)
                or plan.source.source_end < previous.source.source_end
            ):
                raise ValueError("Gap or backwards range in accepted Silver coverage")
            if (plan.source.mapping_version, plan.source.projection_version) != (
                previous.source.mapping_version,
                previous.source.projection_version,
            ):
                raise ValueError("Silver mapper/projection changed; new validated rebuild required")
            if plan.source.approval_digest != previous.source.approval_digest:
                raise ValueError("Source exception policy changed; new isolated rebuild required")
            if {t.table: t.table_uuid for t in plan.source.tables} != {
                t.table: t.table_uuid for t in previous.source.tables
            }:
                raise ValueError("Silver table identity changed; new validated rebuild required")
            if (
                plan.source_cutoff < previous.source_cutoff
                or plan.observation_day < previous.observation_day
            ):
                raise ValueError("Accepted cutoff/observation date cannot go backwards")
        self.db.execute(
            "INSERT INTO control.runs VALUES (?,?,?,?,NULL)",
            [plan.run_id, plan.identity, canonical(plan), Phase.PLANNED],
        )

    def plan(self, run_id):
        row = self.db.execute("SELECT plan FROM control.runs WHERE run_id=?", [run_id]).fetchone()
        if not row:
            raise ValueError("Unknown run: " + run_id)
        return RunPlan.model_validate_json(row[0])

    def phase(self, run_id):
        row = self.db.execute("SELECT phase FROM control.runs WHERE run_id=?", [run_id]).fetchone()
        if not row:
            raise ValueError("Unknown run: " + run_id)
        return Phase(row[0])

    def advance(self, run_id, expected, target):
        phases = list(Phase)
        if phases.index(target) != phases.index(expected) + 1 or self.phase(run_id) != expected:
            raise ValueError("Invalid phase transition")
        self.db.execute(
            "UPDATE control.runs SET phase=?,last_error=NULL WHERE run_id=?", [target, run_id]
        )

    def checkpoint(self, run_id, name, receipt):
        self.plan(run_id)
        raw = canonical(receipt)
        old = self.db.execute(
            "SELECT receipt FROM control.checkpoints WHERE run_id=? AND name=?", [run_id, name]
        ).fetchone()
        if old and old[0] != raw:
            raise ValueError("Conflicting durable checkpoint: " + name)
        if not old:
            self.db.execute("INSERT INTO control.checkpoints VALUES (?,?,?)", [run_id, name, raw])

    def receipt(self, run_id, name):
        import json

        row = self.db.execute(
            "SELECT receipt FROM control.checkpoints WHERE run_id=? AND name=?", [run_id, name]
        ).fetchone()
        return json.loads(row[0]) if row else None

    def error(self, run_id, exc):
        # Do not persist exception messages: database/network failures may expose credentials.
        self.db.execute(
            "UPDATE control.runs SET last_error=? WHERE run_id=?", [type(exc).__name__, run_id]
        )

    def accept(self, plan, receipt):
        if self.phase(plan.run_id) != Phase.ACTIVE:
            raise ValueError("Cannot accept state before acknowledged activation")
        if (
            receipt.get("release_id") != plan.run_id
            or receipt.get("plan_identity") != plan.identity
        ):
            raise ValueError("Active receipt differs from immutable plan")
        self.db.execute(
            "INSERT OR REPLACE INTO control.accepted VALUES (?,?,?,?)",
            [plan.target_namespace, plan.run_id, receipt["release_id"], canonical(receipt)],
        )

    def status(self, run_id):
        plan = self.plan(run_id)
        row = self.db.execute(
            "SELECT phase,last_error FROM control.runs WHERE run_id=?", [run_id]
        ).fetchone()
        checkpoints = [
            n
            for (n,) in self.db.execute(
                "SELECT name FROM control.checkpoints WHERE run_id=? ORDER BY name", [run_id]
            ).fetchall()
        ]
        return {
            "run_id": run_id,
            "target": plan.target_namespace,
            "phase": row[0],
            "last_error_type": row[1],
            "checkpoints": checkpoints,
            "plan_identity": plan.identity,
            "equivalent_source": self.receipt(run_id, "equivalent_source"),
        }

    def accept_equivalent(self, plan, receipt):
        if self.phase(plan.run_id) != Phase.INPUTS_FROZEN:
            raise ValueError("Equivalent source must be frozen before acceptance")
        old = self.db.execute(
            "SELECT release_id FROM control.accepted WHERE target=?", [plan.target_namespace]
        ).fetchone()
        if old is None or receipt.get("release_id") != old[0] or plan.base_release != old[0]:
            raise ValueError("Equivalent source base does not match accepted release")
        self.checkpoint(plan.run_id, "equivalent_source", receipt)
        self.db.execute(
            "UPDATE control.accepted SET run_id=? WHERE target=?",
            [plan.run_id, plan.target_namespace],
        )
        self.db.execute(
            "UPDATE control.runs SET phase=?,last_error=NULL WHERE run_id=?",
            [Phase.COMMITTED, plan.run_id],
        )
