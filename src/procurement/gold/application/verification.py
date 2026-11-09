"""Exact full active-generation reconciliation; bounded transfer, explicit full IO."""

from procurement.gold.domain.schema import TABLES
from .state import StateStore


def verify(root, publisher):
    with StateStore(root) as state:
        rows = state.db.execute("SELECT release_id FROM control.accepted").fetchall()
        if len(rows) != 1:
            raise ValueError("No unique accepted v2 generation")
        plan = state.plan(rows[0][0])
        state.db.execute("SET memory_limit=?", [plan.budget.memory_limit])
        state.db.execute("SET threads=?", [plan.budget.threads])
        if not publisher.is_active(plan):
            raise ValueError("Remote pointer differs; reconcile/resume before verification")
        publisher.ensure_views(plan)
        evidence = {}
        for name, table in TABLES.items():
            ddl = ",".join(c.name + " " + c.sql_type for c in table.columns)
            state.db.execute("CREATE OR REPLACE TEMP TABLE _v2_verify (" + ddl + ")")
            for batch in publisher.db.execute("SELECT * FROM gold_v2." + name).to_arrow_reader(
                batch_size=8192
            ):
                state.db.register("_v2_verify_batch", batch)
                try:
                    state.db.execute("INSERT INTO _v2_verify SELECT * FROM _v2_verify_batch")
                finally:
                    state.db.unregister("_v2_verify_batch")
            bad = 0
            for bucket in range(16):
                where = f"hash({table.key})%16={bucket}"
                bad += state.db.execute(
                    f"SELECT count(*) FROM ((SELECT * FROM _v2_verify WHERE {where} EXCEPT ALL SELECT * FROM accepted.{name} WHERE {where}) UNION ALL (SELECT * FROM accepted.{name} WHERE {where} EXCEPT ALL SELECT * FROM _v2_verify WHERE {where}))"
                ).fetchone()[0]
            if bad:
                raise ValueError("Active snapshot differs from accepted checkpoint: " + name)
            evidence[name] = {
                "matches_checkpoint": True,
                "rows": state.db.execute("SELECT count(*) FROM _v2_verify").fetchone()[0],
            }
        return {"verified": True, "release_id": plan.run_id, "tables": evidence}
