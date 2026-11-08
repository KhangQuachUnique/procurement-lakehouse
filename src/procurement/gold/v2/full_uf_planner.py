"""Full-rebuild planner: compact UF and numeric, bucketed charge aggregation.

Only private planning checkpoints commit here. Public source/index/ledger accept
remains the existing post-activation transaction. Incremental planner is unchanged.
"""

from .contracts import INPUT_TABLES
from .planning import CHILDREN, LINKS, ROOTS
from .relationship_index import ensure_index
from .uf_graph import prepare, solve


def _step(db, name, sql):
    if db.execute("SELECT count(*) FROM full_plan.parts WHERE name=?", [name]).fetchone()[0]:
        return
    db.execute("BEGIN")
    try:
        db.execute(sql)
        db.execute("INSERT INTO full_plan.parts VALUES (?)", [name])
        db.execute("COMMIT")
    except BaseException:
        db.execute("ROLLBACK")
        raise


def prepare_full(db, plan, checkpoint_dir, *, buckets=64, on_progress=None):
    if plan.mode != "backfill" or plan.base_release is not None:
        raise ValueError("Full UF planner requires a fresh independent rebuild")
    if any(
        db.execute(f"SELECT EXISTS(SELECT 1 FROM input_cache.{name})").fetchone()[0]
        for name in INPUT_TABLES
    ):
        raise ValueError("Independent full planner refuses an accepted input cache")
    identity = plan.identity
    db.execute("CREATE SCHEMA IF NOT EXISTS full_plan")
    db.execute(
        "CREATE TABLE IF NOT EXISTS full_plan.meta(identity VARCHAR,last_id BIGINT,batch BIGINT,used_rows BIGINT,complete BOOLEAN)"
    )
    meta = db.execute("SELECT * FROM full_plan.meta").fetchall()
    if meta and meta[0][0] != identity:
        raise ValueError("Full planner input identity changed")
    if not meta:
        db.execute("BEGIN")
        try:
            db.execute("INSERT INTO full_plan.meta VALUES (?,-1,0,0,FALSE)", [identity])
            db.execute("CREATE TABLE full_plan.parts(name VARCHAR PRIMARY KEY)")
            db.execute("CREATE TABLE full_plan.charges(node VARCHAR,bucket INTEGER)")
            db.execute(
                "CREATE TABLE full_plan.partial_sizes(component_id UINTEGER,row_count BIGINT)"
            )
            db.execute("CREATE TABLE full_plan.assignments(component_id UINTEGER,batch BIGINT)")
            db.execute("COMMIT")
        except BaseException:
            db.execute("ROLLBACK")
            raise
    if not meta or not meta[0][4]:
        prepare(
            db,
            "relation_stage.new_nodes",
            "relation_stage.new_links",
            identity,
            buckets=buckets,
            on_progress=on_progress,
        )
        solve(db, checkpoint_dir, identity, on_progress=on_progress)
        # Each source charge is exactly the original planner charge; only the
        # physical representation/join/aggregation strategy changes.
        arms = {}
        for name in ROOTS:
            arms[name] = f"SELECT '{name}:'||entity_id node FROM frozen.{name}"
            arms[name + "_observation"] = (
                f"SELECT '{name}:'||r.entity_id node FROM frozen.{name} r JOIN frozen.observation o USING(observation_id)"
            )
        for child, parent in CHILDREN.items():
            arms[child] = (
                f"SELECT '{parent}:'||p.entity_id node FROM frozen.{child} c JOIN frozen.{parent} p USING(revision_id)"
            )
        for name, sql in arms.items():
            _step(
                db,
                "charge_" + name,
                f"INSERT INTO full_plan.charges SELECT node,CAST(hash(node)%{buckets} AS INTEGER) FROM ({sql})",
            )
        for bucket in range(buckets):
            _step(
                db,
                f"size_{bucket}",
                f"""INSERT INTO full_plan.partial_sizes
              SELECT l.min_id,count(*) FROM full_plan.charges c
              JOIN uf_graph.dictionary d ON d.node=c.node AND d.bucket={bucket}
              JOIN uf_graph.labels l USING(node_id)
              WHERE c.bucket={bucket} GROUP BY l.min_id""",
            )
        _step(
            db,
            "sizes",
            """CREATE TABLE full_plan.sizes AS
          SELECT component_id,sum(row_count)::BIGINT row_count
          FROM full_plan.partial_sizes GROUP BY component_id""",
        )
        oversized = db.execute(
            "SELECT component_id,row_count FROM full_plan.sizes WHERE row_count>? ORDER BY row_count DESC LIMIT 1",
            [plan.budget.batch_rows],
        ).fetchone()
        if oversized:
            raise ValueError(f"Whole dependency component exceeds batch row budget: {oversized}")
        _step(
            db,
            "ordered",
            """CREATE TABLE full_plan.ordered AS
          SELECT c.min_id component_id,coalesce(s.row_count,0) row_count
          FROM (SELECT DISTINCT min_id FROM uf_graph.labels) c
          LEFT JOIN full_plan.sizes s ON s.component_id=c.min_id ORDER BY c.min_id""",
        )
        while True:
            last, batch, used = db.execute(
                "SELECT last_id,batch,used_rows FROM full_plan.meta"
            ).fetchone()
            page = db.execute(
                "SELECT component_id,row_count FROM full_plan.ordered WHERE component_id>? ORDER BY component_id LIMIT 1024",
                [last],
            ).fetchall()
            if not page:
                break
            assignments = []
            for component, rows in page:
                if used and used + rows > plan.budget.batch_rows:
                    batch += 1
                    used = 0
                assignments.append((component, batch))
                used += rows
                last = component
            db.execute("BEGIN")
            try:
                db.executemany("INSERT INTO full_plan.assignments VALUES (?,?)", assignments)
                db.execute(
                    "UPDATE full_plan.meta SET last_id=?,batch=?,used_rows=?", [last, batch, used]
                )
                db.execute("COMMIT")
            except BaseException:
                db.execute("ROLLBACK")
                raise
        db.execute("UPDATE full_plan.meta SET complete=TRUE")
    # Derived graph views match the existing batch/business-engine contract.
    # No old accepted contributions are read by this independent full planner.
    db.execute("BEGIN")
    try:
        ensure_index(db, "input_cache", None, ROOTS, LINKS)
        db.execute("DROP SCHEMA IF EXISTS graph CASCADE")
        db.execute("CREATE SCHEMA graph")
        for name in ("new_nodes", "new_links", "changed_nodes"):
            db.execute(f"CREATE VIEW graph.{name} AS SELECT * FROM relation_stage.{name}")
        for name in ("nodes", "impacted"):
            db.execute(f"CREATE VIEW graph.{name} AS SELECT * FROM uf_graph.components")
        db.execute("""CREATE VIEW graph.links AS SELECT d.node,e.token
          FROM uf_graph.edges e JOIN uf_graph.dictionary d USING(node_id)""")
        db.execute("""CREATE VIEW graph.sizes AS SELECT d.node component,s.row_count
          FROM full_plan.sizes s JOIN uf_graph.dictionary d ON d.node_id=s.component_id""")
        db.execute("""CREATE VIEW graph.batches AS SELECT d.node component,a.batch
          FROM full_plan.assignments a JOIN uf_graph.dictionary d ON d.node_id=a.component_id""")
        db.execute("COMMIT")
    except BaseException:
        db.execute("ROLLBACK")
        raise
    nodes = db.execute("SELECT count(*) FROM uf_graph.dictionary").fetchone()[0]
    batches = db.execute("SELECT coalesce(max(batch)+1,0) FROM full_plan.assignments").fetchone()[0]
    return {
        "affected_nodes": nodes,
        "batches": batches,
        "index_strategy": "independent_full_union_find",
    }
