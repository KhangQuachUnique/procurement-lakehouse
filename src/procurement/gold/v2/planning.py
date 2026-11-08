"""One old/new relationship graph for impact closure and component batching.

Only relational grouping; never create a persisted graph platform. Source month
is not a business batch. The graph includes old links so reparenting retracts
both previous and new owner contributions.
"""

from .contracts import INPUT_TABLES, identifier
from .relationship_index import ensure_index, impact_graph

ROOTS = ("package_revision", "plan_revision", "notice_revision", "result_revision")
LINKS = {
    "package_revision": (
        ("package", "entity_id"),
        ("package", "json_extract_string(root_json,'$.id')"),
        ("package_code", "package_no"),
        ("plan", "plan_id"),
        ("plan_code", "plan_no"),
    ),
    "plan_revision": (
        ("plan", "entity_id"),
        ("plan", "json_extract_string(root_json,'$.id')"),
        ("plan_code", "plan_no"),
    ),
    "notice_revision": (
        ("package", "package_id"),
        ("package_code", "package_no"),
        ("notice", "notify_no"),
    ),
    "result_revision": (
        ("package", "bid_id"),
        ("package_code", "package_no"),
        ("notice", "notify_no"),
        ("result", "coalesce(result_id,entity_id)"),
    ),
}
CHILDREN = {
    "package_location": "package_revision",
    "award_lot": "result_revision",
    "award_contractor": "result_revision",
}


def plan_components(
    db,
    *,
    current="frozen",
    previous="input_cache",
    bootstrap=False,
    batch_rows=10000,
    cache_epoch=None,
    stage_identity=None,
):
    identifier(current)
    identifier(previous)
    if not isinstance(batch_rows, int) or isinstance(batch_rows, bool) or batch_rows < 1:
        raise ValueError("Positive batch row budget required")
    db.execute(f"CREATE SCHEMA IF NOT EXISTS {previous}")
    db.execute("DROP SCHEMA IF EXISTS graph CASCADE")
    db.execute("CREATE SCHEMA graph")
    for name in INPUT_TABLES:
        db.execute(
            f"CREATE TABLE IF NOT EXISTS {previous}.{name} AS SELECT * FROM {current}.{name} LIMIT 0"
        )
        if stage_identity is not None:
            from .relationship_stage import verify_stage

            verify_stage(db, stage_identity)
            db.execute(f"CREATE VIEW graph.diff_{name} AS SELECT * FROM relation_stage.diff_{name}")
        else:
            db.execute(
                f"CREATE TABLE graph.diff_{name} AS "
                f"(SELECT * FROM {current}.{name} EXCEPT ALL SELECT * FROM {previous}.{name}) UNION ALL "
                f"(SELECT * FROM {previous}.{name} EXCEPT ALL SELECT * FROM {current}.{name})"
            )
    index_strategy = ensure_index(db, previous, cache_epoch, ROOTS, LINKS)
    db.execute("CREATE TABLE graph.seeds(node VARCHAR)")
    for name in ROOTS:
        union = f"(SELECT * FROM {current}.{name} UNION ALL SELECT * FROM {previous}.{name})"
        db.execute(f"INSERT INTO graph.seeds SELECT '{name}:'||entity_id FROM graph.diff_{name}")
        db.execute(
            f"INSERT INTO graph.seeds SELECT '{name}:'||entity_id FROM {union} WHERE observation_id IN (SELECT observation_id FROM graph.diff_observation)"
        )
    for child, parent in CHILDREN.items():
        db.execute(
            f"INSERT INTO graph.seeds SELECT '{parent}:'||entity_id FROM (SELECT * FROM {current}.{parent} UNION ALL SELECT * FROM {previous}.{parent}) p WHERE revision_id IN (SELECT revision_id FROM graph.diff_{child})"
        )
    impact_graph(db, current, ROOTS, LINKS, bootstrap, staged=stage_identity is not None)
    # Count all versions/children/observations rather than just distinct roots.
    charges = []
    for name in ROOTS:
        charges.append(f"SELECT '{name}:'||entity_id node FROM {current}.{name}")
        charges.append(
            f"SELECT '{name}:'||r.entity_id FROM {current}.{name} r JOIN {current}.observation o USING(observation_id)"
        )
    for child, parent in CHILDREN.items():
        charges.append(
            f"SELECT '{parent}:'||p.entity_id FROM {current}.{child} c JOIN {current}.{parent} p USING(revision_id)"
        )
    db.execute(
        "CREATE TABLE graph.sizes AS SELECT component,count(*) row_count FROM ("
        + " UNION ALL ".join(charges)
        + ") c JOIN graph.impacted USING(node) GROUP BY component"
    )
    oversized = db.execute(
        "SELECT component,row_count FROM graph.sizes WHERE row_count>? ORDER BY row_count DESC LIMIT 1",
        [batch_rows],
    ).fetchone()
    if oversized:
        raise ValueError(f"Whole dependency component exceeds batch row budget: {oversized}")
    assignments = []
    batch = 0
    used = 0
    # Old-only components cost zero but still need an empty replacement batch.
    for component, rows in db.execute(
        "SELECT i.component,coalesce(max(s.row_count),0) FROM graph.impacted i LEFT JOIN graph.sizes s USING(component) GROUP BY i.component ORDER BY i.component"
    ).fetchall():
        if used and used + rows > batch_rows:
            batch += 1
            used = 0
        assignments.append((component, batch))
        used += rows
    db.execute("CREATE TABLE graph.batches(component VARCHAR PRIMARY KEY,batch BIGINT)")
    if assignments:
        db.executemany("INSERT INTO graph.batches VALUES (?,?)", assignments)
    return {
        "index_strategy": index_strategy,
        "affected_nodes": db.execute("SELECT count(*) FROM graph.impacted").fetchone()[0],
        "batches": batch + 1 if assignments else 0,
    }


def select_batch(db, batch, source="frozen"):
    identifier(source)
    db.execute("DROP SCHEMA IF EXISTS scoped CASCADE")
    db.execute("CREATE SCHEMA scoped")
    for name in ROOTS:
        db.execute(
            f"CREATE TABLE scoped.{name} AS SELECT * FROM {source}.{name} WHERE '{name}:'||entity_id IN (SELECT node FROM graph.impacted JOIN graph.batches USING(component) WHERE batch=?)",
            [batch],
        )
    for child, parent in CHILDREN.items():
        db.execute(
            f"CREATE TABLE scoped.{child} AS SELECT * FROM {source}.{child} WHERE revision_id IN (SELECT revision_id FROM scoped.{parent})"
        )
    ids = " UNION ".join("SELECT observation_id FROM scoped." + n for n in ROOTS)
    db.execute(
        f"CREATE TABLE scoped.observation AS SELECT * FROM {source}.observation WHERE observation_id IN ({ids})"
    )


def affected_owners(db):
    """Stable package/unresolved aliases match the preserved resolution contracts."""
    db.execute("CREATE OR REPLACE TABLE affected(owner_key VARCHAR PRIMARY KEY)")
    values = [
        "SELECT 'PKG:'||substr(node,length('package_revision:')+1) owner_key FROM graph.impacted WHERE starts_with(node,'package_revision:')"
    ]
    for name, match in (
        ("notice_revision", "_gtmp_notice_package_match"),
        ("result_revision", "_gtmp_result_package_match"),
    ):
        for state in ("UNRESOLVED", "AMBIGUOUS"):
            values.append(
                f"SELECT 'PKG_{state}:{match}:'||substr(node,{len(name) + 2}) FROM graph.impacted WHERE starts_with(node,'{name}:')"
            )
    db.execute("INSERT INTO affected " + " UNION ".join(values))
