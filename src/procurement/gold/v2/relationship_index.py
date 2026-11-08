"""Accepted, disk-backed adjacency. Candidate edits never mutate accepted links.

The release epoch is the published release, not a source/no-op run ID. Caller
commits index acceptance with source cache/ledger acceptance in one transaction.
"""

from .contracts import identifier


def source_links(
    db,
    source,
    nodes,
    links,
    roots,
    families,
    changed=None,
    *,
    chunk_nodes=25000,
    checkpoints=None,
    on_chunk=None,
):
    """Disjoint keyset chunks; all aliases/versions of a node stay together.

    Heap destinations deliberately avoid ART indexes. DISTINCT over ALL alias
    families in each chunk enforces pair uniqueness. Keyset intervals do not
    overlap, and durable chunk data + cursor commit atomically when requested.
    """
    identifier(source)
    if type(chunk_nodes) is not int or chunk_nodes < 1:
        raise ValueError("Positive relationship chunk bound required")
    for name in roots:
        condition = "entity_id IS NOT NULL"
        if changed:
            condition += f" AND '{name}:'||entity_id IN (SELECT node FROM {changed})"
        saved = (
            db.execute(
                f"SELECT last_key,complete FROM {checkpoints} WHERE root=?", [name]
            ).fetchone()
            if checkpoints
            else None
        )
        if saved and saved[1]:
            continue
        last = saved[0] if saved else None
        while True:
            params = []
            after = ""
            if last is not None:
                after = " AND entity_id>?"
                params.append(last)
            db.execute(
                f"CREATE OR REPLACE TEMP TABLE relation_chunk AS SELECT DISTINCT entity_id "
                f"FROM {source}.{name} WHERE {condition}{after} ORDER BY entity_id LIMIT {chunk_nodes}",
                params,
            )
            count, end = db.execute("SELECT count(*),max(entity_id) FROM relation_chunk").fetchone()
            if not count:
                if checkpoints:
                    db.execute(
                        f"INSERT OR REPLACE INTO {checkpoints} VALUES (?,?,TRUE)", [name, last]
                    )
                break
            selected = f"{source}.{name} WHERE entity_id IN (SELECT entity_id FROM relation_chunk)"
            aliases = " UNION ALL ".join(
                f"SELECT '{name}:'||entity_id node,'{family}:'||trim({expression}) token FROM {selected} "
                f"AND nullif(trim({expression}),'') IS NOT NULL"
                for family, expression in families[name]
            )
            if checkpoints:
                db.execute("BEGIN")
            try:
                db.execute(f"INSERT INTO {nodes} SELECT '{name}:'||entity_id FROM relation_chunk")
                db.execute(f"INSERT INTO {links} SELECT DISTINCT node,token FROM ({aliases})")
                if checkpoints:
                    db.execute(
                        f"INSERT OR REPLACE INTO {checkpoints} VALUES (?,?,FALSE)", [name, end]
                    )
                    db.execute("COMMIT")
            except BaseException:
                if checkpoints:
                    db.execute("ROLLBACK")
                raise
            last = end
            if on_chunk:
                on_chunk(name, count, last)


def ensure_index(db, previous, epoch, roots, families):
    db.execute("CREATE SCHEMA IF NOT EXISTS relation_cache")
    db.execute("CREATE TABLE IF NOT EXISTS relation_cache.meta(epoch VARCHAR,version INTEGER)")
    state = db.execute("SELECT epoch,version FROM relation_cache.meta").fetchall()
    if epoch is not None and state == [(epoch, 2)]:
        return "accepted_index"
    # Missing/stale indexes are not evidence of missing relationships. Rebuild
    # from the accepted input cache, never from a partly calculated candidate.
    db.execute("CREATE OR REPLACE TABLE relation_cache.nodes(node VARCHAR)")
    db.execute("CREATE OR REPLACE TABLE relation_cache.links(node VARCHAR,token VARCHAR)")
    source_links(db, previous, "relation_cache.nodes", "relation_cache.links", roots, families)
    db.execute("DELETE FROM relation_cache.meta")
    db.execute("INSERT INTO relation_cache.meta VALUES (?,2)", [epoch])
    return "rebuilt_from_accepted_cache"


def accept_index(db, release_id):
    """Only after remote activation; caller transaction includes input_cache."""
    db.execute(
        "DELETE FROM relation_cache.links WHERE node IN (SELECT node FROM graph.changed_nodes)"
    )
    db.execute(
        "DELETE FROM relation_cache.nodes WHERE node IN (SELECT node FROM graph.changed_nodes)"
    )
    db.execute("INSERT INTO relation_cache.nodes SELECT * FROM graph.new_nodes")
    db.execute("INSERT INTO relation_cache.links SELECT * FROM graph.new_links")
    db.execute("DELETE FROM relation_cache.meta")
    db.execute("INSERT INTO relation_cache.meta VALUES (?,2)", [release_id])


def impact_graph(db, current, roots, families, bootstrap, *, staged=False):
    if staged:
        for name in ("changed_nodes", "new_nodes", "new_links"):
            db.execute(f"CREATE VIEW graph.{name} AS SELECT * FROM relation_stage.{name}")
    else:
        db.execute("CREATE TABLE graph.changed_nodes(node VARCHAR)")
        for name in roots:
            db.execute(
                f"INSERT INTO graph.changed_nodes SELECT DISTINCT '{name}:'||entity_id FROM graph.diff_{name} WHERE entity_id IS NOT NULL"
            )
        db.execute("CREATE TABLE graph.new_nodes(node VARCHAR)")
        db.execute("CREATE TABLE graph.new_links(node VARCHAR,token VARCHAR)")
        source_links(
            db,
            current,
            "graph.new_nodes",
            "graph.new_links",
            roots,
            families,
            "graph.changed_nodes",
        )
    db.execute("CREATE TABLE graph.reached(node VARCHAR)")
    if bootstrap:
        db.execute(
            "INSERT INTO graph.reached SELECT node FROM relation_cache.nodes UNION SELECT node FROM graph.new_nodes"
        )
    else:
        db.execute(
            "INSERT INTO graph.reached SELECT DISTINCT node FROM graph.seeds WHERE node IS NOT NULL AND (node IN (SELECT node FROM relation_cache.nodes) OR node IN (SELECT node FROM graph.new_nodes))"
        )
    db.execute("CREATE OR REPLACE TEMP TABLE frontier AS SELECT * FROM graph.reached")
    db.execute("CREATE OR REPLACE TEMP TABLE seen_tokens(token VARCHAR)")
    # Frontier exploration is over narrow keys only. Both old and replacement
    # links stay present until impact closure and retraction are complete.
    while not bootstrap and db.execute("SELECT EXISTS(SELECT 1 FROM frontier)").fetchone()[0]:
        db.execute("""CREATE OR REPLACE TEMP TABLE next_tokens AS
          SELECT token FROM (
            SELECT token FROM relation_cache.links WHERE node IN (SELECT node FROM frontier)
            UNION SELECT token FROM graph.new_links WHERE node IN (SELECT node FROM frontier)
          ) WHERE token NOT IN (SELECT token FROM seen_tokens)""")
        db.execute("INSERT INTO seen_tokens SELECT * FROM next_tokens")
        db.execute("""CREATE OR REPLACE TEMP TABLE frontier AS
          SELECT node FROM (
            SELECT node FROM relation_cache.links WHERE token IN (SELECT token FROM next_tokens)
            UNION SELECT node FROM graph.new_links WHERE token IN (SELECT token FROM next_tokens)
          ) WHERE node NOT IN (SELECT node FROM graph.reached)""")
        db.execute("INSERT INTO graph.reached SELECT * FROM frontier")
    db.execute("CREATE TABLE graph.nodes AS SELECT node,node component FROM graph.reached")
    db.execute("""CREATE TABLE graph.links AS
      SELECT node,token FROM relation_cache.links WHERE node IN (SELECT node FROM graph.reached)
      UNION SELECT node,token FROM graph.new_links WHERE node IN (SELECT node FROM graph.reached)""")
    # Separate blocking GROUP BY operators. The former monolithic query held
    # token aggregation and node aggregation intermediates concurrently.
    while True:
        db.execute("""CREATE OR REPLACE TABLE graph.token_labels AS
          SELECT token,min(component) component FROM graph.links JOIN graph.nodes USING(node) GROUP BY token""")
        db.execute("""CREATE OR REPLACE TABLE graph.node_labels AS
          SELECT node,min(component) component FROM graph.links JOIN graph.token_labels USING(token) GROUP BY node""")
        db.execute("""CREATE OR REPLACE TEMP TABLE next_components AS
          SELECT n.node,least(n.component,coalesce(l.component,n.component)) component
          FROM graph.nodes n LEFT JOIN graph.node_labels l USING(node)""")
        changed = db.execute(
            "SELECT EXISTS(SELECT 1 FROM next_components a JOIN graph.nodes b USING(node) WHERE a.component<>b.component)"
        ).fetchone()[0]
        db.execute(
            "UPDATE graph.nodes SET component=n.component FROM next_components n WHERE graph.nodes.node=n.node"
        )
        if not changed:
            break
    db.execute("CREATE TABLE graph.impacted AS SELECT * FROM graph.nodes")
