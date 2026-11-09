"""Resumable private link staging, separate from accepted Gold/index state.

Each source diff and bounded link chunk is durable. The immutable run identity
binds staging to frozen inputs. Only completed staging is visible to the planner.
"""

from procurement.gold.domain.models import INPUT_TABLES
from .relationship_index import source_links


def prepare_stage(db, plan, roots, families, *, chunk_nodes=25000, on_chunk=None):
    db.execute("CREATE SCHEMA IF NOT EXISTS input_cache")
    for name in INPUT_TABLES:
        db.execute(
            f"CREATE TABLE IF NOT EXISTS input_cache.{name} AS SELECT * FROM frozen.{name} LIMIT 0"
        )
    exists = db.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema='relation_stage' AND table_name='meta'"
    ).fetchone()[0]
    old = (
        db.execute("SELECT run_id,identity,complete FROM relation_stage.meta").fetchone()
        if exists
        else None
    )
    if old and old[:2] != (plan.run_id, plan.identity):
        phase = db.execute("SELECT phase FROM control.runs WHERE run_id=?", [old[0]]).fetchone()
        if phase != ("COMMITTED",):
            raise ValueError(
                "Cannot replace unfinished relationship staging with different run/inputs"
            )
        old = None
    if old is None:
        db.execute("BEGIN")
        try:
            db.execute("DROP SCHEMA IF EXISTS relation_stage CASCADE")
            db.execute("CREATE SCHEMA relation_stage")
            db.execute(
                "CREATE TABLE relation_stage.meta(run_id VARCHAR,identity VARCHAR,complete BOOLEAN)"
            )
            db.execute(
                "INSERT INTO relation_stage.meta VALUES (?,?,FALSE)", [plan.run_id, plan.identity]
            )
            db.execute("CREATE TABLE relation_stage.parts(name VARCHAR PRIMARY KEY)")
            db.execute(
                "CREATE TABLE relation_stage.chunks(root VARCHAR PRIMARY KEY,last_key VARCHAR,complete BOOLEAN)"
            )
            db.execute("CREATE TABLE relation_stage.new_nodes(node VARCHAR)")
            db.execute("CREATE TABLE relation_stage.new_links(node VARCHAR,token VARCHAR)")
            db.execute("COMMIT")
        except BaseException:
            db.execute("ROLLBACK")
            raise
    elif old[2]:
        return
    for name in INPUT_TABLES:
        if db.execute("SELECT count(*) FROM relation_stage.parts WHERE name=?", [name]).fetchone()[
            0
        ]:
            continue
        db.execute("BEGIN")
        try:
            db.execute(
                f"CREATE TABLE relation_stage.diff_{name} AS (SELECT * FROM frozen.{name} EXCEPT ALL SELECT * FROM input_cache.{name}) UNION ALL (SELECT * FROM input_cache.{name} EXCEPT ALL SELECT * FROM frozen.{name})"
            )
            db.execute("INSERT INTO relation_stage.parts VALUES (?)", [name])
            db.execute("COMMIT")
        except BaseException:
            db.execute("ROLLBACK")
            raise
    if not db.execute(
        "SELECT count(*) FROM relation_stage.parts WHERE name='changed_nodes'"
    ).fetchone()[0]:
        db.execute("BEGIN")
        try:
            union = " UNION ".join(
                f"SELECT '{n}:'||entity_id node FROM relation_stage.diff_{n} WHERE entity_id IS NOT NULL"
                for n in roots
            )
            db.execute("CREATE TABLE relation_stage.changed_nodes AS " + union)
            db.execute("INSERT INTO relation_stage.parts VALUES ('changed_nodes')")
            db.execute("COMMIT")
        except BaseException:
            db.execute("ROLLBACK")
            raise
    source_links(
        db,
        "frozen",
        "relation_stage.new_nodes",
        "relation_stage.new_links",
        roots,
        families,
        "relation_stage.changed_nodes",
        chunk_nodes=chunk_nodes,
        checkpoints="relation_stage.chunks",
        on_chunk=on_chunk,
    )
    db.execute("UPDATE relation_stage.meta SET complete=TRUE")


def verify_stage(db, identity):
    row = db.execute("SELECT identity,complete FROM relation_stage.meta").fetchone()
    if row != (identity, True):
        raise ValueError("Relationship staging is incomplete or belongs to different inputs")
