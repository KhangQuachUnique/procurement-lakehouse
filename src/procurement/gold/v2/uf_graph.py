"""Disk-backed numeric encoding + compact Union-Find, not global SQL propagation.

SQL handles bounded node-bucket joins and external sort. All graph keys remain
exact strings; hashes choose physical buckets only, never identity/equality.
"""

from array import array
from pathlib import Path

import pyarrow as pa

from .contracts import identifier
from .union_find import UnionFind


def _relation(name):
    for part in name.split("."):
        identifier(part)
    return name


def _step(db, name, query):
    if db.execute("SELECT count(*) FROM uf_graph.parts WHERE name=?", [name]).fetchone()[0]:
        return
    db.execute("BEGIN")
    try:
        db.execute(query)
        db.execute("INSERT INTO uf_graph.parts VALUES (?)", [name])
        db.execute("COMMIT")
    except BaseException:
        db.execute("ROLLBACK")
        raise


def prepare(db, nodes, links, identity, *, buckets=64, on_progress=None):
    nodes, links = _relation(nodes), _relation(links)
    if type(buckets) is not int or not 1 <= buckets <= 4096:
        raise ValueError("Invalid numeric-graph bucket count")
    db.execute("CREATE SCHEMA IF NOT EXISTS uf_graph")
    db.execute("CREATE TABLE IF NOT EXISTS uf_graph.meta(identity VARCHAR,buckets BIGINT)")
    meta = db.execute("SELECT * FROM uf_graph.meta").fetchall()
    if meta and meta != [(identity, buckets)]:
        raise ValueError("Numeric graph belongs to different immutable inputs")
    if not meta:
        db.execute("BEGIN")
        try:
            db.execute("INSERT INTO uf_graph.meta VALUES (?,?)", [identity, buckets])
            db.execute("CREATE TABLE uf_graph.parts(name VARCHAR PRIMARY KEY)")
            db.execute("CREATE TABLE uf_graph.edges(token VARCHAR,node_id UINTEGER)")
            db.execute("CREATE TABLE uf_graph.labels(node_id UINTEGER,min_id UINTEGER)")
            db.execute("CREATE TABLE uf_graph.output_cursor(next_id BIGINT)")
            db.execute("INSERT INTO uf_graph.output_cursor VALUES (0)")
            db.execute("COMMIT")
        except BaseException:
            db.execute("ROLLBACK")
            raise
    for source, predicate in ((nodes, "node IS NULL"), (links, "node IS NULL OR token IS NULL")):
        if db.execute(f"SELECT EXISTS(SELECT 1 FROM {source} WHERE {predicate})").fetchone()[0]:
            raise ValueError("Null graph identity")
    _step(db, "keys", f"CREATE TABLE uf_graph.keys AS SELECT DISTINCT node FROM {nodes}")
    count = db.execute("SELECT count(*) FROM uf_graph.keys").fetchone()[0]
    if count > 2**32 - 1:
        raise ValueError("Graph exceeds uint32 ID envelope")
    _step(
        db,
        "dictionary",
        f"""CREATE TABLE uf_graph.dictionary AS
      SELECT node,CAST(row_number() OVER(ORDER BY node)-1 AS UINTEGER) node_id,
             CAST(hash(node)%{buckets} AS INTEGER) bucket FROM uf_graph.keys""",
    )
    _step(
        db,
        "link_keys",
        f"""CREATE TABLE uf_graph.link_keys AS
      SELECT node,token,CAST(hash(node)%{buckets} AS INTEGER) bucket FROM {links} ORDER BY bucket""",
    )
    for bucket in range(buckets):
        name = f"bucket_{bucket}"
        if db.execute("SELECT count(*) FROM uf_graph.parts WHERE name=?", [name]).fetchone()[0]:
            continue
        if db.execute(f"""SELECT EXISTS(SELECT 1 FROM uf_graph.link_keys l
          WHERE l.bucket={bucket} AND NOT EXISTS(SELECT 1 FROM uf_graph.dictionary d
            WHERE d.bucket={bucket} AND d.node=l.node))""").fetchone()[0]:
            raise ValueError("Edge references absent node")
        _step(
            db,
            name,
            f"""INSERT INTO uf_graph.edges
          SELECT DISTINCT l.token,d.node_id FROM uf_graph.link_keys l
          JOIN uf_graph.dictionary d USING(node)
          WHERE l.bucket={bucket} AND d.bucket={bucket}""",
        )
        if on_progress:
            on_progress("encode", {"done": bucket + 1, "total": buckets, "nodes": count})
    return count


def solve(db, directory, identity, *, checkpoint_edges=500000, on_progress=None):
    directory = Path(directory)
    if checkpoint_edges < 1:
        raise ValueError("Positive edge checkpoint interval required")
    if db.execute("SELECT identity FROM uf_graph.meta").fetchall() != [(identity,)]:
        raise ValueError("Numeric graph identity differs")
    buckets = db.execute("SELECT buckets FROM uf_graph.meta").fetchone()[0]
    completed = db.execute(
        "SELECT count(*) FROM uf_graph.parts WHERE name LIKE 'bucket_%'"
    ).fetchone()[0]
    if completed != buckets:
        raise ValueError("Numeric graph encoding incomplete")
    count = db.execute("SELECT count(*) FROM uf_graph.dictionary").fetchone()[0]
    expected_edges = db.execute("SELECT count(*) FROM uf_graph.edges").fetchone()[0]
    uf = (
        UnionFind.load(directory, identity)
        if (directory / "CURRENT").exists()
        else UnionFind(count)
    )
    if len(uf.parent) != count or uf.edges > expected_edges:
        raise ValueError("Union-Find state differs from encoded graph")
    condition, params = "", []
    if uf.last_token is not None:
        condition = "WHERE token>? OR (token=? AND node_id>?)"
        params = [uf.last_token, uf.last_token, uf.last_node]
    cursor = db.execute(
        f"SELECT token,node_id FROM uf_graph.edges {condition} ORDER BY token,node_id", params
    )
    saved_at = uf.edges
    while rows := cursor.fetchmany(8192):
        uf.feed(rows)
        if uf.edges - saved_at >= checkpoint_edges:
            uf.save(directory, identity)
            saved_at = uf.edges
            if on_progress:
                on_progress(
                    "union", {"done": uf.edges, "total": expected_edges, "array_bytes": uf.bytes}
                )
    if uf.edges != expected_edges:
        raise ValueError("Union-Find input edge count mismatch")
    uf.save(directory, identity)
    # Output ranges and cursor commit together. No full Python label list.
    start = db.execute("SELECT next_id FROM uf_graph.output_cursor").fetchone()[0]
    while start < count:
        end = min(start + 65536, count)
        ids = array("I", range(start, end))
        minima = array("I", (uf.label(i) for i in range(start, end)))
        batch = pa.RecordBatch.from_arrays(
            [
                pa.Array.from_buffers(pa.uint32(), end - start, [None, pa.py_buffer(ids)]),
                pa.Array.from_buffers(pa.uint32(), end - start, [None, pa.py_buffer(minima)]),
            ],
            ["node_id", "min_id"],
        )
        db.register("_uf_output", batch)
        db.execute("BEGIN")
        try:
            db.execute("INSERT INTO uf_graph.labels SELECT * FROM _uf_output")
            db.execute("UPDATE uf_graph.output_cursor SET next_id=?", [end])
            db.execute("COMMIT")
        except BaseException:
            db.execute("ROLLBACK")
            raise
        finally:
            db.unregister("_uf_output")
        start = end
    _step(
        db,
        "components",
        """CREATE TABLE uf_graph.components AS
      SELECT n.node,m.node component FROM uf_graph.dictionary n
      JOIN uf_graph.labels l ON l.node_id=n.node_id
      JOIN uf_graph.dictionary m ON m.node_id=l.min_id""",
    )
    return {
        "nodes": count,
        "edges": uf.edges,
        "tokens": uf.tokens,
        "array_bytes": uf.bytes,
        "algorithm": "rank+path-halving Union-Find; token streaming",
        "scan_passes": 1,
        "canonical_labels": "minimum original node key",
    }
