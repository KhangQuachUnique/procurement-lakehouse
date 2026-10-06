"""Typed Iceberg writes; each table snapshot is staged before release publication."""

import pyarrow as pa

from procurement.storage.iceberg import identifier

TYPED = "revision_id VARCHAR, entity_id VARCHAR, entity_type VARCHAR, business_number VARCHAR, title VARCHAR, buyer_id VARCHAR, buyer_name VARCHAR, amount DECIMAL(38,6), currency VARCHAR, public_date_raw VARCHAR, status_raw VARCHAR, root_json VARCHAR"
SCHEMAS = {
    "observation": "observation_id VARCHAR, resource VARCHAR, source_date DATE, run_id VARCHAR, table_name VARCHAR, file_key VARCHAR, file_sha256 VARCHAR, row_ordinal BIGINT, observed_at TIMESTAMPTZ, source_id VARCHAR, source_version VARCHAR, content_hash VARCHAR, payload_json VARCHAR",
    "entity": "entity_id VARCHAR, namespace VARCHAR, entity_type VARCHAR, id_scheme VARCHAR, source_identity VARCHAR",
    "entity_revision": "revision_id VARCHAR, entity_id VARCHAR, source_version VARCHAR, semantic_hash VARCHAR, mapping_version VARCHAR, payload_json VARCHAR",
    "revision_observation": "observation_id VARCHAR, revision_id VARCHAR",
    "current_entity": "entity_id VARCHAR, revision_id VARCHAR, selection_status VARCHAR, observed_at TIMESTAMPTZ",
    **{kind + "_revision": TYPED for kind in ("project", "plan", "package", "notice", "result", "opening")},
    "child": "child_id VARCHAR, revision_id VARCHAR, kind VARCHAR, role VARCHAR, path VARCHAR, ordinal BIGINT, source_id VARCHAR, payload_json VARCHAR",
    "relationship": "relationship_id VARCHAR, from_revision_id VARCHAR, target_type VARCHAR, id_scheme VARCHAR, target_identity VARCHAR, field VARCHAR, resolution_status VARCHAR, target_entity_id VARCHAR",
    "quality_issue": "issue_id VARCHAR, observation_id VARCHAR, code VARCHAR, path VARCHAR, severity VARCHAR",
    "quarantine": "observation_id VARCHAR, reason VARCHAR",
}


def stage(db, catalog, namespace, tables, release_id, schemas=SCHEMAS):
    namespace = identifier(namespace)
    db.execute(f"CREATE SCHEMA IF NOT EXISTS lake.{namespace}")
    snapshots = {}
    for name, rows in tables.items():
        name = identifier(name)
        schema = schemas[name]
        table = f"lake.{namespace}.{name}"
        db.execute(f"CREATE TABLE IF NOT EXISTS {table} ({schema})")
        metadata = catalog.table(namespace, name)["metadata"]
        if metadata["format-version"] != 2:
            raise ValueError("Only verified Iceberg format v2 is supported")
        # Explicitly separate table commits. A release, never 'latest', provides multi-table consistency.
        db.execute("BEGIN")
        try:
            db.execute(f"DELETE FROM {table}")
            if rows:
                db.register("candidate_rows", pa.Table.from_pylist(rows))
                try:
                    db.execute(f"INSERT INTO {table} BY NAME SELECT * FROM candidate_rows")
                finally:
                    db.unregister("candidate_rows")
            db.execute("COMMIT")
        except BaseException:
            # Never retry an uncertain remote commit. Existing certified snapshot tags remain valid.
            try:
                db.execute("ROLLBACK")
            except Exception:
                pass
            raise
        metadata = catalog.table(namespace, name)["metadata"]
        snapshot_id = metadata.get("current-snapshot-id")
        if snapshot_id is not None and snapshot_id >= 0:
            catalog.pin(namespace, name, snapshot_id, release_id)
            actual = db.sql(f"SELECT count(*) FROM {table} AT (VERSION => {snapshot_id})").fetchone()[0]
        else:
            snapshot_id = None
            actual = 0
        if actual != len(rows):
            raise ValueError(f"Candidate row count mismatch: {name}")
        snapshots[name] = {"snapshot_id": snapshot_id, "rows": len(rows), "schema": schema,
                           "metadata_location": catalog.table(namespace, name)["metadata-location"]}
    return snapshots


def read_pinned(db, release, table):
    entry = release["tables"][table]
    namespace, table = identifier(release["namespace"]), identifier(table)
    snapshot = entry["snapshot_id"]
    if snapshot is None:
        if entry["rows"] != 0:
            raise ValueError("A nonempty table needs a pinned snapshot")
        return []
    if not isinstance(snapshot, int) or snapshot < 0:
        raise ValueError("Invalid snapshot")
    cursor = db.execute(f"SELECT * FROM lake.{namespace}.{table} AT (VERSION => {snapshot})")
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, values, strict=True)) for values in cursor.fetchall()]
