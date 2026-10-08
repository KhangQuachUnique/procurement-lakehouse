"""Typed Iceberg and DuckDB writes; verified 15-table Silver Lakehouse schemas."""

import pyarrow as pa

from procurement.storage.iceberg import identifier

# Standardized 15-Table Silver Lakehouse Schemas
SCHEMAS = {
    # 1. Lineage & Core Identity Tracking (4 tables)
    "observation": (
        "observation_id VARCHAR, resource VARCHAR, source_date DATE, run_id VARCHAR, "
        "table_name VARCHAR, file_key VARCHAR, file_sha256 VARCHAR, row_ordinal BIGINT, "
        "observed_at TIMESTAMPTZ, source_id VARCHAR, source_version VARCHAR, content_hash VARCHAR, payload_json VARCHAR"
    ),
    "entity": (
        "entity_id VARCHAR, namespace VARCHAR, entity_type VARCHAR, id_scheme VARCHAR, source_identity VARCHAR"
    ),
    "entity_revision": (
        "revision_id VARCHAR, entity_id VARCHAR, source_version VARCHAR, semantic_hash VARCHAR, "
        "mapping_version VARCHAR, payload_json VARCHAR"
    ),
    "revision_observation": "observation_id VARCHAR, revision_id VARCHAR",
    "current_entity": "entity_id VARCHAR, revision_id VARCHAR, selection_status VARCHAR, observed_at TIMESTAMPTZ",

    # 2. Entity-Specific Typed Revisions (6 tables)
    "project_revision": (
        "revision_id VARCHAR, entity_id VARCHAR, entity_type VARCHAR, project_no VARCHAR, business_number VARCHAR, "
        "title VARCHAR, investor_code VARCHAR, investor_name VARCHAR, buyer_id VARCHAR, buyer_name VARCHAR, "
        "total_investment DECIMAL(38,6), amount DECIMAL(38,6), currency VARCHAR, "
        "decision_no VARCHAR, decision_date DATE, prov_code VARCHAR, district_code VARCHAR, "
        "public_date DATE, public_date_raw VARCHAR, status_raw VARCHAR, root_json VARCHAR"
    ),
    "plan_revision": (
        "revision_id VARCHAR, entity_id VARCHAR, entity_type VARCHAR, plan_no VARCHAR, plan_version VARCHAR, "
        "business_number VARCHAR, title VARCHAR, invest_target VARCHAR, invest_scale VARCHAR, investor_name VARCHAR, "
        "buyer_id VARCHAR, buyer_name VARCHAR, project_id VARCHAR, project_no VARCHAR, "
        "total_investment DECIMAL(38,6), amount DECIMAL(38,6), currency VARCHAR, "
        "public_date DATE, public_date_raw VARCHAR, status_raw VARCHAR, root_json VARCHAR"
    ),
    "package_revision": (
        "revision_id VARCHAR, entity_id VARCHAR, entity_type VARCHAR, package_no VARCHAR, business_number VARCHAR, "
        "title VARCHAR, plan_no VARCHAR, plan_id VARCHAR, "
        "bid_price DECIMAL(38,6), estimate_price DECIMAL(38,6), amount DECIMAL(38,6), currency VARCHAR, "
        "bid_field VARCHAR, bid_form VARCHAR, bid_mode VARCHAR, contract_type VARCHAR, execution_period VARCHAR, "
        "is_domestic BOOLEAN, is_internet BOOLEAN, public_date DATE, public_date_raw VARCHAR, status_raw VARCHAR, root_json VARCHAR"
    ),
    "notice_revision": (
        "revision_id VARCHAR, entity_id VARCHAR, entity_type VARCHAR, notify_no VARCHAR, notify_version VARCHAR, "
        "business_number VARCHAR, package_no VARCHAR, title VARCHAR, procuring_entity_code VARCHAR, procuring_entity_name VARCHAR, "
        "buyer_id VARCHAR, buyer_name VARCHAR, investor_name VARCHAR, "
        "bid_open_date TIMESTAMPTZ, bid_close_date TIMESTAMPTZ, bid_price DECIMAL(38,6), amount DECIMAL(38,6), currency VARCHAR, "
        "bid_field VARCHAR, bid_form VARCHAR, bid_mode VARCHAR, notification_type VARCHAR, "
        "public_date DATE, public_date_raw VARCHAR, status_raw VARCHAR, root_json VARCHAR"
    ),
    "result_revision": (
        "revision_id VARCHAR, entity_id VARCHAR, entity_type VARCHAR, result_id VARCHAR, result_version VARCHAR, "
        "notify_no VARCHAR, notify_version VARCHAR, business_number VARCHAR, package_no VARCHAR, title VARCHAR, "
        "decision_no VARCHAR, decision_date DATE, total_winning_price DECIMAL(38,6), amount DECIMAL(38,6), currency VARCHAR, "
        "public_date DATE, public_date_raw VARCHAR, status_raw VARCHAR, root_json VARCHAR"
    ),
    "opening_revision": (
        "revision_id VARCHAR, entity_id VARCHAR, entity_type VARCHAR, notify_no VARCHAR, notify_version VARCHAR, "
        "business_number VARCHAR, title VARCHAR, bid_mode VARCHAR, actual_open_date TIMESTAMPTZ, total_bidders BIGINT, "
        "public_date DATE, public_date_raw VARCHAR, status_raw VARCHAR, root_json VARCHAR"
    ),

    # 3. Dedicated Sub-Entities & Relationships (3 tables)
    "lot": (
        "lot_id VARCHAR, revision_id VARCHAR, lot_no VARCHAR, lot_name VARCHAR, "
        "lot_price DECIMAL(38,6), estimate_price DECIMAL(38,6), currency VARCHAR, winning_code VARCHAR, status_raw VARCHAR, raw_json VARCHAR"
    ),
    "bid_participation": (
        "participation_id VARCHAR, revision_id VARCHAR, lot_no VARCHAR, contractor_code VARCHAR, contractor_name VARCHAR, "
        "tax_code VARCHAR, is_consortium BOOLEAN, consortium_name VARCHAR, bid_price DECIMAL(38,6), winning_price DECIMAL(38,6), "
        "currency VARCHAR, is_winner BOOLEAN, evaluation_rank BIGINT, raw_json VARCHAR"
    ),
    "relationship": (
        "relationship_id VARCHAR, from_revision_id VARCHAR, target_type VARCHAR, id_scheme VARCHAR, "
        "target_identity VARCHAR, field VARCHAR, resolution_status VARCHAR, target_entity_id VARCHAR"
    ),
    "child": (
        "child_id VARCHAR, revision_id VARCHAR, kind VARCHAR, role VARCHAR, path VARCHAR, "
        "ordinal BIGINT, source_id VARCHAR, payload_json VARCHAR"
    ),

    # 4. Data Quality & Quarantine Governance (2 tables)
    "quality_issue": "issue_id VARCHAR, observation_id VARCHAR, code VARCHAR, path VARCHAR, severity VARCHAR",
    "quarantine": "observation_id VARCHAR, reason VARCHAR",
}


def stage(db, catalog, namespace, tables, release_id, schemas=SCHEMAS):
    namespace = identifier(namespace)
    db.execute(f"CREATE SCHEMA IF NOT EXISTS lake.{namespace}")
    snapshots = {}
    for name, rows in tables.items():
        name = identifier(name)
        schema = schemas.get(name)
        if schema is None:
            continue
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
            except Exception:  # noqa: BLE001, S110
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
