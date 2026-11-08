"""Isolated v2 Iceberg publication. Never uses the production gold pointer.

Single-writer target/state ownership is required. Table commits are separate;
readers are consistent only through the immutable release snapshot manifest.
"""

import json

from procurement.gold.v2.contracts import canonical, digest, identifier
from procurement.gold.v2.schema import FACTS, TABLES


class IcebergPublisher:
    def __init__(self, connection, catalog):
        self.db, self.catalog = connection, catalog

    @staticmethod
    def control(plan):
        return identifier(plan.target_namespace + "_control")

    def _setup(self, plan):
        if not plan.target_namespace.startswith("gold_v2_"):
            raise ValueError("Non-isolated target refused")
        control = self.control(plan)
        self.db.execute("CREATE SCHEMA IF NOT EXISTS lake." + control)
        self.db.execute(
            f"CREATE TABLE IF NOT EXISTS lake.{control}.manifests (release_id VARCHAR,content_hash VARCHAR,receipt_json VARCHAR)"
        )
        self.db.execute(
            f"CREATE TABLE IF NOT EXISTS lake.{control}.active_release (release_id VARCHAR,content_hash VARCHAR)"
        )
        active = self.db.execute(f"SELECT release_id FROM lake.{control}.active_release").fetchall()
        expected = [] if plan.base_release is None else [(plan.base_release,)]
        if active not in (expected, [(plan.run_id,)]):
            raise ValueError("Remote target no longer at expected base release")

    def _upload(self, local, relation, temp, columns):
        ddl = ",".join(f"{c.name} {c.sql_type}" for c in columns)
        self.db.execute(f"CREATE OR REPLACE TEMP TABLE {temp} ({ddl})")
        for batch in local.execute("SELECT * FROM " + relation).to_arrow_reader(batch_size=8192):
            self.db.register("_v2_upload", batch)
            try:
                self.db.execute(f"INSERT INTO {temp} SELECT * FROM _v2_upload")
            finally:
                self.db.unregister("_v2_upload")

    def publish_verified(self, local, plan, schema):
        self._setup(plan)
        local.execute(
            "CREATE TABLE IF NOT EXISTS control.published_tables(run_id VARCHAR,table_name VARCHAR,receipt VARCHAR,PRIMARY KEY(run_id,table_name))"
        )
        self.db.execute("CREATE SCHEMA IF NOT EXISTS lake." + plan.target_namespace)
        tables = {}
        for name, table in TABLES.items():
            relation = f"lake.{plan.target_namespace}.{name}"
            saved = local.execute(
                "SELECT receipt FROM control.published_tables WHERE run_id=? AND table_name=?",
                [plan.run_id, name],
            ).fetchone()
            if saved:
                item = json.loads(saved[0])
                meta = self.catalog.table(plan.target_namespace, name)["metadata"]
                snap = meta.get("current-snapshot-id")
                snap = None if snap is None or snap < 0 else snap
                if snap != item["snapshot_id"] or meta.get("table-uuid") != item["table_uuid"]:
                    raise ValueError("Another writer changed a partially published table: " + name)
                tables[name] = item
                continue
            ddl = ",".join(f"{c.name} {c.sql_type}" for c in table.columns)
            partition = " PARTITIONED BY (month(report_date))" if name in FACTS else ""
            self.db.execute(f"CREATE TABLE IF NOT EXISTS {relation} ({ddl}){partition}")
            actual = [(r[0], r[1]) for r in self.db.execute("DESCRIBE " + relation).fetchall()]
            if actual != [(c.name, c.sql_type) for c in table.columns]:
                raise ValueError("Remote table schema is not v2: " + name)
            self._upload(local, "outbox." + name, "_v2_rows", table.columns)
            self._upload(
                local,
                "outbox.keys_" + name,
                "_v2_keys",
                tuple(c for c in table.columns if c.name == table.key),
            )
            key = table.key
            cols = ",".join(c.name for c in table.columns)
            assignments = ",".join(f"{c.name}=s.{c.name}" for c in table.columns if c.name != key)
            self.db.execute("BEGIN")
            try:
                self.db.execute(
                    f"DELETE FROM {relation} WHERE {key} IN (SELECT {key} FROM _v2_keys) AND {key} NOT IN (SELECT {key} FROM _v2_rows)"
                )
                self.db.execute(
                    f"MERGE INTO {relation} t USING _v2_rows s ON t.{key}=s.{key} "
                    f"WHEN MATCHED AND to_json(t) IS DISTINCT FROM to_json(s) THEN UPDATE SET {assignments} "
                    f"WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({','.join('s.' + c.name for c in table.columns)})"
                )
                self.db.execute("COMMIT")
            except BaseException:
                self.db.execute("ROLLBACK")
                raise
            # Full touched cohort includes tombstones; exact two-way comparison.
            bad = self.db.execute(
                f"SELECT count(*) FROM ((SELECT * FROM _v2_rows EXCEPT ALL SELECT * FROM {relation} WHERE {key} IN (SELECT {key} FROM _v2_keys)) UNION ALL "
                f"(SELECT * FROM {relation} WHERE {key} IN (SELECT {key} FROM _v2_keys) EXCEPT ALL SELECT * FROM _v2_rows))"
            ).fetchone()[0]
            if bad:
                raise ValueError("Remote changed-cohort mismatch: " + name)
            meta = self.catalog.table(plan.target_namespace, name)["metadata"]
            snap = meta.get("current-snapshot-id")
            snap = None if snap is None or snap < 0 else snap
            if snap is not None:
                self.catalog.pin(plan.target_namespace, name, snap, plan.run_id)
            rows = self.db.execute("SELECT count(*) FROM " + relation).fetchone()[0]
            expected = local.execute(f"SELECT count(*) FROM {schema}.{name}").fetchone()[0]
            if rows != expected:
                raise ValueError("Remote full count differs: " + name)
            item = {"snapshot_id": snap, "table_uuid": meta["table-uuid"], "rows": rows}
            local.execute(
                "INSERT INTO control.published_tables VALUES (?,?,?)",
                [plan.run_id, name, canonical(item)],
            )
            tables[name] = item
        receipt = {
            "release_id": plan.run_id,
            "plan_identity": plan.identity,
            "target_namespace": plan.target_namespace,
            "schema_version": plan.schema_version,
            "source": plan.source.model_dump(mode="json"),
            "source_cutoff": plan.source_cutoff.isoformat(),
            "observation_day": plan.observation_day.isoformat(),
            "tables": tables,
        }
        content = digest(receipt)
        control = self.control(plan)
        existing = self.db.execute(
            f"SELECT content_hash,receipt_json FROM lake.{control}.manifests WHERE release_id=?",
            [plan.run_id],
        ).fetchall()
        if existing and existing != [(content, canonical(receipt))]:
            raise ValueError("Conflicting immutable release manifest")
        if not existing:
            self.db.execute(
                f"INSERT INTO lake.{control}.manifests VALUES (?,?,?)",
                [plan.run_id, content, canonical(receipt)],
            )
        return receipt | {"content_hash": content}

    def activate(self, plan):
        self._setup(plan)
        control = self.control(plan)
        row = self.db.execute(
            f"SELECT content_hash,receipt_json FROM lake.{control}.manifests WHERE release_id=?",
            [plan.run_id],
        ).fetchall()
        if len(row) != 1:
            raise ValueError("Missing unique complete manifest")
        receipt = json.loads(row[0][1])
        if (
            receipt["plan_identity"] != plan.identity
            or set(receipt["tables"]) != set(TABLES)
            or digest(receipt) != row[0][0]
        ):
            raise ValueError("Activation manifest identity mismatch")
        self.db.execute("BEGIN")
        try:
            self.db.execute(f"DELETE FROM lake.{control}.active_release")
            self.db.execute(
                f"INSERT INTO lake.{control}.active_release VALUES (?,?)", [plan.run_id, row[0][0]]
            )
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise
        if not self.is_active(plan):
            raise ValueError("Activation acknowledgement missing")
        return {
            "release_id": plan.run_id,
            "plan_identity": plan.identity,
            "content_hash": row[0][0],
        }

    def is_active(self, plan):
        control = self.control(plan)
        rows = self.db.execute(
            f"SELECT a.release_id,a.content_hash,m.content_hash,m.receipt_json FROM lake.{control}.active_release a JOIN lake.{control}.manifests m USING(release_id)"
        ).fetchall()
        return (
            len(rows) == 1
            and rows[0][0] == plan.run_id
            and rows[0][1] == rows[0][2]
            and json.loads(rows[0][3])["plan_identity"] == plan.identity
            and digest(json.loads(rows[0][3])) == rows[0][1]
        )

    def ensure_views(self, plan, view_schema="gold_v2"):
        identifier(view_schema)
        control = self.control(plan)
        rows = self.db.execute(
            f"SELECT a.content_hash,m.receipt_json FROM lake.{control}.active_release a JOIN lake.{control}.manifests m USING(release_id)"
        ).fetchall()
        if len(rows) != 1 or digest(json.loads(rows[0][1])) != rows[0][0]:
            raise ValueError("Invalid active manifest")
        receipt = json.loads(rows[0][1])
        if set(receipt["tables"]) != set(TABLES):
            raise ValueError("Incomplete active schema")
        self.db.execute("CREATE SCHEMA IF NOT EXISTS " + view_schema)
        for name, item in receipt["tables"].items():
            metadata = self.catalog.table(plan.target_namespace, name)["metadata"]
            if metadata.get("table-uuid") != item["table_uuid"]:
                raise ValueError("Pinned release table identity changed: " + name)
            relation = f"lake.{plan.target_namespace}.{name}"
            if item["snapshot_id"] is not None:
                relation += f" AT (VERSION => {int(item['snapshot_id'])})"
            elif item["rows"] == 0:
                relation = "(SELECT * FROM " + relation + " WHERE FALSE)"
            else:
                raise ValueError("Nonempty release table without a snapshot")
            self.db.execute(
                f"CREATE OR REPLACE VIEW {view_schema}.{identifier(name)} AS SELECT * FROM {relation}"
            )
        return receipt
