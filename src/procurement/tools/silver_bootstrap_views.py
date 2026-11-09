"""Canonical history -> bulk source views, with the SAME ordered upsert result.

This is calculation over local Silver Parquet, not Iceberg publication/readiness.
Narrow key/owner indexes select the final cohort without sorting raw payloads.
"""

from pathlib import Path

import pyarrow as pa

from procurement.silver.domain.schema import get_primary_key
from procurement.storage.iceberg import identifier, literal
from procurement.tools.gold_history_batches import SUPPORTED

OWNER_TABLES = {
    "award_lot": "result",
    "award_contractor": "result",
    "quality_issue": "observation",
    "quarantine": "observation",
}


def create_views(db, entries, *, prefix="bootstrap"):
    """entries are ordered (ordinal, {table: absolute Parquet path}) cohorts.

    Keep ALL rows from the final cohort for a key, including within-cohort
    multiplicity. Child/governance sets follow latest *owner* occurrence, even
    if the owner's latest set is empty. Null governance owners are append-only,
    as in the original writer (SQL IN does not delete null owners).
    """
    identifier(prefix)
    files = {name: [] for name in SUPPORTED}
    manifest = []
    for ordinal, tables in entries:
        for name, path in tables.items():
            if name not in SUPPORTED:
                raise ValueError("Unsupported bootstrap table: " + name)
            path = str(Path(path).resolve())
            files[name].append(path)
            manifest.append({"path": path, "table_name": name, "cohort": ordinal})
    if not manifest or any(not p for p in files.values()):
        raise ValueError("Bulk scope requires explicit typed files for all canonical tables")
    relation = prefix + "_manifest"
    db.register(relation, pa.Table.from_pylist(manifest))
    raw = {}
    for name, paths in sorted(files.items()):
        view = prefix + "_raw_" + name
        db.execute(
            f"CREATE TEMP VIEW {view} AS SELECT p.* EXCLUDE(filename),m.cohort AS __cohort "
            f"FROM read_parquet([{','.join(literal(p) for p in paths)}], "
            f"filename=true,hive_partitioning=false) p "
            f"JOIN {relation} m ON m.path=p.filename AND m.table_name={literal(name)}"
        )
        raw[name] = view
    owners = {}
    for owner, names, field in [
        ("result", ["result_revision", "award_lot", "award_contractor"], "revision_id"),
        ("observation", ["observation", "quality_issue", "quarantine"], "observation_id"),
    ]:
        index = prefix + "_owner_" + owner
        union = " UNION ALL ".join(f"SELECT {field} AS owner,__cohort FROM {raw[n]}" for n in names)
        db.execute(
            f"CREATE TEMP TABLE {index} AS SELECT owner,max(__cohort) AS last_cohort "
            f"FROM ({union}) WHERE owner IS NOT NULL GROUP BY owner"
        )
        owners[owner] = index
    selected = {}
    for name, source in raw.items():
        view = prefix + "_selected_" + name
        if name in OWNER_TABLES:
            owner = OWNER_TABLES[name]
            field = "revision_id" if owner == "result" else "observation_id"
            sql = (
                f"SELECT s.* EXCLUDE(__cohort) FROM {source} s JOIN {owners[owner]} w "
                f"ON s.{field}=w.owner AND s.__cohort=w.last_cohort"
            )
            if owner == "observation":
                sql += (
                    f" UNION ALL SELECT s.* EXCLUDE(__cohort) FROM {source} s WHERE {field} IS NULL"
                )
        else:
            pk = get_primary_key(name)
            if not pk:
                raise ValueError("Unknown canonical primary key")
            index = prefix + "_key_" + name
            db.execute(
                f"CREATE TEMP TABLE {index} AS SELECT {pk} AS owner,max(__cohort) AS last_cohort "
                f"FROM {source} GROUP BY {pk}"
            )
            if db.execute(f"SELECT count(*) FROM {index} WHERE owner IS NULL").fetchone()[0]:
                raise ValueError("Null canonical primary key")
            sql = (
                f"SELECT s.* EXCLUDE(__cohort) FROM {source} s JOIN {index} w "
                f"ON s.{pk}=w.owner AND s.__cohort=w.last_cohort"
            )
        db.execute(f"CREATE TEMP VIEW {view} AS {sql}")
        selected[name] = view
    return {"views": selected, "owners": owners, "raw": raw}
