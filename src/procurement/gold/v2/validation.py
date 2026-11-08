"""Per-fact grain/measure/FK validation of the approved public model."""

from .contracts import identifier
from .schema import FACTS, FOREIGN_KEYS, TABLES, check_schema, group_hash


def validate(db, schema):
    identifier(schema)
    check_schema(db, schema)
    for name, table in TABLES.items():
        relation = f"{schema}.{name}"
        invalid = db.execute(
            f"SELECT count(*) FROM {relation} WHERE {table.key} IS NULL"
        ).fetchone()[0]
        duplicate = db.execute(
            f"SELECT count(*) FROM (SELECT {table.key} FROM {relation} GROUP BY 1 HAVING count(*)>1)"
        ).fetchone()[0]
        if invalid or duplicate:
            raise ValueError("Null or duplicate logical key: " + name)
        if name not in FACTS:
            continue
        bad = db.execute(
            f"SELECT count(*) FROM {relation} WHERE group_key IS DISTINCT FROM {group_hash(table.grain)}"
        ).fetchone()[0]
        if bad:
            raise ValueError("Group key does not match fixed fact grain: " + name)
        for col in table.columns:
            if col.role != "FK":
                continue
            dim, key = FOREIGN_KEYS[col.name]
            null = "FALSE" if col.name == "report_date" else f"f.{col.name} IS NULL"
            bad = db.execute(
                f"SELECT count(*) FROM {relation} f WHERE {null} OR "
                f"(f.{col.name} IS NOT NULL AND NOT EXISTS "
                f"(SELECT 1 FROM {schema}.{dim} d WHERE d.{key}=f.{col.name}))"
            ).fetchone()[0]
            if bad:
                raise ValueError(f"Unresolved FK: {name}.{col.name}")
        for col in table.measures:
            condition = f"{col.name} IS NULL"
            if col.sql_type == "BIGINT":
                condition += f" OR {col.name}<0"
            if db.execute(f"SELECT count(*) FROM {relation} WHERE {condition}").fetchone()[0]:
                raise ValueError(f"Invalid measure: {name}.{col.name}")
    bad = db.execute(
        f"SELECT count(*) FROM {schema}.fact_savings_daily WHERE "
        "descriptive_difference_sum<>descriptive_baseline_sum-descriptive_award_sum "
        "OR verified_saving_sum<>verified_baseline_sum-verified_award_sum"
    ).fetchone()[0]
    if bad:
        raise ValueError("Savings cohort identity failed")
    return {"tables": len(TABLES), "verified": True}
