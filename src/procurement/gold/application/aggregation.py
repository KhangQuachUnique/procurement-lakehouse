"""Owner retraction and fixed-grain candidate aggregation, independent of source SQL.

Private contributions are the replaceable terms of public sums, never public
facts. This layer does not choose business revisions or infer changed owners.
"""

from procurement.gold.domain.models import identifier
from procurement.gold.domain.schema import FACTS, TABLES, create_tables, group_hash
from procurement.gold.domain.validation import validate


def create_ledger(db, schema):
    identifier(schema)
    db.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    for name in FACTS:
        table = TABLES[name]
        fields = ",".join(f"{c.name} {c.sql_type}" for c in table.columns if c.name != "group_key")
        db.execute(
            f"CREATE TABLE IF NOT EXISTS {schema}.{name} (owner_key VARCHAR NOT NULL,{fields})"
        )


def prepare_candidate(db, run_id, *, contributions, affected, dimensions, observation_day):
    """Inside caller transaction. Never edits accepted state or public remote data.

    Inputs are local schemas/relations. Contributions include the COMPLETE new
    replacement for every owner in affected, including explicit empty deletion.
    """
    for value in (run_id, contributions, dimensions):
        identifier(value)
    for part in affected.split("."):
        identifier(part)
    candidate, ledger = "candidate_" + run_id, "candidate_ledger_" + run_id
    create_tables(db, candidate)
    create_ledger(db, "accepted_ledger")
    create_tables(db, "accepted")
    create_ledger(db, ledger)
    for name in FACTS:
        table = TABLES[name]
        expected = [("owner_key", "VARCHAR")] + [
            (c.name, c.sql_type) for c in table.columns if c.name != "group_key"
        ]
        actual = [(r[0], r[1]) for r in db.execute(f"DESCRIBE {contributions}.{name}").fetchall()]
        if actual != expected:
            raise ValueError("Contribution schema mismatch: " + name)
        invalid = db.execute(
            f"SELECT count(*) FROM {contributions}.{name} c WHERE owner_key IS NULL "
            f"OR trim(owner_key)='' OR NOT EXISTS (SELECT 1 FROM {affected} a WHERE a.owner_key=c.owner_key)"
        ).fetchone()[0]
        if invalid:
            raise ValueError("New contribution outside explicitly affected owners: " + name)
        db.execute(f"DELETE FROM {ledger}.{name}")
        db.execute(
            f"INSERT INTO {ledger}.{name} SELECT * FROM accepted_ledger.{name} "
            f"WHERE owner_key NOT IN (SELECT owner_key FROM {affected})"
        )
        db.execute(f"INSERT INTO {ledger}.{name} SELECT * FROM {contributions}.{name}")
        if name == "fact_status_daily":
            db.execute(f"UPDATE {ledger}.{name} SET report_date=?", [observation_day])
        grain = ",".join(table.grain)
        measures = ",".join(
            f"CAST(SUM({c.name}) AS {c.sql_type}) AS {c.name}" for c in table.measures
        )
        db.execute(f"DELETE FROM {candidate}.{name}")
        if name == "fact_status_daily":
            # Keep previous actual observation days, but replace this day's whole snapshot.
            db.execute(
                f"INSERT INTO {candidate}.{name} SELECT * FROM accepted.{name} WHERE report_date<>?",
                [observation_day],
            )
            db.execute(
                f"INSERT INTO {candidate}.{name} BY NAME SELECT {group_hash(table.grain)} group_key,"
                f"{grain},{measures} FROM {ledger}.{name} GROUP BY {grain}"
            )
        else:
            # Recompute only old/new touched groups, never append partial totals.
            touched = "touched_" + name
            db.execute(
                f"CREATE OR REPLACE TEMP TABLE {touched} AS "
                f"SELECT DISTINCT {group_hash(table.grain)} group_key FROM accepted_ledger.{name} "
                f"WHERE owner_key IN (SELECT owner_key FROM {affected}) UNION "
                f"SELECT DISTINCT {group_hash(table.grain)} FROM {contributions}.{name}"
            )
            db.execute(
                f"INSERT INTO {candidate}.{name} SELECT * FROM accepted.{name} "
                f"WHERE group_key NOT IN (SELECT group_key FROM {touched})"
            )
            db.execute(
                f"INSERT INTO {candidate}.{name} BY NAME SELECT {group_hash(table.grain)} group_key,"
                f"{grain},{measures} FROM {ledger}.{name} "
                f"WHERE {group_hash(table.grain)} IN (SELECT group_key FROM {touched}) GROUP BY {grain}"
            )
    for name in (n for n in TABLES if n not in FACTS):
        table = TABLES[name]
        cols = ",".join(c.name for c in table.columns)
        db.execute(f"DELETE FROM {candidate}.{name}")
        # Supplied dimensions are a complete current dictionary, including keys
        # still referenced by retained historical observations.
        db.execute(f"INSERT INTO {candidate}.{name} SELECT {cols} FROM {dimensions}.{name}")
    evidence = validate(db, candidate)
    return {"candidate_schema": candidate, "candidate_ledger": ledger, **evidence}


def accept_candidate(db, run_id):
    """Caller has checked active pointer; transactionally accept local state."""
    identifier(run_id)
    candidate, ledger = "candidate_" + run_id, "candidate_ledger_" + run_id
    validate(db, candidate)
    for name in TABLES:
        db.execute(f"DELETE FROM accepted.{name}")
        db.execute(f"INSERT INTO accepted.{name} SELECT * FROM {candidate}.{name}")
    for name in FACTS:
        db.execute(f"DELETE FROM accepted_ledger.{name}")
        db.execute(f"INSERT INTO accepted_ledger.{name} SELECT * FROM {ledger}.{name}")
