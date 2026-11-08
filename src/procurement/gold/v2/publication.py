"""Durable changed-key outbox and an injected publisher contract."""

from typing import Protocol

from .contracts import RunPlan, identifier
from .schema import TABLES


class Publisher(Protocol):
    def publish_verified(self, db, plan: RunPlan, schema: str) -> dict: ...
    def activate(self, plan: RunPlan) -> dict: ...
    def is_active(self, plan: RunPlan) -> bool: ...


def prepare_outbox(db, plan, schema):
    identifier(schema)
    db.execute("CREATE SCHEMA IF NOT EXISTS outbox")
    counts = {}
    for name, table in TABLES.items():
        key = table.key
        db.execute(f"""CREATE OR REPLACE TABLE outbox.keys_{name} AS
          SELECT {key} FROM ((SELECT * FROM {schema}.{name} EXCEPT ALL SELECT * FROM accepted.{name})
                     UNION ALL (SELECT * FROM accepted.{name} EXCEPT ALL SELECT * FROM {schema}.{name})) delta
          GROUP BY {key}""")
        db.execute(
            f"CREATE OR REPLACE TABLE outbox.{name} AS SELECT * FROM {schema}.{name} WHERE {key} IN (SELECT {key} FROM outbox.keys_{name})"
        )
        counts[name] = {
            "changed_keys": db.execute(f"SELECT count(*) FROM outbox.keys_{name}").fetchone()[0],
            "replacement_rows": db.execute(f"SELECT count(*) FROM outbox.{name}").fetchone()[0],
        }
    return {"run_id": plan.run_id, "plan_identity": plan.identity, "tables": counts}
