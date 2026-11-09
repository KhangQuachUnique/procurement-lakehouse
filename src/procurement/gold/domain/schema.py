"""Exact approved Gold dimensional schema, with explicit per-fact grains and foreign keys.

Hexagonal Domain Schema Definition for lake.gold_v2.
"""

import json
from dataclasses import dataclass
from pathlib import Path

from .models import identifier


@dataclass(frozen=True)
class Column:
    name: str
    sql_type: str
    role: str | None


@dataclass(frozen=True)
class Table:
    name: str
    columns: tuple[Column, ...]

    @property
    def key(self) -> str:
        return next(c.name for c in self.columns if c.role == "PK")

    @property
    def grain(self) -> tuple[str, ...]:
        if not self.name.startswith("fact_"):
            raise ValueError("Dimensions do not have an aggregate grain")
        return tuple(c.name for c in self.columns if c.role == "FK") + (
            ("notice_family",)
            if self.name == "fact_notice_daily"
            else ("recipient_allocation_state",)
            if self.name == "fact_supplier_daily"
            else ()
        )

    @property
    def measures(self) -> tuple[Column, ...]:
        if not self.name.startswith("fact_"):
            return ()
        return tuple(c for c in self.columns if c.name != self.key and c.name not in self.grain)


_SCHEMA_PATH = Path(__file__).with_name("schema.json")
if not _SCHEMA_PATH.exists():
    _SCHEMA_PATH = Path(__file__).parents[1] / "v2" / "schema.json"

_SCHEMA = json.loads(_SCHEMA_PATH.read_text())
TABLES = {
    name: Table(name, tuple(Column(c["name"], c["type"], c["role"]) for c in cols))
    for name, cols in _SCHEMA.items()
}
FACTS = tuple(n for n in TABLES if n.startswith("fact_"))
DIMENSIONS = tuple(n for n in TABLES if n.startswith("dim_"))
FOREIGN_KEYS = {
    "report_date": ("dim_date", "date_key"),
    "geography_key": ("dim_geography", "geography_key"),
    "sector_key": ("dim_sector", "sector_key"),
    "method_key": ("dim_procurement_method", "method_key"),
    "value_band_key": ("dim_value_band", "band_key"),
    "currency_key": ("dim_currency", "currency_key"),
    "party_key": ("dim_party", "party_key"),
    "metric_key": ("dim_duration_metric", "metric_key"),
    "status_key": ("dim_progress_status", "status_key"),
}


def create_tables(db, schema: str):
    identifier(schema)
    db.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    for table in TABLES.values():
        fields = ",".join(f"{c.name} {c.sql_type}" for c in table.columns)
        db.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{table.name} ({fields})")
    check_schema(db, schema)


def check_schema(db, schema: str):
    identifier(schema)
    names = {
        n
        for (n,) in db.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_catalog=current_database() "
            "AND table_schema=?",
            [schema],
        ).fetchall()
    }
    if names != set(TABLES):
        raise ValueError(
            f"Expected exactly 16 contract tables: missing={set(TABLES) - names}, extra={names - set(TABLES)}"
        )
    for table in TABLES.values():
        actual = [(r[0], r[1]) for r in db.execute(f"DESCRIBE {schema}.{table.name}").fetchall()]
        if actual != [(c.name, c.sql_type) for c in table.columns]:
            raise ValueError("Schema differs from approved contract: " + table.name)


def group_hash(expressions: list[str] | tuple[str, ...]) -> str:
    return (
        "sha256(to_json(list_value("
        + ",".join(f"CAST({x} AS VARCHAR)" for x in expressions)
        + ")))"
    )
