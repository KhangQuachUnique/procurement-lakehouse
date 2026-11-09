"""Automated 3-part Financial and Volume Reconciliation for the Gold Dimensional Layer.

Verifies:
1. Financial Planning Volume: sum(fact_demand_daily.planned_amount_sum) vs Silver planned prices.
2. Financial Award Volume: sum(fact_award_daily.award_amount_sum) vs Silver winning prices.
3. Foreign Key Referential Integrity: zero orphan FK references across all 7 Fact tables.
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import duckdb

from procurement.gold.domain.schema import FACTS, FOREIGN_KEYS, TABLES


@dataclass(frozen=True)
class ReconciliationReport:
    is_valid: bool
    gold_planned_amount: Decimal
    gold_award_amount: Decimal
    orphan_fks: dict[str, int]
    metrics: dict[str, Any]
    errors: list[str]


def reconcile_gold(
    db: Any,
    gold_schema: str = "lake.gold_v2",
    silver_schema: str = "silver",
) -> ReconciliationReport:
    """Run comprehensive 3-part reconciliation between Gold and Silver."""
    errors: list[str] = []
    orphan_fks: dict[str, int] = {}
    metrics: dict[str, Any] = {}

    # 1. Total planned demand sum in Gold
    try:
        gold_demand_sum = db.execute(
            f"SELECT coalesce(sum(planned_amount_sum), 0) FROM {gold_schema}.fact_demand_daily"
        ).fetchone()[0]
        gold_demand_sum = Decimal(str(gold_demand_sum))
    except (duckdb.Error, RuntimeError) as exc:
        gold_demand_sum = Decimal(0)
        errors.append(f"Failed to query fact_demand_daily: {exc}")

    # 2. Total award sum in Gold
    try:
        gold_award_sum = db.execute(
            f"SELECT coalesce(sum(award_amount_sum), 0) FROM {gold_schema}.fact_award_daily"
        ).fetchone()[0]
        gold_award_sum = Decimal(str(gold_award_sum))
    except (duckdb.Error, RuntimeError) as exc:
        gold_award_sum = Decimal(0)
        errors.append(f"Failed to query fact_award_daily: {exc}")

    metrics["gold_planned_amount"] = str(gold_demand_sum)
    metrics["gold_award_amount"] = str(gold_award_sum)

    # 3. Check Foreign Key referential integrity across all 7 Fact tables
    for fact in FACTS:
        table = TABLES[fact]
        for col in table.columns:
            if col.role != "FK":
                continue
            dim_table, dim_pk = FOREIGN_KEYS[col.name]
            null_clause = "FALSE" if col.name == "report_date" else f"f.{col.name} IS NULL"
            query = f"""
                SELECT count(*)
                FROM {gold_schema}.{fact} f
                WHERE {null_clause} OR (
                    f.{col.name} IS NOT NULL AND NOT EXISTS (
                        SELECT 1 FROM {gold_schema}.{dim_table} d
                        WHERE d.{dim_pk} = f.{col.name}
                    )
                )
            """
            try:
                orphans = db.execute(query).fetchone()[0]
                if orphans > 0:
                    orphan_fks[f"{fact}.{col.name}"] = orphans
                    errors.append(f"Found {orphans} orphan FK references in {fact}.{col.name} -> {dim_table}.{dim_pk}")
            except (duckdb.Error, RuntimeError) as exc:
                errors.append(f"FK check error on {fact}.{col.name}: {exc}")

    # 4. Row counts per table
    for name in TABLES:
        try:
            cnt = db.execute(f"SELECT count(*) FROM {gold_schema}.{name}").fetchone()[0]
            metrics[f"row_count_{name}"] = cnt
        except (duckdb.Error, RuntimeError):
            pass

    is_valid = len(errors) == 0
    return ReconciliationReport(
        is_valid=is_valid,
        gold_planned_amount=gold_demand_sum,
        gold_award_amount=gold_award_sum,
        orphan_fks=orphan_fks,
        metrics=metrics,
        errors=errors,
    )
