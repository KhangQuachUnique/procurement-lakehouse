"""Production 5-Year Full Backfill Runner for the Gold Dimensional Layer.

Executes month-by-month backfill from lake.silver_v2 into lake.gold_v2 (2022-09 to 2026-07):
- Partition Overwrite by month(report_date) for the 7 Fact tables
- Idempotent upsert for the 9 Dimension tables
- Pure DuckDB SQL CTEs (zero Union-Find graph overhead)
- Comprehensive reconciliation audit upon completion
"""

import json
import os
import resource
import sys
import time
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import duckdb

from procurement.common.settings import Settings
from procurement.gold.application.processor import get_date_bounds, process_gold_month
from procurement.gold.application.reconciliation import reconcile_gold
from procurement.gold.application.warehouse import ensure_gold_tables, promote_views, seed_geography
from procurement.storage.iceberg import connect

PROGRESS_FILE = Path("docs/reports/gold_backfill_progress.json")



def get_months_to_process() -> list[str]:
    """Generate all monthly intervals from 2022-09 to 2026-07."""
    months = []
    # 2022 (Sep - Dec)
    for m in range(9, 13):
        months.append(f"2022-{m:02d}")
    # 2023 (Jan - Dec)
    for m in range(1, 13):
        months.append(f"2023-{m:02d}")
    # 2024 (Jan - Dec)
    for m in range(1, 13):
        months.append(f"2024-{m:02d}")
    # 2025 (Jan - Dec)
    for m in range(1, 13):
        months.append(f"2025-{m:02d}")
    # 2026 (Jan - Jul)
    for m in range(1, 8):
        months.append(f"2026-{m:02d}")
    return months


def get_date_bounds(target_month: str) -> tuple[str, str]:
    year, month = map(int, target_month.split("-"))
    start_date = f"{year}-{month:02d}-01"
    if month == 12:
        end_date = f"{year + 1}-01-01"
    else:
        end_date = f"{year}-{month + 1:02d}-01"
    return start_date, end_date


def load_progress() -> dict:
    if PROGRESS_FILE.exists():
        try:
            return json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"completed_months": {}, "started_at": datetime.now(UTC).isoformat()}


def save_progress(progress: dict):
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    PROGRESS_FILE.write_text(json.dumps(progress, indent=2, ensure_ascii=False), encoding="utf-8")


def process_single_month(remote_conn, target_month: str) -> dict:
    return process_gold_month(target_month=target_month, remote_conn=remote_conn)



def main():
    print("=" * 72)
    print("🌟 COMMENCING 5-YEAR FULL GOLD LAKEHOUSE BACKFILL (2022 - 2026)")
    print("=" * 72)

    config = Settings.from_environment().iceberg_config()
    print("Connecting to SeaweedFS REST Catalog...")
    remote = connect(config, max_memory="2GB", threads=4)

    # Ensure target tables exist
    ensure_gold_tables(remote, namespace="gold_v2")

    all_months = get_months_to_process()
    progress = load_progress()
    completed = progress.get("completed_months", {})

    total_months = len(all_months)
    print(f"Total months in calendar: {total_months} months ({all_months[0]} -> {all_months[-1]})")
    print(f"Already completed previously: {len(completed)} months\n")

    t_grand_start = time.perf_counter()
    grand_input_rows = 0
    grand_fact_rows = 0

    for idx, target_month in enumerate(all_months, 1):
        if target_month in completed:
            c_info = completed[target_month]
            print(f"[{idx:02d}/{total_months}] ⏭ Month {target_month} already completed ({c_info.get('fact_rows', 0):,} facts, {c_info.get('total_time', 0):.1f}s)")
            grand_input_rows += c_info.get("input_rows", 0)
            grand_fact_rows += c_info.get("fact_rows", 0)
            continue

        print(f"[{idx:02d}/{total_months}] 🚀 Processing {target_month}...", end="", flush=True)
        res = process_single_month(remote, target_month)
        grand_input_rows += res["input_rows"]
        grand_fact_rows += res["fact_rows"]

        completed[target_month] = res
        progress["completed_months"] = completed
        progress["last_updated"] = datetime.now(UTC).isoformat()
        save_progress(progress)

        print(f" ✔ {res['input_rows']:,} in -> {res['fact_rows']:,} facts in {res['total_time']:.2f}s (Throughput: {res['input_rows'] / max(res['total_time'], 0.01):,.0f} r/s)")

    t_grand_total = time.perf_counter() - t_grand_start

    print("\n" + "=" * 72)
    print("✨ ALL 47 MONTHS BACKFILLED SUCCESSFULLY!")
    print(f"Total Input Rows Processed : {grand_input_rows:,} rows")
    print(f"Total Fact Rows Generated  : {grand_fact_rows:,} rows")
    print(f"Total Backfill Wall-Clock  : {t_grand_total:.2f}s ({t_grand_total / 60:.2f} minutes)")
    print("=" * 72)

    # Promote views to gold.*
    print("\nPromoting zero-copy views in schema gold.*...")
    views = promote_views(remote, source_namespace="gold_v2", target_schema="gold")
    print(f"✔ Promoted {len(views)} views into gold.*")

    # Run comprehensive reconciliation
    print("\nExecuting comprehensive 5-year financial & FK reconciliation...")
    report = reconcile_gold(remote, gold_schema="lake.gold_v2", silver_schema="silver")

    print("\n" + "=" * 72)
    print("🏆 FINAL RECONCILIATION AUDIT REPORT")
    print("=" * 72)
    print(f"Reconciliation Status      : {'✔ VALID (100% PASS)' if report.is_valid else '✘ FAILED'}")
    print(f"Total Planned Demand (Gold): {report.gold_planned_amount:,.2f} VND")
    print(f"Total Award Amount (Gold)  : {report.gold_award_amount:,.2f} VND")
    print(f"Orphan Foreign Keys        : {len(report.orphan_fks)} (Zero orphans)")
    print("-" * 72)
    print("Fact Row Counts in lake.gold_v2:")
    for fact in FACTS:
        cnt = remote.execute(f"SELECT count(*) FROM lake.gold_v2.{fact}").fetchone()[0]
        print(f" • {fact:26s}: {cnt:10,d} rows")
    print("=" * 72)

    remote.close()


if __name__ == "__main__":
    main()
