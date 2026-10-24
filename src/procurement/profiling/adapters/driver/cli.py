"""Driver adapter implementing CLI interface for profiling."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(".env")

from procurement.profiling.adapters.driven.jsonl_stream import (
    JsonlRecordStreamAdapter,
)
from procurement.profiling.adapters.driven.parquet_stream import (
    S3ParquetRecordStreamAdapter,
)
from procurement.profiling.adapters.driven.postgres_metadata import (
    PostgresActiveBronzeAdapter,
)
from procurement.profiling.adapters.driven.report_export import (
    MarkdownAndJsonReportAdapter,
)
from procurement.profiling.application.service import ProfilingService
from procurement.profiling.domain.models import TableProfile, WindowDrift

LIFECYCLE_TABLE_ORDER: dict[str, int] = {
    "project_detail": 1,
    "khlcnt_plan_detail": 2,
    "khlcnt_bid_package_detail": 3,
    "notify_contractor_standard_detail": 4,
    "notify_contractor_vk_adb_detail": 5,
    "notify_contractor_reoffer_detail": 6,
    "contractor_result_detail": 7,
    "bid_opening_detail": 99,
}

LIFECYCLE_RESOURCE_ORDER: dict[str, int] = {
    "project": 1,
    "khlcnt": 2,
    "notify_contractor": 3,
    "contractor_result": 4,
    "bid_opening": 99,
}


def run_cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Hexagonal profiling CLI for Bronze datasets.")
    parser.add_argument("--resource", type=str, default="all", help="Resource name to profile or 'all'")
    parser.add_argument("--table", type=str, default=None, help="Specific table name")
    parser.add_argument("--start-date", type=str, default=None, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default=None, help="End date (YYYY-MM-DD)")
    parser.add_argument("--max-records", type=int, default=1000, help="Max records to profile per table")
    parser.add_argument("--jsonl", type=str, default=None, help="Path to local JSONL file to profile")
    parser.add_argument("--output-json", type=str, default="exports/profiling/latest/profile.json", help="Path to write profile.json")
    parser.add_argument("--output-report", type=str, default="exports/profiling/latest/profiling-report.md", help="Path to write profiling-report.md")
    parser.add_argument("--temp-dir", type=str, default="temp/bid_opening", help="Directory to isolate experimental bid_opening profiling")
    parser.add_argument("--sample-step", type=int, default=1, help="Systematic sampling step")

    args = parser.parse_args(argv)

    # Wire hexagonal dependencies
    service = ProfilingService(
        metadata_port=PostgresActiveBronzeAdapter(),
        bronze_stream_port=S3ParquetRecordStreamAdapter(),
        jsonl_stream_port=JsonlRecordStreamAdapter(),
        report_export_port=MarkdownAndJsonReportAdapter(),
    )

    profiles: dict[str, TableProfile] = {}
    temp_profiles: dict[str, TableProfile] = {}
    drifts: list[WindowDrift] = []
    summary: list[dict] = []
    temp_summary: list[dict] = []

    if args.jsonl:
        print(f"Profiling local JSONL file: {args.jsonl}")
        p = service.profile_jsonl_file(
            args.jsonl,
            max_records=args.max_records,
            sample_step=args.sample_step,
        )
        profiles[p.table_name] = p
    else:
        print("Connecting to PostgreSQL metadata to discover active Bronze resources...")
        raw_summary = service.get_summary()
        raw_summary.sort(key=lambda r: LIFECYCLE_RESOURCE_ORDER.get(str(r.get("resource", "")), 50))
        active_tables = service.get_active_tables()
        active_tables.sort(key=lambda t: LIFECYCLE_TABLE_ORDER.get(str(t.get("table_name", "")), 50))

        for s in raw_summary:
            if s.get("resource") == "bid_opening" and args.resource == "all":
                temp_summary.append(s)
            else:
                summary.append(s)

        tables_to_profile = []
        temp_tables_to_profile = []
        for t in active_tables:
            res_name = t["resource"]
            tbl_name = t["table_name"]
            if args.resource != "all" and args.resource != res_name:
                continue
            if args.table and args.table != tbl_name:
                continue
            if res_name == "bid_opening" and args.resource == "all":
                temp_tables_to_profile.append((res_name, tbl_name))
            else:
                tables_to_profile.append((res_name, tbl_name))

        print(f"Found {len(tables_to_profile)} active production tables to profile:")
        for r_name, t_name in tables_to_profile:
            print(f"  - Resource: {r_name}, Table: {t_name}")

        for r_name, t_name in tables_to_profile:
            print(f"\nProfiling table: {t_name} (resource: {r_name})...")
            p = service.profile_active_table(
                table_name=t_name,
                resource=r_name,
                start_date=args.start_date,
                end_date=args.end_date,
                max_records=args.max_records,
                sample_step=args.sample_step,
            )
            profiles[t_name] = p
            print(f"  -> Profiled {p.profiled_records} records, discovered {len(p.fields)} nested field paths.")

        if temp_tables_to_profile:
            print(f"\nIsolating {len(temp_tables_to_profile)} experimental resource(s) (bid_opening) into {args.temp_dir}...")
            for r_name, t_name in temp_tables_to_profile:
                p = service.profile_active_table(
                    table_name=t_name,
                    resource=r_name,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    max_records=args.max_records,
                    sample_step=args.sample_step,
                )
                temp_profiles[t_name] = p
                print(f"  -> [TEMP] Profiled {t_name}: {p.profiled_records} records, {len(p.fields)} nested field paths.")

            temp_dir_path = Path(args.temp_dir)
            t_json, t_report = service.export_reports(
                temp_profiles,
                [],
                temp_summary,
                json_path=temp_dir_path / "bid_opening_field_profile.json",
                report_path=temp_dir_path / "bid_opening_profiling_report.md",
            )
            print(f"  -> Saved isolated bid_opening profile to: {t_json.resolve()}")
            print(f"  -> Saved isolated bid_opening report to: {t_report.resolve()}")

        if "project_detail" in profiles:
            print("\nAnalyzing window drift for project_detail across 2022 and 2025...")
            windows = [("2022-01-01", "2022-12-31"), ("2025-01-01", "2025-12-31")]
            _, win_drifts = service.compare_windows(
                table_name="project_detail",
                resource="project",
                windows=windows,
                max_records_per_window=args.max_records,
            )
            drifts.extend(win_drifts)

    j_out, r_out = service.export_reports(
        profiles,
        drifts,
        summary,
        json_path=args.output_json,
        report_path=args.output_report,
    )
    print(f"\nSaved profile metrics to: {j_out.resolve()}")
    print(f"Report written to: {r_out.resolve()}")
    return 0


if __name__ == "__main__":
    sys.exit(run_cli())
