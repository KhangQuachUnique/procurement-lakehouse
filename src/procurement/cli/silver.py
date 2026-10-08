"""Command Line Interface for Silver Layer operations and inspections.

Usage examples:
  uv run python -m procurement.cli.silver inspect --resource project --date 2022-01-01
  uv run python -m procurement.cli.silver test
"""

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from typing import Any

import pyarrow.parquet as pq
import s3fs
from dotenv import load_dotenv

import procurement.processing.silver.transformers  # noqa: F401 (Triggers auto-registration)
from procurement.processing.silver.protocols import RawRecordEnvelope
from procurement.processing.silver.registry import get_transformer, list_registered_tables

# Ensure Vietnamese utf-8 display in Windows terminal
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")


def get_s3_fs() -> tuple[s3fs.S3FileSystem, str]:
    load_dotenv()
    endpoint = os.getenv("OBJECT_STORAGE_ENDPOINT", "http://127.0.0.1:8333")
    key = os.getenv("OBJECT_STORAGE_ACCESS_KEY", "muasamcong")
    secret = os.getenv("OBJECT_STORAGE_SECRET_KEY", "muasamcong123")
    bucket = os.getenv("OBJECT_STORAGE_BUCKET", "procurement-lakehouse")
    fs = s3fs.S3FileSystem(client_kwargs={"endpoint_url": endpoint}, key=key, secret=secret)
    return fs, bucket


def cmd_inspect(args: argparse.Namespace) -> None:
    """Inspect and test transformation on a real Bronze record."""
    table_name = args.table or f"{args.resource}_detail"
    transformer = get_transformer(table_name)
    if not transformer:
        print(f"[ERROR] No transformer registered for table '{table_name}'.")
        print(f"Available registered tables: {', '.join(list_registered_tables())}")
        return

    fs, bucket = get_s3_fs()
    table_path = f"{bucket}/bronze/muasamcong/{table_name}"
    if args.date:
        table_path = f"{table_path}/source_date={args.date}"

    if not fs.exists(table_path):
        print(f"[ERROR] Path not found in storage: {table_path}")
        return

    # Find first parquet file
    parquet_files = [f for f in fs.find(table_path) if f.endswith(".parquet")]
    if not parquet_files:
        print(f"[WARN] No Parquet files found under: {table_path}")
        return

    target_file = parquet_files[0]
    print(f"--> Reading Bronze Parquet: {target_file}")

    with fs.open(target_file, "rb") as stream:
        table = pq.read_table(stream)
        total_rows = len(table)
        row_idx = min(args.row, total_rows - 1)
        row_dict = table.to_pylist()[row_idx]

    payload: dict[str, Any] = (
        json.loads(row_dict["payload"]) if isinstance(row_dict["payload"], str) else row_dict["payload"]
    )

    envelope = RawRecordEnvelope(
        namespace="muasamcong",
        table_name=table_name,
        resource=args.resource or table_name.replace("_detail", ""),
        source_date=str(row_dict["source_date"]),
        run_id=row_dict["run_id"],
        row_ordinal=row_idx,
        file_key=target_file,
        file_sha256="inspect_sha256",
        source_id=row_dict["source_id"],
        source_version=row_dict.get("source_version"),
        content_hash=row_dict["content_hash"],
        ingested_at=datetime.now(UTC),
        payload=payload,
    )

    result = transformer.transform(envelope)

    print("\n" + "=" * 65)
    print(f"  SILVER TRANSFORMATION REPORT: {table_name.upper()}")
    print("=" * 65)

    if result.quarantine:
        print("STATUS:        [QUARANTINED - CÁCH LY]")
        print(f"Lý do cách ly: {result.quarantine.reason}")
    else:
        print("STATUS:        [ACCEPTED - HỢP LỆ]")
        print(f"Entity Type:   {result.entity.entity_type}")
        print(f"Entity ID:     {result.entity.entity_id}")
        print(f"Natural Key:   {result.entity.source_identity}")
        print(f"Revision ID:   {result.revision.revision_id}")

        print("\n--- THUỘC TÍNH ĐÃ CHUẨN HÓA (TYPED ATTRIBUTES) ---")
        if result.typed_attributes:
            for k, v in result.typed_attributes.items():
                if k != "root_json":
                    print(f"  {k:<20}: {v}")

        print("\n--- QUAN HỆ NGOẠI (REFERENCES) ---")
        if result.references:
            for ref in result.references:
                print(f"  -> Trỏ tới {ref.target_type} ({ref.id_scheme}={ref.target_identity}) qua trường {ref.field_name}")
        else:
            print("  (Không có quan hệ cha)")

        print("\n--- CÁC THÀNH PHẦN CON (CHILDREN) ---")
        print(f"  Tổng số thành phần con: {len(result.children)}")
        for ch in result.children[:3]:
            print(f"  - [{ch.kind}] role={ch.role}, path={ch.path}, source_id={ch.source_id}")
        if len(result.children) > 3:
            print(f"  ... và {len(result.children) - 3} thành phần khác.")

    print("\n--- CHẤT LƯỢNG DỮ LIỆU (QUALITY ISSUES) ---")
    if result.issues:
        for iss in result.issues:
            print(f"  [{iss.severity.upper()}] code={iss.code}, path={iss.path}")
    else:
        print("  (0 cảnh báo/lỗi)")
    print("=" * 65 + "\n")


def cmd_run(args: argparse.Namespace) -> None:
    """Run Silver transformation for a specific partition date and write Parquet to S3."""
    import pyarrow as pa

    from procurement.processing.silver.revisions import assemble, validate

    table_name = args.table or f"{args.resource}_detail"
    transformer = get_transformer(table_name)
    if not transformer:
        print(f"[ERROR] No transformer registered for table '{table_name}'.")
        return

    fs, bucket = get_s3_fs()
    table_path = f"{bucket}/bronze/muasamcong/{table_name}/source_date={args.date}"
    if not fs.exists(table_path):
        print(f"[ERROR] Bronze date path not found: {table_path}")
        return

    parquet_files = [f for f in fs.find(table_path) if f.endswith(".parquet")]
    if not parquet_files:
        print(f"[WARN] No Parquet files found in: {table_path}")
        return

    print(f"\n[START] Processing Bronze '{table_name}' for date {args.date}...")
    print(f"  Found {len(parquet_files)} parquet file(s).")

    all_transformed = []
    total_bronze_records = 0

    for file_path in parquet_files:
        with fs.open(file_path, "rb") as stream:
            pq_table = pq.read_table(stream)
            records = pq_table.to_pylist()
            total_bronze_records += len(records)

            for ord_idx, row_dict in enumerate(records):
                raw = row_dict["payload"]
                payload = json.loads(raw) if isinstance(raw, str) else raw

                envelope = RawRecordEnvelope(
                    namespace="muasamcong",
                    table_name=table_name,
                    resource=args.resource or table_name.replace("_detail", ""),
                    source_date=str(row_dict["source_date"]),
                    run_id=row_dict["run_id"],
                    row_ordinal=ord_idx,
                    file_key=file_path,
                    file_sha256="verified_sha256",
                    source_id=row_dict["source_id"],
                    source_version=row_dict.get("source_version"),
                    content_hash=row_dict["content_hash"],
                    ingested_at=datetime.now(UTC),
                    payload=payload,
                )
                res = transformer.transform(envelope)
                all_transformed.append(res.to_legacy_dict())

    # Reconcile & assemble tables
    assembled_tables = assemble(all_transformed)
    report = validate(assembled_tables)

    # Write each non-empty table to S3 under silver/muasamcong/<table>/source_date=<date>/data.parquet
    target_base = f"{bucket}/silver/muasamcong"
    written_tables = {}

    for tbl_name, rows in assembled_tables.items():
        if not rows:
            continue
        out_path = f"{target_base}/{tbl_name}/source_date={args.date}/data.parquet"
        pa_table = pa.Table.from_pylist(rows)
        with fs.open(out_path, "wb") as out_stream:
            pq.write_table(pa_table, out_stream)
        written_tables[tbl_name] = (len(rows), out_path)

    print("\n" + "=" * 65)
    print(f"  SILVER MATERIALIZATION COMPLETED FOR DATE: {args.date}")
    print("=" * 65)
    print(f"Bronze Input Records:  {total_bronze_records}")
    print(f"Accepted Records:      {report['accepted']}")
    print(f"Quarantined Records:   {report['quarantined']}")
    print(f"Quality Issues:        {report['issues']}")
    print("\n--- CÁC FILE PARQUET ĐÃ GHI VÀO S3 ---")
    for tbl_name, (count, s3_loc) in written_tables.items():
        print(f"  ✓ {tbl_name:<22}: {count:>4} dòng -> s3://{s3_loc}")
    print("=" * 65 + "\n")


def cmd_list(args: argparse.Namespace) -> None:
    """List all registered transformers."""
    registered = list_registered_tables()
    print("\nRegistered Silver Transformers:")
    for tbl in registered:
        trans = get_transformer(tbl)
        print(f"  - Table: {tbl:<35} -> Entity: {trans.entity_type} (Scheme: {trans.id_scheme})")
    print(f"\nTotal: {len(registered)} registered transformer(s).\n")


def cmd_query(args: argparse.Namespace) -> None:
    """Query Silver tables using DuckDB."""
    from urllib.parse import urlparse

    import duckdb

    fs, bucket = get_s3_fs()
    endpoint = os.getenv("OBJECT_STORAGE_ENDPOINT", "http://127.0.0.1:8333")
    key = os.getenv("OBJECT_STORAGE_ACCESS_KEY", "muasamcong")
    secret = os.getenv("OBJECT_STORAGE_SECRET_KEY", "muasamcong123")
    parsed_netloc = urlparse(endpoint).netloc
    use_ssl = urlparse(endpoint).scheme == "https"

    con = duckdb.connect()
    try:
        try:
            con.load_extension("httpfs")
        except duckdb.Error:
            con.install_extension("httpfs")
            con.load_extension("httpfs")

        con.execute(
            f"""
            CREATE SECRET silver_s3 (
                TYPE s3,
                KEY_ID '{key}',
                SECRET '{secret}',
                ENDPOINT '{parsed_netloc}',
                REGION 'us-east-1',
                URL_STYLE 'path',
                USE_SSL {str(use_ssl).lower()},
                SCOPE 's3://{bucket}'
            )
            """
        )
        con.execute("CREATE SCHEMA IF NOT EXISTS silver")

        # Discover and create views for all silver tables
        silver_prefix = f"{bucket}/silver"
        if fs.exists(silver_prefix):
            for root in fs.ls(silver_prefix, detail=False):
                table_name = root.rstrip("/").rsplit("/", 1)[-1]
                sub = fs.ls(root, detail=False)
                has_sub = any(not s.rstrip("/").rsplit("/", 1)[-1].startswith("source_date=") for s in sub)
                tables_to_view = [s.rstrip("/").rsplit("/", 1)[-1] for s in sub] if has_sub else [table_name]
                for tbl in tables_to_view:
                    pattern = f"s3://{bucket}/silver/*/{tbl}/**/*.parquet"
                    try:
                        con.execute(
                            f"""
                            CREATE OR REPLACE VIEW silver.{tbl} AS
                            SELECT * FROM read_parquet(
                                '{pattern}',
                                hive_partitioning = true,
                                union_by_name = true
                            )
                            """
                        )
                    except duckdb.Error:
                        pass

        if args.sql:
            sql = args.sql
        elif args.table:
            cols = args.columns or "*"
            sql = f"SELECT {cols} FROM silver.{args.table}"
            if args.date:
                sql += f" WHERE source_date = '{args.date}'"
            if args.limit:
                sql += f" LIMIT {args.limit}"
        else:
            print("[ERROR] Please specify either --table <name> or --sql <query>")
            return

        print(f"\n--> Running SQL query on Silver:\n    {sql}\n")
        res = con.execute(sql)
        desc = [d[0] for d in res.description] if res.description else []
        rows = res.fetchall()
        if not rows:
            print("  (0 rows returned)")
            return

        # Format column widths for neat tabular display
        str_rows = [[str(val) if val is not None else "NULL" for val in row] for row in rows]
        col_widths = [len(h) for h in desc]
        for row in str_rows:
            for i, val in enumerate(row):
                col_widths[i] = max(col_widths[i], min(len(val), 40))

        header_str = " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(desc))
        sep_str = "-+-".join("-" * col_widths[i] for i in range(len(desc)))
        print(header_str)
        print(sep_str)
        for row in str_rows:
            print(" | ".join((v[:37] + "..." if len(v) > 40 else v).ljust(col_widths[i]) for i, v in enumerate(row)))
        print(f"\nTotal: {len(rows)} row(s).\n")
    except Exception as exc:
        print(f"[ERROR] Query failed: {exc}")
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Procurement Lakehouse - Silver CLI")
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    # inspect command (dry-run inspection)
    inspect_parser = subparsers.add_parser("inspect", help="Inspect and transform a Bronze record (dry-run)")
    inspect_parser.add_argument("--resource", default="project", help="Resource name (project, plan, package, etc.)")
    inspect_parser.add_argument("--table", default=None, help="Exact Bronze table name")
    inspect_parser.add_argument("--date", default="2022-01-01", help="Source date YYYY-MM-DD")
    inspect_parser.add_argument("--row", type=int, default=0, help="Row index in file")
    inspect_parser.set_defaults(func=cmd_inspect)

    # run command (materialize to S3)
    run_parser = subparsers.add_parser("run", help="Run transformation and write Silver Parquet to S3")
    run_parser.add_argument("--resource", default="project", help="Resource name")
    run_parser.add_argument("--table", default=None, help="Exact Bronze table name")
    run_parser.add_argument("--date", required=True, help="Partition date YYYY-MM-DD")
    run_parser.set_defaults(func=cmd_run)

    # list command
    list_parser = subparsers.add_parser("list", help="List all registered transformers")
    list_parser.set_defaults(func=cmd_list)

    # query command (query Silver via DuckDB)
    query_parser = subparsers.add_parser("query", help="Query Silver Parquet tables via DuckDB SQL")
    query_parser.add_argument("--table", default=None, help="Silver table name (e.g. project_revision, current_entity)")
    query_parser.add_argument("--columns", default=None, help="Columns to select (comma separated)")
    query_parser.add_argument("--date", default=None, help="Filter by source_date YYYY-MM-DD")
    query_parser.add_argument("--limit", type=int, default=10, help="Row limit (default: 10)")
    query_parser.add_argument("--sql", default=None, help="Direct SQL query to execute")
    query_parser.set_defaults(func=cmd_query)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
