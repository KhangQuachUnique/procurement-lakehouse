from __future__ import annotations

import argparse
import time
from collections import Counter, defaultdict
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, timedelta
from functools import partial
from urllib.parse import urlparse

import duckdb
import s3fs

from procurement.common.attempts import effective_attempt
from procurement.common.catalog import RESOURCE_CATALOG, SUPPORTED_RESOURCES
from procurement.common.settings import settings
from procurement.metadata.models import PartitionIdentity
from procurement.models.control import DayManifest
from procurement.storage.control import read_run_manifest
from procurement.storage.io import read_json
from procurement.storage.object_store import create_s3_filesystem

BRONZE_SCHEMA = "bronze_raw"
SECRET_NAME = "bronze_store"
DEFAULT_UI_PORT = 4213


@dataclass(frozen=True, order=True)
class BronzeTable:
    dataset: str | None
    table: str

    def parquet_glob(self, bucket: str) -> str:
        parts = [f"s3://{bucket}", "bronze"]
        if self.dataset is not None:
            parts.append(self.dataset)
        parts.extend((self.table, "**", "*.parquet"))
        return "/".join(parts)


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _parse_endpoint(endpoint: str) -> tuple[str, bool]:
    parsed = urlparse(endpoint)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(
            "OBJECT_STORAGE_ENDPOINT must be an absolute http(s) URL, "
            f"got {endpoint!r}"
        )
    if parsed.path not in {"", "/"} or parsed.params or parsed.query or parsed.fragment:
        raise ValueError(
            "OBJECT_STORAGE_ENDPOINT must not contain a path, query, or fragment"
        )
    return parsed.netloc, parsed.scheme == "https"


def _list_directories(fs: s3fs.S3FileSystem, prefix: str) -> list[str]:
    try:
        entries = fs.ls(prefix, detail=True)
    except FileNotFoundError:
        return []

    directories: set[str] = set()
    for entry in entries:
        if isinstance(entry, str):
            name = entry
            entry_type = None
        else:
            name = str(entry.get("name", ""))
            entry_type = entry.get("type")

        if entry_type not in {None, "directory", "dir"}:
            continue

        directory = name.rstrip("/").rsplit("/", maxsplit=1)[-1]
        if directory:
            directories.add(directory)

    return sorted(directories)


def _discover_bronze_tables(
    fs: s3fs.S3FileSystem,
    bucket: str,
) -> list[BronzeTable]:
    """Discover DLT Bronze tables without treating the dataset as a table.

    Current DLT filesystem layout is bronze/<dataset>/<table>/... . The fallback
    also understands the older/direct bronze/<table>/... layout.
    """

    bronze_prefix = f"{bucket}/bronze"
    root_directories = _list_directories(fs, bronze_prefix)
    discovered: set[BronzeTable] = set()

    for root_name in root_directories:
        child_directories = _list_directories(fs, f"{bronze_prefix}/{root_name}")
        data_children = [name for name in child_directories if not name.startswith("_dlt_")]

        is_direct_table = not data_children or any(
            name.startswith(("source_date=", "run_id=")) for name in data_children
        )
        if is_direct_table:
            if not root_name.startswith("_dlt_"):
                discovered.add(BronzeTable(dataset=None, table=root_name))
            continue

        for table in data_children:
            discovered.add(BronzeTable(dataset=root_name, table=table))

    return sorted(discovered)


def _configure_s3_secret(con: duckdb.DuckDBPyConnection) -> None:
    access_key = settings.OBJECT_STORAGE_ACCESS_KEY
    secret_key = settings.OBJECT_STORAGE_SECRET_KEY
    bucket = settings.OBJECT_STORAGE_BUCKET

    if not access_key or not secret_key:
        raise RuntimeError(
            "OBJECT_STORAGE_ACCESS_KEY and OBJECT_STORAGE_SECRET_KEY are required"
        )

    endpoint, use_ssl = _parse_endpoint(settings.OBJECT_STORAGE_ENDPOINT)
    _load_extension(con, "httpfs")
    con.execute(
        f"""
        CREATE SECRET {SECRET_NAME} (
            TYPE s3,
            KEY_ID {_sql_literal(access_key)},
            SECRET {_sql_literal(secret_key)},
            ENDPOINT {_sql_literal(endpoint)},
            REGION 'us-east-1',
            URL_STYLE 'path',
            USE_SSL {str(use_ssl).lower()},
            SCOPE {_sql_literal(f"s3://{bucket}")}
        )
        """
    )


def _load_extension(con, name):
    try:
        con.load_extension(name)
    except duckdb.Error:
        con.install_extension(name)
        con.load_extension(name)


def select_snapshot_files(
    metadata_service: Any,
    *,
    tables: list[BronzeTable],
    start: date | None = None,
    end: date | None = None,
    year: int | None = None,
    committed_days: dict | None = None,
) -> dict[BronzeTable, list[str]]:
    """Resolve current committed files using MetadataService snapshots instead of S3 globs."""
    start_d = start or (date(year, 1, 1) if year else date(2020, 1, 1))
    end_d = end or (date(year, 12, 31) if year else date(2030, 12, 31))

    table_map = {t.table: t for t in tables}
    files = {table: [] for table in tables}

    cur = start_d
    dates = []
    while cur <= end_d:
        dates.append(cur)
        cur += timedelta(days=1)

    for definition in RESOURCE_CATALOG:
        resource_tables = [t for t in tables if t.table in definition.tables]
        if not resource_tables:
            continue
        identities = [
            PartitionIdentity(source=definition.identity.source, resource=definition.identity.resource, source_date=d)
            for d in dates
        ]
        snapshots = metadata_service.get_snapshot(identities)
        if committed_days is not None:
            committed_days[definition.identity.resource] = {
                str(snap.partition.identity.source_date): snap.commit for snap in snapshots
            }
        for snap in snapshots:
            for file_desc in snap.files:
                table_obj = table_map.get(file_desc.table_name)
                if table_obj:
                    files[table_obj].append(f"s3://{file_desc.bucket}/{file_desc.object_key}")
    return files


def _select_current_files(fs, *, bucket, tables, year=None, workers=16, issues=None,
                          start=None, end=None, file_metadata=None, committed_days=None,
                          metadata_service=None):
    """Freeze one SUCCESS per resource/day before binding any Parquet views.

    If metadata_service is provided, resolves files via consistent DB snapshots.
    Otherwise, reads independent small manifests concurrently from object storage.
    """
    if metadata_service is not None:
        return select_snapshot_files(
            metadata_service,
            tables=tables,
            start=start,
            end=end,
            year=year,
            committed_days=committed_days,
        )

    files = {table: [] for table in tables}
    issues = [] if issues is None else issues
    start = start or (date(year, 1, 1) if year else date.min)
    end = end or (date(year, 12, 31) if year else date.max)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for definition in RESOURCE_CATALOG:
            resource_tables = [table for table in tables if table.table in definition.tables
                               and table.dataset in (None, definition.identity.source)]
            if not resource_tables:
                continue
            identity = definition.identity
            prefix = f"{bucket}/_control/{identity.source}/{identity.resource}"
            run_ids = [name.removeprefix("run_id=") for name in _list_directories(fs, prefix)
                       if name.startswith("run_id=")]
            runs = [run for run in pool.map(partial(read_run_manifest, fs, identity), run_ids)
                    if run is not None and run.start_date <= end and run.end_date >= start]
            runs.sort(key=lambda run: run.started_at, reverse=True)
            day_keys = []
            prefixes = [f"{prefix}/run_id={run.run_id}" for run in runs]
            listings = pool.map(lambda path: _list_directories(fs, path), prefixes)
            for run, run_prefix, names in zip(runs, prefixes, listings, strict=True):
                if run.source != identity.source or run.resource != identity.resource:
                    raise ValueError("Run manifest resource mismatch")
                for name in names:
                    if name.startswith("source_date=") and str(start) <= name.removeprefix("source_date=") <= str(end):
                        day_keys.append(f"{run_prefix}/{name}/day.json")

            def read_day(key, prefix=prefix, identity=identity):
                data = read_json(fs, key)
                if data is None:
                    return None
                day = DayManifest.model_validate(data)
                expected = f"{prefix}/run_id={day.run_id}/source_date={day.source_date}/day.json"
                if key != expected or day.source != identity.source or day.resource != identity.resource:
                    raise ValueError("Day manifest lineage mismatch")
                return day

            by_date = defaultdict(list)
            for day in pool.map(read_day, day_keys):
                if day is not None:
                    by_date[day.source_date].append(day)
            selected = {str(day): chosen for day, attempts in by_date.items()
                        if (chosen := effective_attempt(attempts)) is not None}
            if committed_days is not None:
                committed_days[identity.resource] = selected
            found = Counter()
            date_pattern = "*" if year is None else f"{year}-*"

            def table_files(table, date_pattern=date_pattern):
                dataset = f"/{table.dataset}" if table.dataset else ""
                pattern = (f"{bucket}/bronze{dataset}/{table.table}/"
                           f"source_date={date_pattern}/run_id=*/*.parquet")
                if file_metadata is not None:
                    details = fs.glob(pattern, detail=True)
                    return sorted(details), details
                return sorted(fs.glob(pattern)), {}

            for table, (keys, details) in zip(resource_tables, pool.map(table_files, resource_tables), strict=True):
                for key in keys:
                    parts = key.rsplit("/", 3)
                    day = selected.get(parts[-3].removeprefix("source_date="))
                    if day is not None and parts[-2] == f"run_id={day.run_id}" and day.bronze_records:
                        files[table].append(f"s3://{key}")
                        if file_metadata is not None:
                            file_metadata[f"s3://{key}"] = details[key]
                        found[str(day.source_date)] += 1
            for day, manifest in selected.items():
                if manifest.bronze_records and not found[day]:
                    issues.append({"resource": identity.resource, "source_date": day,
                                   "run_id": manifest.run_id, "expected_records": manifest.bronze_records,
                                   "issue": "committed_files_missing"})
                    print(f"WARNING: {identity.resource}/{day}: missing committed files "
                          f"(run={manifest.run_id}, expected={manifest.bronze_records}); "
                          "this day is absent from query results.", flush=True)
            print(f"{identity.resource}: {len(selected)} committed days, {sum(found.values())} files", flush=True)
    return files


def _create_selection_issues(con, issues):
    """Expose incomplete selection in the UI without substituting historical data."""
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze_meta")
    con.execute("""CREATE OR REPLACE TABLE bronze_meta.selection_issues (
        resource VARCHAR, source_date DATE, run_id VARCHAR, expected_records BIGINT, issue VARCHAR
    )""")
    if issues:
        con.executemany("INSERT INTO bronze_meta.selection_issues VALUES (?, ?, ?, ?, ?)",
                        [(row["resource"], row["source_date"], row["run_id"],
                          row["expected_records"], row["issue"]) for row in issues])


def _create_bronze_views(
    con: duckdb.DuckDBPyConnection,
    *,
    bucket: str,
    tables: Iterable[BronzeTable],
    files_by_table: dict[BronzeTable, list[str]] | None = None,
    schema: str = BRONZE_SCHEMA,
    union_by_name: bool = False,
) -> list[str]:
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_identifier(schema)}")
    table_list = list(tables)
    name_counts = Counter(item.table for item in table_list)
    created: list[str] = []

    for item in table_list:
        if name_counts[item.table] == 1:
            view_name = item.table
        else:
            dataset = item.dataset or "root"
            view_name = f"{dataset}__{item.table}"

        qualified_name = (
            f"{_quote_identifier(schema)}.{_quote_identifier(view_name)}"
        )
        paths = None if files_by_table is None else files_by_table[item]
        if paths == []:
            con.execute(f"""CREATE OR REPLACE VIEW {qualified_name} AS SELECT
                NULL::VARCHAR source_id, NULL::VARCHAR source_version, NULL::VARCHAR run_id,
                NULL::DATE source_date, NULL::TIMESTAMPTZ ingested_at,
                NULL::VARCHAR content_hash, NULL::VARCHAR payload, NULL::VARCHAR filename
                WHERE false""")
            created.append(view_name)
            continue
        source = (_sql_literal(item.parquet_glob(bucket)) if paths is None else
                  "[" + ",".join(_sql_literal(path) for path in paths) + "]")
        con.execute(
            f"""
            CREATE OR REPLACE VIEW {qualified_name} AS
            SELECT *
            FROM read_parquet(
                {source},
                hive_partitioning = true,
                union_by_name = {str(union_by_name).lower()},
                filename = true
            )
            """
        )
        created.append(view_name)

    return created


def _create_notify_view(con, created):
    tables = [table for table in created if table.startswith("notify_contractor_") and table.endswith("_detail")]
    if tables:
        branches = [f"SELECT *, {_sql_literal(table)} AS detail_table FROM "
                    f"{BRONZE_SCHEMA}.{_quote_identifier(table)}" for table in tables]
        con.execute(f"CREATE OR REPLACE VIEW {BRONZE_SCHEMA}.notify_contractor AS "
                    + " UNION ALL BY NAME ".join(branches))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Open a local DuckDB UI for browsing raw Bronze parquet data"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_UI_PORT,
        help=f"DuckDB UI port (default: {DEFAULT_UI_PORT})",
    )
    parser.add_argument("--year", type=int, help="Only open partitions from this year")
    parser.add_argument("--resource", choices=SUPPORTED_RESOURCES, action="append",
                        help="Only open this resource (repeatable)")
    parser.add_argument("--history", action="store_true",
                        help="Also open all attempts in bronze_history (slower)")
    parser.add_argument("--union-by-name", action="store_true",
                        help="Inspect every Parquet schema for legacy schema differences (slower)")
    return parser


def open_bronze_explorer(*, port: int = DEFAULT_UI_PORT, year=None, resources=None,
                        history=False, union_by_name=False) -> None:
    if not 1 <= port <= 65535:
        raise ValueError("port must be between 1 and 65535")
    if year is not None and not 1 <= year <= 9999:
        raise ValueError("year must be between 1 and 9999")

    started = time.perf_counter()
    print("Selecting committed Bronze files...", flush=True)
    fs = create_s3_filesystem()
    tables = _discover_bronze_tables(fs, settings.OBJECT_STORAGE_BUCKET)
    definitions = [d for d in RESOURCE_CATALOG if resources is None or d.identity.resource in resources]
    tables = [t for t in tables if any(t.table in d.tables and t.dataset in (None, d.identity.source)
                                     for d in definitions)]
    if not tables:
        raise RuntimeError(
            f"No Bronze tables found in s3://{settings.OBJECT_STORAGE_BUCKET}/bronze"
        )
    issues = []
    selected = _select_current_files(fs, bucket=settings.OBJECT_STORAGE_BUCKET, tables=tables,
                                     year=year, issues=issues)

    con = duckdb.connect()
    try:
        _configure_s3_secret(con)
        _create_selection_issues(con, issues)
        created = _create_bronze_views(
            con,
            bucket=settings.OBJECT_STORAGE_BUCKET,
            tables=tables,
            files_by_table=selected,
            union_by_name=union_by_name,
        )
        _create_notify_view(con, created)
        if history:
            _create_bronze_views(con, bucket=settings.OBJECT_STORAGE_BUCKET, tables=tables,
                                 schema="bronze_history", union_by_name=True)

        _load_extension(con, "ui")
        con.execute(f"SET ui_local_port = {port}")
        con.execute("CALL start_ui()")

        print(f"Bronze explorer is running ({time.perf_counter() - started:.1f}s startup).")
        print(f"UI: http://localhost:{port}")
        print(f"Schema: {BRONZE_SCHEMA}")
        print("Views use a fixed committed snapshot. Restart after repair to refresh.")
        if issues:
            print(f"INCOMPLETE: {len(issues)} day(s) missing files. "
                  "See SELECT * FROM bronze_meta.selection_issues;")
        print("Use --union-by-name if legacy Parquet files have different schemas.")
        print("Views:")
        for table in created:
            print(f"  - {BRONZE_SCHEMA}.{table}")
        print("Press Ctrl+C to stop.")

        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        print("\nStopping Bronze explorer.")
    finally:
        try:
            con.execute("CALL stop_ui_server()")
        except duckdb.Error:
            pass
        con.close()


def main() -> None:
    args = _build_parser().parse_args()
    open_bronze_explorer(port=args.port, year=args.year, resources=args.resource,
                        history=args.history, union_by_name=args.union_by_name)


if __name__ == "__main__":
    main()
