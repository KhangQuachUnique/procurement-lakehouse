from __future__ import annotations

import argparse
import time
from collections.abc import Iterable
from urllib.parse import urlparse

import duckdb
import s3fs

from procurement.common.settings import settings
from procurement.storage.object_store import create_s3_filesystem

BRONZE_SCHEMA = "bronze_raw"
SECRET_NAME = "bronze_store"
DEFAULT_UI_PORT = 4213


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


def _discover_bronze_tables(
    fs: s3fs.S3FileSystem,
    bucket: str,
) -> list[str]:
    prefix = f"{bucket}/bronze"
    try:
        entries = fs.ls(prefix, detail=True)
    except FileNotFoundError:
        return []

    tables: set[str] = set()
    for entry in entries:
        if isinstance(entry, str):
            name = entry
            entry_type = None
        else:
            name = str(entry.get("name", ""))
            entry_type = entry.get("type")

        if entry_type not in {None, "directory", "dir"}:
            continue

        table = name.rstrip("/").rsplit("/", maxsplit=1)[-1]
        if table:
            tables.add(table)

    return sorted(tables)


def _configure_s3_secret(con: duckdb.DuckDBPyConnection) -> None:
    access_key = settings.OBJECT_STORAGE_ACCESS_KEY
    secret_key = settings.OBJECT_STORAGE_SECRET_KEY
    bucket = settings.OBJECT_STORAGE_BUCKET

    if not access_key or not secret_key:
        raise RuntimeError(
            "OBJECT_STORAGE_ACCESS_KEY and OBJECT_STORAGE_SECRET_KEY are required"
        )

    endpoint, use_ssl = _parse_endpoint(settings.OBJECT_STORAGE_ENDPOINT)
    con.install_extension("httpfs")
    con.load_extension("httpfs")
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


def _create_bronze_views(
    con: duckdb.DuckDBPyConnection,
    *,
    bucket: str,
    tables: Iterable[str],
) -> list[str]:
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_identifier(BRONZE_SCHEMA)}")
    created: list[str] = []

    for table in tables:
        parquet_glob = f"s3://{bucket}/bronze/{table}/**/*.parquet"
        qualified_name = (
            f"{_quote_identifier(BRONZE_SCHEMA)}.{_quote_identifier(table)}"
        )
        con.execute(
            f"""
            CREATE OR REPLACE VIEW {qualified_name} AS
            SELECT *
            FROM read_parquet(
                {_sql_literal(parquet_glob)},
                hive_partitioning = true,
                union_by_name = true,
                filename = true
            )
            """
        )
        created.append(table)

    return created


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
    return parser


def open_bronze_explorer(*, port: int = DEFAULT_UI_PORT) -> None:
    if not 1 <= port <= 65535:
        raise ValueError("port must be between 1 and 65535")

    fs = create_s3_filesystem()
    tables = _discover_bronze_tables(fs, settings.OBJECT_STORAGE_BUCKET)
    if not tables:
        raise RuntimeError(
            f"No Bronze tables found in s3://{settings.OBJECT_STORAGE_BUCKET}/bronze"
        )

    con = duckdb.connect()
    try:
        _configure_s3_secret(con)
        created = _create_bronze_views(
            con,
            bucket=settings.OBJECT_STORAGE_BUCKET,
            tables=tables,
        )

        con.install_extension("ui")
        con.load_extension("ui")
        con.execute(f"SET ui_local_port = {port}")
        con.execute("CALL start_ui()")

        print("Bronze explorer is running.")
        print(f"UI: http://localhost:{port}")
        print(f"Schema: {BRONZE_SCHEMA}")
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
    open_bronze_explorer(port=args.port)


if __name__ == "__main__":
    main()
