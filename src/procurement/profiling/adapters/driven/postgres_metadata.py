"""Driven adapter implementing ActiveBronzeMetadataPort via PostgreSQL bronze_meta schema."""

from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from procurement.infrastructure.database import create_application_engine
from procurement.profiling.ports.outbound.bronze_metadata_port import (
    ActiveBronzeMetadataPort,
    ActiveCommitFileDescriptor,
)


class PostgresActiveBronzeAdapter(ActiveBronzeMetadataPort):
    """Queries PostgreSQL `bronze_meta` schema for active commits and partitions."""

    def __init__(self, engine: Engine | None = None) -> None:
        self.engine = engine or create_application_engine()

    def get_active_summary(self) -> list[dict[str, Any]]:
        query = text("""
            SELECT 
                p.source,
                p.resource,
                COUNT(DISTINCT p.id) AS total_partitions,
                COUNT(DISTINCT p.current_commit_id) AS active_commits,
                MIN(p.source_date) AS min_date,
                MAX(p.source_date) AS max_date,
                COALESCE(SUM(cf.row_count), 0) AS total_rows,
                COALESCE(SUM(cf.size_bytes), 0) AS total_bytes,
                COUNT(cf.object_key) AS total_files
            FROM bronze_meta.partitions p
            JOIN bronze_meta.commits c ON p.current_commit_id = c.id
            LEFT JOIN bronze_meta.commit_files cf ON c.id = cf.commit_id
            GROUP BY p.source, p.resource
            ORDER BY p.source, p.resource;
        """)
        with self.engine.connect() as conn:
            rows = conn.execute(query).mappings().all()
            return [dict(r) for r in rows]

    def get_active_tables(self) -> list[dict[str, Any]]:
        query = text("""
            SELECT 
                p.resource,
                cf.table_name,
                COUNT(DISTINCT p.id) AS partition_count,
                COUNT(cf.object_key) AS file_count,
                COALESCE(SUM(cf.row_count), 0) AS total_rows,
                COALESCE(SUM(cf.size_bytes), 0) AS total_bytes,
                MIN(p.source_date) AS min_date,
                MAX(p.source_date) AS max_date
            FROM bronze_meta.partitions p
            JOIN bronze_meta.commits c ON p.current_commit_id = c.id
            JOIN bronze_meta.commit_files cf ON c.id = cf.commit_id
            GROUP BY p.resource, cf.table_name
            ORDER BY total_rows DESC;
        """)
        with self.engine.connect() as conn:
            rows = conn.execute(query).mappings().all()
            return [dict(r) for r in rows]

    def get_active_files(
        self,
        *,
        resource: str | None = None,
        table_name: str | None = None,
        start_date: date | str | None = None,
        end_date: date | str | None = None,
        max_files: int | None = None,
    ) -> list[ActiveCommitFileDescriptor]:
        conditions = ["p.current_commit_id = c.id", "c.id = cf.commit_id"]
        params: dict[str, Any] = {}

        if resource:
            conditions.append("p.resource = :resource")
            params["resource"] = resource
        if table_name:
            conditions.append("cf.table_name = :table_name")
            params["table_name"] = table_name
        if start_date:
            conditions.append("p.source_date >= :start_date")
            params["start_date"] = str(start_date)
        if end_date:
            conditions.append("p.source_date <= :end_date")
            params["end_date"] = str(end_date)

        where_clause = " AND ".join(conditions)
        limit_clause = f"LIMIT {int(max_files)}" if max_files else ""

        query = text(f"""
            SELECT 
                c.id AS commit_id,
                p.id AS partition_id,
                p.source,
                p.resource,
                p.source_date,
                cf.table_name,
                cf.bucket,
                cf.object_key,
                cf.file_number,
                cf.row_count,
                cf.size_bytes,
                cf.sha256
            FROM bronze_meta.partitions p
            JOIN bronze_meta.commits c ON p.current_commit_id = c.id
            JOIN bronze_meta.commit_files cf ON c.id = cf.commit_id
            WHERE {where_clause}
            ORDER BY p.source_date ASC, cf.file_number ASC
            {limit_clause};
        """)

        with self.engine.connect() as conn:
            rows = conn.execute(query, params).mappings().all()
            return [
                ActiveCommitFileDescriptor(
                    commit_id=str(r["commit_id"]),
                    partition_id=str(r["partition_id"]),
                    source=r["source"],
                    resource=r["resource"],
                    source_date=r["source_date"],
                    table_name=r["table_name"],
                    bucket=r["bucket"],
                    object_key=r["object_key"],
                    file_number=r["file_number"],
                    row_count=r["row_count"],
                    size_bytes=r["size_bytes"],
                    sha256=r["sha256"],
                )
                for r in rows
            ]
