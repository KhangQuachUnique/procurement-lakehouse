"""Resolve pinned, published Bronze manifests directly from PostgreSQL bronze_meta."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from sqlalchemy import Engine, text


@dataclass(frozen=True)
class BronzeFileSpec:
    """One committed Parquet file in SeaweedFS S3."""

    table_name: str
    bucket: str
    object_key: str
    row_count: int
    sha256: str | None

    @property
    def s3a_uri(self) -> str:
        clean_key = self.object_key.lstrip("/")
        return f"s3a://{self.bucket}/{clean_key}"


@dataclass(frozen=True)
class BronzeDayManifest:
    """Immutable pinned Bronze input for a single (resource, source_date)."""

    resource: str
    source_date: date
    commit_id: str
    attempt_id: str
    expected_records: int
    files: tuple[BronzeFileSpec, ...]

    @property
    def is_empty(self) -> bool:
        return self.expected_records == 0 or not self.files


def resolve_bronze_manifests(
    engine: Engine,
    resource: str,
    start_date: date,
    end_date: date,
    *,
    table_names: tuple[str, ...] | None = None,
) -> dict[date, BronzeDayManifest]:
    """Batch-query latest published Bronze commits for a resource across [start_date, end_date]."""
    query = text(
        """
        SELECT
            p.source_date,
            CAST(p.current_commit_id AS TEXT) AS commit_id,
            CAST(c.attempt_id AS TEXT) AS attempt_id,
            COALESCE(c.record_count, 0) AS expected_records,
            cf.table_name,
            cf.bucket,
            cf.object_key,
            COALESCE(cf.row_count, 0) AS row_count,
            cf.sha256
        FROM bronze_meta.partitions p
        JOIN bronze_meta.commits c ON c.id = p.current_commit_id
        LEFT JOIN bronze_meta.commit_files cf ON cf.commit_id = p.current_commit_id
        WHERE p.resource = :resource
          AND p.source_date BETWEEN :start_date AND :end_date
          AND p.current_commit_id IS NOT NULL
        ORDER BY p.source_date, cf.table_name, cf.object_key
        """
    )
    grouped: dict[date, tuple[str, str, int, list[BronzeFileSpec]]] = {}
    with engine.connect() as conn:
        rows = conn.execute(
            query,
            {"resource": resource, "start_date": start_date, "end_date": end_date},
        ).fetchall()

    for row in rows:
        src_date: date = row[0]
        commit_id: str = row[1]
        attempt_id: str = row[2]
        expected_records: int = int(row[3])
        tbl_name: str | None = row[4]
        bucket: str | None = row[5]
        obj_key: str | None = row[6]
        row_count: int = int(row[7])
        sha256: str | None = row[8]

        if src_date not in grouped:
            grouped[src_date] = (commit_id, attempt_id, expected_records, [])

        if tbl_name and bucket and obj_key:
            if table_names is None or tbl_name in table_names:
                grouped[src_date][3].append(
                    BronzeFileSpec(
                        table_name=tbl_name,
                        bucket=bucket,
                        object_key=obj_key,
                        row_count=row_count,
                        sha256=sha256,
                    )
                )

    return {
        d: BronzeDayManifest(
            resource=resource,
            source_date=d,
            commit_id=cid,
            attempt_id=aid,
            expected_records=exp,
            files=tuple(flist),
        )
        for d, (cid, aid, exp, flist) in sorted(grouped.items())
    }
