"""Durable PostgreSQL receipt tracking for Spark Silver (per resource, day, transform_version)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import Engine, text


@dataclass(frozen=True)
class SilverReceipt:
    resource: str
    source_date: date
    namespace: str
    input_commit_id: str
    transform_version: str
    batch_id: str
    status: str  # PENDING | RUNNING | COMMITTING | VERIFYING | PUBLISHED | FAILED
    snapshot_map: dict[str, int]
    metrics: dict[str, Any]
    error_message: str | None = None


class ReceiptStore:
    """Manages silver_spark_meta.receipts in PostgreSQL for idempotent incremental & backfill runs."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def ensure_schema(self) -> None:
        with self._engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver_spark_meta"))
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS silver_spark_meta.receipts (
                        namespace VARCHAR(128) NOT NULL,
                        resource VARCHAR(64) NOT NULL,
                        source_date DATE NOT NULL,
                        input_commit_id VARCHAR(128) NOT NULL,
                        transform_version VARCHAR(64) NOT NULL,
                        batch_id VARCHAR(128) NOT NULL,
                        status VARCHAR(32) NOT NULL,
                        snapshot_map_json TEXT NOT NULL DEFAULT '{}',
                        metrics_json TEXT NOT NULL DEFAULT '{}',
                        error_message TEXT,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (namespace, resource, source_date)
                    )
                    """
                )
            )

    def list_published(
        self,
        namespace: str,
        resource: str,
        start_date: date,
        end_date: date,
    ) -> dict[date, SilverReceipt]:
        query = text(
            """
            SELECT source_date, input_commit_id, transform_version, batch_id, status,
                   snapshot_map_json, metrics_json, error_message
            FROM silver_spark_meta.receipts
            WHERE namespace = :ns
              AND resource = :resource
              AND source_date BETWEEN :start_date AND :end_date
              AND status = 'PUBLISHED'
            """
        )
        with self._engine.connect() as conn:
            rows = conn.execute(
                query,
                {
                    "ns": namespace,
                    "resource": resource,
                    "start_date": start_date,
                    "end_date": end_date,
                },
            ).fetchall()

        return {
            r[0]: SilverReceipt(
                resource=resource,
                source_date=r[0],
                namespace=namespace,
                input_commit_id=r[1],
                transform_version=r[2],
                batch_id=r[3],
                status=r[4],
                snapshot_map=json.loads(r[5] or "{}"),
                metrics=json.loads(r[6] or "{}"),
                error_message=r[7],
            )
            for r in rows
        }

    def upsert_many(self, receipts: list[SilverReceipt]) -> None:
        if not receipts:
            return
        now = datetime.now(timezone.utc)
        stmt = text(
            """
            INSERT INTO silver_spark_meta.receipts (
                namespace, resource, source_date, input_commit_id, transform_version,
                batch_id, status, snapshot_map_json, metrics_json, error_message, updated_at
            ) VALUES (
                :namespace, :resource, :source_date, :input_commit_id, :transform_version,
                :batch_id, :status, :snapshot_map_json, :metrics_json, :error_message, :updated_at
            )
            ON CONFLICT (namespace, resource, source_date) DO UPDATE SET
                input_commit_id = EXCLUDED.input_commit_id,
                transform_version = EXCLUDED.transform_version,
                batch_id = EXCLUDED.batch_id,
                status = EXCLUDED.status,
                snapshot_map_json = EXCLUDED.snapshot_map_json,
                metrics_json = EXCLUDED.metrics_json,
                error_message = EXCLUDED.error_message,
                updated_at = EXCLUDED.updated_at
            """
        )
        payload = [
            {
                "namespace": r.namespace,
                "resource": r.resource,
                "source_date": r.source_date,
                "input_commit_id": r.input_commit_id,
                "transform_version": r.transform_version,
                "batch_id": r.batch_id,
                "status": r.status,
                "snapshot_map_json": json.dumps(r.snapshot_map, sort_keys=True),
                "metrics_json": json.dumps(r.metrics, sort_keys=True),
                "error_message": r.error_message,
                "updated_at": now,
            }
            for r in receipts
        ]
        with self._engine.begin() as conn:
            conn.execute(stmt, payload)
