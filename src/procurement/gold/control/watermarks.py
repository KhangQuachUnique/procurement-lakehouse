"""PostgreSQL ``gold_spark_meta.runs`` store for Gold run receipts & Silver snapshot watermarks."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from typing import Any

from sqlalchemy import Engine, text


class GoldWatermarkStore:
    """Persists Gold run metadata and the pinned Silver Iceberg snapshot_ids per run."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def ensure_schema(self) -> None:
        with self._engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold_spark_meta"))
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS gold_spark_meta.runs (
                        run_id TEXT PRIMARY KEY,
                        gold_namespace TEXT NOT NULL,
                        core_namespace TEXT,
                        silver_namespace TEXT NOT NULL,
                        mode TEXT NOT NULL,
                        event_start DATE NOT NULL,
                        event_end DATE NOT NULL,
                        observation_day DATE NOT NULL,
                        status TEXT NOT NULL,
                        silver_snapshots_json TEXT,
                        table_counts_json TEXT,
                        wall_s DOUBLE PRECISION,
                        error_message TEXT,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
            )

    def record_run(
        self,
        *,
        run_id: str,
        gold_namespace: str,
        core_namespace: str | None,
        silver_namespace: str,
        mode: str,
        event_start: date,
        event_end: date,
        observation_day: date,
        status: str,
        silver_snapshots: dict[str, int | None],
        table_counts: dict[str, int] | None = None,
        wall_s: float | None = None,
        error_message: str | None = None,
    ) -> None:
        now = datetime.now(UTC)
        with self._engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO gold_spark_meta.runs (
                        run_id, gold_namespace, core_namespace, silver_namespace, mode,
                        event_start, event_end, observation_day, status,
                        silver_snapshots_json, table_counts_json, wall_s, error_message,
                        created_at, updated_at
                    ) VALUES (
                        :run_id, :gold_namespace, :core_namespace, :silver_namespace, :mode,
                        :event_start, :event_end, :observation_day, :status,
                        :silver_snapshots_json, :table_counts_json, :wall_s, :error_message,
                        :now, :now
                    )
                    ON CONFLICT (run_id) DO UPDATE SET
                        status = EXCLUDED.status,
                        silver_snapshots_json = EXCLUDED.silver_snapshots_json,
                        table_counts_json = EXCLUDED.table_counts_json,
                        wall_s = EXCLUDED.wall_s,
                        error_message = EXCLUDED.error_message,
                        updated_at = EXCLUDED.updated_at
                    """
                ),
                {
                    "run_id": run_id,
                    "gold_namespace": gold_namespace,
                    "core_namespace": core_namespace,
                    "silver_namespace": silver_namespace,
                    "mode": mode,
                    "event_start": event_start,
                    "event_end": event_end,
                    "observation_day": observation_day,
                    "status": status,
                    "silver_snapshots_json": json.dumps(silver_snapshots),
                    "table_counts_json": json.dumps(table_counts or {}),
                    "wall_s": wall_s,
                    "error_message": error_message,
                    "now": now,
                },
            )

    def get_latest_published_watermarks(
        self,
        *,
        gold_namespace: str,
        silver_namespace: str,
    ) -> dict[str, Any] | None:
        """Return the latest PUBLISHED run's Silver snapshot watermarks (for incremental runs)."""
        with self._engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT run_id, event_start, event_end, observation_day, silver_snapshots_json
                    FROM gold_spark_meta.runs
                    WHERE gold_namespace = :gold_namespace
                      AND silver_namespace = :silver_namespace
                      AND status = 'PUBLISHED'
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                ),
                {"gold_namespace": gold_namespace, "silver_namespace": silver_namespace},
            ).fetchone()
        if not row:
            return None
        return {
            "run_id": row[0],
            "event_start": row[1],
            "event_end": row[2],
            "observation_day": row[3],
            "silver_snapshots": json.loads(row[4]) if row[4] else {},
        }
