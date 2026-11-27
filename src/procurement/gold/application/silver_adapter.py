"""Compatibility adapter between legacy Silver (silver_v3) and Spark Silver (silver_spark).

Legacy Silver exposes an `observation` table and `observation_id`/`is_current` columns on
root revision tables. Spark Silver stores `source_date`, `ingested_at`, `run_id`,
`content_hash`, and `source_version` directly on each revision table without an
`observation` table or `linkage_index`.

This module keeps both layouts working seamlessly without modifying `silver_v3` behavior.
"""

from collections.abc import Sequence

from procurement.common.silver_gold_contract import INPUT_TABLES, projection
from procurement.gold.domain.models import identifier

ROOT_REVISION_TABLES: tuple[str, ...] = (
    "package_revision",
    "notice_revision",
    "result_revision",
    "plan_revision",
    "opening_revision",
    "contract_revision",
    "contract_performance_revision",
)

NON_OBSERVATION_INPUT_TABLES: tuple[str, ...] = tuple(
    t for t in INPUT_TABLES if t != "observation"
)

_SOURCE_ID_CANDIDATES: tuple[str, ...] = (
    "source_id",
    "source_document_id",
    "result_id",
    "source_contract_id",
    "source_evaluation_id",
)


def adapt_projection(columns: Sequence[str], table_name: str) -> list[str]:
    """Build Gold-compatible SELECT list for either legacy Silver or Spark Silver."""
    selected = list(projection(columns, table_name))
    col_set = set(columns)
    if table_name in ROOT_REVISION_TABLES and "observation_id" not in col_set:
        selected.append(f"'{table_name}:' || revision_id AS observation_id")
    # Defensive FK aliases in case an older Spark Silver table still uses source_*_id.
    if table_name == "package_revision" and "plan_id" not in col_set and "source_plan_id" in col_set:
        selected.append("source_plan_id AS plan_id")
    if table_name == "plan_revision" and "project_id" not in col_set and "source_project_id" in col_set:
        selected.append("source_project_id AS project_id")
    return selected


def table_exists(db, schema: str, table_name: str, catalog: str | None = None) -> bool:
    """Check whether a table or view exists in the given DuckDB catalog/schema."""
    if catalog is None:
        row = db.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_catalog = current_database() AND table_schema = ? AND table_name = ?",
            [schema, table_name],
        ).fetchone()
    else:
        row = db.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_catalog = ? AND table_schema = ? AND table_name = ?",
            [catalog, schema, table_name],
        ).fetchone()
    return bool(row and row[0] > 0)


def ensure_observation_table(
    db,
    schema: str = "frozen",
    *,
    fallback_date: str | None = None,
) -> None:
    """Create or backfill `{schema}.observation` from root revision tables.

    For legacy Silver (`silver_v3`), fills any missing observation rows for upstream
    revisions pulled outside the active window. For Spark Silver (`silver_spark`),
    synthesizes the full `{schema}.observation` table from revision metadata columns
    (`source_date`, `ingested_at`, `run_id`, `source_version`, `content_hash`).
    """
    identifier(schema)
    if not table_exists(db, schema, "observation"):
        db.execute(
            f"""
            CREATE TABLE {schema}.observation (
                observation_id VARCHAR,
                resource VARCHAR,
                source_date DATE,
                run_id VARCHAR,
                table_name VARCHAR,
                file_key VARCHAR,
                file_sha256 VARCHAR,
                row_ordinal BIGINT,
                observed_at TIMESTAMPTZ,
                source_id VARCHAR,
                source_version VARCHAR,
                content_hash VARCHAR
            )
            """
        )

    fallback_date_sql = f"DATE '{fallback_date}'" if fallback_date else "DATE '1970-01-01'"
    fallback_ts_sql = (
        f"CAST(DATE '{fallback_date}' AS TIMESTAMPTZ)"
        if fallback_date
        else "TIMESTAMPTZ '1970-01-01 00:00:00+00'"
    )

    for tbl in ROOT_REVISION_TABLES:
        if not table_exists(db, schema, tbl):
            continue
        cols = {r[0] for r in db.execute(f"DESCRIBE {schema}.{tbl}").fetchall()}
        obs_id_expr = (
            "r.observation_id" if "observation_id" in cols else f"'{tbl}:' || r.revision_id"
        )
        if "source_date" in cols and "public_date" in cols:
            source_date_expr = f"COALESCE(r.source_date, r.public_date, {fallback_date_sql})"
        elif "source_date" in cols:
            source_date_expr = f"COALESCE(r.source_date, {fallback_date_sql})"
        elif "public_date" in cols:
            source_date_expr = f"COALESCE(r.public_date, {fallback_date_sql})"
        else:
            source_date_expr = fallback_date_sql

        ts_parts = []
        if "ingested_at" in cols:
            ts_parts.append("CAST(r.ingested_at AS TIMESTAMPTZ)")
        if "source_date" in cols:
            ts_parts.append("CAST(r.source_date AS TIMESTAMPTZ)")
        if "public_date" in cols:
            ts_parts.append("CAST(r.public_date AS TIMESTAMPTZ)")
        ts_parts.append(fallback_ts_sql)
        observed_at_expr = f"COALESCE({', '.join(ts_parts)})"

        run_id_expr = "COALESCE(r.run_id, 'spark_silver')" if "run_id" in cols else "'range_run'"

        src_id_col = next((c for c in _SOURCE_ID_CANDIDATES if c in cols), None)
        if src_id_col and "entity_id" in cols:
            source_id_expr = f"COALESCE(r.{src_id_col}, r.entity_id)"
        elif src_id_col:
            source_id_expr = f"r.{src_id_col}"
        elif "entity_id" in cols:
            source_id_expr = "r.entity_id"
        else:
            source_id_expr = "r.revision_id"

        source_version_expr = (
            "COALESCE(CAST(r.source_version AS VARCHAR), '1')"
            if "source_version" in cols
            else "'1'"
        )
        content_hash_expr = (
            "COALESCE(r.content_hash, 'h')" if "content_hash" in cols else "'h'"
        )

        db.execute(
            f"""
            INSERT INTO {schema}.observation (
                observation_id, resource, source_date, run_id, table_name,
                file_key, file_sha256, row_ordinal, observed_at, source_id, source_version, content_hash
            )
            SELECT DISTINCT
                {obs_id_expr},
                '{tbl}',
                {source_date_expr},
                {run_id_expr},
                '{tbl}',
                'k',
                's',
                0,
                {observed_at_expr},
                {source_id_expr},
                {source_version_expr},
                {content_hash_expr}
            FROM {schema}.{tbl} r
            WHERE {obs_id_expr} IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM {schema}.observation o WHERE o.observation_id = {obs_id_expr}
              )
            """
        )

    db.execute(
        f"""
        CREATE OR REPLACE TABLE {schema}.observation AS
        SELECT * EXCLUDE (rn) FROM (
            SELECT *, row_number() OVER (PARTITION BY observation_id ORDER BY observed_at DESC NULLS LAST) AS rn
            FROM {schema}.observation
        ) WHERE rn = 1
        """
    )


def prepare_business_source(
    db,
    source: str,
    *,
    adapted_schema: str = "gold_spark_source",
) -> str:
    """Return a schema exposing the full Gold INPUT_TABLES contract.

    If `{source}.observation` already exists (legacy Silver or pre-adapted `frozen`),
    returns `source` directly with zero overhead. Otherwise synthesizes projected views
    and `{adapted_schema}.observation` over the Spark Silver tables in `{source}`.
    """
    identifier(source)
    if table_exists(db, source, "observation"):
        return source

    for name in NON_OBSERVATION_INPUT_TABLES:
        db.execute(f"SELECT * FROM {source}.{name} LIMIT 0")

    db.execute(f"DROP SCHEMA IF EXISTS {adapted_schema} CASCADE")
    db.execute(f"CREATE SCHEMA {adapted_schema}")

    tables_to_view = list(NON_OBSERVATION_INPUT_TABLES)
    if table_exists(db, source, "notice_location"):
        tables_to_view.append("notice_location")

    for name in tables_to_view:
        cols = [r[0] for r in db.execute(f"DESCRIBE {source}.{name}").fetchall()]
        select_cols = ", ".join(adapt_projection(cols, name))
        db.execute(f"CREATE VIEW {adapted_schema}.{name} AS SELECT {select_cols} FROM {source}.{name}")

    ensure_observation_table(db, schema=adapted_schema)
    return adapted_schema
