"""Two-Tier Spark Gold runner (Full WAP Rebuild + Tier 1 Core persistence + Silver watermarks)."""

from __future__ import annotations

import gc
import time
import uuid
from datetime import date
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, functions as F

from procurement_spark.gold.control import GoldWatermarkStore
from procurement_spark.gold.core.award import build_award
from procurement_spark.gold.core.competition import build_competition
from procurement_spark.gold.core.demand import build_demand
from procurement_spark.gold.core.dimensions_base import GoldBuildConfig, build_base_dimensions
from procurement_spark.gold.core.dimensions_biz import build_business_dimensions
from procurement_spark.gold.core.duration import build_duration
from procurement_spark.gold.core.notice import build_notice
from procurement_spark.gold.core.savings import build_savings
from procurement_spark.gold.core.selection import build_selection_views
from procurement_spark.gold.functions import register_gold_functions
from procurement_spark.gold.marts.dimensions import build_public_dimensions
from procurement_spark.gold.marts.facts import build_public_facts
from procurement_spark.gold.quality import run_gold_quality_gate
from procurement_spark.gold.schema import CORE_MERGE_KEYS, GOLD_CORE_DDLS, GOLD_MART_DDLS
from procurement_spark.gold.source import (
    load_silver_incremental_deltas,
    load_silver_views,
    read_silver_snapshot_ids,
)
from procurement_spark.platform.config import SparkSilverSettings
from procurement_spark.platform.iceberg_writer import (
    abort_wap_branch,
    merge_into_table,
    publish_wap_branch,
)


def _ensure_gold_namespace_and_tables(
    spark: SparkSession,
    *,
    catalog: str,
    namespace: str,
    full_ddls: dict[str, str],
) -> None:
    """Create Iceberg namespace and tables using complete Gold DDL templates."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")
    for table_name, ddl_template in full_ddls.items():
        full_name = f"{catalog}.{namespace}.{table_name}"
        spark.sql(ddl_template.format(full_name=full_name))
from procurement_spark.platform.spark_session import build_spark_session
from procurement_spark.silver.runner import _create_pg_engine


def _stage_full_replace_wap(
    spark: SparkSession,
    *,
    catalog: str,
    namespace: str,
    branch_name: str,
    table_dfs: dict[str, DataFrame],
) -> None:
    """Create WAP branch per table and atomically overwrite all rows on that branch."""
    for table_name in table_dfs:
        full_name = f"{catalog}.{namespace}.{table_name}"
        spark.sql(f"ALTER TABLE {full_name} CREATE OR REPLACE BRANCH `{branch_name}`")

    prev_branch = spark.conf.get("spark.wap.branch", None)
    try:
        spark.conf.set("spark.wap.branch", branch_name)
        for table_name, df in table_dfs.items():
            full_name = f"{catalog}.{namespace}.{table_name}"
            df.writeTo(full_name).overwrite(F.lit(True))
    finally:
        if prev_branch is not None:
            spark.conf.set("spark.wap.branch", prev_branch)
        else:
            spark.conf.unset("spark.wap.branch")


def run_spark_gold_rebuild(
    *,
    event_start: date = date(2022, 1, 1),
    event_end: date = date(2026, 10, 31),
    observation_day: date | None = None,
    silver_namespace: str | None = None,
    gold_namespace: str = "gold_spark",
    core_namespace: str | None = "gold_spark_core",
    persist_core: bool = True,
    settings: SparkSilverSettings | None = None,
    spark: SparkSession | None = None,
) -> dict[str, Any]:
    """Full Two-Tier Gold rebuild on Spark 4.0 + Iceberg WAP.

    1. Pins all 12 Silver tables to their current ``main`` Iceberg snapshot_ids.
    2. Computes Tier 1 (Gold Core atomic entity tables) and optionally publishes them
       to ``lake.<core_namespace>.*`` via WAP for BI drill-down and future MERGE INTO.
    3. Aggregates Tier 2 (17 public dim_* & fact_*_daily) into WAP branch of
       ``lake.<gold_namespace>.*``, runs pre-publish Quality Gate, and fast-forwards.
    4. Records the run & Silver snapshot watermarks in PostgreSQL ``gold_spark_meta.runs``.
    """
    cfg = settings or SparkSilverSettings.from_environment(load_dotenv_file=True)
    src_ns = silver_namespace or cfg.iceberg_namespace
    obs_day = observation_day or event_end
    run_id = f"gold_{uuid.uuid4().hex[:12]}"
    mart_branch = f"wap_{run_id}"
    core_branch = f"wap_core_{run_id}"
    t_total = time.perf_counter()

    build_cfg = GoldBuildConfig(
        event_start=event_start,
        event_end=event_end,
        observation_day=obs_day,
        release_id=run_id,
    )

    engine = _create_pg_engine(cfg.app_database_url)
    watermarks = GoldWatermarkStore(engine)
    watermarks.ensure_schema()

    owns_spark = spark is None
    active_spark = spark or build_spark_session(cfg)
    persisted: list[DataFrame] = []
    silver_snaps: dict[str, int | None] = {}

    def _pin_disk(df: DataFrame, view_name: str) -> DataFrame:
        p = df.persist(StorageLevel.DISK_ONLY)
        p.createOrReplaceTempView(view_name)
        persisted.append(p)
        return p

    def _stage_and_rebind_core(table_name: str, df: DataFrame) -> None:
        """Write one Tier 1 Core table onto core_branch and re-bind core_<tbl> to it."""
        if persist_core and core_namespace:
            full_name = f"{cat}.{core_namespace}.{table_name}"
            df.writeTo(full_name).overwrite(F.lit(True))
            active_spark.read.table(full_name).createOrReplaceTempView(f"core_{table_name}")
        else:
            _pin_disk(df, f"core_{table_name}")

    try:
        cat = cfg.iceberg_catalog_name
        register_gold_functions(active_spark)

        # 1. Pin Silver to a consistent point-in-time snapshot
        silver_snaps = read_silver_snapshot_ids(
            active_spark, catalog=cat, silver_namespace=src_ns
        )
        watermarks.record_run(
            run_id=run_id,
            gold_namespace=gold_namespace,
            core_namespace=core_namespace if persist_core else None,
            silver_namespace=src_ns,
            mode="rebuild",
            event_start=event_start,
            event_end=event_end,
            observation_day=obs_day,
            status="RUNNING",
            silver_snapshots=silver_snaps,
        )

        load_silver_views(
            active_spark,
            catalog=cat,
            silver_namespace=src_ns,
            snapshot_ids=silver_snaps,
        )

        # Prepare Tier 1 Core WAP branch up front so each table streams directly to Iceberg
        # and downstream steps read compact Parquet instead of holding huge DataFrames in RAM.
        core_table_names = list(GOLD_CORE_DDLS.keys())
        core_branch_open = False
        if persist_core and core_namespace:
            _ensure_gold_namespace_and_tables(
                active_spark,
                catalog=cat,
                namespace=core_namespace,
                full_ddls=GOLD_CORE_DDLS,
            )
            for tname in core_table_names:
                active_spark.sql(
                    f"ALTER TABLE {cat}.{core_namespace}.{tname} CREATE OR REPLACE BRANCH `{core_branch}`"
                )
            core_branch_open = True

        try:
            if core_branch_open:
                active_spark.conf.set("spark.wap.branch", core_branch)

            # 2. Tier 1 — Gold Core compute (staged sequentially to keep heap flat)
            build_base_dimensions(active_spark, build_cfg)
            sel = build_selection_views(active_spark)
            for vname in (
                "_gtmp_cand_package",
                "_gtmp_cand_notice",
                "_gtmp_cand_result",
                "_gtmp_result_package_match",
                "_gtmp_notice_package_match",
            ):
                _pin_disk(sel[vname], vname)

            biz = build_business_dimensions(active_spark, build_cfg)
            _stage_and_rebind_core("dim_package", biz["dim_package"])
            _stage_and_rebind_core("dim_scope", biz["dim_scope"])

            dem = build_demand(active_spark, build_cfg)
            _stage_and_rebind_core("fact_procurement_demand", dem["fact_procurement_demand"])
            _stage_and_rebind_core("bridge_location", dem["bridge_location"])

            not_out = build_notice(active_spark, build_cfg)
            _stage_and_rebind_core("fact_tender_notice", not_out["fact_tender_notice"])

            awd = build_award(active_spark, build_cfg)
            _pin_disk(awd["dim_party"], "core_dim_party")
            _stage_and_rebind_core("fact_award", awd["fact_award"])
            _stage_and_rebind_core("bridge_party_member", awd["bridge_party_member"])
            _stage_and_rebind_core("bridge_award_recipient", awd["bridge_award_recipient"])

            sav = build_savings(active_spark, build_cfg)
            _stage_and_rebind_core("fact_savings", sav["fact_savings"])
            _stage_and_rebind_core("bridge_savings_award", sav["bridge_savings_award"])

            dur = build_duration(active_spark, build_cfg)
            _stage_and_rebind_core("fact_selection_round", dur["fact_selection_round"])

            cmp = build_competition(active_spark, build_cfg)
            _stage_and_rebind_core("fact_bidding_competition", cmp["fact_bidding_competition"])

            if core_branch_open:
                active_spark.conf.unset("spark.wap.branch")
                assert core_namespace is not None
                publish_wap_branch(
                    active_spark,
                    catalog=cat,
                    namespace=core_namespace,
                    branch_name=core_branch,
                    table_names=core_table_names,
                )
                core_branch_open = False
                # Re-bind core_* TempViews to published main tables for Tier 2 Marts
                for tname in core_table_names:
                    active_spark.read.table(f"{cat}.{core_namespace}.{tname}").createOrReplaceTempView(
                        f"core_{tname}"
                    )
        except Exception:
            if core_branch_open and core_namespace:
                active_spark.conf.unset("spark.wap.branch")
                abort_wap_branch(
                    active_spark,
                    catalog=cat,
                    namespace=core_namespace,
                    branch_name=core_branch,
                    table_names=core_table_names,
                )
            raise

        # 3. Tier 2 — Public 17 Mart tables via Iceberg WAP + Quality Gate
        mart_dims = build_public_dimensions(active_spark)
        mart_facts = build_public_facts(active_spark, build_cfg)
        all_marts: dict[str, DataFrame] = {**mart_dims, **mart_facts}

        _ensure_gold_namespace_and_tables(
            active_spark,
            catalog=cat,
            namespace=gold_namespace,
            full_ddls=GOLD_MART_DDLS,
        )
        try:
            _stage_full_replace_wap(
                active_spark,
                catalog=cat,
                namespace=gold_namespace,
                branch_name=mart_branch,
                table_dfs=all_marts,
            )
            gate = run_gold_quality_gate(
                active_spark,
                catalog=cat,
                namespace=gold_namespace,
                branch_name=mart_branch,
            )
            if not gate.passed:
                raise ValueError(f"Gold WAP Quality Gate failed: {'; '.join(gate.failures)}")

            publish_wap_branch(
                active_spark,
                catalog=cat,
                namespace=gold_namespace,
                branch_name=mart_branch,
                table_names=list(all_marts.keys()),
            )
        except Exception:
            abort_wap_branch(
                active_spark,
                catalog=cat,
                namespace=gold_namespace,
                branch_name=mart_branch,
                table_names=list(all_marts.keys()),
            )
            raise

        wall_s = round(time.perf_counter() - t_total, 2)
        watermarks.record_run(
            run_id=run_id,
            gold_namespace=gold_namespace,
            core_namespace=core_namespace if persist_core else None,
            silver_namespace=src_ns,
            mode="rebuild",
            event_start=event_start,
            event_end=event_end,
            observation_day=obs_day,
            status="PUBLISHED",
            silver_snapshots=silver_snaps,
            table_counts=gate.table_counts,
            wall_s=wall_s,
        )
        return {
            "run_id": run_id,
            "status": "PUBLISHED",
            "silver_namespace": src_ns,
            "core_namespace": core_namespace if persist_core else None,
            "gold_namespace": gold_namespace,
            "event_start": event_start.isoformat(),
            "event_end": event_end.isoformat(),
            "observation_day": obs_day.isoformat(),
            "award_amount_sum": str(gate.award_amount_sum),
            "table_counts": gate.table_counts,
            "wall_s": wall_s,
        }
    except Exception as exc:
        wall_s = round(time.perf_counter() - t_total, 2)
        try:
            watermarks.record_run(
                run_id=run_id,
                gold_namespace=gold_namespace,
                core_namespace=core_namespace if persist_core else None,
                silver_namespace=src_ns,
                mode="rebuild",
                event_start=event_start,
                event_end=event_end,
                observation_day=obs_day,
                status="FAILED",
                silver_snapshots=silver_snaps,
                wall_s=wall_s,
                error_message=f"{type(exc).__name__}: {exc}",
            )
        except Exception:
            pass
        raise
    finally:
        for df in persisted:
            try:
                df.unpersist()
            except Exception:
                pass
        if owns_spark:
            active_spark.stop()
        engine.dispose()
        gc.collect()


def _filter_silver_views_to_affected(
    spark: SparkSession,
    *,
    catalog: str,
    silver_namespace: str,
    snapshot_ids: dict[str, int | None],
    deltas: dict[str, DataFrame],
) -> int:
    """Register ``silver_*`` TempViews scoped strictly to entities touched by ``deltas``.

    Returns the count of distinct affected business keys (0 means deltas were empty).
    """
    pkg_ids: list[DataFrame] = []
    pkg_nos: list[DataFrame] = []
    notify_nos: list[DataFrame] = []
    plan_ids: list[DataFrame] = []
    contract_codes: list[DataFrame] = []

    if "package_revision" in deltas:
        d = deltas["package_revision"]
        pkg_ids.append(d.select(F.col("entity_id").alias("k")))
        pkg_ids.append(d.select(F.col("source_id").alias("k")))
        pkg_nos.append(d.select(F.col("package_no").alias("k")))
        plan_ids.append(d.select(F.col("plan_id").alias("k")))
    if "plan_revision" in deltas:
        d = deltas["plan_revision"]
        plan_ids.append(d.select(F.col("entity_id").alias("k")))
        plan_ids.append(d.select(F.col("source_id").alias("k")))
    if "notice_revision" in deltas:
        d = deltas["notice_revision"]
        pkg_ids.append(d.select(F.col("package_id").alias("k")))
        pkg_nos.append(d.select(F.col("package_no").alias("k")))
        notify_nos.append(d.select(F.col("notify_no").alias("k")))
    if "opening_revision" in deltas:
        d = deltas["opening_revision"]
        pkg_ids.append(d.select(F.col("package_id").alias("k")))
        pkg_nos.append(d.select(F.col("package_no").alias("k")))
        notify_nos.append(d.select(F.col("notify_no").alias("k")))
    if "result_revision" in deltas:
        d = deltas["result_revision"]
        pkg_ids.append(d.select(F.col("bid_id").alias("k")))
        pkg_nos.append(d.select(F.col("package_no").alias("k")))
        notify_nos.append(d.select(F.col("notify_no").alias("k")))
    if "contract_revision" in deltas:
        d = deltas["contract_revision"]
        notify_nos.append(d.select(F.col("notice_no").alias("k")))
        pkg_ids.append(d.select(F.col("result_source_id").alias("k")))
        contract_codes.append(d.select(F.col("contract_code").alias("k")))
    if "contract_performance_revision" in deltas:
        d = deltas["contract_performance_revision"]
        contract_codes.append(d.select(F.col("contract_code").alias("k")))

    def _union_keys(dfs: list[DataFrame]) -> DataFrame | None:
        if not dfs:
            return None
        out = dfs[0]
        for nxt in dfs[1:]:
            out = out.unionByName(nxt)
        return out.filter(F.col("k").isNotNull() & (F.trim(F.col("k")) != "")).distinct()

    u_pkg_ids = _union_keys(pkg_ids)
    u_pkg_nos = _union_keys(pkg_nos)
    u_notify = _union_keys(notify_nos)
    u_plans = _union_keys(plan_ids)
    u_contracts = _union_keys(contract_codes)

    total_keys = sum(
        df.count() for df in (u_pkg_ids, u_pkg_nos, u_notify, u_plans, u_contracts) if df is not None
    )
    if total_keys == 0:
        return 0

    full_views = load_silver_views(
        spark,
        catalog=catalog,
        silver_namespace=silver_namespace,
        view_prefix="_full_silver_",
        snapshot_ids=snapshot_ids,
    )

    # Register key sets as TempViews for semi-join filtering
    empty_str = spark.createDataFrame([], "k STRING")
    (u_pkg_ids or empty_str).createOrReplaceTempView("_inc_pkg_ids")
    (u_pkg_nos or empty_str).createOrReplaceTempView("_inc_pkg_nos")
    (u_notify or empty_str).createOrReplaceTempView("_inc_notify_nos")
    (u_plans or empty_str).createOrReplaceTempView("_inc_plan_ids")
    (u_contracts or empty_str).createOrReplaceTempView("_inc_contract_codes")

    # Expand 1 hop so related notices/results/packages/contracts in the same procurement chain are included
    spark.sql(
        """
        SELECT * FROM _full_silver_package_revision
        WHERE entity_id IN (SELECT k FROM _inc_pkg_ids)
           OR source_id IN (SELECT k FROM _inc_pkg_ids)
           OR package_no IN (SELECT k FROM _inc_pkg_nos)
           OR plan_id IN (SELECT k FROM _inc_plan_ids)
        """
    ).createOrReplaceTempView("silver_package_revision")

    spark.sql(
        """
        SELECT * FROM _full_silver_plan_revision
        WHERE entity_id IN (SELECT k FROM _inc_plan_ids)
           OR source_id IN (SELECT k FROM _inc_plan_ids)
           OR entity_id IN (SELECT plan_id FROM silver_package_revision WHERE plan_id IS NOT NULL)
           OR source_id IN (SELECT plan_id FROM silver_package_revision WHERE plan_id IS NOT NULL)
        """
    ).createOrReplaceTempView("silver_plan_revision")

    spark.sql(
        """
        SELECT * FROM _full_silver_package_location
        WHERE revision_id IN (SELECT revision_id FROM silver_package_revision)
        """
    ).createOrReplaceTempView("silver_package_location")

    spark.sql(
        """
        SELECT * FROM _full_silver_notice_revision
        WHERE notify_no IN (SELECT k FROM _inc_notify_nos)
           OR package_id IN (SELECT k FROM _inc_pkg_ids)
           OR package_no IN (SELECT k FROM _inc_pkg_nos)
           OR package_no IN (SELECT package_no FROM silver_package_revision WHERE package_no IS NOT NULL)
        """
    ).createOrReplaceTempView("silver_notice_revision")

    spark.sql(
        """
        SELECT * FROM _full_silver_notice_location
        WHERE revision_id IN (SELECT revision_id FROM silver_notice_revision)
        """
    ).createOrReplaceTempView("silver_notice_location")

    spark.sql(
        """
        SELECT * FROM _full_silver_opening_revision
        WHERE notify_no IN (SELECT k FROM _inc_notify_nos)
           OR notify_no IN (SELECT notify_no FROM silver_notice_revision WHERE notify_no IS NOT NULL)
           OR package_id IN (SELECT k FROM _inc_pkg_ids)
           OR package_no IN (SELECT k FROM _inc_pkg_nos)
        """
    ).createOrReplaceTempView("silver_opening_revision")

    spark.sql(
        """
        SELECT * FROM _full_silver_opening_submission
        WHERE revision_id IN (SELECT revision_id FROM silver_opening_revision)
        """
    ).createOrReplaceTempView("silver_opening_submission")

    spark.sql(
        """
        SELECT * FROM _full_silver_result_revision
        WHERE notify_no IN (SELECT k FROM _inc_notify_nos)
           OR notify_no IN (SELECT notify_no FROM silver_notice_revision WHERE notify_no IS NOT NULL)
           OR bid_id IN (SELECT k FROM _inc_pkg_ids)
           OR package_no IN (SELECT k FROM _inc_pkg_nos)
           OR package_no IN (SELECT package_no FROM silver_package_revision WHERE package_no IS NOT NULL)
        """
    ).createOrReplaceTempView("silver_result_revision")

    spark.sql(
        """
        SELECT * FROM _full_silver_award_lot
        WHERE revision_id IN (SELECT revision_id FROM silver_result_revision)
        """
    ).createOrReplaceTempView("silver_award_lot")

    spark.sql(
        """
        SELECT * FROM _full_silver_award_contractor
        WHERE revision_id IN (SELECT revision_id FROM silver_result_revision)
        """
    ).createOrReplaceTempView("silver_award_contractor")

    spark.sql(
        """
        SELECT * FROM _full_silver_contract_revision
        WHERE contract_code IN (SELECT k FROM _inc_contract_codes)
           OR notice_no IN (SELECT k FROM _inc_notify_nos)
           OR notice_no IN (SELECT notify_no FROM silver_result_revision WHERE notify_no IS NOT NULL)
           OR result_source_id IN (SELECT result_id FROM silver_result_revision WHERE result_id IS NOT NULL)
           OR result_source_id IN (SELECT bid_id FROM silver_result_revision WHERE bid_id IS NOT NULL)
        """
    ).createOrReplaceTempView("silver_contract_revision")

    spark.sql(
        """
        SELECT * FROM _full_silver_contract_performance_revision
        WHERE contract_code IN (SELECT k FROM _inc_contract_codes)
           OR contract_code IN (SELECT contract_code FROM silver_contract_revision WHERE contract_code IS NOT NULL)
        """
    ).createOrReplaceTempView("silver_contract_performance_revision")

    _ = full_views
    return total_keys


def run_spark_gold_incremental(
    *,
    event_start: date = date(2022, 1, 1),
    event_end: date = date(2026, 10, 31),
    observation_day: date | None = None,
    silver_namespace: str | None = None,
    gold_namespace: str = "gold_spark",
    core_namespace: str = "gold_spark_core",
    settings: SparkSilverSettings | None = None,
    spark: SparkSession | None = None,
) -> dict[str, Any]:
    """Incremental Two-Tier Gold run using Iceberg ``start-snapshot-id`` + ``MERGE INTO``.

    - Returns ``NO_OP`` immediately if all 12 Silver table snapshots match the last PUBLISHED run.
    - Safely falls back to ``run_spark_gold_rebuild`` if no prior watermark exists or if an
      intermediate Silver snapshot was expired / overwritten (non-append).
    """
    cfg = settings or SparkSilverSettings.from_environment(load_dotenv_file=True)
    src_ns = silver_namespace or cfg.iceberg_namespace
    obs_day = observation_day or event_end
    t_total = time.perf_counter()

    engine = _create_pg_engine(cfg.app_database_url)
    watermarks = GoldWatermarkStore(engine)
    watermarks.ensure_schema()
    prev = watermarks.get_latest_published_watermarks(
        gold_namespace=gold_namespace,
        silver_namespace=src_ns,
    )
    engine.dispose()

    if prev is None or not prev.get("silver_snapshots"):
        return run_spark_gold_rebuild(
            event_start=event_start,
            event_end=event_end,
            observation_day=obs_day,
            silver_namespace=src_ns,
            gold_namespace=gold_namespace,
            core_namespace=core_namespace,
            persist_core=True,
            settings=cfg,
            spark=spark,
        )

    owns_spark = spark is None
    active_spark = spark or build_spark_session(cfg)
    cat = cfg.iceberg_catalog_name
    prev_snaps: dict[str, int | None] = prev["silver_snapshots"]

    try:
        curr_snaps = read_silver_snapshot_ids(
            active_spark, catalog=cat, silver_namespace=src_ns
        )
        if curr_snaps == prev_snaps:
            return {
                "run_id": f"noop_{uuid.uuid4().hex[:8]}",
                "status": "NO_OP",
                "mode": "incremental",
                "silver_namespace": src_ns,
                "core_namespace": core_namespace,
                "gold_namespace": gold_namespace,
                "award_amount_sum": "0",
                "table_counts": {},
                "wall_s": round(time.perf_counter() - t_total, 2),
            }

        try:
            deltas = load_silver_incremental_deltas(
                active_spark,
                catalog=cat,
                silver_namespace=src_ns,
                from_snapshots=prev_snaps,
                to_snapshots=curr_snaps,
            )
            # Trigger lightweight metadata validation on deltas before committing to incremental path
            affected_count = _filter_silver_views_to_affected(
                active_spark,
                catalog=cat,
                silver_namespace=src_ns,
                snapshot_ids=curr_snaps,
                deltas=deltas,
            )
        except Exception:
            # Expired snapshot or non-append overwrite in Silver -> safe automatic full rebuild
            return run_spark_gold_rebuild(
                event_start=event_start,
                event_end=event_end,
                observation_day=obs_day,
                silver_namespace=src_ns,
                gold_namespace=gold_namespace,
                core_namespace=core_namespace,
                persist_core=True,
                settings=cfg,
                spark=active_spark,
            )

        run_id = f"gold_inc_{uuid.uuid4().hex[:10]}"
        core_branch = f"wap_core_{run_id}"
        mart_branch = f"wap_{run_id}"
        build_cfg = GoldBuildConfig(
            event_start=event_start,
            event_end=event_end,
            observation_day=obs_day,
            release_id=run_id,
        )

        if affected_count == 0:
            return {
                "run_id": run_id,
                "status": "NO_OP",
                "mode": "incremental",
                "silver_namespace": src_ns,
                "core_namespace": core_namespace,
                "gold_namespace": gold_namespace,
                "award_amount_sum": "0",
                "table_counts": {},
                "wall_s": round(time.perf_counter() - t_total, 2),
            }

        register_gold_functions(active_spark)
        build_base_dimensions(active_spark, build_cfg)
        build_selection_views(active_spark)
        biz = build_business_dimensions(active_spark, build_cfg)
        dem = build_demand(active_spark, build_cfg)
        not_out = build_notice(active_spark, build_cfg)
        awd = build_award(active_spark, build_cfg)
        sav = build_savings(active_spark, build_cfg)
        dur = build_duration(active_spark, build_cfg)
        cmp = build_competition(active_spark, build_cfg)

        delta_core: dict[str, DataFrame] = {
            "dim_package": biz["dim_package"],
            "dim_scope": biz["dim_scope"],
            "bridge_location": dem["bridge_location"],
            "bridge_party_member": awd["bridge_party_member"],
            "bridge_award_recipient": awd["bridge_award_recipient"],
            "bridge_savings_award": sav["bridge_savings_award"],
            "fact_procurement_demand": dem["fact_procurement_demand"],
            "fact_tender_notice": not_out["fact_tender_notice"],
            "fact_award": awd["fact_award"],
            "fact_savings": sav["fact_savings"],
            "fact_selection_round": dur["fact_selection_round"],
            "fact_bidding_competition": cmp["fact_bidding_competition"],
        }

        # MERGE INTO Tier 1 Gold Core on WAP branch
        core_names = list(delta_core.keys())
        for tname in core_names:
            active_spark.sql(
                f"ALTER TABLE {cat}.{core_namespace}.{tname} CREATE OR REPLACE BRANCH `{core_branch}`"
            )
        try:
            active_spark.conf.set("spark.wap.branch", core_branch)
            for tname, df in delta_core.items():
                merge_into_table(
                    active_spark,
                    full_name=f"{cat}.{core_namespace}.{tname}",
                    source_df=df,
                    merge_keys=CORE_MERGE_KEYS[tname],
                )
            active_spark.conf.unset("spark.wap.branch")
            publish_wap_branch(
                active_spark,
                catalog=cat,
                namespace=core_namespace,
                branch_name=core_branch,
                table_names=core_names,
            )
        except Exception:
            active_spark.conf.unset("spark.wap.branch")
            abort_wap_branch(
                active_spark,
                catalog=cat,
                namespace=core_namespace,
                branch_name=core_branch,
                table_names=core_names,
            )
            raise

        # Re-bind core_* TempViews to full updated Gold Core + merge new dims, then refresh Tier 2 Marts
        for tname in core_names:
            active_spark.read.table(f"{cat}.{core_namespace}.{tname}").createOrReplaceTempView(
                f"core_{tname}"
            )
        # Union existing public dimensions with any newly observed dimension rows
        for dim_name, key_col in (
            ("dim_date", "date_key"),
            ("dim_currency", "currency_key"),
            ("dim_geography", "geography_key"),
            ("dim_sector", "sector_key"),
            ("dim_value_band", "band_key"),
            ("dim_procurement_method", "method_key"),
            ("dim_party", "party_key"),
        ):
            existing = active_spark.read.table(f"{cat}.{gold_namespace}.{dim_name}")
            new_dim = active_spark.table(f"core_{dim_name}").select(*existing.columns)
            merged_dim = new_dim.unionByName(
                existing.join(new_dim.select(key_col), on=key_col, how="left_anti")
            )
            merged_dim.createOrReplaceTempView(f"core_{dim_name}")

        mart_dims = build_public_dimensions(active_spark)
        mart_facts = build_public_facts(active_spark, build_cfg)
        all_marts = {**mart_dims, **mart_facts}

        try:
            _stage_full_replace_wap(
                active_spark,
                catalog=cat,
                namespace=gold_namespace,
                branch_name=mart_branch,
                table_dfs=all_marts,
            )
            gate = run_gold_quality_gate(
                active_spark,
                catalog=cat,
                namespace=gold_namespace,
                branch_name=mart_branch,
            )
            if not gate.passed:
                raise ValueError(f"Gold incremental WAP Gate failed: {'; '.join(gate.failures)}")
            publish_wap_branch(
                active_spark,
                catalog=cat,
                namespace=gold_namespace,
                branch_name=mart_branch,
                table_names=list(all_marts.keys()),
            )
        except Exception:
            abort_wap_branch(
                active_spark,
                catalog=cat,
                namespace=gold_namespace,
                branch_name=mart_branch,
                table_names=list(all_marts.keys()),
            )
            raise

        wall_s = round(time.perf_counter() - t_total, 2)
        eng2 = _create_pg_engine(cfg.app_database_url)
        try:
            wm2 = GoldWatermarkStore(eng2)
            wm2.record_run(
                run_id=run_id,
                gold_namespace=gold_namespace,
                core_namespace=core_namespace,
                silver_namespace=src_ns,
                mode="incremental",
                event_start=event_start,
                event_end=event_end,
                observation_day=obs_day,
                status="PUBLISHED",
                silver_snapshots=curr_snaps,
                table_counts=gate.table_counts,
                wall_s=wall_s,
            )
        finally:
            eng2.dispose()

        return {
            "run_id": run_id,
            "status": "PUBLISHED",
            "mode": "incremental",
            "affected_keys": affected_count,
            "silver_namespace": src_ns,
            "core_namespace": core_namespace,
            "gold_namespace": gold_namespace,
            "award_amount_sum": str(gate.award_amount_sum),
            "table_counts": gate.table_counts,
            "wall_s": wall_s,
        }
    finally:
        if owns_spark:
            active_spark.stop()
        gc.collect()
