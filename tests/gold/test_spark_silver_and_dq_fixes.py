"""Unit tests for Gold SQL DQ fixes and Spark Silver (lake.silver_spark.*) compatibility."""

from datetime import UTC, date, datetime
from decimal import Decimal
import re

import duckdb

from procurement.gold.application.compute.engine import STEPS, run_business
from procurement.gold.application.compute.projection import project
from procurement.gold.application.processor import process_gold_range
from procurement.gold.application.silver_adapter import adapt_projection
from procurement.gold.domain.schema import FACTS, TABLES
from procurement_spark.silver.resources.registry import SPARK_RESOURCES
from tests.gold.test_business import populated


def _spark_ddl_to_duckdb(ddl: str) -> str:
    """Convert a Spark Iceberg column DDL string to DuckDB SQL types."""
    out_lines = []
    for raw_line in ddl.strip().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("--"):
            continue
        # Spark TIMESTAMP maps to Iceberg v2 timestamptz (DuckDB TIMESTAMPTZ).
        line = re.sub(r"\bSTRING\b", "VARCHAR", line)
        line = re.sub(r"\bTIMESTAMP\b", "TIMESTAMPTZ", line)
        line = re.sub(r"\bINT\b", "INTEGER", line)
        out_lines.append(line)
    return "\n".join(out_lines)


def _create_spark_silver_schema(conn, schema_prefix: str) -> None:
    """Materialize all Spark Silver Iceberg tables from SPARK_RESOURCES DDLs in DuckDB."""
    created: set[str] = set()
    for resource in SPARK_RESOURCES.values():
        for tbl_name, ddl in resource.iceberg_ddls.items():
            if tbl_name in created:
                continue
            created.add(tbl_name)
            duck_ddl = _spark_ddl_to_duckdb(ddl)
            conn.execute(f"CREATE TABLE {schema_prefix}.{tbl_name} ({duck_ddl})")


# ---------------------------------------------------------------------------
# 1. Gold SQL DQ bug fixes
# ---------------------------------------------------------------------------


def test_savings_contract_matching_single_lot_vs_multi_lot_vs_package(db, plan):
    """savings_04.sql must match single-lot and package awards to contracts without double-counting multi-lot awards."""
    populated(db)

    # Fixture p2 / rev_res_2 is a single-lot effective award (lot_no='1', lot_price=120M, winning_price=100M).
    # Link a contract_revision to rev_res_2 via result_source_id ('RES_002') at 95M.
    db.execute(
        """
        INSERT INTO silver.observation (
            observation_id, resource, source_date, run_id, table_name,
            file_key, file_sha256, row_ordinal, observed_at, source_id, source_version, content_hash
        ) VALUES (
            'obs_ctr_single', 'contract_performance', DATE '2025-02-01', 'run1', 'contract_revision',
            'k', 's', 1, TIMESTAMPTZ '2025-02-01 00:00:00+00', 'c_single', '1', 'hc1'
        )
        """
    )
    db.execute(
        """
        INSERT INTO silver.contract_revision (
            revision_id, entity_id, observation_id, contract_code, contract_no,
            price_after_tax, currency, result_source_id, public_date
        ) VALUES (
            'crev_single', 'ce_single', 'obs_ctr_single', 'CT_SINGLE', 'CT_SINGLE',
            95000000, 'VND', 'RES_002', DATE '2025-02-01'
        )
        """
    )

    # Fixture p14 / rev_res_14 is a multi-lot effective award (2 lots: alot_14_1, alot_14_2).
    # Link a contract_revision to rev_res_14 via result_source_id ('RES_014') at 60M.
    db.execute(
        """
        INSERT INTO silver.observation (
            observation_id, resource, source_date, run_id, table_name,
            file_key, file_sha256, row_ordinal, observed_at, source_id, source_version, content_hash
        ) VALUES (
            'obs_ctr_multi', 'contract_performance', DATE '2025-02-02', 'run1', 'contract_revision',
            'k', 's', 1, TIMESTAMPTZ '2025-02-02 00:00:00+00', 'c_multi', '1', 'hc2'
        )
        """
    )
    db.execute(
        """
        INSERT INTO silver.contract_revision (
            revision_id, entity_id, observation_id, contract_code, contract_no,
            price_after_tax, currency, result_source_id, public_date
        ) VALUES (
            'crev_multi', 'ce_multi', 'obs_ctr_multi', 'CT_MULTI', 'CT_MULTI',
            60000000, 'VND', 'RES_014', DATE '2025-02-02'
        )
        """
    )

    # Also add a whole-package (0-lot) award on p12 (planned_package_amount=50M) and a contract for it.
    db.execute(
        """
        INSERT INTO silver.observation (
            observation_id, resource, source_date, run_id, table_name,
            file_key, file_sha256, row_ordinal, observed_at, source_id, source_version, content_hash
        ) VALUES
        (
            'obs_r_pkg_only', 'contractor_result', DATE '2025-02-05', 'run1', 'result_revision',
            'k', 's', 1, TIMESTAMPTZ '2025-02-05 00:00:00+00', 'res_pkg_only', '1', 'hrp'
        ),
        (
            'obs_c_pkg_only', 'contract_performance', DATE '2025-02-06', 'run1', 'contract_revision',
            'k', 's', 1, TIMESTAMPTZ '2025-02-06 00:00:00+00', 'c_pkg_only', '1', 'hcp'
        )
        """
    )
    db.execute(
        """
        INSERT INTO silver.result_revision (
            revision_id, entity_id, observation_id, result_id, result_version,
            bid_id, package_no, total_winning_price, currency, public_date, status_raw, root_json
        ) VALUES (
            'r_pkg_only', 're_pkg_only', 'obs_r_pkg_only', 'res_pkg_only', '1',
            'p12', 'PKG-12', 45000000, 'VND', DATE '2025-02-05', 'IS_PUBLISH', '{}'
        )
        """
    )
    db.execute(
        """
        INSERT INTO silver.award_contractor (
            award_contractor_id, award_lot_id, revision_id, contractor_code, contractor_name,
            is_consortium, bid_result
        ) VALUES (
            'ac_pkg_only', NULL, 'r_pkg_only', 'SUP_PKG', 'Pkg Winner', FALSE, 1
        )
        """
    )
    db.execute(
        """
        INSERT INTO silver.contract_revision (
            revision_id, entity_id, observation_id, contract_code, contract_no,
            price_after_tax, currency, result_source_id, public_date
        ) VALUES (
            'crev_pkg_only', 'ce_pkg_only', 'obs_c_pkg_only', 'CT_PKG', 'CT_PKG',
            44000000, 'VND', 'res_pkg_only', DATE '2025-02-06'
        )
        """
    )

    run_business(db, plan, source="silver")

    # 1. Single-lot award (rev_res_2): matched ONCE, exact contract_amount and contract_saving_amount.
    single_rows = db.execute(
        """
        SELECT scope_key, baseline_amount, contract_amount, contract_saving_amount, is_contract_eligible
        FROM business.fact_savings
        WHERE comparison_scope_key LIKE '%PKG:p2:LOT:%'
        """
    ).fetchall()
    assert len(single_rows) == 1
    scope_key, baseline, ctr_amt, ctr_sav, eligible = single_rows[0]
    assert ":LOT:" in scope_key
    assert eligible is True
    assert ctr_amt == Decimal("95000000.000000")
    assert ctr_sav == baseline - Decimal("95000000.000000")

    # 2. Multi-lot award (rev_res_14, 2 lots): NOT matched to package-level contract -> zero double-counting.
    multi_rows = db.execute(
        """
        SELECT is_contract_eligible, contract_amount
        FROM business.fact_savings
        WHERE package_key = 'PKG:p14'
        """
    ).fetchall()
    assert len(multi_rows) == 2
    assert all(not row[0] and row[1] is None for row in multi_rows)

    # 3. Whole-package (0-lot) award (r_pkg_only on p12): still matched cleanly.
    pkg_rows = db.execute(
        """
        SELECT scope_key, baseline_amount, contract_amount, contract_saving_amount, is_contract_eligible
        FROM business.fact_savings
        WHERE package_key = 'PKG:p12'
        """
    ).fetchall()
    assert len(pkg_rows) == 1
    assert ":PACKAGE:" in pkg_rows[0][0]
    assert pkg_rows[0][4] is True
    assert pkg_rows[0][2] == Decimal("44000000.000000")
    assert pkg_rows[0][3] == Decimal("50000000.000000") - Decimal("44000000.000000")


def test_award_single_l_canceled_classified_as_cancelled(db, plan):
    """award_04.sql must classify 'CANCELED' (single-L) as 'cancelled', not 'unresolved_status'."""
    populated(db)
    db.execute(
        "UPDATE silver.result_revision SET status_raw = 'CANCELED' WHERE revision_id = 'rev_res_2'"
    )

    run_business(db, plan, source="silver")

    award_state = db.execute(
        "SELECT award_state, is_amount_eligible FROM business.fact_award WHERE award_business_key LIKE 'AWARD:rev_res_2:%'"
    ).fetchone()
    assert award_state == ("cancelled", False)

    exclusion_reason = db.execute(
        "SELECT descriptive_exclusion_reason FROM business.fact_savings WHERE package_key = 'PKG:p2'"
    ).fetchone()[0]
    assert exclusion_reason == "cancelled_award"


def test_dim_date_guards_out_of_range_raw_date_typos(db, plan):
    """dimensions_base_01.sql must exclude raw date typos < 2000-01-01 or > 2035-12-31 from dim_date."""
    populated(db)
    db.execute(
        "UPDATE silver.result_revision SET decision_date = DATE '1202-10-18' WHERE revision_id = 'rev_res_1_2024'"
    )
    db.execute(
        "UPDATE silver.result_revision SET decision_date = DATE '1984-04-13' WHERE revision_id = 'rev_res_2'"
    )
    db.execute(
        "UPDATE silver.notice_revision SET bid_close_date = TIMESTAMPTZ '2099-01-01 00:00:00+00' WHERE notify_no = 'TBMT_001'"
    )

    run_business(db, plan, source="silver")

    min_d, max_d = db.execute(
        "SELECT MIN(date_key), MAX(date_key) FROM business.dim_date"
    ).fetchone()
    assert min_d >= date(2000, 1, 1)
    assert max_d <= date(2035, 12, 31)
    bad_count = db.execute(
        "SELECT count(*) FROM business.dim_date WHERE date_key IN (DATE '1202-10-18', DATE '1984-04-13', DATE '2099-01-01')"
    ).fetchone()[0]
    assert bad_count == 0


def test_competition_excludes_non_vnd_monetary_sums_while_keeping_bidder_counts(db, plan):
    """competition.py must keep non-VND bidders in counts while excluding non-VND amounts from VND sums."""
    populated(db)
    db.execute(
        """
        INSERT INTO silver.observation (
            observation_id, resource, source_date, run_id, table_name,
            file_key, file_sha256, row_ordinal, observed_at, source_id, source_version, content_hash
        ) VALUES (
            'obs_open_fx', 'bid_opening', DATE '2025-02-10', 'run1', 'opening_revision',
            'k', 's', 1, TIMESTAMPTZ '2025-02-10 00:00:00+00', 'open_fx', '1', 'hofx'
        )
        """
    )
    db.execute(
        """
        INSERT INTO silver.opening_revision (
            revision_id, entity_id, observation_id, notify_no, package_id, package_no,
            actual_open_date, public_date, total_bidders
        ) VALUES (
            'orev_fx', 'oe_fx', 'obs_open_fx', 'IB-FX-01', 'p12', 'PKG-12',
            TIMESTAMPTZ '2025-02-10 09:00:00+00', DATE '2025-02-10', 2
        )
        """
    )
    db.execute(
        """
        INSERT INTO silver.opening_submission (
            submission_id, revision_id, contractor_code, contractor_name,
            bid_price, currency, discount_rate, final_bid_price, tech_result
        ) VALUES
            ('sub_vnd', 'orev_fx', 'SUP_VND', 'VND Bidder', 40000000, 'VND', 5.0, 38000000, '1'),
            ('sub_usd', 'orev_fx', 'SUP_USD', 'USD Bidder', 100000, 'USD', 0.0, 100000, '1')
        """
    )

    run_business(db, plan, source="silver")

    row = db.execute(
        """
        SELECT total_bidders, tech_passed_count, total_bid_amount, total_discount_amount, total_final_bid_amount
        FROM business.fact_bidding_competition
        WHERE opening_key = 'OPN:orev_fx'
        """
    ).fetchone()
    assert row is not None
    total_bidders, tech_passed, bid_amt, disc_amt, final_amt = row
    assert total_bidders == 2
    assert tech_passed == 2
    assert bid_amt == Decimal("40000000.000000")
    assert disc_amt == Decimal("2000000.000000")
    assert final_amt == Decimal("38000000.000000")


# ---------------------------------------------------------------------------
# 2. Spark Silver (SPARK_RESOURCES DDLs) compatibility tests
# ---------------------------------------------------------------------------


def test_all_9_gold_compute_steps_bind_and_execute_against_spark_resources_ddls(db, plan):
    """Dry-run bind & execute of all 9 Gold compute steps + projection over raw SPARK_RESOURCES DDLs."""
    db.execute("CREATE SCHEMA spark_silver")
    _create_spark_silver_schema(db, "spark_silver")

    # Confirm no legacy observation / linkage_index tables exist.
    legacy = db.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_schema = 'spark_silver' AND table_name IN ('observation', 'linkage_index')"
    ).fetchone()[0]
    assert legacy == 0

    executed_steps: list[str] = []
    counts = run_business(db, plan, source="spark_silver", on_step=executed_steps.append)
    assert len(executed_steps) == len(STEPS) == 9
    assert set(counts.keys()) == {
        "fact_procurement_demand",
        "fact_tender_notice",
        "fact_award",
        "fact_savings",
        "fact_selection_round",
        "fact_bidding_competition",
    }

    projected = project(db, plan)
    assert set(projected.keys()) == set(FACTS)


def test_gold_processor_end_to_end_against_spark_silver_namespace(monkeypatch):
    """process_gold_range works against a Spark Silver catalog (no observation, linkage_index, is_current)."""
    remote = duckdb.connect(":memory:")
    remote.execute("ATTACH ':memory:' AS lake")
    remote.execute("CREATE SCHEMA lake.silver_spark")
    remote.execute("CREATE SCHEMA lake.gold_v4_spark_test")
    _create_spark_silver_schema(remote, "lake.silver_spark")

    # DuckDB in-memory catalogs don't support Iceberg's PARTITIONED BY syntax, so
    # create the exact 17 Gold tables in lake.gold_v4_spark_test without PARTITIONED BY.
    def _ensure_in_memory_gold(conn, namespace="gold_v2"):
        for name, table in TABLES.items():
            cols_ddl = ", ".join(f"{c.name} {c.sql_type}" for c in table.columns)
            conn.execute(f"CREATE TABLE IF NOT EXISTS lake.{namespace}.{name} ({cols_ddl})")

    monkeypatch.setattr(
        "procurement.gold.application.processor.ensure_gold_tables",
        _ensure_in_memory_gold,
    )

    # Populate minimal linked records across all 6 Spark Silver resources in 2025-01.
    # Also add an upstream plan_revision in 2024-12 to exercise _pull_upstream_references without is_current / linkage_index.
    remote.execute(
        """
        INSERT INTO lake.silver_spark.plan_revision (
            revision_id, entity_id, source_id, source_version, source_date, ingested_at,
            run_id, content_hash, plan_no, title, investor_name, project_id, project_no,
            currency, public_date, dq_status
        ) VALUES (
            'plrev1', 'ple1', 'plan_src_1', '1', DATE '2024-12-20', TIMESTAMPTZ '2024-12-20 10:00:00+00',
            'spark_run', 'hpl1', 'PL240001', 'Plan 1', 'Investor A', 'proj_1', 'PRJ01',
            'VND', DATE '2024-12-20', 'PASS'
        )
        """
    )
    remote.execute(
        """
        INSERT INTO lake.silver_spark.package_revision (
            revision_id, entity_id, source_id, source_date, ingested_at,
            run_id, content_hash, package_no, title, plan_no, plan_id,
            bid_price, currency, bid_field, bid_form, bid_mode, public_date, dq_status
        ) VALUES (
            'pkgrev1', 'pkge1', 'pkg_src_1', DATE '2025-01-05', TIMESTAMPTZ '2025-01-05 08:00:00+00',
            'spark_run', 'hpkg1', 'BP250001', 'Package 1', 'PL240001', 'plan_src_1',
            100000000, 'VND', 'XL', 'DTRR', '1_MTHS', DATE '2025-01-05', 'PASS'
        )
        """
    )
    remote.execute(
        """
        INSERT INTO lake.silver_spark.notice_revision (
            revision_id, entity_id, source_family, source_document_id, source_version, source_date,
            ingested_at, run_id, content_hash, notify_no, notify_version, package_id, package_no,
            plan_id, plan_no, title, procuring_entity_code, procuring_entity_name,
            investor_code, investor_name, bid_field, bid_form, bid_mode, status_raw,
            bid_price, currency, guarantee_amount, guarantee_currency, guarantee_form,
            public_date, published_at, original_published_at, bid_close_date, dq_status
        ) VALUES (
            'notrev1', 'note1', 'standard', 'not_src_1', '00', DATE '2025-01-06',
            TIMESTAMPTZ '2025-01-06 08:00:00+00', 'spark_run', 'hnot1', 'IB250001', '00',
            'pkg_src_1', 'BP250001', 'plan_src_1', 'PL240001', 'Notice 1',
            'PE01', 'Procurer A', 'INV01', 'Investor A', 'XL', 'DTRR', '1_MTHS', '01',
            100000000, 'VND', 1000000, 'VND', 'BANK_GUARANTEE',
            DATE '2025-01-06', TIMESTAMPTZ '2025-01-06 08:00:00+00',
            TIMESTAMPTZ '2025-01-06 08:00:00+00', TIMESTAMPTZ '2025-01-16 09:00:00+00', 'PASS'
        )
        """
    )
    remote.execute(
        """
        INSERT INTO lake.silver_spark.opening_revision (
            revision_id, entity_id, notify_no, notify_version, source_version, source_date,
            ingested_at, run_id, content_hash, package_no, package_id,
            actual_open_date, total_bidders, public_date, dq_status
        ) VALUES (
            'oprev1', 'ope1', 'IB250001', '00', '1', DATE '2025-01-16',
            TIMESTAMPTZ '2025-01-16 10:00:00+00', 'spark_run', 'hop1', 'BP250001', 'pkg_src_1',
            TIMESTAMPTZ '2025-01-16 09:30:00+00', 2, DATE '2025-01-16', 'PASS'
        )
        """
    )
    remote.execute(
        """
        INSERT INTO lake.silver_spark.opening_submission (
            submission_id, revision_id, source_date, ordinal, contractor_code, contractor_name,
            bid_price, currency, discount_rate, final_bid_price, tech_result
        ) VALUES
            ('osub1', 'oprev1', DATE '2025-01-16', 1, 'C01', 'Winner Co', 95000000, 'VND', 0, 95000000, '1'),
            ('osub2', 'oprev1', DATE '2025-01-16', 2, 'C02', 'FX Bidder', 4000, 'USD', 0, 4000, '1')
        """
    )
    remote.execute(
        """
        INSERT INTO lake.silver_spark.result_revision (
            revision_id, entity_id, result_id, result_version, source_version, source_date,
            ingested_at, run_id, content_hash, notify_no, bid_id, package_no, plan_no,
            total_winning_price, currency, decision_date, public_date, status_raw, dq_status
        ) VALUES (
            'resrev1', 'rese1', 'res_src_1', '1', '1', DATE '2025-01-20',
            TIMESTAMPTZ '2025-01-20 12:00:00+00', 'spark_run', 'hres1', 'IB250001',
            'pkg_src_1', 'BP250001', 'PL240001', 95000000, 'VND',
            DATE '1202-10-18', DATE '2025-01-20', 'IS_PUBLISH', 'WARN'
        )
        """
    )
    remote.execute(
        """
        INSERT INTO lake.silver_spark.award_lot (
            award_lot_id, revision_id, source_date, ordinal, source_lot_id,
            lot_no, lot_price, winning_price, currency
        ) VALUES (
            'alot1', 'resrev1', DATE '2025-01-20', 1, 'slot1',
            '1', 100000000, 95000000, 'VND'
        )
        """
    )
    remote.execute(
        """
        INSERT INTO lake.silver_spark.award_contractor (
            award_contractor_id, award_lot_id, revision_id, source_date, ordinal,
            contractor_code, contractor_name, is_consortium, bid_result, currency
        ) VALUES (
            'actr1', 'alot1', 'resrev1', DATE '2025-01-20', 1,
            'C01', 'Winner Co', FALSE, 1, 'VND'
        )
        """
    )
    remote.execute(
        """
        INSERT INTO lake.silver_spark.contract_revision (
            revision_id, entity_id, source_contract_id, source_version, source_date,
            ingested_at, run_id, content_hash, contract_code, contract_no,
            price_after_tax, currency, notice_no, result_source_id, public_date, dq_status
        ) VALUES (
            'ctrrev1', 'ctre1', 'ctr_src_1', '1', DATE '2025-01-25',
            TIMESTAMPTZ '2025-01-25 12:00:00+00', 'spark_run', 'hctr1', 'CT250001', 'CT250001',
            92000000, 'VND', 'IB250001', 'res_src_1', DATE '2025-01-25', 'PASS'
        )
        """
    )

    result = process_gold_range(
        start_date="2025-01-01",
        end_date="2025-02-01",
        remote_conn=remote,
        silver_namespace="silver_spark",
        gold_namespace="gold_v4_spark_test",
        memory_limit="512MB",
        threads=1,
    )

    assert result["input_rows"] > 0
    assert result["facts"]["fact_demand_daily"] == 1
    assert result["facts"]["fact_notice_daily"] == 1
    assert result["facts"]["fact_award_daily"] == 1
    assert result["facts"]["fact_savings_daily"] == 1
    assert result["facts"]["fact_bidding_competition_daily"] == 1

    # Verify contract savings populated for single-lot award.
    sav = remote.execute(
        "SELECT contract_sample_count, contract_amount_sum, contract_saving_sum "
        "FROM lake.gold_v4_spark_test.fact_savings_daily"
    ).fetchone()
    assert sav == (1, Decimal("92000000.000000"), Decimal("8000000.000000"))

    # Verify raw decision_date typo 1202-10-18 did NOT enter dim_date.
    min_date = remote.execute("SELECT MIN(date_key) FROM lake.gold_v4_spark_test.dim_date").fetchone()[0]
    assert min_date >= date(2000, 1, 1)

    # Verify competition counted both bidders (2) but only summed VND amount (95M).
    comp = remote.execute(
        "SELECT total_bidders, total_bid_amount FROM lake.gold_v4_spark_test.fact_bidding_competition_daily"
    ).fetchone()
    assert comp == (2, Decimal("95000000.000000"))

    # Verify all 17 Gold contract tables exist in lake.gold_v4_spark_test.
    gold_tables = {
        r[0]
        for r in remote.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog = 'lake' AND table_schema = 'gold_v4_spark_test'"
        ).fetchall()
    }
    assert set(TABLES.keys()).issubset(gold_tables)
    remote.close()


def test_adapt_projection_defensive_aliases_and_observation_id():
    """adapt_projection synthesizes observation_id for Spark Silver and keeps legacy Silver untouched."""
    spark_pkg_cols = ["revision_id", "entity_id", "source_id", "source_plan_id", "public_date"]
    adapted = adapt_projection(spark_pkg_cols, "package_revision")
    assert "'package_revision:' || revision_id AS observation_id" in adapted
    assert "source_plan_id AS plan_id" in adapted

    legacy_pkg_cols = ["revision_id", "entity_id", "observation_id", "plan_id", "public_date"]
    legacy = adapt_projection(legacy_pkg_cols, "package_revision")
    assert not any("AS observation_id" in expr for expr in legacy)
    assert not any("AS plan_id" in expr for expr in legacy)
