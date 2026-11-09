"""Unit tests for Gold Dagster Asset definitions, Star Schema Lineage, and Asset Checks."""

from dagster import AssetKey, Definitions

from procurement.orchestration.definitions import defs
from procurement.orchestration.gold_assets import (
    GOLD_ASSET_CHECKS,
    GOLD_DIMENSIONS_ASSETS,
    GOLD_FACT_CHECKS,
    GOLD_FACT_SPECS,
    gold_facts_monthly,
)


def test_gold_dagster_definitions_loadable():
    Definitions.validate_loadable(defs)
    assert defs.resolve_job_def("lakehouse_end_to_end")
    assert defs.resolve_job_def("gold_dimensions_sync")
    assert defs.resolve_job_def("gold_facts_monthly_job")


def test_gold_dimensions_assets_registered():
    dim_keys = {a.key.to_user_string() for a in GOLD_DIMENSIONS_ASSETS}
    expected_dims = {
        "gold_dim_geography",
        "gold_dim_date",
        "gold_dim_sector",
        "gold_dim_procurement_method",
        "gold_dim_bidding_type",
        "gold_dim_value_band",
        "gold_dim_currency",
        "gold_dim_duration_metric",
        "gold_dim_progress_status",
        "gold_dim_party",
    }
    assert dim_keys == expected_dims
    for a in GOLD_DIMENSIONS_ASSETS:
        assert a.group_names_by_key[a.key] == "gold_dimensions"


def test_gold_facts_specs_and_lineage_dependencies():
    fact_specs = {s.key.to_user_string(): s for s in GOLD_FACT_SPECS}
    expected_facts = {
        "gold_fact_demand_daily",
        "gold_fact_notice_daily",
        "gold_fact_award_daily",
        "gold_fact_savings_daily",
        "gold_fact_duration_daily",
        "gold_fact_supplier_daily",
        "gold_fact_status_daily",
    }
    assert set(fact_specs.keys()) == expected_facts

    for spec in fact_specs.values():
        assert spec.group_name == "gold_facts"

    # Verify key lineage upstream dependencies
    # Demand -> silver_package_revision + dims
    demand_deps = {d.asset_key.to_user_string() for d in fact_specs["gold_fact_demand_daily"].deps}
    assert "silver_package_revision" in demand_deps
    assert "gold_dim_geography" in demand_deps
    assert "gold_dim_sector" in demand_deps

    # Notice -> silver_notice_revision + dims
    notice_deps = {d.asset_key.to_user_string() for d in fact_specs["gold_fact_notice_daily"].deps}
    assert "silver_notice_revision" in notice_deps
    assert "gold_dim_procurement_method" in notice_deps

    # Award -> silver_result_revision + silver_award_lot + dims
    award_deps = {d.asset_key.to_user_string() for d in fact_specs["gold_fact_award_daily"].deps}
    assert "silver_result_revision" in award_deps
    assert "silver_award_lot" in award_deps
    assert "gold_dim_geography" in award_deps

    # Savings -> silver_package_revision + silver_result_revision + dims
    savings_deps = {d.asset_key.to_user_string() for d in fact_specs["gold_fact_savings_daily"].deps}
    assert "silver_package_revision" in savings_deps
    assert "silver_result_revision" in savings_deps

    # Duration -> silver_plan_revision + silver_notice_revision + silver_result_revision + dims
    duration_deps = {d.asset_key.to_user_string() for d in fact_specs["gold_fact_duration_daily"].deps}
    assert "silver_plan_revision" in duration_deps
    assert "silver_notice_revision" in duration_deps
    assert "silver_result_revision" in duration_deps
    assert "gold_dim_duration_metric" in duration_deps

    # Supplier -> silver_result_revision + silver_award_contractor + dim_party
    supplier_deps = {d.asset_key.to_user_string() for d in fact_specs["gold_fact_supplier_daily"].deps}
    assert "silver_result_revision" in supplier_deps
    assert "silver_award_contractor" in supplier_deps
    assert "gold_dim_party" in supplier_deps

    # Status -> silver_package_revision + silver_result_revision + dim_progress_status
    status_deps = {d.asset_key.to_user_string() for d in fact_specs["gold_fact_status_daily"].deps}
    assert "silver_package_revision" in status_deps
    assert "silver_result_revision" in status_deps
    assert "gold_dim_progress_status" in status_deps


def test_gold_asset_checks_configured():
    assert len(GOLD_FACT_CHECKS) == 9
    assert len(GOLD_ASSET_CHECKS) == 1
    assert next(iter(GOLD_ASSET_CHECKS[0].check_keys)).name == "full_gold_reconciliation_audit"

