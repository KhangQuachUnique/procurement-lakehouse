from datetime import date

import pytest
from pydantic import ValidationError

from procurement.gold.application.compute.engine import run_business
from procurement.gold.application.compute.projection import project
from procurement.gold.domain.models import RunPlan
from procurement.gold.domain.policies import Geography, ReferencePolicy, ValueBand
from tests.gold.test_business import populated


def policy():
    return ReferencePolicy(
        geography=(
            Geography(
                province_code="HNI",
                version_id="old",
                province_name="Old Hanoi",
                valid_from=date(2020, 1, 1),
                valid_to=date(2025, 1, 1),
            ),
            Geography(
                province_code="HNI",
                version_id="new",
                province_name="New Hanoi",
                valid_from=date(2025, 1, 1),
            ),
        ),
        value_bands=(
            ValueBand(
                band_key="small",
                currency_key="VND",
                policy_version="v1",
                min_amount=0,
                max_amount=100000001,
                band_label="Small",
                band_order=0,
            ),
        ),
    )


def test_versioned_policies_are_applied_only_to_fact_columns_present(db, plan):
    populated(db)
    refs = policy()
    request = RunPlan.model_validate(
        plan.model_dump()
        | {"reference_version": refs.version, "references_json": refs.model_dump_json()}
    )
    run_business(db, request, source="silver")
    project(db, request)
    assert (
        db.execute(
            "SELECT count(*) FROM changes.fact_demand_daily WHERE geography_key=?",
            [refs.geography[0].key],
        ).fetchone()[0]
        > 0
    )
    assert (
        db.execute(
            "SELECT count(*) FROM changes.fact_savings_daily WHERE value_band_key=?",
            [refs.value_bands[0].key],
        ).fetchone()[0]
        > 0
    )
    # Removing geography/currency from duration grain must not break policy application.
    assert db.execute("SELECT count(*) FROM changes.fact_duration_daily").fetchone()[0] > 0
    assert (
        db.execute(
            "SELECT count(*) FROM dimensions.dim_geography WHERE geography_key=?",
            [refs.geography[1].key],
        ).fetchone()[0]
        == 1
    )


def test_overlapping_and_nonfinite_intervals_fail_closed():
    refs = policy()
    with pytest.raises(ValidationError, match="Overlapping"):
        ReferencePolicy(
            geography=refs.geography
            + (
                Geography(
                    province_code="HNI",
                    version_id="bad",
                    province_name="X",
                    valid_from=date(2024, 1, 1),
                ),
            )
        )
    with pytest.raises(ValidationError):
        ValueBand(
            band_key="x",
            currency_key="VND",
            policy_version="v1",
            min_amount="NaN",
            max_amount=None,
            band_label="X",
            band_order=0,
        )
    with pytest.raises(ValidationError, match="Duplicate"):
        ReferencePolicy(value_bands=refs.value_bands + refs.value_bands)


def test_reference_content_must_match_plan_identity(plan):
    with pytest.raises(ValidationError, match="content/version"):
        RunPlan.model_validate(plan.model_dump() | {"references_json": policy().model_dump_json()})


def test_policy_calendar_updates_dimension_even_without_source_changes(db, plan):
    from procurement.gold.domain.policies import apply_dictionary
    from procurement.gold.domain.schema import create_tables

    refs = ReferencePolicy(
        geography=(
            Geography(
                province_code="HNI",
                version_id="future",
                province_name="Future",
                valid_from=date(2026, 11, 8),
            ),
        )
    )
    first = RunPlan.model_validate(
        plan.model_dump()
        | {"reference_version": refs.version, "references_json": refs.model_dump_json()}
    )
    create_tables(db, "dimensions")
    apply_dictionary(db, first)
    assert db.execute("SELECT is_current FROM dimensions.dim_geography").fetchone()[0] is False
    second = RunPlan.model_validate(first.model_dump() | {"observation_day": date(2026, 11, 8)})
    apply_dictionary(db, second)
    assert db.execute("SELECT is_current FROM dimensions.dim_geography").fetchone()[0] is True
    assert db.execute("SELECT count(*) FROM dimensions.dim_geography").fetchone()[0] == 1
