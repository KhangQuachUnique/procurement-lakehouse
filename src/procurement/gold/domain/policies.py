"""Authoritative geography, monetary value bands, and classification policies.

Hexagonal Domain Policy Definitions for lake.gold_v2.
"""

from datetime import date
from decimal import Decimal

from pydantic import Field, model_validator

from .models import Contract, digest
from .schema import FACTS, TABLES


class Geography(Contract):
    province_code: str = Field(min_length=1)
    version_id: str = Field(min_length=1)
    province_name: str = Field(min_length=1)
    region_name: str | None = None
    valid_from: date
    valid_to: date | None = None

    @model_validator(mode="after")
    def valid(self):
        if self.valid_to is not None and self.valid_to <= self.valid_from:
            raise ValueError("Inverted geography interval")
        return self

    @property
    def key(self) -> str:
        return "GEO_REF_" + digest([self.province_code, self.version_id])


class ValueBand(Contract):
    band_key: str = Field(min_length=1)
    currency_key: str = Field(min_length=1)
    policy_version: str = Field(min_length=1)
    min_amount: Decimal
    max_amount: Decimal | None = None
    band_label: str = Field(min_length=1)
    band_order: int = Field(ge=0, strict=True)

    @model_validator(mode="after")
    def valid(self):
        if not self.min_amount.is_finite() or self.min_amount < 0:
            raise ValueError("Invalid minimum amount")
        if self.max_amount is not None and (
            not self.max_amount.is_finite() or self.max_amount <= self.min_amount
        ):
            raise ValueError("Invalid upper amount boundary")
        return self

    @property
    def key(self) -> str:
        return "BAND_REF_" + digest([self.band_key, self.currency_key, self.policy_version])


class ReferencePolicy(Contract):
    geography: tuple[Geography, ...] = ()
    value_bands: tuple[ValueBand, ...] = ()

    @model_validator(mode="after")
    def valid(self):
        keys = set()
        intervals = {}
        for row in self.geography:
            if row.key in keys:
                raise ValueError("Duplicate geography identity")
            keys.add(row.key)
            spans = intervals.setdefault(row.province_code, [])
            end = row.valid_to or date.max
            if any(row.valid_from < b and a < end for a, b in spans):
                raise ValueError("Overlapping geography intervals")
            spans.append((row.valid_from, end))
        keys = set()
        intervals = {}
        for row in self.value_bands:
            if row.key in keys:
                raise ValueError("Duplicate band identity")
            keys.add(row.key)
            spans = intervals.setdefault(row.currency_key, [])
            end = row.max_amount or Decimal("Infinity")
            if any(row.min_amount < b and a < end for a, b in spans):
                raise ValueError("Overlapping monetary intervals")
            spans.append((row.min_amount, end))
        return self

    @property
    def version(self) -> str:
        return digest(self) if self.geography or self.value_bands else "raw_v1"


def apply_dictionary(db, plan):
    policy = ReferencePolicy.model_validate(plan.references)
    if policy.version != plan.reference_version:
        raise ValueError("Reference policy content/version mismatch")
    for row in policy.geography:
        db.execute(
            "INSERT INTO dimensions.dim_geography SELECT ?,?,?,?,?,FALSE,FALSE,?,?,? WHERE NOT EXISTS (SELECT 1 FROM dimensions.dim_geography WHERE geography_key=?)",
            [
                row.key,
                row.version_id,
                row.province_code,
                row.province_name,
                row.region_name,
                row.valid_from,
                row.valid_to,
                row.valid_from <= plan.observation_day
                and (row.valid_to is None or plan.observation_day < row.valid_to),
                row.key,
            ],
        )
    for row in policy.value_bands:
        db.execute(
            "INSERT INTO dimensions.dim_value_band SELECT ?,?,?,?,?,?,? WHERE NOT EXISTS (SELECT 1 FROM dimensions.dim_value_band WHERE band_key=?)",
            [
                row.key,
                row.currency_key,
                row.policy_version,
                row.min_amount,
                row.max_amount,
                row.band_label,
                row.band_order,
                row.key,
            ],
        )
    for row in policy.geography:
        current = row.valid_from <= plan.observation_day and (
            row.valid_to is None or plan.observation_day < row.valid_to
        )
        db.execute(
            "UPDATE dimensions.dim_geography SET is_current=? WHERE geography_key=?",
            [current, row.key],
        )


def apply(db, plan):
    policy = ReferencePolicy.model_validate(plan.references)
    apply_dictionary(db, plan)
    for fact in FACTS:
        grain = TABLES[fact].grain
        if "geography_key" in grain:
            for row in policy.geography:
                db.execute(
                    f"UPDATE changes.{fact} SET geography_key=? WHERE geography_key=? AND report_date>=? AND (? IS NULL OR report_date<?)",
                    [
                        row.key,
                        "GEO_" + row.province_code,
                        row.valid_from,
                        row.valid_to,
                        row.valid_to,
                    ],
                )
        if "value_band_key" in grain:
            for row in policy.value_bands:
                db.execute(
                    f"""UPDATE changes.{fact} c SET value_band_key=? FROM business.fact_procurement_demand p
                  WHERE c.owner_key=p.package_key AND p.is_amount_eligible AND p.currency_key=?
                  AND p.planned_package_amount>=? AND (? IS NULL OR p.planned_package_amount<?)""",
                    [row.key, row.currency_key, row.min_amount, row.max_amount, row.max_amount],
                )
