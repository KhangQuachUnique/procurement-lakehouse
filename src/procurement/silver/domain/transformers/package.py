"""Silver transformer for KHLCNT Package detail.

Bronze table: khlcnt_bid_package_detail, whose payload is a top-level
Package DTO. The transformer emits typed package_revision fields
plus location and lot child components.
"""

from __future__ import annotations

from typing import Any, Callable

from procurement.silver.domain.parsers import (
    clean_pipe_code,
    clean_text,
    parse_boolean,
    parse_date,
    parse_money,
)
from procurement.silver.domain.protocols import (
    ExtractedChild,
    ExtractedReference,
    QualityIssue,
)
from procurement.silver.domain.registry import register_transformer
from procurement.silver.domain.selection import digest
from procurement.silver.domain.transformers.base import (
    BaseResourceTransformer,
    canonical_json,
)

TABLE_NAME = "khlcnt_bid_package_detail"
INVALID_IDENTIFIERS = frozenset({"N/A", "NA", "NULL", "NONE"})

PACKAGE_REVISION_COLUMNS = (
    "revision_id",
    "entity_id",
    "entity_type",
    "package_no",
    "title",
    "plan_no",
    "plan_id",
    "bid_price",
    "currency",
    "process_apply",
    "bid_field",
    "bid_form",
    "bid_mode",
    "capital_detail",
    "contract_type",
    "execution_period",
    "execution_period_unit",
    "is_domestic",
    "is_internet",
    "bid_start_unit",
    "bid_start_year",
    "bid_start_quarter",
    "bid_start_month",
    "public_date",
    "root_json",
)

PACKAGE_LOCATION_COLUMNS = (
    "location_id",
    "revision_id",
    "location_ordinal",
    "province_code",
    "province_name",
    "district_code",
    "district_name",
    "ward_code",
    "ward_name",
)

PACKAGE_LOT_COLUMNS = (
    "lot_id",
    "revision_id",
    "source_lot_id",
    "lot_ordinal",
    "lot_no",
    "lot_name",
    "lot_price",
    "currency",
    "medicine_code",
    "quantity",
    "uom",
    "planned_unit_price",
)


def _valid_reference(value: Any) -> str | None:
    """Preserve historic numeric Plan IDs and prefix-bearing IDs verbatim."""
    result = clean_text(value)
    return None if result is None or result.upper() in INVALID_IDENTIFIERS else result


def _nonnegative_int(value: Any) -> int | None:
    """Accept integers / integer strings, never bool, float or fractional text."""
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ValueError("Boolean is not an integer")
    if isinstance(value, int):
        number = value
    elif isinstance(value, str) and value.strip().isascii() and value.strip().isdigit():
        number = int(value.strip())
    else:
        raise ValueError("Expected nonnegative integer")
    if number < 0:
        raise ValueError("Expected nonnegative integer")
    return number


def _positive_year(value: Any) -> int | None:
    number = _nonnegative_int(value)
    return None if number in (None, 0) else number


def _month(value: Any) -> int | None:
    number = _nonnegative_int(value)
    if number in (None, 0):
        return None  # 0 is a source sentinel for quarter-based bidding dates.
    if not 1 <= number <= 12:
        raise ValueError("Month outside 1..12")
    return number


def _quarter(value: Any) -> str | None:
    result = clean_text(value)
    return None if result in (None, "0") else result


@register_transformer(TABLE_NAME)
class PackageDetailTransformer(BaseResourceTransformer):
    """Transform one immutable Package observation into a typed revision."""

    @property
    def table_name(self) -> str:
        return TABLE_NAME

    @property
    def entity_type(self) -> str:
        return "package"

    @property
    def id_scheme(self) -> str:
        # Existing Silver contract uses 'uuid' as a scheme label, even for
        # source IDs prefixed with 'mi_' and historical numeric identifiers.
        return "uuid"

    def extract_root(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        return payload if isinstance(payload, dict) and payload else None

    def extract_source_identity(self, root: dict[str, Any]) -> str | None:
        return _valid_reference(root.get("id"))

    @staticmethod
    def _issue(
        issues: list[QualityIssue],
        obs_id: str,
        code: str,
        path: str,
    ) -> None:
        issues.append(
            QualityIssue(
                issue_id=digest([obs_id, code, path]),
                observation_id=obs_id,
                code=code,
                path=path,
                severity="warn",
            )
        )

    def _optional(
        self,
        value: Any,
        *,
        parser: Callable[[Any], Any],
        path: str,
        issue_code: str,
        issues: list[QualityIssue],
        obs_id: str,
    ) -> Any:
        try:
            return parser(value)
        except (TypeError, ValueError):
            self._issue(issues, obs_id, issue_code, path)
            return None

    def extract_attributes(
        self,
        root: dict[str, Any],
        revision_id: str,
        entity_id: str,
        issues: list[QualityIssue],
        obs_id: str,
    ) -> dict[str, Any]:
        amount = self._optional(
            root.get("bidPrice"),
            parser=parse_money,
            path="bidPrice",
            issue_code="invalid_money",
            issues=issues,
            obs_id=obs_id,
        )
        currency = clean_text(root.get("bidPriceUnit"))
        if amount is not None and currency is None:
            self._issue(issues, obs_id, "unknown_currency", "bidPriceUnit")

        # Fallback date resolution for Iceberg partitioning
        public_date = (
            self._optional(
                root.get("createdDate"),
                parser=parse_date,
                path="createdDate",
                issue_code="invalid_date",
                issues=issues,
                obs_id=obs_id,
            )
            or self._optional(
                root.get("originalCreateDate"),
                parser=parse_date,
                path="originalCreateDate",
                issue_code="invalid_date",
                issues=issues,
                obs_id=obs_id,
            )
            or self._optional(
                root.get("exchangeDate"),
                parser=parse_date,
                path="exchangeDate",
                issue_code="invalid_date",
                issues=issues,
                obs_id=obs_id,
            )
        )

        typed = {
            "revision_id": revision_id,
            "entity_id": entity_id,
            "entity_type": self.entity_type,
            "package_no": clean_text(root.get("bidNo")),
            "title": clean_text(root.get("bidName")),
            "plan_no": _valid_reference(root.get("planNo")),
            # Source planId is a reference, not the canonical Silver FK.
            "plan_id": None,
            "bid_price": amount,
            "currency": currency,
            "process_apply": clean_text(root.get("processApply")),
            "bid_field": clean_text(root.get("bidField")),
            "bid_form": clean_text(root.get("bidForm")),
            "bid_mode": clean_text(root.get("bidMode")),
            "capital_detail": clean_text(root.get("capitalDetail")),
            "contract_type": clean_text(root.get("ctype")),
            "execution_period": self._optional(
                root.get("cperiod"),
                parser=_nonnegative_int,
                path="cperiod",
                issue_code="invalid_count",
                issues=issues,
                obs_id=obs_id,
            ),
            "execution_period_unit": clean_text(root.get("cperiodUnit")),
            "is_domestic": self._optional(
                root.get("isDomestic"),
                parser=parse_boolean,
                path="isDomestic",
                issue_code="invalid_boolean",
                issues=issues,
                obs_id=obs_id,
            ),
            "is_internet": self._optional(
                root.get("isInternet"),
                parser=parse_boolean,
                path="isInternet",
                issue_code="invalid_boolean",
                issues=issues,
                obs_id=obs_id,
            ),
            "bid_start_unit": clean_text(root.get("bidStartUnit")),
            "bid_start_year": self._optional(
                root.get("bidStartYear"),
                parser=_positive_year,
                path="bidStartYear",
                issue_code="invalid_year",
                issues=issues,
                obs_id=obs_id,
            ),
            "bid_start_quarter": _quarter(root.get("bidStartQuarter")),
            "bid_start_month": self._optional(
                root.get("bidStartMonth"),
                parser=_month,
                path="bidStartMonth",
                issue_code="invalid_month",
                issues=issues,
                obs_id=obs_id,
            ),
            "public_date": public_date,
            "root_json": canonical_json(root),
        }
        assert set(typed) == set(PACKAGE_REVISION_COLUMNS)
        return typed

    def extract_references(
        self,
        root: dict[str, Any],
        revision_id: str,
    ) -> list[ExtractedReference]:
        plan_id = _valid_reference(root.get("planId"))
        plan_no = _valid_reference(root.get("planNo"))
        if plan_id is not None:
            scheme, identity, field = "uuid", plan_id, "planId"
        elif plan_no is not None:
            scheme, identity, field = "number", plan_no, "planNo"
        else:
            return []
        return [
            ExtractedReference(
                relationship_id=digest([revision_id, "plan", scheme, identity, field]),
                from_revision_id=revision_id,
                target_type="plan",
                id_scheme=scheme,
                target_identity=identity,
                field_name=field,
            )
        ]

    def extract_children(
        self,
        payload: dict[str, Any],
        revision_id: str,
        issues: list[QualityIssue],
        obs_id: str,
    ) -> list[ExtractedChild]:
        children: list[ExtractedChild] = []
        locations = payload.get("bidLocation")
        if locations is None:
            locations = []
        if not isinstance(locations, list):
            self._issue(issues, obs_id, "invalid_child_collection", "bidLocation")
            locations = []
        for ordinal, location in enumerate(locations):
            path = f"bidLocation[{ordinal}]"
            if not isinstance(location, dict):
                self._issue(issues, obs_id, "invalid_child", path)
                continue
            child_id = digest([revision_id, "location", ordinal])
            location_row = {
                "location_id": child_id,
                "revision_id": revision_id,
                "location_ordinal": ordinal,
                "province_code": clean_pipe_code(location.get("provCode")),
                "province_name": clean_text(location.get("provName")),
                "district_code": clean_pipe_code(location.get("districtCode")),
                "district_name": clean_text(location.get("districtName")),
                "ward_code": clean_pipe_code(location.get("wardCode")),
                "ward_name": clean_text(location.get("wardName")),
            }
            assert set(location_row) == set(PACKAGE_LOCATION_COLUMNS)
            children.append(
                ExtractedChild(
                    child_id=child_id,
                    revision_id=revision_id,
                    kind="package_location",
                    role="bid_location",
                    path=path,
                    ordinal=ordinal,
                    source_id=clean_text(location.get("id")),
                    payload_json=canonical_json(location_row),
                    typed_table="package_location",
                    typed_attributes=location_row,
                )
            )

        lots = payload.get("bidpBidLotList")
        if lots is None:
            lots = []
        if not isinstance(lots, list):
            self._issue(issues, obs_id, "invalid_child_collection", "bidpBidLotList")
            lots = []
        for ordinal, lot in enumerate(lots):
            path = f"bidpBidLotList[{ordinal}]"
            if not isinstance(lot, dict):
                self._issue(issues, obs_id, "invalid_child", path)
                continue
            source_lot_id = _valid_reference(lot.get("id"))
            lot_id = digest([revision_id, "lot", ordinal, source_lot_id])
            lot_price = self._optional(
                lot.get("lotPrice"),
                parser=parse_money,
                path=f"{path}.lotPrice",
                issue_code="invalid_money",
                issues=issues,
                obs_id=obs_id,
            )
            quantity = self._optional(
                lot.get("quantity"),
                parser=parse_money,
                path=f"{path}.quantity",
                issue_code="invalid_quantity",
                issues=issues,
                obs_id=obs_id,
            )
            unit_price = self._optional(
                lot.get("pricePlan"),
                parser=parse_money,
                path=f"{path}.pricePlan",
                issue_code="invalid_money",
                issues=issues,
                obs_id=obs_id,
            )
            # Inherit only an explicitly supplied package currency; never
            # hardcode VND. Preserve an explicit lot-level currency if present.
            lot_currency = clean_text(lot.get("lotPriceUnit")) or clean_text(
                payload.get("bidPriceUnit")
            )
            lot_row = {
                "lot_id": lot_id,
                "revision_id": revision_id,
                "source_lot_id": source_lot_id,
                "lot_ordinal": ordinal,
                "lot_no": clean_text(lot.get("lotNo")),
                "lot_name": clean_text(lot.get("lotName")),
                "lot_price": lot_price,
                "currency": lot_currency,
                "medicine_code": clean_text(lot.get("medicineCode")),
                "quantity": quantity,
                "uom": clean_text(lot.get("uom")),
                "planned_unit_price": unit_price,
            }
            assert set(lot_row) == set(PACKAGE_LOT_COLUMNS)
            children.append(
                ExtractedChild(
                    child_id=lot_id,
                    revision_id=revision_id,
                    kind="package_lot",
                    role="package_lot",
                    path=path,
                    ordinal=ordinal,
                    source_id=source_lot_id,
                    payload_json=canonical_json(lot_row),
                    typed_table="package_lot",
                    typed_attributes=lot_row,
                )
            )
        return children


__all__ = [
    "PACKAGE_LOCATION_COLUMNS",
    "PACKAGE_LOT_COLUMNS",
    "PACKAGE_REVISION_COLUMNS",
    "PackageDetailTransformer",
]
