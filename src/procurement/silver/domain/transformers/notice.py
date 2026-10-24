"""Loss-aware notice adapters for standard, VK/ADB and reoffer payloads."""

import json
import re
from dataclasses import replace
from typing import Any, ClassVar

from procurement.silver.domain.contracts import VERSION
from procurement.silver.domain.parsers import (
    clean_pipe_code,
    clean_text,
    lookup_path,
    parse_boolean,
    parse_date,
    parse_iso_datetime,
    parse_money,
)
from procurement.silver.domain.protocols import (
    ExtractedChild,
    ExtractedReference,
    QualityIssue,
    QuarantineRecord,
    TransformedRecord,
)
from procurement.silver.domain.registry import register_transformer
from procurement.silver.domain.selection import digest
from procurement.silver.domain.transformers.base import BaseResourceTransformer, canonical_json

NOTICE_MAPPING_VERSION = "notice-2"


class NoticeTransformer(BaseResourceTransformer):
    """Explicit family mappings; no implicit price/currency or winner inference."""

    @property
    def entity_type(self) -> str:
        return "notice"

    @property
    def id_scheme(self) -> str:
        return "notify_no"

    mapping_version = NOTICE_MAPPING_VERSION
    revision_contracts: ClassVar[dict] = {**VERSION, "notice": NOTICE_MAPPING_VERSION}
    family = "standard"

    @property
    def table_name(self) -> str:
        return "notify_contractor_standard_detail"
    roots = ("bidoNotifyContractorM", "bidNoContractorResponse.bidNotification")

    def root_path(self, payload):
        candidates = [path for path in self.roots if isinstance(lookup_path(payload, path), dict)]
        return next(
            (
                path
                for path in candidates
                if self.extract_source_identity(lookup_path(payload, path))
            ),
            next(iter(candidates), None),
        )

    def extract_root(self, payload):
        path = self.root_path(payload)
        return lookup_path(payload, path) if path is not None else None

    def extract_source_identity(self, root):
        return clean_text(root.get("reofferNo" if self.family == "reoffer" else "notifyNo"))

    def transform(self, envelope):
        if envelope.ingested_at.tzinfo is None:
            raise ValueError("Observation time must have a timezone")
        root = self.extract_root(envelope.payload)
        if root is None:
            return super().transform(envelope)
        conflicts = []
        for field in ("id", "notifyNo", "notifyVersion", "bidId"):
            values = {
                clean_text(candidate.get(field))
                for path in self.roots
                if isinstance(candidate := lookup_path(envelope.payload, path), dict)
            } - {None}
            if len(values) > 1:
                conflicts.append(field)
        identity = self.extract_source_identity(root)
        version_field = "reofferVersion" if self.family == "reoffer" else "notifyVersion"
        version = clean_text(root.get(version_field))
        if identity and clean_text(envelope.source_id) != identity:
            conflicts.append("source_id")
        if (
            version
            and envelope.source_version is not None
            and clean_text(envelope.source_version) != version
        ):
            conflicts.append("source_version")
        if conflicts:
            oid, observation = self.build_observation(envelope)
            return TransformedRecord(
                observation=observation,
                quarantine=QuarantineRecord(oid, "conflicting_identity"),
                issues=[
                    QualityIssue(
                        digest([oid, "conflicting_identity", path]),
                        oid,
                        "conflicting_identity",
                        path,
                        "error",
                    )
                    for path in conflicts
                ],
            )
        # Use explicit source revision when the envelope has no version; preserve original observation.
        result = super().transform(
            replace(envelope, source_version=version or envelope.source_version)
        )
        result.observation = self.build_observation(envelope)[1]
        if result.typed_attributes is not None:
            attrs = result.typed_attributes
            root_path = self.root_path(envelope.payload)
            attrs["source_root_path"] = root_path
            # Only package metadata from the embedded package; never use priceInit as package budget.
            sources = json.loads(attrs["field_sources_json"])
            sources = {
                col: f"{root_path}.{path}" if root_path else path for col, path in sources.items()
            }
            context = envelope.payload.get(
                "bidDetail" if self.family == "reoffer" else "bidpPlanDetail"
            )
            if isinstance(context, dict):
                prefix = "bidDetail" if self.family == "reoffer" else "bidpPlanDetail"
                if clean_text(context.get("id")) == attrs["package_id"] and attrs["package_id"]:
                    for col, field in (("plan_id", "planId"), ("plan_version", "planVersion")):
                        attrs[col] = clean_text(context.get(field))
                        if field in context:
                            sources[col] = f"{prefix}.{field}"
                else:
                    self.issue(
                        result.issues,
                        result.observation["observation_id"],
                        "unverified_embedded_package",
                        prefix + ".id",
                    )
                # Fallback for fields residing in embedded plan detail
                if attrs.get("is_multi_lot") is None and "isMultiLot" in context:
                    attrs["is_multi_lot"] = parse_boolean(context.get("isMultiLot"))
                if attrs.get("bid_field") is None and "bidField" in context:
                    attrs["bid_field"] = clean_text(context.get("bidField"))
                if attrs.get("contract_duration_raw") is None and "cperiod" in context:
                    attrs["contract_duration_raw"] = clean_text(context.get("cperiod"))
                    attrs["contract_duration_unit"] = clean_text(context.get("cperiodUnit"))

            if attrs.get("is_multi_lot") is None and isinstance(envelope.payload.get("bidoBidStatus"), dict):
                attrs["is_multi_lot"] = parse_boolean(envelope.payload["bidoBidStatus"].get("isMultiLot"))

            # Temporal cross-field validation
            close_dt = attrs.get("bid_close_date")
            open_dt = attrs.get("bid_open_date")
            pub_dt = attrs.get("published_at")
            if close_dt and open_dt and close_dt > open_dt:
                self.issue(
                    result.issues,
                    result.observation["observation_id"],
                    "invalid_date_order",
                    "bidCloseDate > bidOpenDate",
                )
            if pub_dt and close_dt and pub_dt > close_dt:
                self.issue(
                    result.issues,
                    result.observation["observation_id"],
                    "invalid_date_order",
                    "publishedAt > bidCloseDate",
                )

            attrs["field_sources_json"] = canonical_json(sources)
        return result

    @staticmethod
    def issue(issues, oid, code, path):
        issues.append(QualityIssue(digest([oid, code, path]), oid, code, path))

    def parsed(self, value, parser, issues, oid, path, code):
        if value is None or value == "":
            return None
        try:
            if parser in (parse_date, parse_iso_datetime) and clean_text(value) is None:
                raise ValueError("Invalid date type")
            parsed = parser(value)
            return parsed.isoformat() if parser is parse_iso_datetime and parsed else parsed
        except (ValueError, TypeError, OverflowError):
            self.issue(issues, oid, code, path)
            return None

    def extract_attributes(self, root, revision_id, entity_id, issues, obs_id):
        sources = {}

        def field(column, source, parser=clean_text, code="invalid_value"):
            if source in root:
                sources[column] = source
            return self.parsed(root.get(source), parser, issues, obs_id, source, code)

        attrs = {
            "revision_id": revision_id,
            "entity_id": entity_id,
            "entity_type": "notice",
            "source_table": self.table_name,
            "source_family": self.family,
            "root_json": canonical_json(root),
            "timestamp_policy": "naive=Asia/Ho_Chi_Minh",
            "plan_id": None,
            "plan_version": None,
        }
        text_fields = {
            "source_document_id": "id",
            "notify_no": "notifyNo",
            "notify_version": "notifyVersion",
            "package_id": "bidId",
            "package_no": "bidNo",
            "title": "bidName",
            "plan_no": "planNo",
            "plan_name": "planName",
            "plan_type_raw": "planType",
            "project_name": "projectName",
            "procuring_entity_code": "procuringEntityCode",
            "procuring_entity_name": "procuringEntityName",
            "investor_code": "investorCode",
            "investor_name": "investorName",
            "capital_detail": "capitalDetail",
            "process_apply_raw": "processApply",
            "bid_field": "investField",
            "bid_form": "bidForm",
            "bid_mode": "bidMode",
            "notification_type": "notifyType",
            "status_raw": "status",
            "currency": "bidPriceUnit",
            "estimate_currency": "bidEstimatePriceUnit",
            "guarantee_currency": "bidGuaranteeUnit",
            "reoffer_currency": "reofferPriceUnit",
            "contract_type": "cType" if self.family == "vk_adb" else "contractType",
            "contract_duration_raw": "cPeriod" if self.family == "vk_adb" else "contractPeriod",
            "contract_duration_unit": "cPeriodUnit"
            if self.family == "vk_adb"
            else "contractPeriodUnit",
            "bid_validity_raw": "bidValidityPeriod",
            "bid_validity_unit": "bidValidityPeriodUnit",
            "guarantee_form": "bidGuaranteeForm" if self.family == "vk_adb" else "guaranteeForm",
            "is_medicine_raw": "isMedicine",
            "medical_type_raw": "medicalType",
            "fee_type_raw": "feeType",
            "issue_location": "issueLocation",
            "receive_location": "receiveLocation",
            "open_location": "bidOpenLocation",
            "execution_location": "executionLocation",
            "shortlist_no": "shortListNo",
            "cancel_reason": "cancelReason",
            "cancel_type_raw": "cancelType",
            "cancel_decision_no": "cancelDecisionNo",
            "cancel_decision_agency": "cancelDecisionAgency",
            "cancel_file_id": "cancelFileAttachId",
            "cancel_file_name": "cancelFileAttachName",
            "report_file_id": "reportFileId",
            "appraisal_file_id": "appraisalFileId",
            "reoffer_no": "reofferNo",
            "reoffer_version": "reofferVersion",
            "reoffer_status_raw": "statusReoffer",
            "public_date_raw": "publicDate",
            "original_public_date_raw": "originalPublicDate",
            "bid_open_date_raw": "bidOpenDate",
            "bid_close_date_raw": "bidCloseDate",
        }
        for col, source in text_fields.items():
            attrs[col] = field(col, source)
        for col, source in {
            "bid_price": "bidPrice",
            "estimate_price": "bidEstimatePrice",
            "guarantee_amount": "bidGuaranteeValue"
            if self.family == "vk_adb"
            else "guaranteeValue",
            "initial_price": "priceInit",
            "price_step": "priceStep",
            "fee_amount": "feeValue",
        }.items():
            attrs[col] = field(col, source, parse_money, "invalid_money")
        for col, source in {
            "published_at": "publicDate",
            "original_published_at": "originalPublicDate",
            "bid_open_date": "bidOpenDate",
            "bid_close_date": "bidCloseDate",
            "reoffer_open_at": "reofferOpenDate",
            "reoffer_close_at": "reofferCloseDate",
            "cancelled_at": "cancelDate",
        }.items():
            attrs[col] = field(col, source, parse_iso_datetime, "invalid_datetime")
        for col, source in {
            "public_date": "publicDate",
            "plan_decision_date": "planDecisionDate",
            "cancel_decision_date": "cancelDecisionDate",
        }.items():
            attrs[col] = field(col, source, parse_date, "invalid_date")
        for col, source in {
            "is_internet": "isInternet",
            "is_domestic": "isDomestic",
            "is_multi_lot": "isMultiLot",
            "is_prequalification": "isPrequalification",
        }.items():
            attrs[col] = field(col, source, parse_boolean, "invalid_boolean")
        attrs.update(
            business_number=attrs["notify_no"],
            buyer_id=attrs["procuring_entity_code"],
            buyer_name=attrs["procuring_entity_name"],
            amount=attrs["bid_price"],
        )
        # For standard domestic notices, guarantee and estimate prices share the tender's currency
        if self.family == "standard":
            primary_currency = attrs.get("currency") or "VND"
            if attrs.get("estimate_currency") is None and attrs.get("estimate_price") is not None:
                attrs["estimate_currency"] = primary_currency
            if attrs.get("guarantee_currency") is None and attrs.get("guarantee_amount") is not None:
                attrs["guarantee_currency"] = primary_currency

        for amount, currency in (
            ("bid_price", "currency"),
            ("estimate_price", "estimate_currency"),
            ("guarantee_amount", "guarantee_currency"),
            ("initial_price", "reoffer_currency"),
            ("price_step", "reoffer_currency"),
        ):
            if attrs[amount] is not None and attrs[currency] is None:
                self.issue(issues, obs_id, "unknown_currency", amount)
        attrs["field_sources_json"] = canonical_json(sources)
        return attrs

    def extract_references(self, root, revision_id):
        return [
            ExtractedReference(
                digest([revision_id, target, field]),
                revision_id,
                target,
                scheme,
                clean_text(root.get(field)),
                field,
            )
            for target, scheme, field in (
                ("package", "uuid", "bidId"),
                ("plan", "number", "planNo"),
            )
        ]

    def extract_children(self, payload, revision_id, issues, obs_id):
        root_path = self.root_path(payload)
        lot_path = f"{root_path}.lotDTOList" if root_path else "lotDTOList"
        mirror_path = "bidNoContractorResponse.bidNotification.lotDTOList"
        if self.family == "standard" and isinstance(lookup_path(payload, mirror_path), list):
            lot_path = mirror_path

        children = []

        # 1. Target Notice Lots directly
        lots = lookup_path(payload, lot_path)
        if isinstance(lots, list):
            for ordinal, value in enumerate(lots):
                table, attributes = self.child_projection(
                    value, lot_path, ordinal, lot_path, issues, obs_id
                )
                if attributes is not None and table is not None:
                    child_id = digest([revision_id, lot_path, ordinal, value])
                    source_id = clean_text(value.get("id")) if isinstance(value, dict) else None
                    raw = canonical_json(value)
                    attributes.update(
                        child_id=child_id,
                        revision_id=revision_id,
                        source_id=source_id,
                        source_path=lot_path,
                        ordinal=ordinal,
                        raw_json=raw,
                    )
                    children.append(
                        ExtractedChild(
                            child_id=child_id,
                            revision_id=revision_id,
                            kind=table,
                            role="lotDTOList[]",
                            path=lot_path,
                            ordinal=ordinal,
                            source_id=source_id,
                            payload_json=raw,
                            typed_table=table,
                            typed_attributes=attributes,
                        )
                    )

        # 2. Target Performance Locations directly
        for loc_path in ("bidpBidLocationList", "lsBidpBidLocationDTO", "bidDetail.bidLocation"):
            locations = lookup_path(payload, loc_path)
            if isinstance(locations, list):
                for ordinal, value in enumerate(locations):
                    table, attributes = self.child_projection(
                        value, loc_path, ordinal, lot_path, issues, obs_id
                    )
                    if attributes is not None and table is not None:
                        child_id = digest([revision_id, loc_path, ordinal, value])
                        source_id = clean_text(value.get("id")) if isinstance(value, dict) else None
                        raw = canonical_json(value)
                        attributes.update(
                            child_id=child_id,
                            revision_id=revision_id,
                            source_id=source_id,
                            source_path=loc_path,
                            ordinal=ordinal,
                            raw_json=raw,
                        )
                        children.append(
                            ExtractedChild(
                                child_id=child_id,
                                revision_id=revision_id,
                                kind=table,
                                role=re.sub(r"\[\d+\]", "[]", loc_path),
                                path=loc_path,
                                ordinal=ordinal,
                                source_id=source_id,
                                payload_json=raw,
                                typed_table=table,
                                typed_attributes=attributes,
                            )
                        )

        # 3. Target Reoffer Submissions directly
        if self.family == "reoffer":
            for sub_path in ("contractorsResult", "contractorsResultAll"):
                subs = lookup_path(payload, sub_path)
                if isinstance(subs, list):
                    for ordinal, value in enumerate(subs):
                        table, attributes = self.child_projection(
                            value, sub_path, ordinal, lot_path, issues, obs_id
                        )
                        if attributes is not None and table is not None:
                            child_id = digest([revision_id, sub_path, ordinal, value])
                            source_id = (
                                clean_text(value.get("id")) if isinstance(value, dict) else None
                            )
                            raw = canonical_json(value)
                            attributes.update(
                                child_id=child_id,
                                revision_id=revision_id,
                                source_id=source_id,
                                source_path=sub_path,
                                ordinal=ordinal,
                                raw_json=raw,
                            )
                            children.append(
                                ExtractedChild(
                                    child_id=child_id,
                                    revision_id=revision_id,
                                    kind=table,
                                    role=sub_path + "[]",
                                    path=sub_path,
                                    ordinal=ordinal,
                                    source_id=source_id,
                                    payload_json=raw,
                                    typed_table=table,
                                    typed_attributes=attributes,
                                )
                            )

        return children

    def child_projection(
        self, value, path, ordinal, lot_path, issues, obs_id
    ) -> tuple[str | None, dict[str, Any] | None]:
        """Only reviewed paths receive typed projections; all others remain raw."""
        if not isinstance(value, dict):
            return None, None

        def parse(key, parser=parse_money, code="invalid_money"):
            return self.parsed(
                value.get(key), parser, issues, obs_id, f"{path}[{ordinal}].{key}", code
            )

        if path == lot_path:
            return "notice_lot", {
                "lot_no": clean_text(value.get("lotNo")),
                "lot_name": clean_text(value.get("lotName")),
                "lot_price": parse("lotPrice"),
                "estimate_price": parse("lotEstimatePrice"),
                "currency": clean_text(value.get("lotPriceUnit")),
                "guarantee_amount": parse("lotGuaranteeValue"),
                "contract_duration_raw": clean_text(value.get("cperiod")),
                "contract_duration_unit": clean_text(value.get("cperiodUnit")),
                "notify_id": clean_text(value.get("notifyId")),
                "notify_no": clean_text(value.get("notifyNo")),
            }
        if path in {"bidpBidLocationList", "lsBidpBidLocationDTO", "bidDetail.bidLocation"}:
            fields = {
                "province_code": "provCode",
                "province_name": "provName",
                "district_code": "districtCode",
                "district_name": "districtName",
                "ward_code": "wardCode",
                "ward_name": "wardName",
            }
            return "notice_location", {
                column: (clean_pipe_code if column.endswith("code") else clean_text)(value.get(key))
                for column, key in fields.items()
            }
        if self.family == "reoffer" and path in {"contractorsResult", "contractorsResultAll"}:
            fields = {
                "submission_id": "reofferSubmissionId",
                "reoffer_id": "reofferId",
                "lot_no": "lotNo",
                "contractor_code": "contractorCode",
                "contractor_name": "contractorName",
                "venture_code": "ventureCode",
                "venture_name": "ventureName",
                "times_raw": "times",
                "offered_at_raw": "reofferDate",
                "form_code": "formCode",
                "form_value": "formValue",
                "currency": "reofferPriceUnit",
            }
            attributes = {column: clean_text(value.get(key)) for column, key in fields.items()}
            attributes.update(
                source_list=path,
                price=parse("reofferPrice"),
                final_price=parse("reofferPriceFinal"),
                offered_at=parse("reofferDate", parse_iso_datetime, "invalid_datetime"),
                is_newest=parse("isNewest", parse_boolean, "invalid_boolean"),
            )
            return "notice_reoffer_submission", attributes
        return None, None


@register_transformer("notify_contractor_standard_detail")
class StandardNoticeTransformer(NoticeTransformer):
    """Standard notice, retaining both root mirrors in raw evidence."""


@register_transformer("notify_contractor_vk_adb_detail")
class VkAdbNoticeTransformer(NoticeTransformer):
    @property
    def table_name(self) -> str:
        return "notify_contractor_vk_adb_detail"

    family = "vk_adb"
    roots = ("bidoNotifyContractorP",)


@register_transformer("notify_contractor_reoffer_detail")
class ReofferNoticeTransformer(NoticeTransformer):
    @property
    def table_name(self) -> str:
        return "notify_contractor_reoffer_detail"

    family = "reoffer"
    roots = ("",)

    @property
    def id_scheme(self) -> str:
        return "reoffer_no"
