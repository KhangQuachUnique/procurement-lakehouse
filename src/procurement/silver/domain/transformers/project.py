"""Reference Implementation: Transformer for Project Detail (table: project_detail).

Team members can use this file as a template for implementing their assigned resources.
"""

from typing import Any

from procurement.silver.domain.parsers import (
    clean_pipe_code,
    clean_text,
    lookup_path,
    parse_date,
    parse_money,
)
from procurement.silver.domain.protocols import (
    ExtractedChild,
    ExtractedReference,
    QualityIssue,
    RawRecordEnvelope,
    TransformedRecord,
)
from procurement.silver.domain.registry import register_transformer
from procurement.silver.domain.selection import digest
from procurement.silver.domain.transformers.base import (
    BaseResourceTransformer,
    canonical_json,
)


@register_transformer("project_detail")
class ProjectDetailTransformer(BaseResourceTransformer):
    """Transformer for procurement projects (Dự án đầu tư)."""

    @property
    def table_name(self) -> str:
        return "project_detail"

    @property
    def entity_type(self) -> str:
        return "project"

    @property
    def id_scheme(self) -> str:
        return "uuid"

    def extract_root(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        """Extract root dictionary from payload, merging projectDTO with top-level metadata."""
        pdto = lookup_path(payload, "projectDTO")
        if not isinstance(pdto, dict):
            return payload if isinstance(payload, dict) and "id" in payload else None

        merged = {**payload, **pdto}
        for loc_key in ("provCode", "provName", "districtCode", "districtName", "plocation"):
            if loc_key in payload and not pdto.get(loc_key):
                merged[loc_key] = payload[loc_key]
        return merged

    def extract_source_identity(self, root: dict[str, Any]) -> str | None:
        """The primary key of project is 'id' inside projectDTO."""
        return clean_text(root.get("id"))

    def extract_attributes(
        self,
        root: dict[str, Any],
        revision_id: str,
        entity_id: str,
        issues: list[QualityIssue],
        obs_id: str,
    ) -> dict[str, Any]:
        """Normalize domain attributes for project."""
        # Monetary validation
        raw_invest_total = root.get("investTotal")
        amount = None
        if raw_invest_total is not None and raw_invest_total != "":
            try:
                amount = parse_money(raw_invest_total)
            except ValueError:
                issues.append(
                    QualityIssue(
                        issue_id=digest([obs_id, "invalid_money", "investTotal"]),
                        observation_id=obs_id,
                        code="invalid_money",
                        path="projectDTO.investTotal",
                        severity="warn",
                    )
                )

        currency = clean_text(root.get("investTotalUnit") or root.get("investUnit"))
        if amount is not None and currency is None:
            issues.append(
                QualityIssue(
                    issue_id=digest([obs_id, "unknown_currency", "investUnit"]),
                    observation_id=obs_id,
                    code="unknown_currency",
                    path="projectDTO.investUnit",
                    severity="warn",
                )
            )

        # Temporal validation: decision date should not be after publication date
        dec_date = parse_date(root.get("decisionDate"))
        pub_date = parse_date(root.get("publicDate"))
        if dec_date and pub_date and dec_date > pub_date:
            issues.append(
                QualityIssue(
                    issue_id=digest([obs_id, "invalid_date_order", "decisionDate"]),
                    observation_id=obs_id,
                    code="invalid_date_order",
                    path="projectDTO.decisionDate",
                    severity="warn",
                )
            )

        return {
            "revision_id": revision_id,
            "entity_id": entity_id,
            "entity_type": self.entity_type,
            "project_no": clean_text(root.get("no")),
            "business_number": clean_text(root.get("no")),
            "title": clean_text(root.get("name")),
            "investor_code": clean_text(root.get("investorCode")),
            "investor_name": clean_text(root.get("investorName")),
            "buyer_id": clean_text(root.get("investorCode")),
            "buyer_name": clean_text(root.get("investorName")),
            "total_investment": amount,
            "amount": amount,
            "currency": currency,
            "decision_no": clean_text(root.get("decisionNo")),
            "decision_date": dec_date,
            "prov_code": clean_pipe_code(root.get("provCode")),
            "district_code": clean_pipe_code(root.get("districtCode")),
            "public_date": pub_date,
            "public_date_raw": clean_text(root.get("publicDate")),
            "status_raw": clean_text(root.get("status")),
            "root_json": canonical_json(root),
        }

    def transform(self, envelope: RawRecordEnvelope) -> TransformedRecord:
        """Override transform to provide explicit source_version fallback from projectDTO."""
        from dataclasses import replace

        pdto = lookup_path(envelope.payload, "projectDTO")
        version = None
        if isinstance(pdto, dict):
            version = clean_text(pdto.get("version"))
        if not version and isinstance(envelope.payload, dict):
            version = clean_text(envelope.payload.get("pversion") or envelope.payload.get("version"))

        target_envelope = envelope
        if envelope.source_version is None and version:
            target_envelope = replace(envelope, source_version=version)

        result = super().transform(target_envelope)
        result.observation = self.build_observation(envelope)[1]
        return result

    def extract_references(
        self,
        root: dict[str, Any],
        revision_id: str,
    ) -> list[ExtractedReference]:
        """Extract foreign key references to linked procurement plans (KHLCNT)."""
        refs: list[ExtractedReference] = []
        plan_list = root.get("bidpProjectProjectPlanList") or root.get("projectPlanList")
        if isinstance(plan_list, list):
            for ordinal, item in enumerate(plan_list):
                if isinstance(item, dict):
                    plan_no = clean_text(item.get("planNo") or item.get("no"))
                    if plan_no:
                        refs.append(
                            ExtractedReference(
                                relationship_id=digest([revision_id, "plan", "number", plan_no, ordinal]),
                                from_revision_id=revision_id,
                                target_type="plan",
                                id_scheme="number",
                                target_identity=plan_no,
                                field_name="bidpProjectProjectPlanList.planNo",
                            )
                        )
        return refs

    def extract_children(
        self,
        payload: dict[str, Any],
        revision_id: str,
        issues: list[QualityIssue],
        obs_id: str,
    ) -> list[ExtractedChild]:
        """Extract location components if available."""
        children: list[ExtractedChild] = []
        root = payload.get("projectDTO")
        if not isinstance(root, dict):
            root = {}

        # Extract primary location as a child component
        prov_code = clean_pipe_code(payload.get("provCode") or root.get("provCode"))
        prov_name = clean_text(payload.get("provName") or root.get("provName"))
        district_code = clean_pipe_code(payload.get("districtCode") or root.get("districtCode"))
        district_name = clean_text(payload.get("districtName") or root.get("districtName"))
        location_name = clean_text(payload.get("plocation") or root.get("plocation"))

        if prov_code or district_code or location_name or prov_name or district_name:
            loc_data = {
                "prov_code": prov_code,
                "prov_name": prov_name,
                "district_code": district_code,
                "district_name": district_name,
                "location_name": location_name,
            }
            child_id = digest([revision_id, "location", 0, loc_data])
            typed_attributes = {
                "location_id": child_id,
                "revision_id": revision_id,
                "province_code": prov_code,
                "province_name": prov_name,
                "district_code": district_code,
                "district_name": district_name,
                "ward_code": None,
                "ward_name": None,
                "location_name": location_name,
            }
            children.append(
                ExtractedChild(
                    child_id=child_id,
                    revision_id=revision_id,
                    kind="location",
                    role="primary_location",
                    path="projectDTO.location",
                    ordinal=0,
                    source_id=prov_code,
                    payload_json=canonical_json(loc_data),
                    typed_table="project_location",
                    typed_attributes=typed_attributes,
                )
            )

        return children
