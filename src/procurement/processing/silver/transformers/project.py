"""Reference Implementation: Transformer for Project Detail (table: project_detail).

Team members can use this file as a template for implementing their assigned resources.
"""

from typing import Any

from procurement.processing.silver.parsers import (
    clean_text,
    lookup_path,
    parse_date,
    parse_money,
)
from procurement.processing.silver.protocols import (
    ExtractedChild,
    ExtractedReference,
    QualityIssue,
)
from procurement.processing.silver.registry import register_transformer
from procurement.processing.silver.selection import digest
from procurement.processing.silver.transformers.base import (
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
        """Extract 'projectDTO' root from the payload."""
        root = lookup_path(payload, "projectDTO")
        return root if isinstance(root, dict) else None

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

        currency = clean_text(root.get("investUnit"))
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
            "decision_date": parse_date(root.get("decisionDate")),
            "prov_code": clean_text(root.get("provCode")),
            "district_code": clean_text(root.get("districtCode")),
            "public_date": parse_date(root.get("publicDate")),
            "public_date_raw": parse_date(root.get("publicDate")),
            "status_raw": clean_text(root.get("status")),
            "root_json": canonical_json(root),
        }

    def extract_references(
        self,
        root: dict[str, Any],
        revision_id: str,
    ) -> list[ExtractedReference]:
        """Projects are root-level entities and typically do not point to parent entities."""
        return []

    def extract_children(
        self,
        payload: dict[str, Any],
        revision_id: str,
        issues: list[QualityIssue],
        obs_id: str,
    ) -> list[ExtractedChild]:
        """Extract location components if available."""
        children: list[ExtractedChild] = []
        root = payload.get("projectDTO", {})

        # Extract primary location as a child component
        prov_code = clean_text(root.get("provCode"))
        district_code = clean_text(root.get("districtCode"))
        location_name = clean_text(root.get("plocation"))

        if prov_code or district_code or location_name:
            loc_data = {
                "prov_code": prov_code,
                "district_code": district_code,
                "location_name": location_name,
            }
            child_id = digest([revision_id, "location", 0, loc_data])
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
                )
            )

        return children
