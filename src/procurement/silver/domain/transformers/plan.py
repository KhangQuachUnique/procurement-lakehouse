"""Silver transformer for KHLCNT Plan detail (khlcnt_plan_detail).

The transformer emits the typed business columns of plan_revision.
The shared Silver framework owns entity/revision identifiers, lineage,
reference resolution, and persistence.
"""

from typing import Any, Callable

from procurement.silver.domain.parsers import (
    clean_project_reference,
    clean_text,
    lookup_path,
    parse_date,
    parse_money,
)
from procurement.silver.domain.protocols import ExtractedReference, QualityIssue
from procurement.silver.domain.registry import register_transformer
from procurement.silver.domain.selection import digest
from procurement.silver.domain.transformers.base import (
    BaseResourceTransformer,
    canonical_json,
)

ROOT_PATH = "bidPoBidpPlanProjectDetailView"


@register_transformer("khlcnt_plan_detail")
class PlanDetailTransformer(BaseResourceTransformer):
    """Transform one KHLCNT Plan observation into a typed Plan revision."""

    @property
    def table_name(self) -> str:
        return "khlcnt_plan_detail"

    @property
    def entity_type(self) -> str:
        return "plan"

    @property
    def id_scheme(self) -> str:
        # Must match the framework contract for Plan source IDs.
        return "uuid"

    def extract_root(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        """Only the Plan DTO is the authoritative root for this resource."""
        root = lookup_path(payload, ROOT_PATH)
        return root if isinstance(root, dict) else None

    def extract_source_identity(self, root: dict[str, Any]) -> str | None:
        """Use the Plan source ID, not planNo or the Bronze row source_id."""
        return clean_text(root.get("id"))

    @staticmethod
    def _parse_bid_pack(value: Any) -> int | None:
        """Parse bidPack as an exact, non-negative integer (no floats)."""
        if value is None or value == "":
            return None
        if isinstance(value, bool):
            raise ValueError("bidPack must not be boolean")
        if isinstance(value, int):
            parsed = value
        elif (
            isinstance(value, str)
            and value.strip().isascii()
            and value.strip().isdigit()
        ):
            parsed = int(value.strip())
        else:
            raise ValueError("bidPack must be a non-negative integer")
        if parsed < 0:
            raise ValueError("bidPack must be non-negative")
        return parsed

    @staticmethod
    def _parse_optional(
        value: Any,
        *,
        parser: Callable[[Any], Any],
        field: str,
        code: str,
        issues: list[QualityIssue],
        obs_id: str,
    ) -> Any:
        """Preserve the Plan; warn and null only the invalid optional field."""
        try:
            return parser(value)
        except (ValueError, TypeError):
            issues.append(
                QualityIssue(
                    issue_id=digest([obs_id, code, field]),
                    observation_id=obs_id,
                    code=code,
                    path=f"{ROOT_PATH}.{field}",
                    severity="warn",
                )
            )
            return None

    def extract_attributes(
        self,
        root: dict[str, Any],
        revision_id: str,
        entity_id: str,
        issues: list[QualityIssue],
        obs_id: str,
    ) -> dict[str, Any]:
        """Return the agreed typed fields for plan_revision."""
        amount = self._parse_optional(
            root.get("investTotal"),
            parser=parse_money,
            field="investTotal",
            code="invalid_money",
            issues=issues,
            obs_id=obs_id,
        )
        currency = clean_text(root.get("investTotalUnit"))
        if amount is not None and currency is None:
            issues.append(
                QualityIssue(
                    issue_id=digest([obs_id, "unknown_currency", "investTotalUnit"]),
                    observation_id=obs_id,
                    code="unknown_currency",
                    path=f"{ROOT_PATH}.investTotalUnit",
                    severity="warn",
                )
            )

        decision_date = self._parse_optional(
            root.get("decisionDate"),
            parser=parse_date,
            field="decisionDate",
            code="invalid_date",
            issues=issues,
            obs_id=obs_id,
        )
        public_date = self._parse_optional(
            root.get("publicDate"),
            parser=parse_date,
            field="publicDate",
            code="invalid_date",
            issues=issues,
            obs_id=obs_id,
        )
        bid_pack = self._parse_optional(
            root.get("bidPack"),
            parser=self._parse_bid_pack,
            field="bidPack",
            code="invalid_count",
            issues=issues,
            obs_id=obs_id,
        )

        return {
            "revision_id": revision_id,
            "entity_id": entity_id,
            "entity_type": self.entity_type,
            "plan_no": clean_text(root.get("planNo")),
            "title": clean_text(root.get("name")),
            "investor_name": clean_text(root.get("investorName")),
            # Source pid is NOT a Silver FK. It is recorded in relationship
            # via extract_references(); a later resolver populates this field.
            "project_id": None,
            "project_no": clean_project_reference(root.get("pno")),
            "plan_type": clean_text(root.get("planType")),
            "total_investment": amount,
            "currency": currency,
            "decision_date": decision_date,
            "decision_no": clean_text(root.get("decisionNo")),
            "decision_agency": clean_text(root.get("decisionAgency")),
            "bid_pack": bid_pack,
            "location": clean_text(root.get("location")),
            "public_date": public_date,
            "status_raw": clean_text(root.get("status")),
            "root_json": canonical_json(root),
        }

    def extract_references(
        self,
        root: dict[str, Any],
        revision_id: str,
    ) -> list[ExtractedReference]:
        """Declare Plan -> Project by pid, with pno as a guarded fallback.

        Reference extraction is independent of planType (DTPT/DTMS/TX/KHAC).
        Actual Project presence in Silver is resolved by the shared pipeline.
        """
        pid = clean_project_reference(root.get("pid"))
        pno = clean_project_reference(root.get("pno"))

        if pid is not None:
            # "uuid" is the current shared Project id_scheme even when the
            # historic source identifier consists of digits only.
            scheme, identity, field = "uuid", pid, "pid"
        elif pno is not None:
            scheme, identity, field = "number", pno, "pno"
        else:
            return []

        return [
            ExtractedReference(
                relationship_id=digest(
                    [revision_id, "project", scheme, identity, field]
                ),
                from_revision_id=revision_id,
                target_type="project",
                id_scheme=scheme,
                target_identity=identity,
                field_name=field,
            )
        ]


__all__ = ["PlanDetailTransformer"]
