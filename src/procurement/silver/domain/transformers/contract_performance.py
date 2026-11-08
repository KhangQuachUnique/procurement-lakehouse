"""Minimal contract execution projections; unselected fields remain in root_json/Bronze."""

from typing import ClassVar

from procurement.silver.domain.parsers import clean_text, parse_boolean, parse_date, parse_money
from procurement.silver.domain.protocols import QualityIssue, RawRecordEnvelope
from procurement.silver.domain.registry import register_transformer
from procurement.silver.domain.selection import digest
from procurement.silver.domain.transformers.base import BaseResourceTransformer, canonical_json

CONTRACT_TEXT_FIELDS = {
    "source_contract_id": "id",
    "contract_code": "contractCode",
    "contract_no": "contractNo",
    "contract_version": "contractVersion",
    "contract_name": "contractName",
    "package_name": "bidName",
    "investor_code": "investorCode",
    "investor_name": "investorName",
    "contractor_code": "contractorCode",
    "contractor_name": "contractorName",
    "currency": "priceAfterUnit",
    "notice_source_id": "notifyId",
    "notice_no": "notifyNo",
    "notice_version": "notifyVersion",
    "result_source_id": "resultId",
    "result_version": "resultVersion",
    "published_at_raw": "publicDate",
}
CONTRACT_DATE_FIELDS = {
    "signed_date": "contractSignDate",
    "effective_date": "contractEffectiveDate",
    "public_date": "publicDate",
}
PERFORMANCE_TEXT_FIELDS = {
    "source_evaluation_id": "id",
    "evaluation_no": "idNo",
    "evaluation_version": "version",
    "contract_code": "contractCode",
    "contract_version": "contractVersion",
    "investor_code": "investorCode",
    "investor_name": "investorName",
    "contractor_code": "contractorCode",
    "contractor_name": "contractorName",
    "progress_status_code": "contractStatus",
    "quality_status_code": "contractQualityPer",
    "published_at_raw": "publicDate",
}
PERFORMANCE_DATE_FIELDS = {
    "public_date": "publicDate",
    "planned_completion_date": "completePlanDate",
    "actual_completion_date": "completeRealityDate",
    "adjusted_completion_date": "completeAdjustedDate",
}


class _ExecutionTransformer(BaseResourceTransformer):
    mapping_version = "contract-performance-2"
    revision_contracts: ClassVar[dict[str, str]] = {
        "mapping": mapping_version,
        "identity": "1",
        "semantic_root": "1",
    }
    root_key: str
    identity_field: str
    text_fields: dict[str, str]
    date_fields: dict[str, str]

    def extract_root(self, payload):
        root = payload.get(self.root_key)
        return root if isinstance(root, dict) else None

    def extract_source_identity(self, root):
        return clean_text(root.get(self.identity_field))

    def semantic_payload(self, envelope, root):
        # A linked sibling report or versionsList change is not a revision of this root.
        return root

    def extract_attributes(self, root, revision_id, entity_id, issues, obs_id):
        def parsed(source, parser):
            try:
                return parser(root.get(source))
            except ValueError:
                issues.append(self.issue(obs_id, "invalid_value", source))
                return None

        result = {
            "revision_id": revision_id,
            "entity_id": entity_id,
            "entity_type": self.entity_type,
            **{column: clean_text(root.get(source)) for column, source in self.text_fields.items()},
            **{column: parsed(source, parse_date) for column, source in self.date_fields.items()},
            "root_json": canonical_json(root),
        }
        if self.entity_type == "contract":
            result["price_after_tax"] = parsed("priceAfter", parse_money)
        else:
            result["is_completed"] = parsed("isContractComplete", parse_boolean)
            if result["progress_status_code"] not in {None, "01", "02", "03"}:
                issues.append(self.issue(obs_id, "unknown_progress_status", "contractStatus"))
        return result

    def issue(self, observation_id, code, source):
        return QualityIssue(
            issue_id=digest([observation_id, code, source]),
            observation_id=observation_id,
            code=code,
            path=f"{self.root_key}.{source}",
            severity="warn",
        )

    def transform(self, envelope: RawRecordEnvelope):
        result = super().transform(envelope)
        if result.typed_attributes is not None:
            result.typed_attributes.update(
                {
                    "source_date": envelope.source_date,
                    "observed_at": envelope.ingested_at.isoformat(),
                }
            )
            status = envelope.payload.get("resolution", {}).get("status")
            if self.entity_type == "contract_performance" and status != "resolved":
                result.issues.append(
                    self.issue(
                        result.observation["observation_id"],
                        status
                        if status in {"missing_contract", "missing_contract_version"}
                        else "missing_resolution",
                        "contractVersion",
                    )
                )
        return result


@register_transformer("contract_detail")
class ContractTransformer(_ExecutionTransformer):
    @property
    def table_name(self) -> str:
        return "contract_detail"

    @property
    def entity_type(self) -> str:
        return "contract"

    @property
    def id_scheme(self) -> str:
        return "contract_code"

    root_key = "contract"
    identity_field = "contractCode"
    text_fields = CONTRACT_TEXT_FIELDS
    date_fields = CONTRACT_DATE_FIELDS


@register_transformer("contract_performance_detail")
class ContractPerformanceTransformer(_ExecutionTransformer):
    @property
    def table_name(self) -> str:
        return "contract_performance_detail"

    @property
    def entity_type(self) -> str:
        return "contract_performance"

    @property
    def id_scheme(self) -> str:
        return "uuid"

    root_key = "performance"
    identity_field = "id"
    text_fields = PERFORMANCE_TEXT_FIELDS
    date_fields = PERFORMANCE_DATE_FIELDS
