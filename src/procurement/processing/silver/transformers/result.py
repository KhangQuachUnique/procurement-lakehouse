"""Transformer for Contractor Result Detail (table: contractor_result_detail).

Transforms Bronze records of KQLCNT (Kết quả lựa chọn nhà thầu) into Silver
result_revision, lot, and bid_participation records following the standard schema.
"""

from typing import Any

from procurement.processing.silver.parsers import (
    clean_text,
    lookup_path,
    parse_boolean,
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


@register_transformer("contractor_result_detail")
class ResultDetailTransformer(BaseResourceTransformer):
    """Transformer for procurement results (Kết quả lựa chọn nhà thầu)."""

    @property
    def table_name(self) -> str:
        return "contractor_result_detail"

    @property
    def entity_type(self) -> str:
        return "result"

    @property
    def id_scheme(self) -> str:
        return "uuid"

    def extract_root(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        """Extract root dictionary from bideContractorInputResultDTO.

        Falls back to top-level payload if the DTO key is missing but an 'id' exists.
        """
        dto = lookup_path(payload, "bideContractorInputResultDTO")
        if not isinstance(dto, dict):
            return payload if isinstance(payload, dict) and "id" in payload else None
        return dto

    def extract_source_identity(self, root: dict[str, Any]) -> str | None:
        """The primary key of result is 'id' inside bideContractorInputResultDTO."""
        return clean_text(root.get("id"))

    def extract_attributes(
        self,
        root: dict[str, Any],
        revision_id: str,
        entity_id: str,
        issues: list[QualityIssue],
        obs_id: str,
    ) -> dict[str, Any]:
        """Normalize domain attributes for result_revision."""
        # Monetary validation - total winning price (bidPrice in the DTO)
        raw_bid_price = root.get("bidPrice")
        total_winning_price = None
        if raw_bid_price is not None and raw_bid_price != "":
            try:
                total_winning_price = parse_money(raw_bid_price)
            except ValueError:
                issues.append(
                    QualityIssue(
                        issue_id=digest([obs_id, "invalid_money", "bidPrice"]),
                        observation_id=obs_id,
                        code="invalid_money",
                        path="bideContractorInputResultDTO.bidPrice",
                        severity="warn",
                    )
                )

        currency = clean_text(root.get("bidPriceUnit"))
        if total_winning_price is not None and currency is None:
            issues.append(
                QualityIssue(
                    issue_id=digest([obs_id, "unknown_currency", "bidPriceUnit"]),
                    observation_id=obs_id,
                    code="unknown_currency",
                    path="bideContractorInputResultDTO.bidPriceUnit",
                    severity="warn",
                )
            )

        # Decision date validation
        decision_date = None
        raw_decision_date = root.get("decisionDate")
        if raw_decision_date is not None and raw_decision_date != "":
            try:
                decision_date = parse_date(raw_decision_date)
            except ValueError:
                issues.append(
                    QualityIssue(
                        issue_id=digest([obs_id, "invalid_date", "decisionDate"]),
                        observation_id=obs_id,
                        code="invalid_date",
                        path="bideContractorInputResultDTO.decisionDate",
                        severity="warn",
                    )
                )

        # Public date validation
        public_date = None
        raw_public_date = root.get("publicDate")
        if raw_public_date is not None and raw_public_date != "":
            try:
                public_date = parse_date(raw_public_date)
            except ValueError:
                issues.append(
                    QualityIssue(
                        issue_id=digest([obs_id, "invalid_date", "publicDate"]),
                        observation_id=obs_id,
                        code="invalid_date",
                        path="bideContractorInputResultDTO.publicDate",
                        severity="warn",
                    )
                )

        return {
            "revision_id": revision_id,
            "entity_id": entity_id,
            "entity_type": self.entity_type,
            "result_id": clean_text(root.get("id")),
            "result_version": clean_text(root.get("resultVersion")),
            "notify_no": clean_text(root.get("notifyNo")),
            "notify_version": clean_text(root.get("notifyVersion")),
            "package_no": clean_text(root.get("bidNo")),
            "title": clean_text(root.get("bidName")),
            "decision_no": clean_text(root.get("decisionNo")),
            "decision_date": decision_date,
            "total_winning_price": total_winning_price,
            "currency": currency,
            "public_date": public_date,
            "status_raw": clean_text(root.get("status")),
            "root_json": canonical_json(root),
        }

    def extract_references(
        self,
        root: dict[str, Any],
        revision_id: str,
    ) -> list[ExtractedReference]:
        """Extract foreign key references to notice (by notify_no) and package (by bidId)."""
        refs: list[ExtractedReference] = []

        # Reference to notice via notifyNo
        notify_no = clean_text(root.get("notifyNo"))
        refs.append(
            ExtractedReference(
                relationship_id=digest([revision_id, "notice", "notify_no", notify_no]),
                from_revision_id=revision_id,
                target_type="notice",
                id_scheme="notify_no",
                target_identity=notify_no,
                field_name="notifyNo",
            )
        )

        # Reference to package via bidId
        bid_id = clean_text(root.get("bidId"))
        refs.append(
            ExtractedReference(
                relationship_id=digest([revision_id, "package", "uuid", bid_id]),
                from_revision_id=revision_id,
                target_type="package",
                id_scheme="uuid",
                target_identity=bid_id,
                field_name="bidId",
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
        """Extract lot and bid_participation children from lotResultDTO."""
        children: list[ExtractedChild] = []
        root = payload.get("bideContractorInputResultDTO", {})
        lot_list = root.get("lotResultDTO")
        if not isinstance(lot_list, list):
            return children

        for lot_idx, lot_raw in enumerate(lot_list):
            if not isinstance(lot_raw, dict):
                continue

            lot_no = clean_text(lot_raw.get("lotNo"))

            # Parse lot-level monetary values
            lot_price = None
            raw_lot_price = lot_raw.get("lotPrice")
            if raw_lot_price is not None and raw_lot_price != "":
                try:
                    lot_price = parse_money(raw_lot_price)
                except ValueError:
                    issues.append(
                        QualityIssue(
                            issue_id=digest([obs_id, "invalid_money", f"lot[{lot_idx}].lotPrice"]),
                            observation_id=obs_id,
                            code="invalid_money",
                            path=f"bideContractorInputResultDTO.lotResultDTO[{lot_idx}].lotPrice",
                            severity="warn",
                        )
                    )

            estimate_price = None
            raw_estimate = lot_raw.get("estimatePrice")
            if raw_estimate is not None and raw_estimate != "":
                try:
                    estimate_price = parse_money(raw_estimate)
                except ValueError:
                    issues.append(
                        QualityIssue(
                            issue_id=digest([obs_id, "invalid_money", f"lot[{lot_idx}].estimatePrice"]),
                            observation_id=obs_id,
                            code="invalid_money",
                            path=f"bideContractorInputResultDTO.lotResultDTO[{lot_idx}].estimatePrice",
                            severity="warn",
                        )
                    )

            lot_data = {
                "lot_no": lot_no,
                "lot_name": clean_text(lot_raw.get("lotName")),
                "lot_price": lot_price,
                "estimate_price": estimate_price,
                "currency": clean_text(lot_raw.get("bidPriceUnit") or root.get("bidPriceUnit")),
                "winning_code": clean_text(lot_raw.get("winningCode")),
                "status_raw": clean_text(lot_raw.get("status")),
            }
            lot_id = digest([revision_id, "lot", lot_idx, lot_data])
            children.append(
                ExtractedChild(
                    child_id=lot_id,
                    revision_id=revision_id,
                    kind="lot",
                    role="result_lot",
                    path=f"bideContractorInputResultDTO.lotResultDTO[{lot_idx}]",
                    ordinal=lot_idx,
                    source_id=lot_no,
                    payload_json=canonical_json(lot_data),
                )
            )

            # Extract bid participations (contractors) within each lot
            contractor_list = lot_raw.get("contractorList")
            if not isinstance(contractor_list, list):
                continue

            for c_idx, contractor in enumerate(contractor_list):
                if not isinstance(contractor, dict):
                    continue

                # Parse contractor bid price
                bid_price = None
                raw_bid = contractor.get("bidPrice")
                if raw_bid is not None and raw_bid != "":
                    try:
                        bid_price = parse_money(raw_bid)
                    except ValueError:
                        issues.append(
                            QualityIssue(
                                issue_id=digest([obs_id, "invalid_money", f"lot[{lot_idx}].contractor[{c_idx}].bidPrice"]),
                                observation_id=obs_id,
                                code="invalid_money",
                                path=f"bideContractorInputResultDTO.lotResultDTO[{lot_idx}].contractorList[{c_idx}].bidPrice",
                                severity="warn",
                            )
                        )

                # Parse winning price
                winning_price = None
                raw_winning = contractor.get("bidWiningPrice")
                if raw_winning is not None and raw_winning != "":
                    try:
                        winning_price = parse_money(raw_winning)
                    except ValueError:
                        issues.append(
                            QualityIssue(
                                issue_id=digest([obs_id, "invalid_money", f"lot[{lot_idx}].contractor[{c_idx}].bidWiningPrice"]),
                                observation_id=obs_id,
                                code="invalid_money",
                                path=f"bideContractorInputResultDTO.lotResultDTO[{lot_idx}].contractorList[{c_idx}].bidWiningPrice",
                                severity="warn",
                            )
                        )

                # Determine winner status
                is_winner = None
                raw_winner = contractor.get("isWinner")
                if raw_winner is not None:
                    try:
                        is_winner = parse_boolean(raw_winner)
                    except ValueError:
                        pass
                # Fallback: if winning price exists, likely a winner
                if is_winner is None and winning_price is not None:
                    is_winner = True

                participant_data = {
                    "lot_no": lot_no,
                    "contractor_code": clean_text(contractor.get("contractorCode")),
                    "contractor_name": clean_text(contractor.get("contractorName")),
                    "tax_code": clean_text(contractor.get("taxCode")),
                    "is_consortium": None,
                    "consortium_name": clean_text(contractor.get("consortiumName")),
                    "bid_price": bid_price,
                    "winning_price": winning_price,
                    "currency": clean_text(
                        contractor.get("bidPriceUnit")
                        or lot_raw.get("bidPriceUnit")
                        or root.get("bidPriceUnit")
                    ),
                    "is_winner": is_winner,
                    "evaluation_rank": contractor.get("evaluationRank"),
                }

                raw_consortium = contractor.get("isConsortium")
                if raw_consortium is not None:
                    try:
                        participant_data["is_consortium"] = parse_boolean(raw_consortium)
                    except ValueError:
                        pass

                participant_id = digest([revision_id, "participant", lot_idx, c_idx, participant_data])
                children.append(
                    ExtractedChild(
                        child_id=participant_id,
                        revision_id=revision_id,
                        kind="participant",
                        role="bid_participation",
                        path=f"bideContractorInputResultDTO.lotResultDTO[{lot_idx}].contractorList[{c_idx}]",
                        ordinal=c_idx,
                        source_id=clean_text(contractor.get("taxCode") or contractor.get("contractorCode")),
                        payload_json=canonical_json(participant_data),
                    )
                )

        return children
