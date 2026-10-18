"""Transformer for Contractor Selection Result (table: contractor_result_detail)."""

import contextlib
from decimal import Decimal
from typing import Any

from procurement.silver.domain.parsers import (
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


@register_transformer("contractor_result_detail")
class ContractorResultTransformer(BaseResourceTransformer):
    """Transformer for procurement contractor award results (Kết quả lựa chọn nhà thầu)."""

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
        """Extract root dictionary from payload (bideContractorInputResultDTO)."""
        root = lookup_path(payload, "bideContractorInputResultDTO")
        if isinstance(root, dict):
            return root
        if isinstance(payload, dict) and "id" in payload:
            return payload
        return None

    def extract_source_identity(self, root: dict[str, Any]) -> str | None:
        """Primary key of result is 'id' inside bideContractorInputResultDTO."""
        return clean_text(root.get("id"))

    def extract_attributes(
        self,
        root: dict[str, Any],
        revision_id: str,
        entity_id: str,
        issues: list[QualityIssue],
        obs_id: str,
    ) -> dict[str, Any]:
        """Normalize domain attributes for contractor selection result."""
        notify_no = clean_text(root.get("notifyNo"))
        package_no = clean_text(root.get("bidNo"))
        business_number = notify_no or package_no

        # Calculate total winning price across lots if available
        total_winning: Decimal | None = None
        lots = root.get("lotResultDTO")
        if isinstance(lots, list):
            for lot in lots:
                if isinstance(lot, dict):
                    for c in lot.get("contractorList", []):
                        if isinstance(c, dict) and c.get("bidResult") == 1:
                            raw_win = (
                                c.get("bidWiningPrice")
                                or c.get("lotFinalPrice")
                                or c.get("lotPrice")
                            )
                            if raw_win is not None and raw_win != "":
                                with contextlib.suppress(Exception):
                                    price = Decimal(str(raw_win))
                                    total_winning = (total_winning or Decimal(0)) + price

        # Currency fallback
        currency = clean_text(root.get("bidPriceUnit") or root.get("bidEstimatePriceUnit")) or "VND"
        total_winning_str = f"{total_winning:.6f}" if total_winning is not None else None

        dec_date = parse_date(root.get("decisionDate"))
        pub_date = parse_date(root.get("publicDate"))
        if dec_date and pub_date and dec_date > pub_date:
            issues.append(
                QualityIssue(
                    issue_id=digest([obs_id, "invalid_date_order", "decisionDate"]),
                    observation_id=obs_id,
                    code="invalid_date_order",
                    path="bideContractorInputResultDTO.decisionDate",
                    severity="warn",
                )
            )

        return {
            "revision_id": revision_id,
            "entity_id": entity_id,
            "entity_type": self.entity_type,
            "result_id": clean_text(root.get("id")),
            "result_version": clean_text(root.get("resultVersion")),
            "notify_no": notify_no,
            "notify_version": clean_text(root.get("notifyVersion")),
            "business_number": business_number,
            "bid_id": clean_text(root.get("bidId")),
            "package_no": package_no,
            "title": clean_text(root.get("bidName")),
            "procuring_entity_code": clean_text(root.get("procuringEntityCode")),
            "procuring_entity_name": clean_text(root.get("procuringEntityName")),
            "buyer_id": clean_text(root.get("procuringEntityCode")),
            "buyer_name": clean_text(root.get("procuringEntityName")),
            "investor_code": clean_text(root.get("investorCode")),
            "investor_name": clean_text(root.get("investorName")),
            "decision_no": clean_text(root.get("decisionNo")),
            "decision_date": dec_date,
            "decision_agency": clean_text(root.get("decisionAgency")),
            "total_winning_price": total_winning_str,
            "amount": total_winning_str,
            "currency": currency,
            "public_date": pub_date,
            "public_date_raw": clean_text(root.get("publicDate")),
            "status_raw": clean_text(root.get("status")),
            "root_json": canonical_json(root),
        }

    def transform(self, envelope: RawRecordEnvelope) -> TransformedRecord:
        """Override transform to provide explicit source_version fallback from resultVersion."""
        from dataclasses import replace

        root = self.extract_root(envelope.payload)
        version = None
        if isinstance(root, dict):
            version = clean_text(root.get("resultVersion"))

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
        """Extract links to notice (via notifyNo), package (via bidId), and plan (via planNo)."""
        refs = []
        notify_no = clean_text(root.get("notifyNo"))
        if notify_no:
            refs.append(
                ExtractedReference(
                    digest([revision_id, "notice", notify_no]),
                    revision_id,
                    "notice",
                    "notify_no",
                    notify_no,
                    "notifyNo",
                )
            )
        bid_id = clean_text(root.get("bidId"))
        if bid_id:
            refs.append(
                ExtractedReference(
                    digest([revision_id, "package", bid_id]),
                    revision_id,
                    "package",
                    "uuid",
                    bid_id,
                    "bidId",
                )
            )
        plan_no = clean_text(root.get("planNo"))
        if plan_no:
            refs.append(
                ExtractedReference(
                    digest([revision_id, "plan", plan_no]),
                    revision_id,
                    "plan",
                    "number",
                    plan_no,
                    "planNo",
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
        """Extract award_lot and award_contractor sub-entities."""
        children: list[ExtractedChild] = []
        root = payload.get("bideContractorInputResultDTO")
        if not isinstance(root, dict):
            root = payload if isinstance(payload, dict) and "id" in payload else {}

        currency = clean_text(root.get("bidEstimatePriceUnit")) or "VND"
        lots = root.get("lotResultDTO")
        if not isinstance(lots, list):
            return children

        for ordinal, lot in enumerate(lots):
            if not isinstance(lot, dict):
                continue

            lot_id = clean_text(lot.get("id"))
            lot_no = clean_text(lot.get("lotNo") or lot.get("bidNo")) or str(ordinal + 1)
            lot_name = clean_text(lot.get("lotName") or lot.get("bidName"))

            # Lot price (estimate or budget)
            lot_price_str = None
            raw_lot_price = lot.get("lotPrice")
            if raw_lot_price is not None and raw_lot_price != "":
                try:
                    lot_price_str = parse_money(raw_lot_price)
                except ValueError:
                    issues.append(
                        QualityIssue(
                            issue_id=digest([obs_id, "invalid_money", f"lot[{ordinal}].lotPrice"]),
                            observation_id=obs_id,
                            code="invalid_money",
                            path=f"bideContractorInputResultDTO.lotResultDTO[{ordinal}].lotPrice",
                            severity="warn",
                        )
                    )

            lot_winning_code = clean_text(lot.get("winningCode"))
            lot_status = (
                clean_text(str(lot.get("status"))) if lot.get("status") is not None else None
            )
            award_lot_child_id = digest([revision_id, "award_lot", ordinal, lot_id or lot_no])

            # Process contractors under this lot
            contractors = lot.get("contractorList")
            lot_winning_price_dec: Decimal | None = None
            if isinstance(contractors, list):
                for c_idx, c in enumerate(contractors):
                    if not isinstance(c, dict):
                        continue

                    c_id = clean_text(c.get("id"))
                    contractor_code = clean_text(c.get("orgCode") or c.get("taxCode")) or "UNKNOWN"
                    contractor_name = (
                        clean_text(
                            c.get("orgFullname")
                            or c.get("newContractorName")
                            or c.get("contractorName")
                        )
                        or "UNKNOWN"
                    )
                    tax_code = clean_text(c.get("taxCode"))
                    country = clean_text(c.get("taxNation")) or "VN"
                    venture_code = clean_text(c.get("ventureCode"))
                    venture_name = clean_text(c.get("ventureName"))
                    is_consortium = bool(venture_code or venture_name)
                    role = clean_text(c.get("role"))

                    # Awarded price
                    raw_c_price = (
                        c.get("bidWiningPrice") or c.get("lotFinalPrice") or c.get("lotPrice")
                    )
                    awarded_price_str = None
                    if raw_c_price is not None and raw_c_price != "":
                        with contextlib.suppress(ValueError, ArithmeticError):
                            awarded_price_str = parse_money(raw_c_price)
                            if c.get("bidResult") == 1:
                                lot_winning_price_dec = (
                                    lot_winning_price_dec or Decimal(0)
                                ) + Decimal(str(raw_c_price))

                    c_child_id = digest(
                        [
                            revision_id,
                            "award_contractor",
                            ordinal,
                            c_idx,
                            c_id or contractor_code,
                        ]
                    )
                    contractor_attrs = {
                        "award_contractor_id": c_child_id,
                        "award_lot_id": award_lot_child_id,
                        "revision_id": revision_id,
                        "contractor_code": contractor_code,
                        "contractor_name": contractor_name,
                        "tax_code": tax_code,
                        "country": country,
                        "is_consortium": is_consortium,
                        "venture_code": venture_code,
                        "venture_name": venture_name,
                        "role": role,
                        "awarded_price": awarded_price_str,
                        "evaluation_rank": None,
                    }
                    children.append(
                        ExtractedChild(
                            child_id=c_child_id,
                            revision_id=revision_id,
                            kind="award_contractor",
                            role="contractorList[]",
                            path=f"lotResultDTO[{ordinal}].contractorList[{c_idx}]",
                            ordinal=c_idx,
                            source_id=c_id or contractor_code,
                            payload_json=canonical_json(c),
                            typed_table="award_contractor",
                            typed_attributes=contractor_attrs,
                        )
                    )

            # Calculate savings
            winning_price_str = (
                f"{lot_winning_price_dec:.6f}" if lot_winning_price_dec is not None else None
            )
            saving_amount_str = None
            saving_rate = None
            if lot_price_str is not None and lot_winning_price_dec is not None:
                lot_p_dec = Decimal(lot_price_str)
                saving_dec = lot_p_dec - lot_winning_price_dec
                saving_amount_str = f"{saving_dec:.6f}"
                if lot_p_dec > Decimal(0):
                    saving_rate = round(saving_dec / lot_p_dec, 4)

            lot_attrs = {
                "award_lot_id": award_lot_child_id,
                "revision_id": revision_id,
                "lot_no": lot_no,
                "lot_name": lot_name,
                "lot_price": lot_price_str,
                "winning_price": winning_price_str,
                "saving_amount": saving_amount_str,
                "saving_rate": saving_rate,
                "currency": currency,
                "winning_code": lot_winning_code,
                "status": lot_status,
            }
            children.append(
                ExtractedChild(
                    child_id=award_lot_child_id,
                    revision_id=revision_id,
                    kind="award_lot",
                    role="lotResultDTO[]",
                    path=f"lotResultDTO[{ordinal}]",
                    ordinal=ordinal,
                    source_id=lot_id or lot_no,
                    payload_json=canonical_json(lot),
                    typed_table="award_lot",
                    typed_attributes=lot_attrs,
                )
            )

        return children
